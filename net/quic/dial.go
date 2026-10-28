package quic

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"time"
)

// Dial opens a connection to addr and completes the handshake.
//
// pc is any [net.PacketConn]; a [net.UDPConn] is given the socket options
// RFC 9000 asks for, as the package documentation says under "The socket
// is the caller's".
//
// cfg's callbacks run on the connection's receive goroutine and a slow one
// stalls that connection's reading: "The TLS callbacks are the caller's",
// in the package documentation, says what that means and where the slow
// work goes instead.
//
// cfg must name at least one application protocol in NextProtos — ALPN is
// mandatory in QUIC (RFC 9001 §8.1) — and set MinVersion to
// [tls.VersionTLS13]: QUIC uses TLS 1.3 only (RFC 9001 §4.2), and a Config
// built by hand defaults to a lower minimum that crypto/tls would refuse
// at handshake. Both are checked before anything is sent, by Dial and
// [NewListener] alike. All three TLS 1.3 suites are spoken:
// the AES-GCM pair from the standard library, ChaCha20-Poly1305 from
// golang.org/x/crypto, the only module outside the standard library this
// package imports.
//
// A cfg with a [tls.ClientSessionCache] resumes a session the server has
// issued a ticket for (PSK, RFC 9001 §4.6): the handshake still takes one
// round trip and Dial does not even offer 0-RTT, which leaves the ticket's
// 0-RTT unspent at the server; [DialEarly] is the one that sends in it.
// A ticket is presented once: Dial wraps the cache, on a copy of cfg, so
// that a ticket taken out of it is removed — RFC 9001 §4.5 and RFC 9846
// Appendix C.4, since a ticket reused lets anyone on the path tie the
// connections together — and the ticket the server issues on the resumed
// connection refills it. A cache that ignores Put(key, nil) reuses its
// tickets anyway; that is the caller's choice to make, not the default.
//
// crypto/tls refuses RSA keys under 1024 bits and no more. RFC 9846
// Appendix C.2 asks applications to hold certification paths to at least
// 2048-bit RSA or 224-bit ECDSA: a cfg that wants that enforces it in
// VerifyConnection, on the public keys of [tls.ConnectionState.PeerCertificates]
// (an *rsa.PublicKey's N.BitLen()); this package adds no bound of its own.
//
// The handshake runs under ctx. It ends when ctx is cancelled — promptly,
// with an error that wraps both [ErrQUIC] and the context's cause — or at
// ctx's deadline; a ctx without a deadline gives up after
// [DefaultHandshakeTimeout]. Once Dial has returned, ctx no longer has any
// hold on the connection.
func Dial(ctx context.Context, pc net.PacketConn, addr net.Addr, cfg *tls.Config, params TransportParameters) (*Conn, error) {
	c, err := newClientConn(ctx, pc, addr, cfg, params)
	if err != nil {
		return nil, err
	}
	if err := c.handshake(ctx); err != nil {
		return nil, err // the handshake path closes on every failure
	}
	return c, nil
}

// newClientConn is what Dial and DialEarly share: the checks, the
// identifiers, the Initial keys, and a crypto/tls client with session
// events on, so that every ticket is stored with the server's parameters
// and only DialEarly offers 0-RTT (handleTLSEvent).
func newClientConn(ctx context.Context, pc net.PacketConn, addr net.Addr, cfg *tls.Config, params TransportParameters) (*Conn, error) {
	if err := params.check(); err != nil {
		return nil, err
	}
	if err := checkTLSConfig(cfg); err != nil {
		return nil, err
	}
	if err := checkOwnCertificates(cfg); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, abandonedErr(ctx)
	}
	dcid, err := randomID(8)
	if err != nil {
		return nil, err
	}
	scid, err := randomID(8)
	if err != nil {
		return nil, err
	}
	owner, err := newOwnerRegistrar()
	if err != nil {
		return nil, err
	}
	c := newConn(pc, addr, dcid, scid, true, params)
	c.registrar = owner
	c.initialDCID = dcid
	c.params.initialSourceCID = scid

	if err := c.installInitial(dcid); err != nil {
		_ = c.Close() // newConn started the timer loop; an early failure must stop it
		return nil, err
	}
	c.tls = tls.QUICClient(&tls.QUICConfig{TLSConfig: clientTLSConfig(cfg), EnableSessionEvents: true})
	return c, nil
}

// installEarlySealer installs a DialEarly client's 0-RTT write keys, takes
// the ticket's remembered parameters as the server's, and lets DialEarly
// return: streams may open and write from here, in 0-RTT packets.
func (c *Conn) installEarlySealer(suiteID uint16, secret []byte) error {
	s, err := suiteFor(suiteID)
	if err != nil {
		return err
	}
	sealer, err := newSealerSuite(secret, s)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.earlySealer = sealer
	c.applyRememberedLocked(c.remembered)
	c.mu.Unlock()
	close(c.earlyAccepted)
	return nil
}

// DefaultHandshakeTimeout is how long a handshake runs before it fails,
// when no context deadline says otherwise.
const DefaultHandshakeTimeout = 10 * time.Second

// Accept completes a server handshake for a connection whose first
// datagram, first, has already been read.
//
// It serves exactly one connection per [net.PacketConn] — for a shared
// socket serving many connections at once, see [Listener] instead, which
// also adds connection-identifier demultiplexing and optional Retry-based
// address validation on top.
//
// The peer's address is as unvalidated here as it is behind a Listener: the
// first datagram's source can be forged, so until a Handshake-level packet
// from it decrypts, sends are held to RFC 9000 §8.1's anti-amplification
// limit of three times the bytes received — the same accounting
// Listener-managed connections pay, with the same implicit release.
//
// first.ECN is the mark the first datagram arrived with. A client tests the
// path by marking its first packets ECT(0), and fails ECN validation when
// the acknowledgement covering them does not count the marks (RFC 9000
// §13.4.2.1). A caller that wants ECN therefore reads the datagram from a
// socket [udp.EnableECN] was applied to and passes what [udp.PacketECN]
// finds in its out-of-band bytes. Left at zero, the packet counts as
// Not-ECT and the client sends unmarked from then on.
//
// cfg.Certificates is refused here when a leaf carries a Key Usage without
// digitalSignature or a chain member is signed with SHA-1 (RFC 9846
// §4.5.1.2, §4.3.3), as [NewListener] refuses it; what GetCertificate or
// GetConfigForClient return is the caller's to hold to the same rules, as
// is matching the chain to the client's signature_algorithms_cert, which
// crypto/tls does not do.
//
// A server that keeps pc open after the connection, to accept the next one,
// should set [TransportParameters.HoldClosing]: "Servers that retain an open socket
// for accepting new connections SHOULD NOT end the closing or draining
// state early" (RFC 9000 §10.2). Without it, the connection stops reading
// pc the moment it closes, and a late packet of the peer reaches whatever
// reads pc next.
func accept(pc net.PacketConn, first datagram, cfg *tls.Config, params TransportParameters) (*Conn, error) {
	if err := params.check(); err != nil {
		return nil, err
	}
	if err := checkTLSConfig(cfg); err != nil {
		return nil, err
	}
	if err := checkOwnCertificates(cfg); err != nil {
		return nil, err
	}
	h, _, err := parseLongHeader(first.Payload)
	if err != nil {
		return nil, err
	}
	if h.Type != packetInitial {
		return nil, fmt.Errorf("%w: a connection opening with a %d packet", ErrQUIC, h.Type)
	}
	// RFC 9000 §14.1: a server discards an Initial carried in a datagram
	// under 1200 bytes. A real client pads its first flight; what arrives
	// shorter is a forgery, and answering it would amplify it toward
	// whoever its source address names.
	if len(first.Payload) < 1200 {
		return nil, fmt.Errorf("%w: a client Initial in a %d-byte datagram; §14.1 requires 1200", ErrQUIC, len(first.Payload))
	}
	// RFC 9000 §7.2: a client's first Initial carries a destination
	// identifier of at least eight bytes; a shorter one is refused before any
	// Initial secrets are derived from it.
	if len(h.DCID) < 8 {
		return nil, fmt.Errorf("%w: a client Initial with a %d-byte destination identifier; §7.2 requires 8", ErrQUIC, len(h.DCID))
	}
	c, err := newServerConn(pc, first.Peer, h, cfg, params, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	// Nothing proves the source address yet — the datagram handed in could
	// name any victim. The §8.1 limit holds until the handshake's first
	// decrypted Handshake-level packet validates the address (processPacket
	// clears it there), exactly as on a Listener-managed connection.
	c.mu.Lock()
	c.amplActive = true
	c.mu.Unlock()
	if err := c.receiveGathered(rawDatagram{data: first.Payload, from: first.Peer, ecn: first.ECN}); err != nil {
		c.endTLS()
		return nil, err // receive only fails through abort, which closed
	}
	if err := c.handshake(context.Background()); err != nil {
		return nil, err // the handshake path closes on every failure
	}
	return c, nil
}

// newServerConn bootstraps a server-side Conn from a client's first Initial
// packet: picks this end's connection identifier, derives Initial secrets,
// and starts the TLS handshake machinery. accept and [Listener] share this
// and diverge only after: Accept drives the handshake synchronously against
// its own exclusive socket, Listener feeds it datagrams from its shared
// demux loop instead.
//
// rt is non-nil only on a [Listener]'s post-Retry path, and must be applied
// here rather than by the caller afterwards: start() serialises the
// transport parameters the moment it runs, so an OriginalDCID corrected any
// later is an OriginalDCID the handshake never carried.
// prepare, when non-nil, runs on the connection after it exists and before
// its transport parameters are serialised — the one window in which a
// [Listener] can finish what only it knows: the stateless reset token for
// the connection's own identifier (§10.3, derived from the listener's key
// so a reset can be honoured statelessly later), the demux hooks, and the
// preferred address with its freshly minted identifier (§9.6). Standalone
// accept passes nil: no token (which §10.3 permits), no preferred
// address. keys, when non-nil, are the Initial keys a [Listener]'s decrypt
// check already derived from h.DCID; nil derives them here.
func newServerConn(pc net.PacketConn, peer net.Addr, h longHeader, cfg *tls.Config, params TransportParameters, rt *retryToken, keys *initialKeys, prepare func(*Conn) error) (*Conn, error) {
	scid, err := randomID(8)
	if err != nil {
		return nil, err
	}
	// The client's chosen destination identifier keys the Initial secrets and
	// is echoed back in the transport parameters, which is what binds this
	// handshake to the packets that carried it.
	c := newConn(pc, peer, append([]byte(nil), h.SCID...), scid, false, params)
	c.odcid = append([]byte(nil), h.DCID...)
	c.params.originalDCID = c.odcid
	c.params.initialSourceCID = scid
	if rt != nil {
		// After a Retry, this Initial's own destination identifier is the
		// one the Retry handed out — the true original lived only in the
		// token, and RFC 9000 §7.3 wants it echoed as OriginalDCID with the
		// Retry's identifier in RetrySourceCID beside it. Copied, not
		// borrowed: the token's bytes belong to a datagram buffer.
		c.odcid = append([]byte(nil), rt.odcid...)
		c.params.originalDCID = c.odcid
		c.params.retrySourceCID = append([]byte(nil), h.DCID...)
	}
	// Standalone, the connection owns its socket and issues identifiers
	// through an ownerRegistrar; a listener's prepare replaces it with the
	// demux table.
	owner, err := newOwnerRegistrar()
	if err != nil {
		_ = c.Close()
		return nil, err
	}
	c.registrar = owner
	if prepare != nil {
		if err := prepare(c); err != nil {
			_ = c.Close()
			return nil, err
		}
	}

	if keys != nil {
		var s secrets
		if s, err = keys.secrets(); err == nil {
			err = c.installInitialKeys(s, keys.opener)
		}
	} else {
		err = c.installInitial(h.DCID)
	}
	if err != nil {
		_ = c.Close() // newConn started the timer loop; an early failure must stop it
		return nil, err
	}
	// Session events on: every resumption reaches handleTLSEvent, which
	// refuses its 0-RTT unless this connection's listener admits it — a
	// ticket marked for early data by another instance sharing the
	// Config's ticket keys included, which crypto/tls alone would accept.
	c.tls = tls.QUICServer(&tls.QUICConfig{TLSConfig: cfg, EnableSessionEvents: true})
	if err := c.start(); err != nil {
		_ = c.Close()
		return nil, err
	}
	return c, nil
}

func newConn(pc net.PacketConn, peer net.Addr, dcid, scid []byte, isClient bool, params TransportParameters) *Conn {
	c := assembleConn(pc, peer, dcid, scid, isClient, params)
	go c.timerLoop()
	return c
}

// assembleConn is newConn without the timer loop. Tests that call
// actOnDeadlines themselves build on it: with the loop running, the scheduler
// decides whether a deadline fires between two of their steps.
func assembleConn(pc net.PacketConn, peer net.Addr, dcid, scid []byte, isClient bool, params TransportParameters) *Conn {
	// This end always keeps at least one spare identifier slot for the peer
	// to rotate into; below the protocol minimum of 2 the announcement would
	// be refused by any conformant peer (§18.2), so the zero value and any
	// degenerate configuration both land on the package's working default.
	if params.ActiveConnIDLimit < 2 {
		params.ActiveConnIDLimit = 4
	}
	// An algorithm this build does not know runs the default, and Stats
	// says so: the choice is local, so there is no peer to refuse it.
	if params.Congestion.Algorithm > CongestionBBR {
		params.Congestion.Algorithm = CongestionNewReno
	}
	if params.Congestion.SlowStart > SlowStartHyStart {
		params.Congestion.SlowStart = SlowStartDefault
	}
	c := &Conn{
		pc: pc, peer: peer, dcid: dcid, scid: scid, isClient: isClient,
		params:              params,
		streams:             make(map[uint64]*Stream),
		acceptReady:         make(chan struct{}, 1),
		closed:              make(chan struct{}),
		gone:                make(chan struct{}),
		earlyAccepted:       make(chan struct{}),
		hsComplete:          make(chan struct{}),
		timerKick:           make(chan struct{}, 1),
		maxDatagram:         1200, // until a probe proves the path carries more (pmtu.go)
		connSend:            sendQuota{},
		connRecv:            newRecvQuota(params.InitialMaxData),
		localMaxStreamsBidi: params.InitialMaxStreamsBidi,
		localMaxStreamsUni:  params.InitialMaxStreamsUni,
		idleTimeout:         params.MaxIdleTimeout,
	}
	c.closing.hold = params.HoldClosing
	// §14: the datagrams this end sends must not be fragmented, so the
	// socket is asked to say no (DF) before the first one leaves.
	c.fragmentable = !dfSocket(pc)
	// ECN marks and counts need a socket that reports the field; one that
	// cannot leaves the path on failed, which marks nothing (§13.4.2.2).
	c.ecnRecv = ecnSocket(pc)
	c.ecn.reset(c.ecnRecv)
	c.cond.L = &c.mu
	// RFC 9000 §17.4: an endpoint that does not implement the latency spin
	// bit SHOULD still set it to a random value per connection, so that
	// disabling is not itself a signal. Re-rolled when the DCID rotates —
	// same linkability epoch as the identifier.
	c.rollSpinLocked()
	// Sequence 0 is this connection's original identifier; alternatives a
	// registrar issues later are numbered from 1.
	c.localCIDs = []localCID{{seq: 0, cid: scid}}
	c.nextLocalSeq = 1
	if !isClient {
		// A server knows the peer's identifier (the client's SCID) at
		// construction; a client learns it from the server's first
		// authenticated packet and seeds the pool in pinPeerSCID.
		c.peerCIDs = []peerCID{{seq: 0, cid: dcid, used: true}}
		c.peerSCID = dcid
		c.peerCIDSet = true
	}
	c.cc = newController(c.params.Congestion, c.maxDatagram, &c.delivery)
	// §7.7: the pacer's bucket is the initial window, which is also the
	// freshly-built controller's cwnd.
	c.pacer.burst = int64(c.cc.window())
	if c.idleTimeout != 0 {
		c.idleDeadline = time.Now().Add(c.idleTimeout)
	}
	return c
}
