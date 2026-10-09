package quic

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"sync"
	"time"

	"github.com/mlagarrigue/sluice/net/quic/udp"
)

// listenerCIDLen is the length every connection identifier a [Listener]
// issues gets — the same 8 bytes [Dial] and standalone [Accept] have always
// used (randomID(8), every call site), so a short-header packet's DCID can
// be peeled off with [ParseShortHeader] without first knowing which
// connection it belongs to.
const listenerCIDLen = 8

// defaultMaxPendingConns is [ListenerConfig.MaxPendingConns]'s value when
// left zero: generous for a server under ordinary load, small enough that a
// flood of Initials that never complete a handshake cannot grow the demux
// table without bound.
const defaultMaxPendingConns = 1024

// ListenerConfig governs how a [Listener] treats a new connection attempt
// before it has any reason to trust it.
type ListenerConfig struct {
	// AlwaysRetry makes every new attempt pay one extra round trip for a
	// server-generated Retry (RFC 9000 §8.1.2) before its handshake
	// proceeds — the address is then known good before the connection is
	// even created. Off by default: the anti-amplification limit
	// (quic/send.go, quic/receive.go) is the default defense, validates
	// implicitly on the first Handshake-level packet, and costs nothing
	// extra for the common case. Real deployments typically turn this on
	// only under suspected load or attack, not unconditionally.
	//
	// Off is not defenseless: a new Initial must arrive in a 1200-byte
	// datagram (§14.1) and decrypt under the keys its own destination
	// identifier derives before any connection state exists for it, and
	// once the pending-handshake table passes half of MaxPendingConns the
	// listener switches to the Retry flow on its own — spoofed-source
	// floods then cost the attacker a provable round trip per slot instead
	// of filling the table for free.
	AlwaysRetry bool

	// MaxPendingConns bounds connections whose handshake has not yet
	// completed, and sizes the queue of completed ones waiting for
	// [Listener.Accept]. A completed connection that finds that queue full
	// keeps its pending slot until it gets in: the queue is the accept
	// backlog, and an application that stops accepting must push back on
	// new handshakes rather than let peers park unbounded connections on
	// it. Beyond the bound, a new Initial is refused (CONNECTION_REFUSED)
	// — the client retries, the same as it must under any transient
	// overload. Zero means [defaultMaxPendingConns].
	MaxPendingConns int

	// PreferredAddress is a second socket, bound to the address clients
	// should move to once their handshake confirms (RFC 9000 §9.6): a
	// server reached through a shared or load-balanced address that would
	// rather carry the connection directly, for instance. The listener
	// reads it alongside its main socket and announces its local address
	// — which must therefore be a concrete IPv4 or IPv6 address, not the
	// unspecified one, or [NewListener] fails — in every handshake, with a
	// connection identifier minted per connection. A client that validates
	// the path moves there on its own (counted in [Conn.Migrations] on both
	// sides), and its connection then sends from this socket (§8.2.2: the
	// answer to a probe must leave by the path it arrived on, which is what
	// this socket is for). A client that never validates it simply stays
	// where it is. nil announces nothing. The listener owns the socket from
	// here and closes it with [Listener.Close].
	PreferredAddress net.PacketConn
}

// Listener owns one [net.PacketConn] shared by many connections, the thing a
// bare [Accept] cannot do on its own (see its doc comment). It demultiplexes
// incoming datagrams by connection identifier, defends RFC 9000 §8's
// anti-amplification limit against an unvalidated address, and optionally
// issues a Retry (see [ListenerConfig.AlwaysRetry]) before trusting one at
// all.
//
// A Listener-managed connection also rotates identifiers and follows a
// migrating client: once its handshake confirms it issues additional
// connection identifiers (NEW_CONNECTION_ID, registered here in the demux
// table so any of them routes), and a client whose packets start arriving
// from a new address — a NAT rebind, typically — is followed there behind a
// PATH_CHALLENGE and the §8.1 amplification limit (RFC 9000 §9). The demux
// is by identifier alone, which is precisely what keeps a migrated client's
// packets reaching their connection whatever address they come from.
type Listener struct {
	pc net.PacketConn
	// sourced is pc itself when it is a UDP socket bound to the unspecified
	// address and the platform reports each datagram's destination
	// (udp.EnablePacketInfo); nil otherwise. Its read loop then hands every
	// datagram a via that answers from the address it arrived on.
	sourced *net.UDPConn
	// preferred is ListenerConfig.PreferredAddress, read by its own
	// loop and dispatched into the same demux table; preferredAddr is what
	// each handshake announces for it.
	preferred     net.PacketConn
	preferredAddr *net.UDPAddr
	cfg           *tls.Config
	params        TransportParameters
	lcfg          ListenerConfig
	key           retryTokenKey

	// mu is taken after a connection's own mu, never before: a connection
	// registers identifiers (addLocalCID) with its lock held, so nothing
	// holding mu may lock a Conn — Close and closeQueued end connections
	// only after releasing it.
	mu sync.Mutex
	// byDCID is this listener's demux table, live for a connection's entire
	// lifetime once registered — including after Accept, since traffic
	// keeps arriving addressed by connection ID for as long as the
	// connection itself does. Closed connections are removed from it, not
	// before.
	byDCID map[string]*Conn
	// keysOf indexes byDCID by connection — every key registered for it —
	// so forgetting a connection deletes its handful of entries instead of
	// scanning the whole table under mu, which every dispatch also takes.
	keysOf map[*Conn][]string
	// pendingConns is only the connections that have not yet reached the
	// accept queue — handshakes in progress, and completed ones waiting
	// for room in a full queue (see MaxPendingConns for why those still
	// count) — the subset Close ends. An entry here is always also in
	// byDCID; the reverse is not true once a connection is accepted.
	pendingConns map[*Conn]struct{}
	closed       bool
	discarded    uint64
	// resetKey derives stateless reset tokens (resetTokenFor); resetWindow
	// and resetsSent are the one-second budget maybeSendStatelessReset
	// spends, and resetsTo its per-source share of it. resetsTo only gains
	// an entry when a reset is sent, so the global budget bounds it too.
	resetKey    [32]byte
	resetWindow time.Time
	resetsSent  int
	resetsTo    map[string]int

	accepted chan *Conn
	stop     chan struct{}
	done     chan struct{} // closed when every read loop has returned
}

// NewListener starts demultiplexing pc. cfg and params are what every
// accepted connection's handshake uses — the same arguments [Accept] takes,
// since a Listener is [Accept] generalised to a shared socket rather than a
// different handshake. pc is any [net.PacketConn]; one from the net/quic/udp
// helper carries the Don't Fragment bit RFC 9000 §14 asks for.
func NewListener(pc net.PacketConn, cfg *tls.Config, params TransportParameters, lcfg ListenerConfig) (*Listener, error) {
	if err := params.check(); err != nil {
		return nil, err
	}
	if err := requireALPN(cfg); err != nil {
		return nil, err
	}
	if lcfg.MaxPendingConns <= 0 {
		lcfg.MaxPendingConns = defaultMaxPendingConns
	}
	key, err := newRetryTokenKey()
	if err != nil {
		return nil, err
	}
	var resetKey [32]byte
	if _, err := rand.Read(resetKey[:]); err != nil {
		return nil, err
	}
	var preferredAddr *net.UDPAddr
	if lcfg.PreferredAddress != nil {
		ua, ok := lcfg.PreferredAddress.LocalAddr().(*net.UDPAddr)
		if !ok || ua == nil || ua.IP == nil || ua.IP.IsUnspecified() {
			return nil, fmt.Errorf("%w: PreferredAddress must be bound to a concrete IPv4 or IPv6 address, not %v — a client has to know where to send",
				ErrQUIC, lcfg.PreferredAddress.LocalAddr())
		}
		preferredAddr = ua
	}
	l := &Listener{
		pc: pc, sourced: packetInfoSocket(pc), preferred: lcfg.PreferredAddress, preferredAddr: preferredAddr,
		cfg: cfg, params: params, lcfg: lcfg, key: key, resetKey: resetKey,
		byDCID:       make(map[string]*Conn),
		keysOf:       make(map[*Conn][]string),
		pendingConns: make(map[*Conn]struct{}),
		accepted:     make(chan *Conn, lcfg.MaxPendingConns),
		stop:         make(chan struct{}),
		done:         make(chan struct{}),
	}
	var loops sync.WaitGroup
	loops.Go(func() { l.readLoop(l.pc) })
	if l.preferred != nil {
		loops.Go(func() { l.readLoop(l.preferred) })
	}
	go func() {
		loops.Wait()
		close(l.done)
	}()
	return l, nil
}

// packetInfoSocket returns pc as a UDP socket with packet information
// enabled, or nil where that is unavailable or pointless. A socket bound to
// one concrete address already answers from it; one bound to 0.0.0.0 or
// [::] on a host with several addresses answers from whichever the route
// picks, and a client that wrote to another sees a reply from a stranger —
// a path change to QUIC (RFC 9000 §9), or no answer at all through a NAT
// or firewall that tracks the pair. Best-effort: without it the listener
// behaves as before.
func packetInfoSocket(pc net.PacketConn) *net.UDPConn {
	uc, ok := pc.(*net.UDPConn)
	if !ok {
		return nil
	}
	ua, ok := uc.LocalAddr().(*net.UDPAddr)
	if !ok || ua == nil || (ua.IP != nil && !ua.IP.IsUnspecified()) {
		return nil
	}
	if udp.EnablePacketInfo(uc) != nil {
		return nil
	}
	return uc
}

// Accept blocks until a new connection's handshake completes, mirroring
// [net.Listener.Accept]'s shape and [Accept]'s own existing return
// contract: what comes back is already past its handshake, not merely
// dialled.
func (l *Listener) Accept() (*Conn, error) {
	select {
	case <-l.stop:
		// Checked first: once Close has run, a connection still queued is
		// Close's to end, not Accept's to hand out.
		return nil, net.ErrClosed
	default:
	}
	select {
	case c := <-l.accepted:
		return c, nil
	case <-l.stop:
		return nil, net.ErrClosed
	}
}

// Close stops the dispatch loop and ends every connection whose handshake
// has not completed, and every completed one still queued for Accept —
// nobody can Accept it any more, and left alone it would hold its
// goroutines until its idle timeout. A connection already handed out by
// Accept is the caller's to manage from here — untouched, exactly as
// [net.Listener.Close] leaves an already-accepted [net.Conn] alone.
//
// The connections end before the sockets close, so their CONNECTION_CLOSE
// still leaves and their peers stop now rather than at their own idle
// timeout.
func (l *Listener) Close() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	l.closed = true
	toClose := make([]*Conn, 0, len(l.pendingConns))
	for c := range l.pendingConns {
		toClose = append(toClose, c)
	}
	l.mu.Unlock()

	close(l.stop)
	for _, c := range toClose {
		_ = c.Close()
	}
	l.closeQueued()
	err := l.pc.Close()
	if l.preferred != nil {
		if perr := l.preferred.Close(); err == nil {
			err = perr
		}
	}
	<-l.done
	return err
}

// closeQueued ends every connection waiting in the accept queue. Close
// calls it, and so does a handshake that queued its connection after
// seeing the listener closed: between them, nothing queued is missed.
func (l *Listener) closeQueued() {
	for {
		select {
		case c := <-l.accepted:
			_ = c.Close()
		default:
			return
		}
	}
}

// DiscardedPackets reports how many datagrams this listener's own front
// door refused to route — unroutable or malformed, or addressed to a
// connection too far behind to take them — and failed socket reads, which
// on Windows include the unauthenticated ICMP port-unreachable; never a
// reason to stop serving everyone else. The same accounting
// [Conn.Stats] keeps for what gets past it.
func (l *Listener) DiscardedPackets() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.discarded
}

func (l *Listener) noteDiscard() {
	l.mu.Lock()
	l.discarded++
	l.mu.Unlock()
}

// readLoop is the one goroutine that ever reads pc — the thing that makes
// sharing one socket across many connections possible at all, since every
// connection reading it independently would race the others for its bytes.
// One runs per socket: the main one, and the preferred-address one when
// configured; both feed the same demux table.
func (l *Listener) readLoop(pc net.PacketConn) {
	buf := make([]byte, max(int(l.params.MaxUDPPayloadSize), minRecvBuf)) //nolint:gosec // G115: the endpoint's own configuration, not peer input
	var sv *sourcedVias
	if l.sourced != nil && pc == net.PacketConn(l.sourced) {
		sv = &sourcedVias{uc: l.sourced, oob: make([]byte, udp.PacketInfoLen), byLocal: make(map[localKey]*sourcedConn)}
	}
	readFails := 0 // consecutive read errors, for the backoff
	for {
		select {
		case <-l.stop:
			return
		default:
		}
		var (
			n    int
			from net.Addr
			err  error
			via  = pc
		)
		if sv != nil {
			n, from, via, err = sv.read(buf)
		} else {
			n, from, err = pc.ReadFrom(buf)
		}
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			// A transient read error is the socket's problem, not any one
			// connection's; try again rather than taking everyone down
			// over it. Counted, and backed off when it repeats: a socket
			// that fails every read would otherwise spin this loop at
			// full CPU for as long as it stays broken.
			l.noteDiscard()
			readFails++
			if readFails > 1 && !readBackoff(readFails, l.stop) {
				return
			}
			continue
		}
		readFails = 0
		l.dispatch(append([]byte(nil), buf[:n]...), from, via)
	}
}

// maxSourcedVias bounds the per-address views one listener keeps. A kernel
// only delivers datagrams addressed to the host's own addresses, so the
// real count is that of the host's interfaces; the bound is for a host
// with more than any server plausibly answers on, which falls back to
// letting the kernel pick.
const maxSourcedVias = 64

// localKey is one local address a datagram reached. The interface is part
// of it only for an IPv6 link-local address, the one case where the
// address alone does not say where to send from.
type localKey struct {
	addr    netip.Addr
	ifindex int
}

// sourcedVias reads a packet-information socket and maps each datagram's
// destination to a stable [sourcedConn]: stable because a connection
// compares the socket a datagram arrived on against its own to detect a
// path change (maybeMigrate), so the same local address must always yield
// the same value. Owned by the one read loop; no locking.
type sourcedVias struct {
	uc      *net.UDPConn
	oob     []byte
	byLocal map[localKey]*sourcedConn
}

func (s *sourcedVias) read(buf []byte) (int, net.Addr, net.PacketConn, error) {
	n, oobn, _, from, err := s.uc.ReadMsgUDP(buf, s.oob)
	if err != nil {
		return 0, nil, nil, err
	}
	dst, ifindex, ok := udp.PacketDestination(s.oob[:oobn])
	if !ok || dst.IsMulticast() || dst.IsUnspecified() {
		return n, from, s.uc, nil
	}
	if !dst.Is6() || dst.Is4In6() || !dst.IsLinkLocalUnicast() {
		ifindex = 0 // let routing pick the interface, as it would anyway
	}
	k := localKey{dst, ifindex}
	v := s.byLocal[k]
	if v == nil {
		if len(s.byLocal) >= maxSourcedVias {
			return n, from, s.uc, nil
		}
		v = &sourcedConn{
			UDPConn: s.uc,
			oob:     udp.AppendPacketSource(nil, dst, ifindex),
			local:   net.UDPAddrFromAddrPort(netip.AddrPortFrom(dst.Unmap(), uint16(s.uc.LocalAddr().(*net.UDPAddr).Port))), //nolint:gosec // G115: a UDP port fits 16 bits
		}
		s.byLocal[k] = v
	}
	return n, from, v, nil
}

// sourcedConn is the listener's unspecified-address socket seen from one
// of the host's addresses: every datagram written through it carries the
// control message that makes it leave from that address. The listener
// still owns and closes the socket.
type sourcedConn struct {
	*net.UDPConn
	oob   []byte // built once; read-only after
	local *net.UDPAddr
}

func (s *sourcedConn) WriteTo(b []byte, addr net.Addr) (int, error) {
	ua, ok := addr.(*net.UDPAddr)
	if !ok {
		return s.UDPConn.WriteTo(b, addr)
	}
	// The AddrPort form is the allocation-free one in package net.
	n, _, err := s.WriteMsgUDPAddrPort(b, s.oob, ua.AddrPort())
	return n, err
}

// LocalAddr reports the address replies leave from, not the wildcard the
// socket is bound to.
func (s *sourcedConn) LocalAddr() net.Addr { return s.local }

// dispatch routes one already-copied datagram (the shared read buffer is
// reused on the next ReadFrom, so the copy happens once, here, rather than
// once per destination) to the connection it names, or treats it as a new
// connection attempt if nothing claims its identifier yet. via is the
// socket it arrived on, which is where any answer leaves from.
func (l *Listener) dispatch(datagram []byte, from net.Addr, via net.PacketConn) {
	dcid := peekDCID(datagram)
	if dcid == nil {
		// Not a packet this listener can route — possibly a long header in a
		// version this package does not speak, which §5.2.2 answers with a
		// Version Negotiation packet rather than silence.
		l.maybeNegotiateVersion(datagram, from, via)
		return
	}

	l.mu.Lock()
	c := l.byDCID[string(dcid)]
	l.mu.Unlock()
	if c != nil {
		// Never block here: this loop serves every connection on the socket,
		// and a connection whose consumer lags (a full accept queue, a
		// stream callback applying back-pressure) legitimately stops
		// draining its channel. Waiting on it would freeze every other
		// connection and every handshake behind one slow peer. Dropping is
		// what the network would do; loss recovery retransmits, and
		// back-pressure stays per connection, through flow control.
		select {
		case c.incoming <- rawDatagram{data: datagram, from: from, via: via}:
		default:
			l.noteDiscard()
		}
		return
	}

	// An unknown identifier may only start a new connection, and only as an
	// Initial — anything else is unauthenticated noise this end never
	// issued a routing entry for (RFC 9000 §5.2, §12.2: discarded, not
	// fatal to anything). The one thing worth saying back: a short-header
	// packet naming an identifier this listener no longer routes is most
	// likely a client still talking to a connection that no longer exists,
	// and §10.3's stateless reset is how it finds out now instead of at its
	// idle timeout. No record of retired identifiers is kept, so the reset
	// goes to any unknown one; the token is derived from the identifier, so
	// only a peer that was handed it for a real connection recognises it.
	if datagram[0]&0x80 == 0 {
		l.maybeSendStatelessReset(datagram, dcid, from, via)
		return
	}
	h, _, err := parseLongHeader(datagram)
	if err != nil || h.Type != packetInitial {
		l.noteDiscard()
		return
	}
	l.handleNewInitial(datagram, from, h, via)
}

// peekDCID reads just enough of a datagram to learn which connection it
// claims to belong to, without the keys needed to authenticate any of it.
func peekDCID(datagram []byte) []byte {
	if len(datagram) == 0 {
		return nil
	}
	if datagram[0]&0x80 != 0 {
		h, _, err := parseLongHeader(datagram)
		if err != nil {
			return nil
		}
		return h.DCID
	}
	h, err := parseShortHeader(datagram, listenerCIDLen)
	if err != nil {
		return nil
	}
	return h.DCID
}

// maybeNegotiateVersion answers a long-header packet in a version this
// package does not speak with a Version Negotiation packet (RFC 9000 §5.2.2,
// a SHOULD): a client offering a future version learns in one round trip
// that only version 1 is served, instead of timing out against silence.
//
// §14.1 gates the answer on the triggering datagram reaching the 1200-byte
// protocol minimum — below it the sender has not paid enough to make this
// listener answer anyone, which is the same anti-amplification stance
// handleNewInitial takes. Anything that is not a plausible foreign-version
// long header is counted and dropped as before.
func (l *Listener) maybeNegotiateVersion(datagram []byte, from net.Addr, via net.PacketConn) {
	dcid, scid, ok := foreignVersionCIDs(datagram)
	if !ok || len(datagram) < 1200 {
		l.noteDiscard()
		return
	}
	pkt := appendVersionNegotiation(nil, scid, dcid, version1)
	_, _ = via.WriteTo(pkt, from) // best effort; the client retries anyway
}

// foreignVersionCIDs peels the version-independent fields (RFC 8999) off a
// long-header packet in a version this package does not speak, so a Version
// Negotiation answer can echo its identifiers. [connectionID] is not used
// here: its 20-byte cap is QUIC version 1's, and a foreign version may name
// identifiers up to the field's 255-byte range.
func foreignVersionCIDs(datagram []byte) (dcid, scid []byte, ok bool) {
	if len(datagram) < 7 || datagram[0]&0x80 == 0 {
		return nil, nil, false
	}
	version := uint32(datagram[1])<<24 | uint32(datagram[2])<<16 | uint32(datagram[3])<<8 | uint32(datagram[4])
	if version == 0 || version == version1 {
		// Version 0 is itself a Version Negotiation packet — answering one
		// with another would be a loop — and version 1 failing to parse is
		// malformed, not foreign.
		return nil, nil, false
	}
	b := datagram[5:]
	n := int(b[0])
	if len(b) < 1+n+1 {
		return nil, nil, false
	}
	dcid, b = b[1:1+n], b[1+n:]
	n = int(b[0])
	if len(b) < 1+n {
		return nil, nil, false
	}
	return dcid, b[1 : 1+n], true
}

// handleNewInitial is reached only for an Initial whose destination
// identifier this listener has never routed before — a genuinely new
// attempt, or (if [ListenerConfig.AlwaysRetry] is on) one still proving its
// address before a Conn is even created.
func (l *Listener) handleNewInitial(datagram []byte, from net.Addr, h longHeader, via net.PacketConn) {
	// RFC 9000 §14.1: a server MUST discard an Initial carried in a
	// datagram under 1200 bytes. A real client always pads; what arrives
	// smaller is a forgery economising on bandwidth — and answering it
	// (with a Retry, or with a whole handshake flight) would make this
	// listener an amplifier aimed at whoever the source address names.
	if len(datagram) < 1200 {
		l.noteDiscard()
		return
	}
	// RFC 9000 §7.2: a client's first Initial names a destination identifier
	// of at least eight bytes. Refused before Initial secrets are derived
	// from it — a key schedule keyed on a degenerate identifier is not worth
	// computing, let alone answering.
	if len(h.DCID) < 8 {
		l.noteDiscard()
		return
	}
	// The Initial must decrypt before it earns anything — a connection, a
	// table entry, even a Retry. Initial keys derive from the packet's own
	// destination identifier, so the check needs no state and costs one
	// key schedule; what it buys is that a flood of sealed-looking noise
	// cannot occupy MaxPendingConns slots for the price of sending it
	// (every slot used to hold TLS state and two goroutines for the full
	// 10-second handshake deadline, decryptable or not).
	keys, ok := initialPacketDecrypts(h)
	if !ok {
		l.noteDiscard()
		return
	}

	l.mu.Lock()
	pending := len(l.pendingConns)
	l.mu.Unlock()
	if pending >= l.lcfg.MaxPendingConns {
		// RFC 9000 §5.2.2: a server with no room SHOULD say so — a
		// stateless CONNECTION_REFUSED close costs one sealed packet
		// against a decrypted ≥1200-byte Initial and spares the client its
		// whole handshake timeout. (This used to be a silent drop, argued
		// as "no different from a listen backlog dropping a SYN" — but TCP
		// has RST for the loud variant, and QUIC's equivalent is this.)
		l.sendStatelessClose(from, h, keys.secrets, transportConnectionRefused, via)
		return
	}

	// The Retry flow handles three cases: the operator asked for it on
	// every attempt; the pending table passed its high-water mark, where
	// handing out stateless Retries is how the remaining slots are kept
	// for addresses that prove themselves; and an Initial already carrying
	// a token, which only the Retry flow can verify — a token must never
	// reach the direct path, where the handshake it starts would not echo
	// retry_source_connection_id and the client would refuse it.
	if len(h.Token) > 0 || l.lcfg.AlwaysRetry || pending >= l.lcfg.MaxPendingConns/2 {
		l.handleRetryFlow(datagram, from, h, &keys, via)
		return
	}
	l.bootstrapConn(datagram, from, h, nil, &keys, via)
}

// initialKeys is what checking a client's first Initial derived: the
// secrets, and the opener that just authenticated it. Handed on so the
// answer — a stateless close or the connection's own Initial keys — does
// not run the key schedule a second time for the same identifier.
type initialKeys struct {
	secrets secrets
	opener  *packetOpener // the client's direction, which a server opens
}

// initialPacketDecrypts checks an Initial against the keys its destination
// identifier derives (RFC 9001 §5.2) — the cheapest possible proof that the
// sender is a QUIC client and not noise shaped like one.
func initialPacketDecrypts(h longHeader) (initialKeys, bool) {
	s, err := initialSecrets(h.DCID)
	if err != nil {
		return initialKeys{}, false
	}
	opener, err := newPacketOpener(s.Client)
	if err != nil {
		return initialKeys{}, false
	}
	if _, _, err = opener.Open(h.Raw, h.PNOffset, 0); err != nil {
		return initialKeys{}, false
	}
	return initialKeys{secrets: s, opener: opener}, true
}

// handleRetryFlow implements RFC 9000 §8.1.2 statelessly: a first Initial
// (no token) gets a Retry and nothing else — no Conn, no table entry, no
// resource an attacker spends by sending one of these and walking away. A
// second Initial carrying a valid token proves the address and continues
// exactly like handleNewInitial would have without AlwaysRetry, except
// already validated.
func (l *Listener) handleRetryFlow(datagram []byte, from net.Addr, h longHeader, keys *initialKeys, via net.PacketConn) {
	if len(h.Token) == 0 {
		newSCID, err := randomID(listenerCIDLen)
		if err != nil {
			return // nothing to send; the client's own retransmission tries again
		}
		token := appendRetryToken(nil, l.key, from, h.DCID, newSCID)
		pkt := appendRetryPacket(nil, h.SCID, newSCID, h.DCID, token)
		_, _ = via.WriteTo(pkt, from) // best effort; a lost Retry just looks like a lost Initial to the client
		return
	}

	rt, err := verifyRetryToken(h.Token, l.key, from)
	if err != nil {
		// RFC 9000 §8.1.2: an Initial whose token cannot be validated SHOULD
		// be answered with an immediate INVALID_TOKEN close, so the client
		// learns now instead of eating its whole handshake timeout. The
		// answer is stateless — no Conn, no table entry — and small against
		// the ≥1200-byte datagram that provably arrived (§8.1's limit allows
		// three times that).
		l.sendStatelessClose(from, h, keys.secrets, transportInvalidToken, via)
		return
	}
	// This Initial's own destination identifier must be the one the Retry
	// offered — anything else is a token replayed onto an attempt it was
	// never issued for: equally impossible to validate, equally worth
	// telling the client about now.
	if !bytes.Equal(h.DCID, rt.newSCID) {
		l.sendStatelessClose(from, h, keys.secrets, transportInvalidToken, via)
		return
	}
	l.bootstrapConn(datagram, from, h, &rt, keys, via)
}

// sendStatelessClose answers one Initial with a stateless Initial-level
// CONNECTION_CLOSE carrying the given transport code — INVALID_TOKEN for a
// token that cannot be validated (§8.1.2), CONNECTION_REFUSED for a full
// pending table (§5.2.2). The keys are the ones the client derived from its
// own destination identifier (RFC 9001 §5.2), so only the endpoint that
// sent — or saw — that Initial can read the answer; the identifiers are
// echoed swapped, exactly as a server's first genuine packet would. s is
// what the decrypt check already derived from h.DCID.
func (l *Listener) sendStatelessClose(from net.Addr, h longHeader, s secrets, code uint64, via net.PacketConn) {
	l.noteDiscard() // the Initial itself is still refused, and still counted
	sealer, err := newPacketSealer(s.Server)
	if err != nil {
		return
	}

	frame := AppendVarint(nil, frameConnectionClose)
	frame = AppendVarint(frame, code)
	frame = AppendVarint(frame, 0) // the frame type being answered; none
	frame = AppendVarint(frame, 0) // no reason phrase, deliberately

	const pnLen = 4
	header := []byte{0xc0 | packetInitial<<4 | pnLen - 1}
	header = append(header, 0, 0, 0, 1, byte(len(h.SCID))) //nolint:gosec // G115: a connection ID is at most 20 bytes; version 1, then the identifiers
	header = append(header, h.SCID...)
	header = append(header, byte(len(h.DCID))) //nolint:gosec // G115: a connection ID is at most 20 bytes
	header = append(header, h.DCID...)
	header = AppendVarint(header, 0) // a server's Initial carries no token (§17.2.2)
	header = AppendVarint(header, uint64(pnLen+len(frame)+16))
	pnOffset := len(header)
	header = append(header, 0, 0, 0, 0) // packet number zero

	pkt, err := sealer.Seal(nil, header, frame, 0, pnOffset, pnLen)
	if err != nil {
		return
	}
	_, _ = via.WriteTo(pkt, from) // best effort; a loss leaves the old timeout path
}

// bootstrapConn is [newServerConn] plus everything a Listener-managed
// connection additionally needs: its demux entries, its listener-fed
// incoming channel instead of exclusive socket ownership, and — for a
// direct (non-Retry) attempt — the same anti-amplification accounting the
// standalone [Accept] applies to its own unvalidated peer.
func (l *Listener) bootstrapConn(datagram []byte, from net.Addr, h longHeader, rt *retryToken, keys *initialKeys, via net.PacketConn) {
	c, err := newServerConn(via, from, h, l.cfg, l.params, rt, keys, l.prepareConn)
	if err != nil {
		return
	}
	if rt == nil {
		// An address that has proved nothing yet pays the RFC 9000 §8.1
		// amplification limit until its first Handshake-level packet
		// decrypts. The Retry path proved itself across that round trip,
		// so no accounting is needed there.
		c.amplActive = true
	}

	firstKey := string(h.DCID)
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		l.forget(c) // the preferred-address identifier may already route here
		_ = c.Close()
		return
	}
	l.routeLocked(string(c.scid), c)
	l.routeLocked(firstKey, c)
	l.pendingConns[c] = struct{}{}
	l.mu.Unlock()

	go l.runHandshake(c, datagram, from)
}

// prepareConn is what newServerConn runs on a listener-managed connection
// before its transport parameters are serialised: the stateless reset token
// for its own identifier, the registrar and feed that make it the
// listener's, and — when the listener has a preferred address — the §9.6
// announcement, with an identifier minted into the demux table now so the
// client's first packet at that address already routes. That identifier is
// sequence 1 of the connection's pool (§5.1.1), and later issuance numbers
// from 2.
func (l *Listener) prepareConn(c *Conn) error {
	t := l.resetTokenFor(c.scid)
	c.params.statelessResetToken = t[:]
	c.registrar = l
	c.incoming = make(chan rawDatagram, 8)
	if l.preferred == nil {
		return nil
	}
	cid, err := l.addLocalCID(c)
	if err != nil {
		return err
	}
	pa := &preferredAddress{cid: cid, statelessResetToken: l.resetTokenFor(cid)}
	if l.preferredAddr.IP.To4() != nil {
		pa.ipv4 = l.preferredAddr
	} else {
		pa.ipv6 = l.preferredAddr
	}
	c.params.preferredAddress = pa
	c.localCIDs = append(c.localCIDs, localCID{seq: 1, cid: cid})
	c.nextLocalSeq = 2
	return nil
}

// runHandshake drives one connection's handshake to completion on its own
// goroutine, never on the shared dispatch loop: a slow or deliberately
// stalled handshake must not head-of-line block every other connection's
// datagrams behind it.
func (l *Listener) runHandshake(c *Conn, first []byte, from net.Addr) {
	accepted := false
	defer func() {
		l.mu.Lock()
		delete(l.pendingConns, c)
		l.mu.Unlock()
		if !accepted {
			l.forget(c)
			_ = c.Close()
		}
	}()

	if err := c.receiveGathered(rawDatagram{data: first, from: from, via: c.pc}); err != nil {
		return // an authenticated violation on its own first packet; abort already closed it
	}
	if err := c.handshake(context.Background()); err != nil {
		return // the handshake path closes on every failure, same as Accept's
	}

	go func() {
		<-c.closed
		l.forget(c)
	}()

	select {
	case l.accepted <- c:
		accepted = true
		// A Close that ran between the handshake finishing and the send
		// above has already drained the queue; this connection would
		// otherwise sit in it until its idle timeout.
		l.mu.Lock()
		closed := l.closed
		l.mu.Unlock()
		if closed {
			l.closeQueued()
		}
	case <-l.stop:
		// The queue is full and nobody will Accept again.
		_ = c.Close()
	case <-c.closed:
		// It ended while waiting for room — the peer closed, or its idle
		// timer fired. Queuing it now would hand Accept a dead connection
		// and keep a pending slot for nothing; the deferred cleanup frees
		// the slot.
	}
}

// routeLocked enters one demux key for c, and records it against c so
// forget can find it. Callers hold mu.
func (l *Listener) routeLocked(key string, c *Conn) {
	l.byDCID[key] = c
	l.keysOf[c] = append(l.keysOf[c], key)
}

// forget removes every demux entry a connection holds once it has no
// further use for them: closed, or never going to be — its scid, the
// client's first identifier, and whatever NEW_CONNECTION_ID issued since,
// all found through keysOf. A key is deleted only while it still routes to
// c: a client's first identifier is its own choice and may since name
// another connection. Idempotent, since a connection that fails its
// handshake and a Listener.Close happening at the same moment can both try.
func (l *Listener) forget(c *Conn) {
	l.mu.Lock()
	for _, k := range l.keysOf[c] {
		if l.byDCID[k] == c {
			delete(l.byDCID, k)
		}
	}
	delete(l.keysOf, c)
	l.mu.Unlock()
}

// addLocalCID and removeLocalCID are the [cidRegistrar] a Listener gives
// each connection it manages: fresh identifiers are minted here because only
// the demux table can prove one unused, and registered here because routing
// is what an identifier *is*. Called with the connection's mu held — the
// established order is Conn.mu then Listener.mu, and nothing under l.mu ever
// takes a connection's lock.
func (l *Listener) addLocalCID(c *Conn) ([]byte, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil, net.ErrClosed
	}
	for range 8 {
		cid, err := randomID(listenerCIDLen)
		if err != nil {
			return nil, err
		}
		if _, taken := l.byDCID[string(cid)]; taken {
			continue // a 64-bit collision; mint another
		}
		l.routeLocked(string(cid), c)
		return cid, nil
	}
	return nil, fmt.Errorf("%w: eight fresh identifiers in a row collided", ErrQUIC)
}

func (l *Listener) removeLocalCID(cid []byte) {
	l.mu.Lock()
	defer l.mu.Unlock()
	c := l.byDCID[string(cid)]
	if c == nil {
		return
	}
	delete(l.byDCID, string(cid))
	keys := l.keysOf[c]
	for i, k := range keys {
		if k == string(cid) {
			keys[i] = keys[len(keys)-1]
			l.keysOf[c] = keys[:len(keys)-1]
			break
		}
	}
}

// resetTokenFor derives the RFC 9000 §10.3 stateless reset token for an
// identifier this listener issued: HMAC-SHA256 of the identifier under a
// per-listener key, truncated to the token's 16 bytes. Derivation is what
// makes the reset *stateless* — the token can be recomputed, and honoured,
// after every trace of the connection is gone.
func (l *Listener) resetTokenFor(cid []byte) [16]byte {
	mac := hmac.New(sha256.New, l.resetKey[:])
	mac.Write(cid)
	var token [16]byte
	copy(token[:], mac.Sum(nil))
	return token
}

// maxResetsPerSecond bounds how many stateless resets one listener emits:
// each is an answer to unauthenticated bytes, and answering every stray at
// line rate would make the listener a traffic generator pointed wherever
// the source addresses claim (§10.3's rate-limit advice).
const maxResetsPerSecond = 16

// maxResetsPerSourcePerSecond is one source address's share of that
// budget. Without it, one sender spraying short headers at random
// identifiers spends the whole budget, and every genuine client whose
// connection this listener lost waits out its idle timeout instead. The
// address is the IP alone: the port is free for a sender to vary.
const maxResetsPerSourcePerSecond = 4

// maybeSendStatelessReset answers one unroutable short-header datagram with
// a stateless reset (§10.3): random-looking bytes ending in the token the
// datagram's destination identifier derives. A real client that kept state
// for a connection this listener has forgotten recognises the token and
// stops immediately, instead of retransmitting into silence until its idle
// timeout. Anyone else sees indistinguishable noise.
func (l *Listener) maybeSendStatelessReset(datagram, dcid []byte, from net.Addr, via net.PacketConn) {
	// §10.3: the reset must be smaller than what triggered it (loop
	// prevention: two confused endpoints answering each other's resets
	// shrink them until one goes under the 21-byte floor and stops), and
	// at least 21 bytes to be one at all.
	if len(datagram) < 22 {
		l.noteDiscard()
		return
	}
	l.mu.Lock()
	now := time.Now()
	if now.Sub(l.resetWindow) >= time.Second {
		l.resetWindow, l.resetsSent = now, 0
		clear(l.resetsTo)
	}
	src := resetSource(from)
	if l.resetsSent >= maxResetsPerSecond || l.resetsTo[src] >= maxResetsPerSourcePerSecond {
		l.mu.Unlock()
		l.noteDiscard()
		return
	}
	l.resetsSent++
	if l.resetsTo == nil {
		l.resetsTo = make(map[string]int, maxResetsPerSecond)
	}
	l.resetsTo[src]++
	l.mu.Unlock()
	l.noteDiscard() // the triggering datagram is still refused, and counted

	size := min(len(datagram)-1, 42) // past parsing doubt, under every trigger
	pkt := make([]byte, size)
	if _, err := rand.Read(pkt); err != nil {
		return
	}
	pkt[0] = 0x40 | (pkt[0] & 0x3f) // the short-header shape §10.3 prescribes
	token := l.resetTokenFor(dcid)
	copy(pkt[len(pkt)-16:], token[:])
	_, _ = via.WriteTo(pkt, from)
}

// resetSource is the per-source key of the stateless reset budget: the IP
// address without its port, or the whole address where it is not IP.
func resetSource(from net.Addr) string {
	if ua, ok := from.(*net.UDPAddr); ok {
		if ip, ok := netip.AddrFromSlice(ua.IP); ok {
			return string(ip.Unmap().AsSlice())
		}
	}
	return from.String()
}
