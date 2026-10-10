package quic

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// A connection: the TLS handshake driven by [crypto/tls], the packets sealed
// by this package, streams assembled out of what arrives, and the transport
// machinery of RFC 9002 — acknowledgements, loss detection, retransmission
// and NewReno congestion control — underneath.
//
// # What this is and is not
//
// Packets that are lost are detected and retransmitted, flow control is
// enforced in both directions, and a hostile datagram is discarded rather
// than believed. The *listener* side of a server — connection-identifier
// demultiplexing, address validation, amplification limits, Retry
// generation — lives in [Listener], its own design unit on top of this one.
// Interoperation is proven against quic-go, both ways, in continuous
// integration (interop/); against any other implementation it is not.
//
// # Failure model
//
// Unauthenticated input never ends a connection: a datagram that does not
// parse, does not authenticate, or arrives from the wrong address is counted
// and dropped, because anyone can send those bytes (RFC 9000 §5.2, §12.2).
// Protocol violations inside an authenticated packet — flow-control breach,
// a final size that moves, a malformed frame — end the connection with a
// CONNECTION_CLOSE, because the peer provably sent them. One stream's
// application-level failure ends that stream, via [Stream.Reset] and
// [Stream.StopSending], and touches nothing else.

// Conn is one QUIC connection over a packet connection.
//
// It is safe for one reader and any number of writers, provided each
// stream has at most one goroutine writing it: every write serialises on
// the connection's lock, so writes on different streams may run from
// different goroutines — the shape a server with per-stream response
// writers uses — while two goroutines writing one stream would interleave
// their bytes in an order neither chose.
type Conn struct {
	// pc is the socket this connection sends on and — unless a [Listener]
	// feeds it (incoming) — reads from. It changes exactly one way: a
	// client's [Conn.Rebind] replaces it, under mu, and the read loop
	// re-reads it per iteration.
	pc  net.PacketConn
	tls *tls.QUICConn

	scid     []byte
	isClient bool
	params   TransportParameters // this endpoint's own announcements

	// peer is where this connection sends. It changes exactly one way: a
	// confirmed server follows its client to a new address (migrateLocked,
	// RFC 9000 §9) — so it is guarded by mu like everything else the send
	// and receive paths share.
	peer net.Addr

	// mu comes before a Listener's mu in the lock order: identifier
	// registration takes the listener's lock under this one.
	mu   sync.Mutex
	cond sync.Cond // credit arriving, or the connection ending

	// writeRefusals counts sends refused by an ICMP surfacing on the
	// socket, and writeRefusal keeps the last one; the handshake adds them
	// to the refusals its reads see. Guarded by mu.
	writeRefusals int
	writeRefusal  error
	// hsSyncRead is true while handshake() reads the socket itself; hsWake
	// asks that read to return early so the loop re-counts refusals: on
	// Linux a closed port's ICMP is reported by whichever syscall comes
	// next, and when that is a probe's second send the read would
	// otherwise block until the handshake deadline. Guarded by mu.
	hsSyncRead bool
	hsWake     bool
	// writeFailures counts every failed send, refusal or not. Guarded by mu.
	writeFailures uint64

	dcid        []byte // a client adopts the server's SCID, then a Retry's
	initialDCID []byte // what keyed the Initial secrets; the server echoes it
	odcid       []byte // server side: the client's first destination choice
	peerCIDSet  bool
	retried     bool
	retryToken  []byte
	retrySCID   []byte // what the Retry carried; retry_source_connection_id must echo it

	spaces [3]space
	rtt    rttEstimator
	cc     newReno
	pacer  pacer
	// paceDeadline is when the earliest pacer-blocked writer may try again;
	// the timer loop broadcasts cond and clears it. Zero while nobody waits.
	paceDeadline time.Time

	ptoCount    int
	maxDatagram int
	spin        byte // 0 or 0x20, OR-ed into every short header (§17.4)
	// blockedResend is when the §4.1 periodic *_BLOCKED re-announcement
	// next fires, zero while no writer is parked at a flow-control limit.
	blockedResend time.Time
	// hdrScratch and pktScratch are writePacketLocked's per-packet buffers,
	// reusable because mu serialises every writer and both are consumed
	// before the function returns (the header into the sealed packet, the
	// packet into WriteTo, which may not retain its argument).
	hdrScratch []byte
	pktScratch []byte
	// gather, dgram, dgramPend and dgramPad are datagram coalescing (RFC
	// 9000 §12.2): while gather is set, Initial and Handshake packets
	// accumulate in dgram — the last one built kept unsealed in dgramPend,
	// so the packet closing the datagram can carry the §14.1 padding as
	// PADDING frames, which dgramPad says an Initial in it requires. See
	// gatherLocked and flushDatagramLocked.
	gather    bool
	dgram     []byte
	dgramPend *pendingPacket
	dgramPad  bool
	// parseScratch and batchScratch are the receive path's two per-packet
	// frame slices, reusable because one receive goroutine processes one
	// packet at a time and no consumer keeps either slice (see frames).
	parseScratch []Frame
	batchScratch []Frame
	// settleScratch carries settleBatchConsumption's per-batch settlement
	// between its lock passes; guarded by deliverMu like the batches it
	// settles.
	settleScratch []settledStream
	// manualCredit hands receive-side crediting to the batch consumer; see
	// SetManualCredit.
	manualCredit bool

	connSend sendQuota
	connRecv recvQuota
	control  []byte // small control frames waiting to ride the next packet
	// controlRetires counts the RETIRE_CONNECTION_ID frames in control —
	// the one control frame a peer can make this end queue at will; see
	// queueRetireLocked.
	controlRetires int
	// grants holds the flow-control credit frames — MAX_DATA, MAX_STREAM_DATA,
	// MAX_STREAMS — queued apart from control because they alone may exceed
	// the congestion window when flushed: a grant is how a blocked peer
	// resumes sending, and a peer that cannot send cannot produce the ACKs
	// that would reopen this end's window — the deadlock RFC 9002 §7's rule
	// would otherwise create. Everything else in control waits for cwnd.
	grants []byte

	peerParams    TransportParameters
	hasPeerParams bool

	peerMaxStreamsBidi  uint64 // how many this end may open
	peerMaxStreamsUni   uint64
	localMaxStreamsBidi uint64 // how many the peer may open
	localMaxStreamsUni  uint64
	openedBidi          uint64 // opened by this end
	openedUni           uint64
	seenPeerBidi        uint64 // highest count the peer has opened
	seenPeerUni         uint64

	idleTimeout        time.Duration
	idleDeadline       time.Time
	handshakeConfirmed bool
	// elicitingSinceRecv is true once an ack-eliciting packet has been sent
	// since the last packet was received and processed. RFC 9000 §10.1 lets a
	// send restart the idle timer only while this is false — the *first*
	// ack-eliciting packet after a receive — so an endpoint transmitting into
	// silence does not keep extending its own idle clock forever.
	elicitingSinceRecv bool
	// peerAddrValidated is the client-side answer to RFC 9002's
	// PeerCompletedAddressValidation: the server acknowledged a Handshake
	// packet, or confirmed the handshake. Until then the server may be
	// amplification-blocked, which changes what a stalled probe means —
	// the backoff is not reset on Initial ACKs and the PTO stays armed
	// even with nothing in flight (§6.2.1, §6.2.2.1).
	peerAddrValidated bool

	// deferred holds packets that arrived before the keys for their level
	// did. RFC 9000 §5.7 allows buffering them, and without it the race is
	// not rare but systematic: a peer finishes its handshake and writes
	// immediately, and the other end has not installed its 1-RTT read keys
	// yet. Bounded, because an unbounded buffer of things that may never
	// become readable is a peer choosing how much memory to occupy. Each
	// entry keeps its arrival time, so the replay does not fold the parking
	// delay into ACK delay or round-trip samples (RFC 9002 §5.3).
	deferred [3][]deferredPacket

	// deliverMu serialises the stream-frame callback and guards the three
	// fields below. It is not mu: the callback reaches back into the
	// connection — to answer on a stream, or to reset one — and holding the
	// connection's own lock across it would deadlock on the first answer.
	//
	// The order between the two is deliverMu then mu, never the reverse.
	deliverMu sync.Mutex
	onFrames  func([]Frame) error
	batchMode bool
	// pending holds every delta delivered before a callback registers, in
	// arrival order, for OnStreamFrames to replay. Until a Read credits
	// anything it is bounded by the connection receive window, since none
	// of its bytes have been credited; pendingBytes guards the frames that
	// carry no bytes. pendingLost says it was abandoned — a Read took
	// bytes, or the guard tripped — and the switch replays read buffers
	// instead.
	pending      [][]Frame
	pendingBytes uint64
	pendingLost  bool
	// readTaken is set by the first Read that takes bytes, under that
	// stream's lock: from then on pending no longer mirrors the read
	// buffers and holding it would only grow.
	readTaken atomic.Bool

	streams     map[uint64]*Stream
	closedIDs   map[uint64]struct{}
	closedOrder []uint64
	accepted    chan *Stream
	closed      chan struct{}
	closeErr    error
	closeSent   bool
	draining    bool
	peerClose   peerClose
	once        sync.Once
	timerKick   chan struct{}

	discarded uint64 // datagrams or packets dropped without processing

	// ku is the 1-RTT key-update state this end's send side keeps
	// (RFC 9001 §6). Guarded by mu.
	ku keyUpdateState
	// unhandledTLSEvents counts crypto/tls events pump has no case for;
	// atomic, as it is bumped outside mu.
	unhandledTLSEvents atomic.Uint64

	// deltaScratch is the reusable buffer stream deliveries append their
	// ordered deltas into, per packet; see takeScratch.
	deltaScratch []byte

	// incoming is non-nil only for a connection a [Listener] manages: it is
	// fed datagrams the listener already read off its one shared socket,
	// instead of this connection reading the socket itself (which would
	// race every other connection sharing it). nil means this Conn owns its
	// net.PacketConn exclusively — [Dial] and standalone [Accept] — and
	// readOne/readLoop read it directly, exactly as before this existed.
	incoming chan rawDatagram

	// amplActive gates the RFC 9000 §8.1 anti-amplification limit: true only
	// while a server connection's peer address is not yet validated —
	// Listener-managed and standalone [Accept] alike, since a first datagram
	// read off a bare socket proves its source no more than one a listener
	// demuxed. Always false for [Dial]. Guarded by mu, like everything else
	// writePacketLocked and receive touch.
	amplActive bool
	amplRecv   int64
	amplSent   int64

	// registrar issues this connection's additional identifiers: the
	// [Listener]'s demux table for a connection it manages, an
	// ownerRegistrar for one that owns its socket. The peer needs them to
	// follow this end across a NAT rebind (RFC 9000 §9.5).
	registrar cidRegistrar
	// localCIDs are the identifiers this end currently answers to, sequence
	// 0 being scid; nextLocalSeq numbers the next one issued.
	localCIDs    []localCID
	nextLocalSeq uint64
	// peerCIDs pools the identifiers the peer issued for itself;
	// peerRetirePrior is the highest Retire Prior To honoured so far, and
	// currentPeerSeq says which pooled entry dcid currently is.
	peerCIDs        []peerCID
	peerRetirePrior uint64
	currentPeerSeq  uint64
	pathProbe       pathProbe
	migrations      uint64
	// lastValidated* is the path a server last proved — address, socket,
	// and the peer identifier sequence used on it — recorded when a
	// migration leaves it: where §9.3.2 sends a migration that never
	// validates back to. lastValidatedPeer nil means none is known.
	lastValidatedPeer net.Addr
	lastValidatedPC   net.PacketConn
	lastValidatedSeq  uint64

	// rxFrom, rxDatagramLen, rxLocalCIDSeq and rxHighest describe the
	// datagram currently being processed — its source address, its size,
	// which local identifier its short header named, and whether its packet
	// is the highest-numbered yet received in its space. Receiving is one
	// goroutine per connection, so per-datagram state can live here; guarded
	// by mu because the frame handlers read it under the lock they already
	// hold.
	rxFrom        net.Addr
	rxDatagramLen int
	rxLocalCIDSeq uint64
	rxHighest     bool
	// rxVia is the socket the datagram being processed arrived on, nil
	// meaning pc. Only a Listener-fed server connection ever sees another
	// one: the listener's preferred-address socket (§9.6).
	rxVia net.PacketConn

	// preferred is, on a client, the server's preferred address of this
	// connection's family (§9.6), kept after the move for
	// preferredAddress; nil when none was offered.
	preferred net.Addr
}

// rawDatagram is one datagram a [Listener] has already read off its shared
// socket, queued for the Conn it was routed to.
type rawDatagram struct {
	data []byte
	from net.Addr
	// via is the socket the datagram arrived on: the listener's main one,
	// or its preferred-address one (§9.6). nil means the main one.
	via net.PacketConn
}

// deferredPacket is one packet parked until the keys for its level arrive,
// stamped with when it actually reached this endpoint.
type deferredPacket struct {
	raw []byte
	at  time.Time
}

// peerClose is what a peer's CONNECTION_CLOSE carried.
type peerClose struct {
	code   uint64
	app    bool
	reason string
	seen   bool
}

// keyUpdateState is what a sender must know to rotate 1-RTT keys and to
// answer the peer's rotation (RFC 9001 §6.1, §6.2, §6.5). The read side's
// state lives on the opener (keyPhase); this is the rest.
type keyUpdateState struct {
	// phase is the Key Phase bit this end's packets carry now: 0, then
	// keyPhaseBit, alternating with each rotation (§6).
	phase byte
	// firstPN is the first packet number sent under the current write keys
	// and acked whether the peer has acknowledged one of them, which is
	// what permits the next rotation (§6.1): keys are then known to be
	// present on both sides. The only initiator here is the AEAD limit
	// (§6.6), a MUST; the three-probe-timeout wait §6.5 asks of a
	// voluntary rotation has no voluntary rotation to apply to.
	firstPN uint64
	acked   bool
	// responseDue is set when the peer initiated: this end's send keys
	// switched in answer and no packet under them has carried an
	// acknowledgement yet (§6.2). A further rotation from the peer in that
	// window is a KEY_UPDATE_ERROR — it rotated twice without waiting.
	responseDue bool
	responsePN  uint64
	// dropPrevAt is when the previous read keys are let go (§6.5: no more
	// than three probe timeouts after the first packet under the new ones);
	// zero when none are held.
	dropPrevAt time.Time
	// updates counts rotations of this end's send keys, for Stats.
	updates uint64
}

// space is one packet-number space: its keys, CRYPTO reassembly, what was
// sent and not yet acknowledged, and what was received and not yet
// acknowledged back.
type space struct {
	sealer     *packetSealer
	opener     *packetOpener
	crypto     reassembly // the peer's handshake bytes, reordered
	cryptoOff  uint64     // this end's send offset
	nextPN     uint64
	largest    uint64 // largest received, for packet-number decoding
	ack        ackTracker
	sent       sentTracker
	retrans    [][]byte // payloads of lost packets, waiting to go again
	lastElicit time.Time
	discarded  bool
}

// The three packet-number spaces (RFC 9000 §12.3). Early data shares the
// application space, which is why there are three and not four.
const (
	spaceInitial = iota
	spaceHandshake
	spaceApplication
)

// levelFor is spaceFor's inverse: which encryption level a packet-number
// space carries.
func levelFor(space int) tls.QUICEncryptionLevel {
	switch space {
	case spaceInitial:
		return tls.QUICEncryptionLevelInitial
	case spaceHandshake:
		return tls.QUICEncryptionLevelHandshake
	default:
		return tls.QUICEncryptionLevelApplication
	}
}

func spaceFor(l tls.QUICEncryptionLevel) int {
	switch l {
	case tls.QUICEncryptionLevelInitial:
		return spaceInitial
	case tls.QUICEncryptionLevelHandshake:
		return spaceHandshake
	default:
		return spaceApplication
	}
}

// requireALPN refuses a tls.Config that names no application protocol. QUIC
// mandates ALPN (RFC 9001 §8.1): against a conformant peer the handshake
// would fail anyway, mid-flight, with a bare no_application_protocol alert —
// refusing at construction turns that interoperability trap into an error
// that names the missing field.
func requireALPN(cfg *tls.Config) error {
	if cfg == nil || len(cfg.NextProtos) == 0 {
		return fmt.Errorf("%w: tls.Config.NextProtos is empty — RFC 9001 §8.1 requires ALPN "+
			"(a conformant peer ends such a handshake with no_application_protocol)", ErrQUIC)
	}
	return nil
}

// Dial opens a connection to addr and completes the handshake.
//
// pc is any [net.PacketConn]; one from the net/quic/udp helper carries the
// Don't Fragment bit RFC 9000 §14 asks for, one from [net.ListenPacket]
// does not.
//
// cfg must name at least one application protocol in NextProtos — ALPN is
// mandatory in QUIC (RFC 9001 §8.1). All three TLS 1.3 suites are spoken:
// the AES-GCM pair from the standard library, ChaCha20-Poly1305 from
// golang.org/x/crypto, the module's one dependency.
//
// The handshake gives up after [DefaultHandshakeTimeout]; [DialContext]
// takes a context instead.
func Dial(pc net.PacketConn, addr net.Addr, cfg *tls.Config, params TransportParameters) (*Conn, error) {
	if err := params.check(); err != nil {
		return nil, err
	}
	return DialContext(context.Background(), pc, addr, cfg, params)
}

// DefaultHandshakeTimeout is how long a handshake runs before it fails,
// when no context deadline says otherwise.
const DefaultHandshakeTimeout = 10 * time.Second

// DialContext is [Dial] under a context. The handshake ends when ctx is
// cancelled — promptly, with an error that wraps both [ErrQUIC] and the
// context's cause — or at ctx's deadline; a ctx without a deadline keeps
// Dial's [DefaultHandshakeTimeout]. Once DialContext has returned, ctx no
// longer has any hold on the connection.
func DialContext(ctx context.Context, pc net.PacketConn, addr net.Addr, cfg *tls.Config, params TransportParameters) (*Conn, error) {
	if err := params.check(); err != nil {
		return nil, err
	}
	if err := requireALPN(cfg); err != nil {
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
	c.tls = tls.QUICClient(&tls.QUICConfig{TLSConfig: cfg})
	if err := c.handshake(ctx); err != nil {
		return nil, err // the handshake path closes on every failure
	}
	return c, nil
}

// Accept completes a server handshake for a connection whose first datagram
// has already been read.
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
func Accept(pc net.PacketConn, peer net.Addr, first []byte, cfg *tls.Config, params TransportParameters) (*Conn, error) {
	if err := params.check(); err != nil {
		return nil, err
	}
	if err := requireALPN(cfg); err != nil {
		return nil, err
	}
	h, _, err := parseLongHeader(first)
	if err != nil {
		return nil, err
	}
	if h.Type != packetInitial {
		return nil, fmt.Errorf("%w: a connection opening with a %d packet", ErrQUIC, h.Type)
	}
	// RFC 9000 §7.2: a client's first Initial carries a destination
	// identifier of at least eight bytes; a shorter one is refused before any
	// Initial secrets are derived from it.
	if len(h.DCID) < 8 {
		return nil, fmt.Errorf("%w: a client Initial with a %d-byte destination identifier; §7.2 requires 8", ErrQUIC, len(h.DCID))
	}
	c, err := newServerConn(pc, peer, h, cfg, params, nil, nil, nil)
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
	if err := c.receiveGathered(rawDatagram{data: first, from: peer}); err != nil {
		return nil, err // receive only fails through abort, which closed
	}
	if err := c.handshake(context.Background()); err != nil {
		return nil, err // the handshake path closes on every failure
	}
	return c, nil
}

// newServerConn bootstraps a server-side Conn from a client's first Initial
// packet: picks this end's connection identifier, derives Initial secrets,
// and starts the TLS handshake machinery. [Accept] and [Listener] share this
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
// [Accept] passes nil: no token (which §10.3 permits), no preferred
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
		err = c.installInitialKeys(keys.secrets, keys.opener)
	} else {
		err = c.installInitial(h.DCID)
	}
	if err != nil {
		_ = c.Close() // newConn started the timer loop; an early failure must stop it
		return nil, err
	}
	c.tls = tls.QUICServer(&tls.QUICConfig{TLSConfig: cfg})
	if err := c.start(); err != nil {
		_ = c.Close()
		return nil, err
	}
	return c, nil
}

func newConn(pc net.PacketConn, peer net.Addr, dcid, scid []byte, isClient bool, params TransportParameters) *Conn {
	// This end always keeps at least one spare identifier slot for the peer
	// to rotate into; below the protocol minimum of 2 the announcement would
	// be refused by any conformant peer (§18.2), so the zero value and any
	// degenerate configuration both land on the package's working default.
	if params.ActiveConnIDLimit < 2 {
		params.ActiveConnIDLimit = 4
	}
	c := &Conn{
		pc: pc, peer: peer, dcid: dcid, scid: scid, isClient: isClient,
		params:              params,
		streams:             make(map[uint64]*Stream),
		closedIDs:           make(map[uint64]struct{}),
		accepted:            make(chan *Stream, 64),
		closed:              make(chan struct{}),
		timerKick:           make(chan struct{}, 1),
		maxDatagram:         1200, // until the peer's max_udp_payload_size says more
		connSend:            sendQuota{},
		connRecv:            newRecvQuota(params.InitialMaxData),
		localMaxStreamsBidi: params.InitialMaxStreamsBidi,
		localMaxStreamsUni:  params.InitialMaxStreamsUni,
		idleTimeout:         params.MaxIdleTimeout,
	}
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
		// authenticated packet and seeds the pool in adoptPeerCID.
		c.peerCIDs = []peerCID{{seq: 0, cid: dcid, used: true}}
	}
	c.cc = newNewReno(c.maxDatagram)
	// §7.7: the pacer's bucket is the initial window, which is also the
	// freshly-built controller's cwnd.
	c.pacer.burst = int64(c.cc.cwnd)
	if c.idleTimeout != 0 {
		c.idleDeadline = time.Now().Add(c.idleTimeout)
	}
	go c.timerLoop()
	return c
}

// rollSpinLocked draws a fresh random spin-bit value for the short headers
// this end sends. This package does not implement the §17.4 latency spin
// protocol; the section says a non-implementing endpoint should then set the
// bit randomly per connection — and per identifier, since the bit's threat
// model is path linkability and the identifier is the linkability unit.
// Callers hold mu (or the Conn is not yet shared).
func (c *Conn) rollSpinLocked() {
	var b [1]byte
	if _, err := rand.Read(b[:]); err == nil && b[0]&1 == 1 {
		c.spin = 0x20
	} else {
		c.spin = 0
	}
}

// installInitial derives the Initial keys, which need no handshake: they come
// from the connection identifier the client picked, so anyone who saw the
// first packet can read them. They authenticate, they do not hide.
func (c *Conn) installInitial(dcid []byte) error {
	s, err := initialSecrets(dcid)
	if err != nil {
		return err
	}
	return c.installInitialKeys(s, nil)
}

// installInitialKeys installs already-derived Initial secrets; opener, when
// non-nil, is one already built for the peer's direction.
func (c *Conn) installInitialKeys(s secrets, opener *packetOpener) error {
	mine, theirs := s.Client, s.Server
	if !c.isClient {
		mine, theirs = s.Server, s.Client
	}
	sealer, err := newPacketSealer(mine)
	if err != nil {
		return err
	}
	if opener == nil {
		if opener, err = newPacketOpener(theirs); err != nil {
			return err
		}
	}
	c.mu.Lock()
	c.spaces[spaceInitial].sealer = sealer
	c.spaces[spaceInitial].opener = opener
	c.mu.Unlock()
	return nil
}

func (c *Conn) start() error {
	c.tls.SetTransportParameters(appendParameters(nil, c.params))
	if err := c.tls.Start(context.Background()); err != nil {
		return fmt.Errorf("quic: starting the handshake: %w", err)
	}
	return nil
}

// handshake runs until TLS says it is done, carrying CRYPTO both ways. Lost
// flights are the timer loop's problem — its probe timeout retransmits them —
// so this loop only has to keep reading.
//
// ctx's deadline replaces the default one, and its cancellation closes the
// connection, which every wait below already watches for.
func (c *Conn) handshake(ctx context.Context) error {
	if c.isClient {
		if err := c.start(); err != nil {
			_ = c.Close()
			return err
		}
	}
	deadline := time.Now().Add(DefaultHandshakeTimeout)
	ctxDeadline, fromCtx := ctx.Deadline()
	if fromCtx {
		deadline = ctxDeadline
	}
	fired := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		c.fail(abandonedErr(ctx))
		_ = c.Close()
		close(fired)
	})
	defer stop()
	c.mu.Lock()
	c.hsSyncRead = true
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.hsSyncRead = false
		c.mu.Unlock()
	}()
	// ctxErr turns a deadline the context set into the context's error,
	// rather than a bare timeout — even when the socket's read deadline
	// fires a hair before the context's own timer marks it done.
	ctxErr := func(err error) error {
		if ctx.Err() != nil {
			return abandonedErr(ctx)
		}
		if fromCtx && !time.Now().Before(deadline) {
			return fmt.Errorf("%w: the handshake was abandoned: %w", ErrQUIC, context.DeadlineExceeded)
		}
		return err
	}
	var (
		heard     bool  // a datagram was read from the socket
		sockErr   error // the last socket read error
		sockErrs  int   // socket read errors so far
		readFails int   // consecutive ones, for the backoff
	)
	for {
		// The flight TLS produces leaves as one datagram where it fits
		// (§12.2): gathered across pump, flushed before the next read.
		c.mu.Lock()
		c.gatherLocked(true)
		c.mu.Unlock()
		done, err := c.pump()
		c.mu.Lock()
		if done {
			c.handshakeComplete()
		}
		c.gatherLocked(false)
		c.mu.Unlock()
		if err != nil {
			c.abort(err)
			return err
		}
		if done {
			if !stop() {
				// The context ended it in the same instant; the
				// connection is closed, or about to be.
				<-fired
				return c.endedDuringHandshakeErr()
			}
			go c.readLoop()
			return nil
		}
		c.mu.Lock()
		wRefusals, wRefusal := c.writeRefusals, c.writeRefusal
		c.mu.Unlock()
		if sockErr == nil {
			sockErr = wRefusal
		}
		if !heard && sockErrs+wRefusals >= handshakeRefusalLimit {
			c.abort(sockErr)
			return sockErr
		}
		if time.Now().After(deadline) {
			err := fmt.Errorf("%w: the handshake did not complete", ErrQUIC)
			if sockErr != nil && !heard {
				// Refused and never heard from: the refusal is the
				// better diagnosis than a bare timeout.
				err = sockErr
			}
			err = ctxErr(err)
			c.abort(err)
			return err
		}
		// A connection that ended under the handshake — a Version
		// Negotiation, the peer's CONNECTION_CLOSE (a listener's stateless
		// refusal included) — is reported now and as itself: reading on
		// would only wait out the deadline and call it a timeout.
		select {
		case <-c.closed:
			return c.endedDuringHandshakeErr()
		default:
		}
		got, err := c.readOne(deadline)
		if got {
			heard = true
			readFails = 0
		}
		if err == nil {
			continue
		}
		select {
		case <-c.closed:
			return c.endedDuringHandshakeErr()
		default:
		}
		if !got && !errors.Is(err, os.ErrDeadlineExceeded) &&
			!errors.Is(err, net.ErrClosed) && !errors.Is(err, errClosed) {
			// A socket read error under the handshake is, on UDP, an ICMP
			// surfacing: ECONNREFUSED on a connected Linux socket,
			// WSAECONNRESET on Windows. Unauthenticated, so read past like
			// readLoop does — one forged ICMP must not kill a handshake —
			// but a port that is really closed answers every flight with
			// one, and Dial should say so, not wait out the deadline.
			// The rule: fail with the socket's error once
			// handshakeRefusalLimit of them arrived with no datagram ever
			// read; once something has been heard, only the deadline ends
			// the handshake.
			c.noteDiscard()
			sockErr = err
			sockErrs++
			readFails++
			if !heard && sockErrs+wRefusals >= handshakeRefusalLimit {
				c.abort(err)
				return err
			}
			if readFails > 1 && !c.readBackoff(readFails) {
				return c.endedDuringHandshakeErr()
			}
			continue
		}
		if !got && errors.Is(err, os.ErrDeadlineExceeded) && ctx.Err() == nil && time.Now().Before(deadline) {
			// Not the deadline: a send refusal woke the read (hsWake) so
			// the refusal count at the top of the loop is re-taken now.
			continue
		}
		if !got && sockErr != nil && !heard && errors.Is(err, os.ErrDeadlineExceeded) {
			err = sockErr // as at the deadline check above
		}
		if errors.Is(err, os.ErrDeadlineExceeded) {
			err = ctxErr(err)
		}
		c.abort(err)
		return err
	}
}

// abandonedErr is the error a handshake ended by its context reports.
func abandonedErr(ctx context.Context) error {
	return fmt.Errorf("%w: the handshake was abandoned: %w", ErrQUIC, context.Cause(ctx))
}

// handshakeRefusalLimit is how many socket refusals, read or send side, a
// handshake that has heard nothing yet takes as "the port is closed". Two,
// not one, so a single forged ICMP is survived; not more, because a closed
// port answers one refusal per flight and flights are PTO-spaced: counting
// the send-side one too, a Dial to a closed port on Linux fails at the
// first probe, ~1 s, where a third would wait to ~3 s.
const handshakeRefusalLimit = 2

// endedDuringHandshakeErr is why a connection closed before its handshake
// finished: the recorded failure, else the peer's close, which a code of
// zero or an application close leaves unrecorded but is still a handshake
// that did not happen.
func (c *Conn) endedDuringHandshakeErr() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closeErr != nil {
		return c.closeErr
	}
	if pc := c.peerClose; pc.seen {
		return fmt.Errorf("%w: the peer closed the connection during the handshake: code %#x", ErrQUIC, pc.code)
	}
	return fmt.Errorf("%w: the connection closed during the handshake", ErrQUIC)
}

// handshakeComplete is the transition TLS reports: the server confirms at
// once and tells the client with HANDSHAKE_DONE; the client confirms when
// that frame arrives (§4.1.2 of RFC 9001). Handshake keys are dropped on
// confirmation (§4.9.2). Called under mu.
func (c *Conn) handshakeComplete() {
	if !c.isClient {
		c.control = AppendVarint(c.control, frameHandshakeDone)
		c.confirmLocked()
		c.flushControlLocked()
	}
}

// confirmLocked marks the handshake confirmed and discards handshake keys.
func (c *Conn) confirmLocked() {
	if c.handshakeConfirmed {
		return
	}
	c.handshakeConfirmed = true
	c.peerAddrValidated = true
	c.discardSpaceLocked(spaceHandshake)
	// Both ends now offer the alternatives a migrating, rebinding or
	// rotation-forcing peer will need.
	c.issueLocalCIDsLocked()
	// A client whose server would rather be reached elsewhere starts
	// validating that path now (§9.6.2) — the earliest moment the section
	// allows, and the connection keeps using the handshake's path until
	// the answer arrives.
	if c.isClient && c.preferred != nil && !sameAddr(c.preferred, c.peer) {
		c.probePreferredAddressLocked(time.Now())
	}
}

// discardSpaceLocked drops a space's keys and every timer and in-flight
// packet with them (RFC 9001 §4.9): what was sent there is either implicitly
// delivered — the handshake moved past it — or gone for good, and
// retransmitting it under discarded keys would be garbage on the wire.
func (c *Conn) discardSpaceLocked(space int) {
	sp := &c.spaces[space]
	if sp.discarded {
		return
	}
	sp.discarded = true
	sp.sealer, sp.opener = nil, nil
	_, inFlight := sp.sent.takeAll()
	sp.retrans = nil
	c.cc.inFlight -= inFlight
	if c.cc.inFlight < 0 {
		c.cc.inFlight = 0
	}
	c.deferred[space] = nil
	sp.ack = ackTracker{}
	c.kickTimer()
}

// pump drains the events crypto/tls has ready, sending what it asks to send
// and installing what it hands back.
func (c *Conn) pump() (done bool, err error) {
	for {
		e := c.tls.NextEvent()
		if e.Kind == tls.QUICNoEvent {
			return false, nil
		}
		if done, err := c.handleTLSEvent(e); done || err != nil {
			return done, err
		}
	}
}

// handleTLSEvent acts on one crypto/tls event; done reports the handshake
// complete.
func (c *Conn) handleTLSEvent(e tls.QUICEvent) (done bool, err error) {
	switch e.Kind {
	case tls.QUICSetReadSecret:
		return false, c.install(e.Level, e.Suite, e.Data, false)
	case tls.QUICSetWriteSecret:
		return false, c.install(e.Level, e.Suite, e.Data, true)
	case tls.QUICWriteData:
		return false, c.sendCrypto(e.Level, e.Data)
	case tls.QUICTransportParameters:
		return false, c.handlePeerParameters(e.Data)
	case tls.QUICHandshakeDone:
		return true, nil
	case tls.QUICErrorEvent:
		// crypto/tls reports a failed handshake here (Go ≥1.25) as
		// well as through HandleData's return; either way the
		// handshake is over. e.Err wraps the AlertError closeCodeFor
		// maps to the §4.8 crypto range.
		return false, fmt.Errorf("quic: TLS: %w", e.Err)
	default:
		// An event this package does not act on — session events it
		// never enables, or a kind a newer crypto/tls added — is not
		// fatal, but it is not silent either: UnhandledTLSEvents.
		c.unhandledTLSEvents.Add(1)
		return false, nil
	}
}

// handlePeerParameters validates and adopts what the peer announced. The
// checks are RFC 9000 §7.3's authentication of the handshake against the
// packets that carried it: the identifiers each side saw on the wire must be
// the ones the handshake names, or someone spliced two conversations.
// idleDuration converts an announced max_idle_timeout to a Duration,
// saturating. The field is a varint of up to 2^62-1 milliseconds, far past
// what a Duration holds, and a wrapped product turns a peer's "practically
// never" into a deadline already passed.
func idleDuration(ms uint64) time.Duration {
	const most = uint64(math.MaxInt64 / int64(time.Millisecond))
	if ms > most {
		return math.MaxInt64
	}
	return time.Duration(ms) * time.Millisecond //nolint:gosec // G115: bounded by most just above
}

func (c *Conn) handlePeerParameters(raw []byte) error {
	p, err := parseParameters(raw)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if !bytes.Equal(p.initialSourceCID, c.dcid) {
		return fmt.Errorf("%w: the peer's initial_source_connection_id is not the identifier its packets carry", ErrQUIC)
	}
	switch {
	case c.isClient:
		if !bytes.Equal(p.originalDCID, c.initialDCID) {
			return fmt.Errorf("%w: the server did not echo the original destination identifier", ErrQUIC)
		}
		// retry_source_connection_id binds this handshake to the Retry that
		// redirected it (RFC 9000 §7.3): present iff a Retry actually
		// happened, and then exactly the identifier that Retry carried —
		// kept in retrySCID, because c.dcid has usually already moved on to
		// the server's own identifier by the time the parameters arrive.
		if c.retried {
			if !bytes.Equal(p.retrySourceCID, c.retrySCID) {
				return fmt.Errorf("%w: the server did not echo the Retry's source identifier", ErrQUIC)
			}
		} else if p.retrySourceCID != nil {
			return fmt.Errorf("%w: retry_source_connection_id without a Retry", ErrQUIC)
		}
	case p.originalDCID != nil:
		return fmt.Errorf("%w: a client sent original_destination_connection_id", ErrQUIC)
	case p.retrySourceCID != nil:
		return fmt.Errorf("%w: a client sent retry_source_connection_id", ErrQUIC)
	case p.sawStatelessResetToken:
		// §18.2: stateless_reset_token and preferred_address are server-only
		// announcements; a client sending either is TRANSPORT_PARAMETER_ERROR.
		return &transportError{
			code: transportParameterError,
			err:  fmt.Errorf("%w: a client sent stateless_reset_token", ErrQUIC),
		}
	case p.sawPreferredAddress:
		return &transportError{
			code: transportParameterError,
			err:  fmt.Errorf("%w: a client sent preferred_address", ErrQUIC),
		}
	}
	if p.MaxUDPPayloadSize != 0 && p.MaxUDPPayloadSize < 1200 {
		return fmt.Errorf("%w: max_udp_payload_size %d is below the protocol minimum", ErrQUIC, p.MaxUDPPayloadSize)
	}
	if p.AckDelayExponent > 20 {
		return fmt.Errorf("%w: ack_delay_exponent %d is over 20", ErrQUIC, p.AckDelayExponent)
	}
	if p.MaxAckDelay >= 1<<14*time.Millisecond {
		return fmt.Errorf("%w: max_ack_delay %v is over the protocol maximum", ErrQUIC, p.MaxAckDelay)
	}

	c.peerParams = p
	c.hasPeerParams = true
	if c.isClient && len(p.statelessResetToken) == 16 {
		// §10.3.1: the handshake parameter's token belongs to the peer's
		// sequence-0 identifier, exactly as a NEW_CONNECTION_ID's token
		// belongs to the identifier it rides with.
		for i := range c.peerCIDs {
			if c.peerCIDs[i].seq == 0 {
				copy(c.peerCIDs[i].token[:], p.statelessResetToken)
			}
		}
	}
	if c.isClient && p.preferredAddress != nil {
		// §5.1.1: the identifier offered with preferred_address is sequence
		// 1 of the peer's pool, token included, whether or not this end
		// ever moves there. The move itself waits for confirmation
		// (confirmLocked), as §9.6 requires.
		pa := p.preferredAddress
		c.peerCIDs = append(c.peerCIDs, peerCID{seq: 1, cid: append([]byte(nil), pa.cid...), token: pa.statelessResetToken})
		c.preferred = pa.addrFor(c.peer)
	}
	c.connSend.grant(p.InitialMaxData)
	c.peerMaxStreamsBidi = p.InitialMaxStreamsBidi
	c.peerMaxStreamsUni = p.InitialMaxStreamsUni
	// c.maxDatagram deliberately ignores p.MaxUDPPayloadSize: the peer may
	// announce room for more, but without path MTU discovery (RFC 9000
	// §14.2-14.3, not implemented) this end has no proof the *path* carries
	// more than the 1200 bytes §14.1 guarantees — and a datagram the path
	// swallows is retransmitted at the same size into the same black hole
	// until the connection stalls to death. 1200 it stays until DPLPMTUD
	// exists.
	// Streams opened before the grants arrived — none in the ordinary order
	// of things, but harmless to repair.
	for _, s := range c.streams {
		s.sendQ.grant(initialStreamSendLimit(p, s.id, c.isClient))
	}
	// The effective idle timeout is the smaller of the two announcements,
	// zero meaning "none" on either side (§10.1).
	peerIdle := p.MaxIdleTimeout
	switch {
	case c.idleTimeout == 0:
		c.idleTimeout = peerIdle
	case peerIdle != 0 && peerIdle < c.idleTimeout:
		c.idleTimeout = peerIdle
	}
	if c.idleTimeout != 0 {
		c.idleDeadline = time.Now().Add(c.idleTimeout)
	}
	c.cond.Broadcast()
	return nil
}

func (c *Conn) install(l tls.QUICEncryptionLevel, suiteID uint16, secret []byte, write bool) error {
	s, err := suiteFor(suiteID)
	if err != nil {
		return err
	}
	space := spaceFor(l)
	c.mu.Lock()
	defer c.mu.Unlock()
	if write {
		sealer, err := newSealerSuite(secret, s)
		if err != nil {
			return err
		}
		c.spaces[space].sealer = sealer
		return nil
	}
	var opener *packetOpener
	if space == spaceApplication {
		// 1-RTT keys rotate (RFC 9001 §6): the opener carries the next
		// phase from the start, so the peer's first rotation costs no
		// derivation on the receive path.
		opener, err = newPhasedOpener(secret, s)
	} else {
		opener, err = newOpenerSuite(secret, s)
	}
	if err != nil {
		return err
	}
	c.spaces[space].opener = opener
	held := c.deferred[space]
	c.deferred[space] = nil
	peer := c.peer
	c.mu.Unlock()
	// The keys just arrived, so whatever was waiting on them can be read
	// now — stamped with when each packet actually arrived, not with now.
	// Unlocked first: opening a packet takes the lock again.
	for _, pkt := range held {
		if err := c.receiveAt(pkt.raw, peer, pkt.at); err != nil {
			c.mu.Lock()
			return err
		}
	}
	c.mu.Lock()
	return nil
}

// rotateSendKeysLocked moves this end's 1-RTT send keys to the next phase
// (RFC 9001 §6.1, §6.4): new key and IV, the phase bit toggled, the
// confidentiality count restarted, and the old keys dropped at once — a
// sender never protects anything under old keys. The next rotation waits
// for an acknowledgement of a packet sent under these.
func (c *Conn) rotateSendKeysLocked() error {
	sp := &c.spaces[spaceApplication]
	next, err := sp.sealer.k.next()
	if err != nil {
		return err
	}
	sp.sealer = &packetSealer{k: next}
	c.ku.phase ^= keyPhaseBit
	c.ku.firstPN = sp.nextPN
	c.ku.acked = false
	c.ku.updates++
	return nil
}

// canInitiateKeyUpdateLocked is RFC 9001 §6.1's two MUSTs: the handshake
// is confirmed and a packet under the current phase was acknowledged.
func (c *Conn) canInitiateKeyUpdateLocked() bool {
	return c.handshakeConfirmed && c.ku.acked && !c.ku.responseDue
}

// initiateKeyUpdateLocked rotates this end's send keys on its own
// initiative (RFC 9001 §6.1), when permitted. Reports whether it did. The
// read keys need no change: the peer's answer arrives under the next phase
// the opener already holds, and is promoted when it authenticates.
func (c *Conn) initiateKeyUpdateLocked() (bool, error) {
	if !c.canInitiateKeyUpdateLocked() {
		return false, nil
	}
	return true, c.rotateSendKeysLocked()
}

// onKeyPhaseOpenedLocked follows a 1-RTT packet that authenticated under
// the opener's next keys at packet number pn: the peer has moved to that
// phase (RFC 9001 §6.2). The read keys advance, with the old ones kept for
// three probe timeouts of stragglers (§6.5), and the send keys follow if
// they are still in the old phase. A second advance while the answer to
// the first has not carried an acknowledgement is the peer rotating twice
// without waiting — KEY_UPDATE_ERROR, as §6.2 allows and nothing sound
// requires tolerating.
func (c *Conn) onKeyPhaseOpenedLocked(opener *packetOpener, pn uint64, now time.Time) error {
	sp := &c.spaces[spaceApplication]
	if sp.opener != opener {
		// Another packet already advanced the phase under the same keys;
		// this one is read under what is now the current set.
		return nil
	}
	if c.ku.responseDue {
		return &transportError{
			code: transportKeyUpdateError,
			err:  fmt.Errorf("%w: the peer rotated its keys again before this end acknowledged the first rotation", ErrQUIC),
		}
	}
	rotated, err := opener.rotated(pn)
	if err != nil {
		return err
	}
	sp.opener = rotated
	c.ku.dropPrevAt = now.Add(3 * c.rtt.pto(c.peerMaxAckDelayLocked()))
	if c.ku.phase == rotated.phase.bit {
		return nil // this end initiated; the peer's answer completes it
	}
	if err := c.rotateSendKeysLocked(); err != nil {
		return err
	}
	c.ku.responseDue, c.ku.responsePN = true, pn
	return nil
}

func randomID(n int) ([]byte, error) {
	b := make([]byte, n)
	_, err := rand.Read(b)
	return b, err
}

var errClosed = errors.New("quic: the connection is closed")

// Err reports why the connection ended, or nil. A clean close — this end's
// [Conn.Close], the peer's CONNECTION_CLOSE with no error, an idle timeout —
// is not an error.
//
// It is safe to call from any goroutine at any time.
func (c *Conn) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closeErr
}

// PeerClosed reports the application close the peer sent, if any: HTTP/3's
// H3_NO_ERROR arrives here, and what it means belongs to the layer that
// knows the codes.
func (c *Conn) PeerClosed() (code uint64, reason string, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.peerClose.code, c.peerClose.reason, c.peerClose.seen && c.peerClose.app
}

// fail records why the connection is ending, keeping the first reason: what
// follows a failure is usually caused by it, and the cause is the useful one.
func (c *Conn) fail(err error) {
	c.mu.Lock()
	if c.closeErr == nil {
		c.closeErr = err
	}
	c.mu.Unlock()
}

// Stats is a snapshot of the counters a connection keeps about what it
// could not process, for the dashboard rather than the data path.
type Stats struct {
	// DiscardedPackets is how many incoming datagrams or packets were dropped
	// without being processed: wrong sender, failed parse, failed
	// authentication — and transient socket read errors, which on Windows
	// include the unauthenticated ICMP port-unreachable. A rising count under
	// load is an attacker probing, or a path corrupting datagrams.
	DiscardedPackets uint64

	// KeyUpdates is how many times this end's 1-RTT send keys rotated
	// (RFC 9001 §6), whichever side initiated. Each rotation renews the
	// AEAD's confidentiality budget (§6.6); a count that never moves on a
	// connection that has sent tens of millions of packets is the thing to
	// look at.
	KeyUpdates uint64

	// WriteFailures is how many datagrams this connection failed to hand to
	// its socket. Sends are best effort — loss recovery resends what a failed
	// write lost, and a CONNECTION_CLOSE that fails to leave is covered by
	// the peer's idle timer — so no write error ends the connection or
	// reaches a caller; a count that keeps rising is a socket that has
	// stopped working where the connection would otherwise just look slow.
	WriteFailures uint64

	// UnhandledTLSEvents is how many crypto/tls QUIC events this connection
	// received of a kind this package does not handle — most likely one a
	// newer Go release added that this package does not know yet.
	UnhandledTLSEvents uint64
}

// Stats reports the connection's counters, consistently with each other.
func (c *Conn) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Stats{
		DiscardedPackets:   c.discarded,
		KeyUpdates:         c.ku.updates,
		WriteFailures:      c.writeFailures,
		UnhandledTLSEvents: c.unhandledTLSEvents.Load(),
	}
}

// LocalParameters reports the transport parameters this endpoint announced.
// A layer above sizing its own queues wants the same bounds the transport
// enforces — most of all the stream limit, which is what caps how many
// requests can be admitted and unanswered at once.
func (c *Conn) LocalParameters() TransportParameters { return c.params }

// Done is closed when the connection ends.
func (c *Conn) Done() <-chan struct{} { return c.closed }
