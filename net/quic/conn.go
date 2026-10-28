// Where to find what in this package's source. The package documentation
// (quic.go) says what it does; this says which file does it.
//
//	conn.go          Conn: its state, the failure model
//	dial.go          Dial and accept: building a Conn, either side
//	handshake.go     the handshake: TLS events, the peer's parameters, keys
//	keyupdate.go     key updates (RFC 9001 §6); tlsconfig.go the tls.Config checks
//	stats.go         Stats, what a connection reports of itself
//	receive.go       a datagram in: the read loops, receiveAt's steps, the frames
//	send.go          a packet out: sendPacketLocked, writePacketLocked, coalescing
//	stream.go        Stream: Read and Write, OnStreamFrames and its credit modes
//	reassembly.go    stream and CRYPTO bytes put back in order
//	flow.go          flow control, both directions, both levels (RFC 9000 §4)
//	ack.go           acknowledgements owed (RFC 9000 §13.2)
//	recovery.go      loss detection and NewReno (RFC 9002)
//	cubic.go         CUBIC; bbr3.go BBR; hystart.go slow-start exit; ecn.go ECN
//	pmtu.go          the datagram size search (DPLPMTUD, RFC 8899)
//	crypto.go        packet protection (RFC 9001)
//	packet.go        packet headers; frame.go frame types and parsing
//	transport.go     transport parameters (RFC 9000 §18)
//	listener.go      Listener: accepting connections on one socket
//	retry.go         a server's Retry; refusal_*.go the peer's ICMP refusals
//	migration.go     connection identifiers and passive migration (RFC 9000 §9)
//	earlyclient.go   0-RTT for a client; earlydata.go 0-RTT for a server
//	close.go         how a connection ends; timer.go the goroutine of its deadlines
//	gso*.go          segmentation offload, per platform

package quic

import (
	"crypto/rand"
	"crypto/tls"
	"errors"
	"net"
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
// integration (interop/), and against ngtcp2 and the other peers of the QUIC
// Interop Runner (see the package documentation).
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

	// peer is where this connection sends. Once the connection is live it
	// changes only through setPeerLocked — a server following its client to
	// a new address (migrateLocked, RFC 9000 §9), a client adopting the
	// preferred address, either falling back to the last validated path —
	// so the registrar follows every move; it is guarded by mu like
	// everything else the send and receive paths share.
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
	// hsDeadline is when the running handshake gives up, hsStart when it
	// began. hsExtendable lets a server's deadline move once, when the
	// client's first authenticated Handshake packet proves its address
	// (extendHandshakeLocked). handshakeBudget is a Listener's
	// HandshakeTimeout, zero meaning [DefaultHandshakeTimeout]. Guarded by
	// mu.
	hsDeadline      time.Time
	hsStart         time.Time
	hsExtendable    bool
	handshakeBudget time.Duration
	// writeFailures counts every failed send, refusal or not. Guarded by mu.
	writeFailures uint64

	dcid        []byte // a client adopts the server's SCID, then a Retry's
	initialDCID []byte // what keyed the Initial secrets; the server echoes it
	odcid       []byte // server side: the client's first destination choice
	peerCIDSet  bool
	// peerSCID is the source identifier the peer's first authenticated
	// long-header packet carried (a server reads it off the client's first
	// Initial before the Conn exists). RFC 9000 §7.2 pins it: every later
	// long-header packet must repeat it, or it is discarded.
	peerSCID   []byte
	retried    bool
	retryToken []byte
	retrySCID  []byte // what the Retry carried; retry_source_connection_id must echo it

	spaces   [3]space
	rtt      rttEstimator
	cc       congestionController
	delivery deliveryEstimator // delivery-rate state (BBR draft §4.1), fed by send.go and sentTracker.release
	pacer    pacer
	// paceDeadline is when the earliest pacer-blocked sender — a parked
	// writer, or the retransmission pass — may try again; the timer loop
	// runs the pass, broadcasts cond and clears it. Zero while nobody waits.
	paceDeadline time.Time
	// writers is how many Stream writes are in progress, writeSent when a
	// write last cleared the pacer to send: whether the sender has data, and
	// since when it has sent none. limitCheck says the next paced packet is
	// the first of a write that started alone (writeBeginLocked).
	// appLimitedMarks counts the writes that found the sender
	// application-limited, appLimitedResends the retransmissions an ACK or
	// a timer decided while it was (checkAppLimitedLocked), for tests.
	writers           int
	writeSent         time.Time
	limitCheck        bool
	appLimitedMarks   int
	appLimitedResends int
	// streamDeadline is the earliest read or write deadline a stream has
	// armed (Stream.SetReadDeadline, SetWriteDeadline); the timer loop wakes
	// what it holds when it matures and recomputes the next from the stream
	// table. Zero while no stream has one. A deadline removed or moved later
	// is not recomputed here: the loop fires once for nothing and moves on.
	streamDeadline time.Time

	ptoCount int
	// lostPackets, bytesRetransmitted, persistentCongestions and ptoFired
	// are the cumulative counters Stats reports: packets declared lost by
	// either detection path (an ACK's packet or time threshold, the loss
	// timer), DPLPMTUD probes excepted; the payload bytes those losses put
	// back on the send queue; persistent-congestion collapses; and probe
	// timeouts that fired. None is ever reset.
	lostPackets           uint64
	bytesRetransmitted    uint64
	persistentCongestions uint64
	ptoFired              uint64
	// maxDatagram bounds every datagram sent on the path in use: 1200 until
	// a DPLPMTUD probe proves more (pmtu.go).
	maxDatagram int
	pmtu        pmtuState
	// fragmentable says Don't Fragment could not be set on pc (dfSocket):
	// a probe on it could arrive in fragments and prove a size the path
	// does not carry whole, so no probe leaves and datagrams stay at 1200
	// (RFC 8899 §3, RFC 9000 §14.2). Per socket, like ecnRecv.
	fragmentable bool
	// pmtuBlackHoles counts the falls back to 1200 (pmtu.go), for Stats.
	pmtuBlackHoles uint64
	spin           byte // 0 or 0x20, OR-ed into every short header (§17.4)
	// blockedResend is when the §4.1 periodic *_BLOCKED re-announcement
	// next fires, zero while no writer is parked at a flow-control limit.
	blockedResend time.Time
	// hdrScratch and pktScratch are writePacketLocked's per-packet buffers,
	// reusable because mu serialises every writer and both are consumed
	// before the function returns (the header into the sealed packet, the
	// packet into WriteTo, which may not retain its argument).
	hdrScratch []byte
	pktScratch []byte
	// ackScratch holds the ACK sendPacketLocked encodes before it knows
	// the packet will leave; consumed within the call, like the two above.
	ackScratch []byte
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
	// batching, batch, batchSeg, batchN and batchShort are send batching:
	// while batching is set (Stream.write), 1-RTT datagrams bound for the
	// current path are sealed back to back into batch — every one batchSeg
	// bytes, but a shorter last that closes it (batchShort) — and leave in
	// one system call. noGSO records that the socket refused segmentation
	// offload once, so later batches go one write per datagram without
	// asking again. batchErr is the first write error of the batches
	// flushed since Stream.write began, which it returns. oobScratch is
	// the control message. See batchLocked and flushBatchLocked.
	batching   bool
	batch      []byte
	batchSeg   int
	batchN     int
	batchShort bool
	noGSO      bool
	segmented  int // batches that left in one segmented write, for tests
	batchErr   error
	oobScratch []byte
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
	// CreditOnRelease.
	manualCredit bool

	connSend sendQuota
	connRecv recvQuota
	control  []byte // small control frames waiting to ride the next packet
	// controlRetires and controlNewCIDs count the RETIRE_CONNECTION_ID and
	// NEW_CONNECTION_ID frames in control — the two control frames a peer
	// can make this end queue at will; see queueRetireLocked and
	// issueLocalCIDsLocked.
	controlRetires int
	controlNewCIDs int
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
	// retiredBidi and retiredUni count the peer's retired streams whose
	// slot has not been granted back yet; see returnStreamSlotLocked.
	retiredBidi uint64
	retiredUni  uint64
	openedBidi  uint64 // opened by this end
	openedUni   uint64
	// bidiBlockedTold and uniBlockedTold record that STREAMS_BLOCKED went
	// out for the current limit of that kind, so a parked OpenStream or
	// OpenUniStream says it once per exhaustion rather than once per
	// wake-up; a MAX_STREAMS that raises the limit clears it.
	bidiBlockedTold bool
	uniBlockedTold  bool
	seenPeerBidi    uint64 // highest count the peer has opened
	seenPeerUni     uint64

	idleTimeout        time.Duration
	idleDeadline       time.Time
	handshakeConfirmed bool
	// keepAlive is the period Conn.SetKeepAlive asked for, zero when off;
	// keepAliveDeadline is when the next PING leaves if nothing else moves
	// first. Both the receive path and every ack-eliciting send push the
	// deadline back, so a PING only ever goes out into real silence.
	keepAlive         time.Duration
	keepAliveDeadline time.Time
	// elicitingSinceRecv is true once an ack-eliciting packet has been sent
	// since the last packet was received and processed. RFC 9000 §10.1 lets a
	// send restart the idle timer only while this is false — the *first*
	// ack-eliciting packet after a receive — so an endpoint transmitting into
	// silence does not keep extending its own idle clock forever.
	elicitingSinceRecv bool
	// peerAddrValidated is the client-side answer to RFC 9002 A.8's
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

	// streams holds every stream that is open at this end, and it is the
	// whole memory of which streams closed: an identifier below what has
	// been opened (seenPeer*, opened*) that is not in the table belongs to
	// a stream that closed, however long ago. No bounded list of closed
	// identifiers stands beside it: one of 1,024 would let the 1025th
	// closure forget the first, so that a late or forged frame on that
	// stream reopened it, delivered an HTTP/3 request twice and credited
	// MAX_STREAMS twice (see streamFor).
	streams map[uint64]*Stream
	// resetIDs holds the streams this end has reset and whose STREAM frames
	// may still be in flight, by the application packet number the reset
	// left at: a lost payload replayed after the reset drops their STREAM
	// and STREAM_DATA_BLOCKED frames (RFC 9000 §3.3, pruneResetLocked). The
	// stream itself may have left streams by then — it is retired as soon
	// as its receiving side ends, an HTTP/3 cancellation's usual order — so
	// the record is this map, pruned once nothing sent before the mark is
	// in flight (pruneResetIDsLocked). Nil until the first reset.
	resetIDs map[uint64]uint64
	// acceptQ holds the streams the peer opened that AcceptStream has not
	// handed out yet, in identifier order per kind; acceptReady wakes a
	// waiting AcceptStream. MAX_STREAMS bounds it, not a capacity.
	acceptQ     []*Stream
	acceptReady chan struct{}
	closed      chan struct{}
	closeErr    error
	closeSent   bool
	draining    bool
	once        sync.Once
	timerKick   chan struct{}
	// closing is what a connection that holds the closing state keeps
	// after its own CONNECTION_CLOSE left (RFC 9000 §10.2.1), or after the
	// peer's for the draining state (§10.2.2);
	// gone is closed when that state ends — at once when there is none —
	// and is what the listener waits on before it forgets the connection.
	// See close.go.
	closing closingState
	gone    chan struct{}

	discarded uint64 // datagrams or packets dropped without processing
	// failedOpens counts the packets that failed authentication over the
	// connection's lifetime, every key and level together (RFC 9001 §6.6).
	failedOpens uint64
	// nonProductive counts the authenticated ack-eliciting packets none of
	// whose frames moved this end's state (RFC 9000 §21.9).
	nonProductive uint64

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
	// net.PacketConn exclusively — [Dial] and standalone accept — and
	// readOne/readLoop read it directly, exactly as before this existed.
	incoming chan rawDatagram

	// amplActive gates the RFC 9000 §8.1 anti-amplification limit: true only
	// while a server connection's peer address is not yet validated —
	// Listener-managed and standalone accept alike, since a first datagram
	// read off a bare socket proves its source no more than one a listener
	// demuxed. Always false for [Dial]. Guarded by mu, like everything else
	// writePacketLocked and receive touch.
	amplActive bool
	amplRecv   int64
	amplSent   int64
	// offPath is the same limit for the one address off the path that
	// this end answers: the source of the datagram in hand when it is not
	// the peer's. Nothing sent there validates it — a probing-only packet
	// does not move the path (§9.1), so no migration seeds amplRecv for
	// it — yet §8 bounds what goes to *any* unvalidated address to three
	// times what came from it, and a PATH_RESPONSE expanded to 1200 bytes
	// per PATH_CHALLENGE would otherwise reflect many times what a spoofed
	// datagram cost its sender (§21.1.1.1). One address at a time: a
	// datagram from another off-path address starts a fresh count, and
	// what the earlier one had left is forgotten, never carried over.
	offPath struct {
		addr       net.Addr
		recv, sent int64
	}

	// registrar issues this connection's additional identifiers: the
	// [Listener]'s demux table for a connection it manages, an
	// ownerRegistrar for one that owns its socket. The peer needs them to
	// follow this end across a NAT rebind (RFC 9000 §9.5).
	registrar cidRegistrar
	// sendsTicket is [ListenerConfig.SessionTickets] or EarlyData on a
	// connection the listener accepts: one session ticket leaves once the
	// handshake completes (issueTicket).
	sendsTicket bool
	// early is the listener's 0-RTT admission (ListenerConfig.EarlyData),
	// nil when 0-RTT is refused — on every client, on every server
	// without it. earlyOpener reads the client's 0-RTT packets once
	// crypto/tls installs the early secret, which it does only when the
	// resumption's 0-RTT was accepted; it is dropped at the first 1-RTT
	// packet (RFC 9001 §4.9.3). deferredEarly holds 0-RTT packets that
	// arrived before that secret. earlyAccepted is closed, and
	// earlyReleased set, once the 1-RTT write keys follow it
	// (releaseEarly).
	early         *earlyGate
	earlyOpener   *packetOpener
	deferredEarly []deferredPacket
	earlyAccepted chan struct{}
	earlyReleased bool
	// earlyTicket is the identifier of the ticket a resuming client
	// presented, reserved in the listener's register by eligible;
	// earlyTaken says installEarly confirmed the reservation (the 0-RTT
	// was accepted), after which it is never given back.
	earlyTicket []byte
	earlyTaken  bool
	// ticketChainOrigin is when the full handshake a resuming client's
	// chain of tickets started from (ticketchain.go), zero on a full
	// handshake. Set on the handshake goroutine, read by issueTicket there.
	ticketChainOrigin time.Time
	// On a client: wantsEarly is DialEarly asking for 0-RTT; remembered is
	// the ticket's parameters it is sent under, adopted as the peer's
	// (paramsRemembered) once earlySealer, the 0-RTT write keys, is in;
	// earlyRejected says the server refused it (rejectEarlyLocked).
	// peerParamsRaw is the server's parameters as they arrived, kept with
	// the tickets this connection stores.
	wantsEarly       bool
	remembered       TransportParameters
	earlySealer      *packetSealer
	paramsRemembered bool
	// earlyOffered says this client's ClientHello offers 0-RTT; earlyHRR
	// that the server answered it with a HelloRetryRequest, which the
	// connection does not survive (errEarlyHelloRetry, handlePeerClose).
	earlyOffered bool
	earlyHRR     bool
	// limitsHeld says the server's flow-control and stream limits arrived
	// while this end was still writing 0-RTT packets, so they wait for the
	// first 1-RTT one (§7.4.1, applyPeerLimitsLocked).
	limitsHeld    bool
	earlyRejected bool
	peerParamsRaw []byte
	// hsComplete is closed when the handshake completes
	// (Conn.HandshakeComplete).
	hsComplete chan struct{}
	// localCIDs are the identifiers this end currently answers to, sequence
	// 0 being scid; nextLocalSeq numbers the next one issued.
	// localRetirePrior is the Retire Prior To this end has asked for: the
	// entries below it wait on the peer's RETIRE_CONNECTION_ID and no
	// longer count as active. peerLocalSeq is the highest local sequence
	// an authenticated packet of the peer's arrived under.
	localCIDs        []localCID
	nextLocalSeq     uint64
	localRetirePrior uint64
	peerLocalSeq     uint64
	// peerCIDs pools the identifiers the peer issued for itself;
	// peerRetirePrior is the highest Retire Prior To honoured so far, and
	// currentPeerSeq says which pooled entry dcid currently is.
	peerCIDs        []peerCID
	peerRetirePrior uint64
	currentPeerSeq  uint64
	// peerRetiredSelf remembers the sequences this end retired of its own
	// accord (retirePeerCIDLocked), newest last and bounded: a NEW_
	// CONNECTION_ID retransmitted for one of them is a duplicate, not a
	// new identifier, and pooling it again pushed the pool past the
	// active_connection_id_limit this end advertised — CONNECTION_ID_LIMIT_
	// ERROR, connection closed. The peer's own Retire Prior To needs no
	// record (peerRetirePrior covers it); this is the half this end chose.
	// A ring rather than a set: §13.3 makes the peer retransmit the frame
	// within a probe timeout or two of the original, and remembering the
	// last few retirements covers that window. Past it the frame is pooled
	// again.
	peerRetiredSelf []uint64
	pathProbe       pathProbe
	migrations      uint64
	// lastValidated* is the path a server last proved — address, socket,
	// and the peer identifier sequence used on it — recorded when a
	// migration leaves it: where §9.3.2 sends a migration that never
	// validates back to. lastValidatedPeer nil means none is known.
	lastValidatedPeer net.Addr
	lastValidatedPC   net.PacketConn
	lastValidatedSeq  uint64
	// pathLocalSeq is the local identifier the path in use last received
	// under: what tells a challenge from a new address apart — the peer
	// probing a new path under a new identifier (§9.5), answered under a
	// fresh one of its own — from a NAT rebinding, which keeps the
	// identifier and may keep the answer's (§9.5's exception).
	pathLocalSeq uint64
	// answered holds the new addresses whose challenges were answered under
	// a fresh peer identifier, with that identifier, oldest first and at
	// most maxAnsweredPaths: a repeat of a challenge is answered under the
	// same one, a migration to one of them adopts it rather than taking
	// another, and two addresses probing in turn (§8.2) keep theirs.
	answered []answeredPath

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
	// rxProgress says whether a frame of the packet being processed moved
	// this end's state; frames clears it per packet, the handlers set it,
	// and a packet that ends without it is counted (RFC 9000 §21.9,
	// nonProductive). The receive goroutine's alone.
	rxProgress bool
	// writersIn counts the goroutines inside [Stream.Write] and the ones
	// waiting for mu to get there. It is taken before the lock, which is
	// the whole point: a writer already holding mu can only see that
	// another one is coming if that other one counted itself before
	// blocking. 0-RTT packets coalesce while it is above one, and the
	// writer that brings it back to zero flushes the datagram they
	// gathered (writePacketLocked).
	writersIn atomic.Int64
	// rxStaleCrypto says the packet being processed repeated Initial
	// CRYPTO bytes already taken: a client resending its ClientHello has
	// not seen this end's answer (RFC 9002 §6.2.3). earlyResends counts
	// the flights resent early for it. The receive goroutine's alone.
	rxStaleCrypto bool
	earlyResends  int
	// rxReplay says the packet being processed was held for keys that were
	// not in yet and is coming back through the receive path now that they
	// are: its datagram was already counted against §8.1's allowance when
	// it first arrived (admitSource), and counting it a second time raised
	// that allowance for free. The receive goroutine's alone.
	rxReplay bool
	// rxVia is the socket the datagram being processed arrived on, nil
	// meaning pc. Only a Listener-fed server connection ever sees another
	// one: the listener's preferred-address socket (§9.6).
	rxVia net.PacketConn
	// rxECN is the ECN codepoint of the datagram being processed, as its
	// socket reported it (§13.4.1); zero where the socket does not.
	rxECN uint8
	// ecnReceived counts every processed packet by the codepoint its
	// datagram arrived with, for Stats; the per-space counts the ACKs
	// report are the ackTrackers'.
	ecnReceived [4]uint64

	// ecn is the current path's ECN validation (§13.4.2); ecnRecv says the
	// kernel reports the field for datagrams read from pc, and dgramECN
	// and batchECN hold the mark of the datagram or batch being built, so
	// every packet in it is recorded with what it actually left with.
	ecn      ecnState
	ecnRecv  bool
	dgramECN uint8
	batchECN uint8

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
	// ecn is the codepoint of the IP header it arrived in, where the socket
	// reports it (§13.4.1).
	ecn uint8
}

// deferredPacket is one packet parked until the keys for its level arrive,
// stamped with when it actually reached this endpoint.
type deferredPacket struct {
	raw []byte
	at  time.Time
	ecn uint8 // its datagram's ECN codepoint, counted when it is finally processed
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
	// ecnSent counts this space's packets that left marked ECT(0), ACK-only
	// ones included (the peer counts them too); ecnLast is the ECT(0),
	// ECT(1) and CE counts of the last ACK that passed validation
	// (§13.4.2.1). Both are the sender's side of ECN; ack.ecn is the
	// receiver's.
	ecnSent uint64
	ecnLast [3]uint64
	// ecnLargest is the largest Largest Acknowledged of the ACK frames read
	// for ECN in this space, once ecnSawAck: the frame field §13.4.2.1's
	// MUST NOT is about, which counts ACK-only packets this end does not
	// track.
	ecnLargest uint64
	ecnSawAck  bool
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

// Datagram is one UDP datagram as read from a socket: who sent it, its
// bytes, and the ECN field of the IP header it arrived in — 0b00 Not-ECT,
// 0b01 ECT(1), 0b10 ECT(0), 0b11 CE, the codepoints [udp.PacketECN]
// reports (RFC 3168 §5). Zero is Not-ECT, the value to leave where the
// socket does not report the field.
type datagram struct {
	Peer    net.Addr
	Payload []byte
	ECN     uint8
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

func randomID(n int) ([]byte, error) {
	b := make([]byte, n)
	_, err := rand.Read(b)
	return b, err
}

var errClosed = errors.New("quic: the connection is closed")

// Err reports why the connection ended: nil while it is open and after
// this end closed it ([Conn.Close], [Conn.CloseWithError], [Conn.Shutdown]);
// otherwise the cause, one of [ErrIdleTimeout], [ErrStatelessReset],
// [ErrVersionNegotiation], a [*ConnectionError] for the peer's
// CONNECTION_CLOSE whatever its code, a [crypto/tls.AlertError] in the
// chain for a handshake this end's TLS refused, or the protocol violation
// that made this end abort. The peer's clean goodbye is a ConnectionError
// too — NO_ERROR, or HTTP/3's H3_NO_ERROR — because the transport cannot
// tell a clean code from a failure; the layer that knows the codes does.
// HTTP/3 reads the peer's clean goodbye this way:
//
//	ce, ok := errors.AsType[*quic.ConnectionError](c.Err())
//	clean := ok && ce.Application && ce.Code == 0x0100 // H3_NO_ERROR
//
// It is safe to call from any goroutine at any time.
func (c *Conn) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closeErr
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

// LocalParameters reports the transport parameters this endpoint announced.
// A layer above sizing its own queues wants the same bounds the transport
// enforces — most of all the stream limit, which is what caps how many
// requests can be admitted and unanswered at once.
func (c *Conn) LocalParameters() TransportParameters { return c.params }

// ConnectionState reports the TLS state the handshake settled: the
// negotiated application protocol (NegotiatedProtocol, what a server
// checks before it speaks h3), the server name the client asked for, the
// peer's certificates, the TLS version. It is what
// [crypto/tls.QUICConn.ConnectionState] returns; [Dial] and
// [Listener.Accept] return only once the handshake is complete, so every field is settled by
// the time a caller can ask.
func (c *Conn) ConnectionState() tls.ConnectionState { return c.tls.ConnectionState() }

// LocalAddr reports the address of the socket this connection sends on:
// the one [Dial] was given, or the listener's, until a client's
// [Conn.Rebind] replaces it or the client adopts its server's preferred
// address. A connection fed by a [Listener] on a Linux socket bound to
// 0.0.0.0 or [::] reports the address its datagrams reach, the one the
// listener answers from; elsewhere, the unspecified address the socket
// was bound to.
func (c *Conn) LocalAddr() net.Addr {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pc.LocalAddr()
}

// RemoteAddr reports the address this connection sends to: the peer as the
// handshake saw it, then wherever the peer has moved to — a server follows
// its client across a NAT rebind (RFC 9000 §9.5), a client moves to the
// preferred address its server offered (§9.6). A log line taken after a
// migration names the new address.
func (c *Conn) RemoteAddr() net.Addr {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.peer
}

// Done is closed when the connection ends.
func (c *Conn) Done() <-chan struct{} { return c.closed }
