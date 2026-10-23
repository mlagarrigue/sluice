// Where to find what in this package's source. The package documentation
// (quic.go) says what it does; this says which file does it.
//
//	conn.go          Conn: its state, Dial, key updates, the failure model
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
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mlagarrigue/sluice/internal/varint"
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
	// presented, found eligible for 0-RTT and spent by installEarly.
	earlyTicket []byte
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
	earlyRejected    bool
	peerParamsRaw    []byte
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
	// answeredFrom and answeredCID are the new address whose challenge was
	// last answered under a fresh peer identifier, and that identifier: a
	// repeat of the challenge is answered under the same one, and a
	// migration to that address adopts it rather than taking another.
	answeredFrom net.Addr
	answeredCID  []byte

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
	// rxStaleCrypto says the packet being processed repeated Initial
	// CRYPTO bytes already taken: a client resending its ClientHello has
	// not seen this end's answer (RFC 9002 §6.2.3). earlyResends counts
	// the flights resent early for it. The receive goroutine's alone.
	rxStaleCrypto bool
	earlyResends  int
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

// checkTLSConfig refuses, before anything is written, a tls.Config that
// crypto/tls or a conformant peer would refuse mid-handshake. Dial, Accept
// and NewListener all pass here, so a hand-built Config fails once, with an
// error naming the field, instead of failing every handshake: a Listener
// whose Config is refused by crypto/tls answers each client with a
// CONNECTION_CLOSE that says nothing about why.
func checkTLSConfig(cfg *tls.Config) error {
	if err := requireALPN(cfg); err != nil {
		return err
	}
	return requireTLS13(cfg)
}

// checkServerCertificates refuses a cfg whose configured chains TLS 1.3
// forbids a server to send, before any packet is answered with them: a leaf
// whose Key Usage extension lacks digitalSignature (RFC 8446 §4.4.2.2) and
// a chain member signed with SHA-1 (§4.2.3), the self-signed anchor
// excepted since no client verifies its signature. crypto/tls reads
// neither, and a conformant client refuses the chain mid-handshake, where a
// Listener can only close and the operator learns nothing of the cause.
// What GetCertificate or GetConfigForClient return is not seen here, nor is
// the client's signature_algorithms_cert, which only crypto/tls sees. A
// certificate that does not parse is left to crypto/tls: this is the
// operator's configuration, not bytes from the network, so one parser
// reporting it is enough.
func checkServerCertificates(cfg *tls.Config) error {
	for i := range cfg.Certificates {
		cert := &cfg.Certificates[i]
		for j, der := range cert.Certificate {
			c := cert.Leaf
			if j > 0 || c == nil {
				var err error
				if c, err = x509.ParseCertificate(der); err != nil {
					continue
				}
			}
			if err := checkServedCertificate(c, j == 0); err != nil {
				return err
			}
		}
	}
	return nil
}

// checkServedCertificate is checkServerCertificates's rule for one
// certificate of a chain, leaf saying whether it is the first.
func checkServedCertificate(c *x509.Certificate, leaf bool) error {
	if leaf && c.KeyUsage != 0 && c.KeyUsage&x509.KeyUsageDigitalSignature == 0 {
		return fmt.Errorf("%w: certificate %q has a Key Usage without digitalSignature (RFC 8446 §4.4.2.2)", ErrQUIC, c.Subject)
	}
	switch c.SignatureAlgorithm {
	case x509.SHA1WithRSA, x509.DSAWithSHA1, x509.ECDSAWithSHA1:
		// A self-issued member above the leaf is the anchor, whose
		// signature no client verifies (nor does crypto/x509, which
		// refuses SHA-1 outright — so issuer and subject tell, not a
		// verification).
		if leaf || !bytes.Equal(c.RawIssuer, c.RawSubject) {
			return fmt.Errorf("%w: certificate %q is signed with SHA-1 (RFC 8446 §4.2.3)", ErrQUIC, c.Subject)
		}
	}
	return nil
}

// clientTLSConfig is cfg as a client handshake uses it: the caller's own
// when it carries no session cache, otherwise a copy whose cache hands each
// ticket out once (singleUseSessionCache). The copy leaves the caller's
// Config untouched — it may be shared with other dialers, or with a TCP
// listener.
func clientTLSConfig(cfg *tls.Config) *tls.Config {
	if cfg.ClientSessionCache == nil {
		return cfg
	}
	c := cfg.Clone()
	c.ClientSessionCache = singleUseSessionCache{inner: cfg.ClientSessionCache}
	return c
}

// singleUseSessionCache hands each session out once: RFC 9001 §4.5 and RFC
// 8446 Appendix C.4 have a client not reuse a ticket, since a ticket seen
// twice on the path ties the two connections together for any observer.
// crypto/tls's own LRU cache keeps an entry across any number of Gets, so
// the entry is removed as it is taken — Put with a nil session is the
// interface's way to remove — and the ticket the server issues on the
// resumed connection puts the next one in. An inner cache that ignores the
// removal reuses tickets, which is then its caller's decision. Safe for
// concurrent use when the inner cache is, as the interface requires.
type singleUseSessionCache struct{ inner tls.ClientSessionCache }

func (c singleUseSessionCache) Get(key string) (*tls.ClientSessionState, bool) {
	cs, ok := c.inner.Get(key)
	if !ok {
		return nil, false
	}
	c.inner.Put(key, nil)
	return sessionCopy(cs), true
}

// sessionCopy is cs over a copy of its session: the connection that resumes
// from it may decline the ticket's 0-RTT (handleTLSEvent, under Dial) by
// clearing SessionState.EarlyData, and the session a cache that ignores
// removals keeps must not lose its 0-RTT to that. A session that cannot be
// read back is handed out as it is.
func sessionCopy(cs *tls.ClientSessionState) *tls.ClientSessionState {
	ticket, state, err := cs.ResumptionState()
	if err != nil || state == nil {
		return cs
	}
	own := *state
	if cp, err := tls.NewResumptionState(ticket, &own); err == nil {
		return cp
	}
	return cs
}

func (c singleUseSessionCache) Put(key string, cs *tls.ClientSessionState) { c.inner.Put(key, cs) }

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

// requireTLS13 refuses a tls.Config whose MinVersion is below TLS 1.3 — the
// zero value of a Config built by hand included. QUIC uses TLS 1.3 only
// (RFC 9001 §4.2), and crypto/tls enforces it when the handshake starts
// ([tls.QUICConn.Start]), where a Listener can only close the connection
// and the client learns nothing of the cause. The Config is refused, not
// corrected: it is the caller's, and may be shared with a TCP listener
// that still wants TLS 1.2.
func requireTLS13(cfg *tls.Config) error {
	if cfg.MinVersion < tls.VersionTLS13 {
		return fmt.Errorf("%w: tls.Config.MinVersion must be tls.VersionTLS13 — QUIC uses TLS 1.3 only "+
			"(RFC 9001 §4.2), and crypto/tls refuses a lower minimum at handshake", ErrQUIC)
	}
	return nil
}

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
// that a ticket taken out of it is removed — RFC 9001 §4.5 and RFC 8446
// Appendix C.4, since a ticket reused lets anyone on the path tie the
// connections together — and the ticket the server issues on the resumed
// connection refills it. A cache that ignores Put(key, nil) reuses its
// tickets anyway; that is the caller's choice to make, not the default.
//
// crypto/tls refuses RSA keys under 1024 bits and no more. RFC 8446
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
// digitalSignature or a chain member is signed with SHA-1 (RFC 8446
// §4.4.2.2, §4.2.3), as [NewListener] refuses it; what GetCertificate or
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
	if err := checkServerCertificates(cfg); err != nil {
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
	dfSocket(pc)
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

// endTLS stops crypto/tls's handshake goroutine. Start runs the handshake on
// a goroutine of its own, which returns when the handshake completes and
// otherwise waits for data until [tls.QUICConn.Close]: a connection
// abandoned mid-handshake — a client that sent one Initial and left, a
// Dial that timed out — kept that goroutine and the TLS state it holds for
// the life of the process. Called by the goroutine that feeds TLS (QUICConn
// is not safe for concurrent use), once nothing more will be fed; a no-op
// on a completed handshake, one never started, or a second call.
func (c *Conn) endTLS() {
	if c.tls != nil {
		_ = c.tls.Close()
	}
}

// handshake runs until TLS says it is done, carrying CRYPTO both ways. Lost
// flights are the timer loop's problem — its probe timeout retransmits them —
// so this loop only has to keep reading.
//
// ctx's deadline replaces the default one, and its cancellation closes the
// connection, which every wait below already watches for.
func (c *Conn) handshake(ctx context.Context) error {
	err := c.driveHandshake(ctx)
	if err != nil {
		c.endTLS()
	}
	return err
}

// driveHandshake is handshake's body; every error it returns has closed the
// connection.
func (c *Conn) driveHandshake(ctx context.Context) error {
	if c.isClient {
		if err := c.start(); err != nil {
			_ = c.Close()
			return err
		}
	}
	start := time.Now()
	c.mu.Lock()
	budget := DefaultHandshakeTimeout
	if c.handshakeBudget > 0 {
		budget = c.handshakeBudget
	}
	deadline := start.Add(budget)
	ctxDeadline, fromCtx := ctx.Deadline()
	if fromCtx {
		deadline = ctxDeadline
	}
	// Only a deadline this package chose may move: a caller's context says
	// what the caller wants, and a client has no address of the peer's to
	// see validated.
	c.hsStart, c.hsDeadline, c.hsExtendable = start, deadline, !fromCtx && !c.isClient
	c.mu.Unlock()
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
		deadline = c.hsDeadline // extendHandshakeLocked may have moved it
		c.mu.Unlock()
		done, err := c.pump()
		if done && err == nil && c.sendsTicket {
			err = c.issueTicket()
		}
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
			// From here the read loop, not the close, frees the socket
			// (enterClosingLocked) — unless the close already ran.
			c.mu.Lock()
			c.closing.loop = c.incoming == nil && !c.closing.left
			c.mu.Unlock()
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
		c.mu.Lock()
		deadline = c.hsDeadline
		c.mu.Unlock()
		if !got && errors.Is(err, os.ErrDeadlineExceeded) && ctx.Err() == nil && time.Now().Before(deadline) {
			// Not the deadline: a send refusal woke the read (hsWake) so
			// the refusal count at the top of the loop is re-taken now,
			// or the deadline moved while the read waited on the old one.
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

// extendHandshakeLocked moves a server's handshake deadline, once, to the
// handshake's start plus the connection's idle timeout, when that is later:
// called when the client's first Handshake packet authenticates, which
// validates its address (RFC 9000 §8.1). From then on the client is as
// proven as any established peer, and RFC 9000 §10.1 ends those by the idle
// timeout, not by a clock started at their first packet. The fixed deadline
// closed handshakes that were alive: under 30 % loss a client's Finished
// went missing, its next probe was due 12 s in, and the server had closed
// with INTERNAL_ERROR at 10 s (runner handshakeloss against ngtcp2 and
// quinn). The pending slot a client can hold grows with it, from the
// HandshakeTimeout to the idle timeout — but only for a client that
// answered from its own address with the handshake keys, which could as
// well complete and hold a connection that long. Callers hold mu.
func (c *Conn) extendHandshakeLocked() {
	if !c.hsExtendable {
		return
	}
	c.hsExtendable = false
	if c.idleTimeout == 0 {
		return
	}
	if d := c.hsStart.Add(c.idleTimeout); d.After(c.hsDeadline) {
		c.hsDeadline = d
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
// finished: the recorded cause — the peer's close is one, whatever its
// code, so a refused ALPN reaches the dialer as the [*ConnectionError]
// carrying 0x0178 — else this end's own Close, which is still a handshake
// that did not happen.
func (c *Conn) endedDuringHandshakeErr() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closeErr != nil {
		return c.closeErr
	}
	return fmt.Errorf("%w: the connection closed during the handshake", ErrQUIC)
}

// issueTicket has crypto/tls write the connection's one session ticket —
// marked for 0-RTT, with what earlyGate needs to admit it, when the
// listener has EarlyData — and sends it, in the application space where RFC 9001 §4.5 puts it, beside
// the HANDSHAKE_DONE handshakeComplete adds — gathered into one datagram.
// Called once the handshake is complete, on a server: before then
// crypto/tls refuses, and on a client it has no ticket to send.
func (c *Conn) issueTicket() error {
	opts := tls.QUICSessionTicketOptions{}
	if c.early != nil {
		opts.EarlyData = true
		opts.Extra = [][]byte{c.early.ticketExtra(time.Now())}
	}
	if err := c.tls.SendSessionTicket(opts); err != nil {
		return err
	}
	_, err := c.pump()
	return err
}

// installEarly installs the 0-RTT read keys of a server whose resumption's
// 0-RTT was accepted. Nothing is read with them yet: releaseEarly does that
// once this end can also write in 1-RTT.
func (c *Conn) installEarly(suiteID uint16, secret []byte) error {
	s, err := suiteFor(suiteID)
	if err != nil {
		return err
	}
	opener, err := newOpenerSuite(secret, s)
	if err != nil {
		return err
	}
	if c.early == nil || !c.early.take(c.earlyTicket, time.Now()) {
		// Another connection took the ticket since it was found eligible
		// — a replay racing the original. crypto/tls has accepted the
		// 0-RTT already; its packets are never read, which the client
		// sees as loss and resends in 1-RTT, after the handshake.
		return nil
	}
	c.mu.Lock()
	c.earlyOpener = opener
	c.mu.Unlock()
	return nil
}

// releaseEarly runs on a server whose 0-RTT was accepted, once its 1-RTT
// write keys are in: crypto/tls hands them out in the same flight as the
// early secret, and nothing — an ACK for a 0-RTT packet, the application's
// first write — can leave in the application space before them. It reads
// the 0-RTT packets that arrived before the early secret and tells the
// listener the connection may be accepted now.
func (c *Conn) releaseEarly() error {
	c.mu.Lock()
	if c.earlyOpener == nil || c.earlyReleased {
		c.mu.Unlock()
		return nil
	}
	c.earlyReleased = true
	held := c.deferredEarly
	c.deferredEarly = nil
	peer, rxECN := c.peer, c.rxECN
	c.mu.Unlock()
	close(c.earlyAccepted)
	for _, pkt := range held {
		c.mu.Lock()
		c.rxECN = pkt.ecn
		c.mu.Unlock()
		if err := c.receiveAt(pkt.raw, peer, pkt.at); err != nil {
			return err
		}
	}
	c.mu.Lock()
	c.rxECN = rxECN
	c.mu.Unlock()
	return nil
}

// HandshakeComplete returns a channel closed when the handshake completes.
// A connection from [Dial] has completed it already, and so has one from
// [Listener.Accept] unless the listener has [ListenerConfig.EarlyData]:
// that one may not have, and what it received until then may be a replay of
// another client's first flight. A connection whose handshake fails never closes the channel:
// select on [Conn.Done] beside it.
func (c *Conn) HandshakeComplete() <-chan struct{} { return c.hsComplete }

// handshakeComplete is the transition TLS reports: the server confirms at
// once and tells the client with HANDSHAKE_DONE; the client confirms when
// that frame arrives (§4.1.2 of RFC 9001). Handshake keys are dropped on
// confirmation (§4.9.2). Called under mu.
func (c *Conn) handshakeComplete() {
	close(c.hsComplete)
	if !c.isClient {
		c.control = varint.Append(c.control, frameHandshakeDone)
		c.confirmLocked()
		c.flushControlLocked()
	}
}

// confirmLocked marks the handshake confirmed and discards handshake keys.
func (c *Conn) confirmLocked() {
	if c.handshakeConfirmed {
		return
	}
	c.rxProgress = true
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
	// DPLPMTUD's BASE state (RFC 9000 §14.3.1): the path may now be probed
	// for more than 1200 bytes.
	c.maybeProbePMTULocked()
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
	c.cc.onLifted(inFlight)
	c.deferred[space] = nil
	sp.ack = ackTracker{}
	// RFC 9002 A.11: the backoff was about this space's unanswered
	// probes, and the handshake just moved past it.
	c.ptoCount = 0
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
	case tls.QUICSetReadSecret, tls.QUICSetWriteSecret:
		if e.Level == tls.QUICEncryptionLevelEarly {
			// The 0-RTT secret, never filed under the application space
			// (spaceFor would, and it would seal 1-RTT packets with a key
			// the peer holds only for 0-RTT). On a server, crypto/tls hands
			// out the read secret only for a resumption whose 0-RTT it
			// accepted, which the QUICResumeSession case below allows only
			// when the listener admits the ticket: the client's 0-RTT
			// packets are read with it (installEarly). On a client, the
			// write secret comes only when DialEarly offered 0-RTT — the
			// same case declines it for Dial — and seals what is sent
			// before the 1-RTT keys (installEarlySealer).
			switch {
			case !c.isClient && e.Kind == tls.QUICSetReadSecret:
				return false, c.installEarly(e.Suite, e.Data)
			case c.isClient && e.Kind == tls.QUICSetWriteSecret && c.wantsEarly:
				return false, c.installEarlySealer(e.Suite, e.Data)
			}
			return false, nil
		}
		if err := c.install(e.Level, e.Suite, e.Data, e.Kind == tls.QUICSetWriteSecret); err != nil {
			return false, err
		}
		if e.Level == tls.QUICEncryptionLevelApplication && e.Kind == tls.QUICSetWriteSecret {
			if !c.isClient {
				return false, c.releaseEarly()
			}
			// RFC 9001 §4.9.3: a client SHOULD discard its 0-RTT keys as
			// soon as it installs 1-RTT keys; from here everything leaves
			// in 1-RTT, retransmissions of 0-RTT data included.
			c.mu.Lock()
			c.earlySealer = nil
			c.cond.Broadcast() // a writer waiting since a refused 0-RTT (awaitSealLocked)
			c.mu.Unlock()
		}
		return false, nil
	case tls.QUICResumeSession:
		if !c.isClient {
			// A client resumes. Its 0-RTT stands only if this listener
			// admits the ticket (ADR 0019); otherwise crypto/tls answers
			// without early_data, and no 0-RTT packet is read (RFC 9001
			// §4.6.2).
			// The ticket is only checked here, and spent in installEarly,
			// once the client has offered 0-RTT and crypto/tls accepted it.
			if e.SessionState.EarlyData {
				var id []byte
				ok := false
				if c.early != nil {
					id, ok = c.early.eligible(e.SessionState.Extra, time.Now())
				}
				if !ok {
					e.SessionState.EarlyData = false
				}
				c.earlyTicket = append([]byte(nil), id...)
			}
			return false, nil
		}
		// This client resumes from a ticket. It offers 0-RTT only under
		// DialEarly, and only with the server's parameters remembered
		// beside the ticket: they are what 0-RTT must be sent under (RFC
		// 9000 §7.4.1). Dial offers none, so its ticket costs the server
		// no 0-RTT admission.
		if e.SessionState.EarlyData {
			p, ok := rememberedParams(e.SessionState.Extra)
			if !c.wantsEarly || !ok {
				e.SessionState.EarlyData = false
			} else {
				c.mu.Lock()
				c.remembered = p
				c.mu.Unlock()
			}
		}
		return false, nil
	case tls.QUICStoreSession:
		// A ticket from the server, stored with the parameters this
		// connection's handshake brought, for a later DialEarly to send
		// 0-RTT under. Session events being on, crypto/tls stores nothing
		// itself. A ticket that fails to store is one resumption less, not
		// a reason to end this connection.
		if raw := c.peerParamsRaw; raw != nil {
			e.SessionState.Extra = append(e.SessionState.Extra, rememberedEntry(raw))
		}
		_ = c.tls.StoreSession(e.SessionState)
		return false, nil
	case tls.QUICRejectedEarlyData:
		// The server refused 0-RTT (§4.6.2). Under DialEarly, what went
		// out in it is undone (rejectEarlyLocked); otherwise nothing was
		// sent under the early secret, and the rejection is a non-event,
		// not one of the kinds UnhandledTLSEvents counts.
		c.mu.Lock()
		if c.wantsEarly {
			c.rejectEarlyLocked()
		}
		c.mu.Unlock()
		return false, nil
	case tls.QUICWriteData:
		return false, c.sendCrypto(e.Level, e.Data)
	case tls.QUICTransportParameters:
		if c.isClient {
			c.peerParamsRaw = append([]byte(nil), e.Data...)
		}
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
// idleDuration converts an announced max_idle_timeout (max_ack_delay uses
// it too) to a Duration, saturating. The field is a varint of up to 2^62-1
// milliseconds, far past what a Duration holds, and a wrapped product turns
// a peer's "practically never" into a deadline already passed. The
// saturation point is a whole number of milliseconds: appendParameters
// re-encodes the Duration in milliseconds, and a value that was not one
// (math.MaxInt64 is not) would parse back different — the drift
// FuzzParseParameters caught.
func idleDuration(ms uint64) time.Duration {
	const most = uint64(math.MaxInt64 / int64(time.Millisecond))
	if ms > most {
		ms = most
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

	// Every refusal below is TRANSPORT_PARAMETER_ERROR (parameterError says
	// which section names it for which check).
	if !p.hasInitialSourceCID {
		// §7.3: mandatory for both endpoints. Checked apart from the
		// comparison below: against a peer whose identifier is zero-length
		// an absent parameter compared equal to the empty identifier.
		return parameterError(fmt.Errorf("%w: the peer omitted initial_source_connection_id", ErrQUIC))
	}
	if !bytes.Equal(p.initialSourceCID, c.dcid) {
		return parameterError(fmt.Errorf("%w: the peer's initial_source_connection_id is not the identifier its packets carry", ErrQUIC))
	}
	switch {
	case c.isClient:
		if !bytes.Equal(p.originalDCID, c.initialDCID) {
			return parameterError(fmt.Errorf("%w: the server did not echo the original destination identifier", ErrQUIC))
		}
		// retry_source_connection_id binds this handshake to the Retry that
		// redirected it (RFC 9000 §7.3): present iff a Retry actually
		// happened, and then exactly the identifier that Retry carried —
		// kept in retrySCID, because c.dcid has usually already moved on to
		// the server's own identifier by the time the parameters arrive.
		if c.retried {
			if !bytes.Equal(p.retrySourceCID, c.retrySCID) {
				return parameterError(fmt.Errorf("%w: the server did not echo the Retry's source identifier", ErrQUIC))
			}
		} else if p.retrySourceCID != nil {
			return parameterError(fmt.Errorf("%w: retry_source_connection_id without a Retry", ErrQUIC))
		}
	// §18.2: the four server-only announcements a client must not send.
	case p.originalDCID != nil:
		return parameterError(fmt.Errorf("%w: a client sent original_destination_connection_id", ErrQUIC))
	case p.retrySourceCID != nil:
		return parameterError(fmt.Errorf("%w: a client sent retry_source_connection_id", ErrQUIC))
	case p.sawStatelessResetToken:
		return parameterError(fmt.Errorf("%w: a client sent stateless_reset_token", ErrQUIC))
	case p.sawPreferredAddress:
		return parameterError(fmt.Errorf("%w: a client sent preferred_address", ErrQUIC))
	}
	// §18.2's own bounds: above 20 and 2^14 or more are "invalid"
	// (max_udp_payload_size below 1200 is refused by the parser, which is
	// where its default is filled in too).
	if p.AckDelayExponent > 20 {
		return parameterError(fmt.Errorf("%w: ack_delay_exponent %d is over 20", ErrQUIC, p.AckDelayExponent))
	}
	if p.MaxAckDelay >= 1<<14*time.Millisecond {
		return parameterError(fmt.Errorf("%w: max_ack_delay %v is over the protocol maximum", ErrQUIC, p.MaxAckDelay))
	}

	c.peerParams = p
	c.hasPeerParams = true
	c.paramsRemembered = false
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
		// §18.2: a server that chose a zero-length identifier for itself
		// cannot offer a preferred address — nothing in its packets would
		// tell the connection apart on the new path — and a client treats
		// the offer as TRANSPORT_PARAMETER_ERROR. Refused before anything
		// of the offer is kept.
		if len(c.peerSCID) == 0 {
			return parameterError(fmt.Errorf("%w: preferred_address from a server using a zero-length connection ID", ErrQUIC))
		}
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
	// c.maxDatagram does not follow p.MaxUDPPayloadSize: the peer announces
	// what it reads, not what the *path* carries, and a datagram the path
	// swallows would be retransmitted at the same size into the same black
	// hole until the connection stalled. It bounds the search instead:
	// once the handshake is confirmed the path is probed up to
	// min(MaxDatagramSize, p.MaxUDPPayloadSize) (pmtu.go), and maxDatagram
	// rises only on a probe's acknowledgement.
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
	rxECN := c.rxECN
	c.mu.Unlock()
	// The keys just arrived, so whatever was waiting on them can be read
	// now — stamped with when each packet actually arrived, not with now,
	// and with the ECN mark of its own datagram, not the current one's.
	// Unlocked first: opening a packet takes the lock again.
	for _, pkt := range held {
		c.mu.Lock()
		c.rxECN = pkt.ecn
		c.mu.Unlock()
		if err := c.receiveAt(pkt.raw, peer, pkt.at); err != nil {
			c.mu.Lock()
			return err
		}
	}
	c.mu.Lock()
	c.rxECN = rxECN
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

// Stats is a snapshot of the counters a connection keeps about what it
// could not process, for the dashboard rather than the data path.
type Stats struct {
	// DiscardedPackets is how many incoming datagrams or packets were dropped
	// without being processed: wrong sender, failed parse, failed
	// authentication — and transient socket read errors, which on Windows
	// include the unauthenticated ICMP port-unreachable. A rising count under
	// load is an attacker probing, or a path corrupting datagrams.
	DiscardedPackets uint64

	// NonProductivePackets is how many authenticated, ack-eliciting packets
	// carried no frame that moved this end's state: PING alone, a
	// PATH_CHALLENGE, a MAX_DATA that raised nothing, bytes already
	// received, a *_BLOCKED frame. RFC 9000 §21.9 asks an endpoint to track
	// the cost of processing against progress and to read an excess of
	// such packets as an attack; this is the tracking, and the reading is
	// the operator's. Nothing is closed or dropped on it: each such packet
	// costs this end what any packet costs — one AEAD open and a parse —
	// and closing the connection would spare that only for packets still
	// sent to a dead identifier, while the frames whose effect costs more
	// than their bytes have bounds of their own. A rate that climbs while
	// nothing else moves is the thing to look at. A limit, if a measurement
	// ever shows such a flood costing more than the Initial flood a
	// Listener already absorbs, would be a named Parameters field.
	NonProductivePackets uint64

	// ECT0Received, ECT1Received and CEReceived count the packets processed
	// whose IP header carried that ECN codepoint (RFC 9000 §13.4.1), the
	// counts this end reports back in its ACK frames. All zero where the
	// socket does not report the field (every platform but Linux today), or
	// when the peer does not mark. A rising CEReceived is the network
	// signalling congestion without dropping.
	ECT0Received uint64
	ECT1Received uint64
	CEReceived   uint64

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

	// SmoothedRTT is the round-trip estimate loss recovery runs on
	// (RFC 9002 §5.3, smoothed_rtt): an exponentially weighted mean of the
	// samples, moved by every ACK that newly covers the largest packet it
	// names, the peer's declared ACK delay subtracted. Zero before the first
	// sample, which the handshake supplies. It is what a probe timeout and
	// the loss time threshold are scaled by.
	SmoothedRTT time.Duration

	// MinRTT is the smallest round trip sampled (§5.2, min_rtt): the path's
	// propagation delay, as close as this end can know it. It only ever
	// falls, except across a migration, which starts it over.
	MinRTT time.Duration

	// RTTVariance is the mean deviation of the samples from SmoothedRTT
	// (§5.3, rttvar). A variance that climbs while the mean holds is a
	// path whose queue is filling and emptying.
	RTTVariance time.Duration

	// LatestRTT is the last round trip sampled (§5.1, latest_rtt), before
	// the peer's declared ACK delay is subtracted: the path as it was one
	// acknowledgement ago, where SmoothedRTT is the path as it has been. A
	// LatestRTT that sits well above MinRTT on a path that loses nothing
	// is a queue building somewhere in front of this sender — the signal
	// a delay-based controller reads, and the one a loss-based one does
	// not.
	LatestRTT time.Duration

	// CongestionWindow is how many bytes the congestion controller lets
	// this end have in flight (RFC 9002 §7): it grows on every ACK, halves
	// on a loss epoch and collapses on persistent congestion (§7.6).
	// A window that stays near 2 × 1200 on a connection that keeps writing
	// is a path losing packets faster than it recovers.
	CongestionWindow int

	// Algorithm is the congestion controller this end sends with
	// ([TransportParameters.Congestion]), as built: an unknown choice reads
	// back as the default it fell to.
	Algorithm CongestionAlgorithm

	// BytesInFlight is how many bytes are sent and neither acknowledged nor
	// declared lost (§B.2). It rises on every ack-eliciting send, falls
	// when an ACK arrives or a loss is declared, and reads zero once
	// everything written has been acknowledged. BytesInFlight approaching
	// CongestionWindow is a sender limited by the path, not by its data.
	BytesInFlight int

	// LostPackets is how many packets loss recovery declared lost (§6.1),
	// by either path that can: an ACK proving a packet three behind or too
	// old, or the loss timer maturing on one left too young to call. A
	// DPLPMTUD probe that goes missing is not counted: its size was the
	// question, and its loss is no congestion event (RFC 9000 §14.4). A
	// retransmission is not a second loss until it is itself declared.
	LostPackets uint64

	// BytesRetransmitted is how many bytes of frames those losses put back
	// on the send queue — payload, not packet size: the headers, the
	// ACK and PADDING frames of a lost packet are not sent again (§6.1,
	// RFC 9000 §13.3) — so it never exceeds LostPackets times the largest
	// datagram the path carried. It counts at the moment of queuing: a
	// byte queued as the connection closes, or in the Initial or Handshake
	// space after its keys are discarded (RFC 9001 §4.9), is counted and
	// never sent. On a path that reorders but loses nothing, every byte
	// here was queued for no reason: the cost of a spurious loss, which
	// LostPackets counts the same as a real one.
	BytesRetransmitted uint64

	// PersistentCongestions is how many times an acknowledgement proved
	// that everything sent over more than three probe intervals was lost
	// (§7.6) and the window collapsed to its minimum. A blackout, a
	// migration to a dead path, or a peer that went away mid-transfer; a
	// count that moves on a path that merely loses packets is a detector
	// firing where the RFC's conditions were not met.
	PersistentCongestions uint64

	// ProbeTimeouts is how many times the probe timeout fired (§6.2):
	// acknowledgements stopped arriving for a whole timeout, and this end
	// sent probes to find out whether anything still gets through. It
	// counts expiries, not probes sent: a pass that probes two packet
	// number spaces together is one. A count that moves on a quiet path is
	// a peer that stalls, or ACKs that do not come back.
	ProbeTimeouts uint64

	// DeliveryRate is how fast the network delivered this end's packets, in
	// bytes per second, as the last reliable sample measured it
	// (draft-ietf-ccwg-bbr-06 §4.1): the bytes an
	// acknowledgement proved delivered, over the longer of the time they
	// took to send and the time they took to be acknowledged. Samples taken
	// while the application had nothing to send, or while BBR's ProbeRTT
	// held the flight down on purpose, are skipped — they measure the
	// sender, not the path — and so are intervals shorter than MinRTT. Zero until the first such sample. It is what the path carried,
	// not what CongestionWindow allowed: over a bottleneck it reads the line
	// rate while the window covers the bandwidth-delay product, and the
	// window per round trip once a loss has cut the window below it.
	DeliveryRate float64

	// DatagramSize is the largest datagram this end sends on the current
	// path: 1200 until a DPLPMTUD probe proves more (RFC 9000 §14.3),
	// then the largest size proven, up to
	// [TransportParameters.MaxDatagramSize]. It starts over at 1200 on
	// every new path.
	DatagramSize int

	// MTUBlackHoles is how many times full-size datagrams stopped getting
	// through after the path had carried them (RFC 8899 §4.3) — three
	// probe timeouts in a row, or three of them lost while only smaller
	// packets were acknowledged — and the datagram size fell back to 1200
	// while the search ran again. A route change or a tunnel set up under
	// the connection; an outage of three probe timeouts counts too.
	MTUBlackHoles uint64
}

// Stats reports the connection's counters, consistently with each other.
func (c *Conn) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Stats{
		DiscardedPackets:      c.discarded,
		NonProductivePackets:  c.nonProductive,
		ECT0Received:          c.ecnReceived[ecnECT0],
		ECT1Received:          c.ecnReceived[ecnECT1],
		CEReceived:            c.ecnReceived[ecnCE],
		KeyUpdates:            c.ku.updates,
		WriteFailures:         c.writeFailures,
		UnhandledTLSEvents:    c.unhandledTLSEvents.Load(),
		SmoothedRTT:           c.rtt.smoothed,
		MinRTT:                c.rtt.min,
		RTTVariance:           c.rtt.variance,
		LatestRTT:             c.rtt.latest,
		CongestionWindow:      c.cc.window(),
		Algorithm:             c.params.Congestion.Algorithm,
		BytesInFlight:         c.cc.bytesInFlight(),
		LostPackets:           c.lostPackets,
		BytesRetransmitted:    c.bytesRetransmitted,
		PersistentCongestions: c.persistentCongestions,
		ProbeTimeouts:         c.ptoFired,
		DeliveryRate:          c.delivery.rate,
		DatagramSize:          c.maxDatagram,
		MTUBlackHoles:         c.pmtuBlackHoles,
	}
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
