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

	"github.com/mlagarrigue/sluice/internal/ipkey"
	"github.com/mlagarrigue/sluice/internal/varint"
	"github.com/mlagarrigue/sluice/net/quic/udp"
)

// listenerCIDLen is the length every connection identifier a [Listener]
// issues gets — the same 8 bytes [Dial] and standalone accept have always
// used (randomID(8), every call site), so a short-header packet's DCID can
// be peeled off with [parseShortHeader] without first knowing which
// connection it belongs to.
// RFC 9000 §8.1 lets an endpoint treat a peer address as validated once
// the peer uses a connection ID this end chose with 64 bits of entropy, as
// these are; not done: validation waits for a Handshake packet or a Retry
// token, one round trip later at most. The earlier lift would only matter
// to a 0-RTT response (served on request, ADR 0019): that response stays
// within three times what the client sent until the round trip that
// validates it, the cost of one round trip on a large answer.
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
	// queue, and an application that stops accepting must push back on
	// new handshakes rather than let peers park unbounded connections on
	// it. Beyond the bound, a new Initial is refused (CONNECTION_REFUSED)
	// — the client retries, the same as it must under any transient
	// overload. Zero means 1024; a negative value is refused by
	// [NewListener]. Accepted connections are not counted here: MaxConns
	// bounds the total.
	MaxPendingConns int

	// HandshakeTimeout bounds how long one handshake may hold a pending
	// slot: a connection that has not completed its handshake by then is
	// closed and its slot given back. The slots are what MaxPendingConns
	// counts, so this is the other half of that bound — the first says how
	// many peers may be mid-handshake at once, this one says for how long,
	// and together they cap what a slow or stalled peer can occupy. Lower
	// it under load; a few seconds is plenty on any path a handshake can
	// succeed on. Zero means [DefaultHandshakeTimeout], the client's
	// figure; a negative value is refused by [NewListener]. Once the
	// client's first Handshake packet authenticates, which validates its
	// address, the bound becomes the handshake's start plus the
	// connection's idle timeout when that is later: a proven client whose
	// Finished was lost is given the time an idle established connection
	// would have.
	HandshakeTimeout time.Duration

	// MaxConns bounds every connection the listener carries — mid-handshake,
	// parked for [Listener.Accept], or accepted and in use — where
	// MaxPendingConns bounds only the first two. Beyond it a new Initial is
	// refused with a stateless CONNECTION_REFUSED (RFC 9000 §5.2.2), the
	// same answer as a full pending table; a connection counts until its
	// closing or draining state ends, three probe timeouts after the close
	// and ten seconds at most (RFC 9000 §10.2). Zero means no bound:
	// unlike MaxPendingConns, a total has no figure that suits every
	// deployment, so an unset one limits nothing. Negative is refused by
	// [NewListener].
	MaxConns int

	// MaxConnsPerIP is the same bound applied per source address, so that
	// one peer cannot hold the whole MaxConns on its own. The port is
	// ignored and an address is counted by its prefix, IPv4PrefixBits or
	// IPv6PrefixBits long; an IPv4-mapped IPv6 address counts as IPv4. The
	// address is the one the first Initial came from: a peer that later
	// migrates to another address keeps its count on the first, since the
	// bound is on admission. Zero means no bound: a figure that is fair
	// to a home connection starves a carrier-grade NAT, so the operator
	// chooses. Negative is refused by [NewListener].
	MaxConnsPerIP int

	// IPv4PrefixBits and IPv6PrefixBits are how much of a source address
	// MaxConnsPerIP counts by: two addresses that share that many leading
	// bits share one count. Zero means 32 and 64. An IPv6 host is handed a
	// whole /64 (RFC 6177), so a count by the full 128 bits is one it steps
	// around by changing address; an IPv4 host rarely holds more than one.
	// A value outside 1..32 or 1..128 is refused by [NewListener].
	IPv4PrefixBits int
	IPv6PrefixBits int

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

	// PreferredIPv4 and PreferredIPv6 name the addresses announced for
	// PreferredAddress's port when that socket is bound to the unspecified
	// address — a dual-stack one on "[::]", typically: each a concrete
	// address of its family this host answers at. The parameter carries one
	// address of each family (RFC 9000 §18.2), and a client only moves to
	// the one of the family its connection already uses (§9.6.1) — a server
	// reached over IPv6 that announces IPv4 alone is never moved to. Both
	// zero, the socket's own bound address is announced, which must then be
	// concrete. With a socket bound to a concrete address, the only address
	// allowed here is that one. An IPv4-mapped IPv6 address counts as IPv4;
	// either set without PreferredAddress is refused by [NewListener].
	PreferredIPv4 netip.Addr
	PreferredIPv6 netip.Addr

	// ConnQueueLen is how many datagrams the listener queues for one
	// connection before it drops them. A 64 KiB response draws back a
	// burst: the client's ACKs and its next request arrive while the
	// connection's read loop is still busy. At 8, HTTP/3 bulk lost 55
	// datagrams in 1,200 requests, and each loss stalled its request for a
	// client PTO (~27 ms, 45% of the send-to-receive time); at 32 none were
	// lost. Zero means 64, twice that margin. The channel itself is about
	// 3.5 KB per connection, but each queued datagram is a copy: a
	// connection whose reader is stalled holds up to ConnQueueLen ×
	// max(MaxUDPPayloadSize, 2048) bytes, 128 KiB at the defaults —
	// the figure to multiply by MaxConns when sizing memory. A negative
	// value is refused by [NewListener].
	ConnQueueLen int

	// RetryTokenTTL is how long a Retry token stays acceptable: generous
	// for one round trip, short enough that a captured token is useless
	// shortly after. A token presented later is refused with INVALID_TOKEN
	// (RFC 9000 §8.1.2). Zero means 10 seconds; a negative value is refused
	// by [NewListener].
	RetryTokenTTL time.Duration

	// MaxSpentRetryTokens bounds the record of Retry tokens that already
	// admitted a connection, which keeps a replayed token from opening a
	// second one (RFC 9000 §8.1.4). An entry lives RetryTokenTTL and costs
	// about 125 bytes — 2.04 MB measured at the default of 16,384; past the
	// bound the oldest is forgotten, and a replay of it becomes possible
	// again, which §8.1.4 allows as "limited". Raise it with the rate of
	// Retry-born connections times RetryTokenTTL. Zero means 16,384; a
	// negative value is refused by [NewListener].
	MaxSpentRetryTokens int

	// StatelessResetsPerSecond bounds the stateless resets the listener
	// sends, each an answer to a short-header datagram no connection here
	// owns — the way a client still holding a connection this listener
	// forgot (a restart) learns it is gone, instead of waiting out its idle
	// timeout. Every reset answers unauthenticated bytes, so unbounded the
	// listener would generate traffic toward whatever source address a
	// sender claims; RFC 9000 §10.3.3 has an endpoint stop once a limit is
	// reached. A server restarting in front of thousands of clients tells
	// them at this rate. Zero means 16; a negative value is refused by
	// [NewListener].
	StatelessResetsPerSecond int

	// StatelessResetsPerAddress is one source address's share of that
	// budget, the separate limit §10.3.3 recommends so that one sender
	// spraying random identifiers cannot spend it all. The address is the
	// IP alone, a port being free for a sender to vary, so every client
	// behind one NAT shares it. Zero means 4; a negative value is refused
	// by [NewListener].
	StatelessResetsPerAddress int

	// StatelessResetKey derives the stateless reset token of every
	// connection identifier the listener issues (RFC 9000 §10.3.2:
	// HMAC-SHA256 of the identifier, truncated to 16 bytes). Zero draws a
	// random key per listener, and a restart then forgets it: a client of
	// the previous process that sends again is answered with a reset it
	// cannot recognise, and waits out its idle timeout instead of closing
	// at once. A key kept across restarts, or shared by every instance
	// behind one address, makes that reset recognised — which is the case
	// stateless reset exists for.
	//
	// Sharing the key has a condition (RFC 9000 §21.11): the instances
	// MUST be arranged so that every packet of a connection identifier
	// reaches the instance that holds the connection, as long as it is
	// active — routing by connection identifier, not by address and port.
	// A listener answers any short-header datagram whose identifier it
	// does not know with a reset under this key, and asks nothing about
	// where the connection might live. Behind a load balancer that routes
	// by the four-tuple, a client whose address changed — a NAT rebinding,
	// a migration — lands on another instance, which resets with the real
	// token a connection still alive on the first; an attacker who can
	// steer that routing ends connections at will. There, give each
	// instance its own key: its resets then stand only for the connections
	// it held itself, which is all a restart needs. Keep it secret: anyone
	// holding it can end any connection of the listeners that share it.
	StatelessResetKey [32]byte

	// TokenKey authenticates Retry tokens (HMAC-SHA256, RFC 9000 §8.1.4).
	// Zero draws a random key per listener, so a token issued before a
	// restart, or by another instance, fails to verify and the client is
	// treated as unvalidated, not refused (§8.1.3). Shared between
	// instances, a token one issued admits a client at another; the record
	// of spent tokens (MaxSpentRetryTokens) stays per listener, so a token
	// replayed at a second instance within RetryTokenTTL opens a second
	// connection — the TTL is then the whole bound. Keep it distinct from
	// StatelessResetKey and secret: anyone holding it can mint tokens that
	// pass address validation.
	TokenKey [32]byte

	// SessionTickets makes every connection send the client one session
	// ticket once its handshake completes (RFC 9846 §4.7.1, carried in a
	// 1-RTT CRYPTO frame as RFC 9001 §4.5 says). A client that presents it
	// on a later connection resumes the session: the server sends no
	// certificate chain and the client verifies none, though the handshake
	// still takes its round trip — no data travels in 0-RTT, and the
	// ticket does not offer it. Off by default, no ticket is sent, which
	// keeps the session state a ticket carries off the wire.
	//
	// The ticket is sealed with the [tls.Config]'s session ticket keys:
	// random per Config unless set ([tls.Config.SetSessionTicketKeys]), so
	// a restart, or another instance, cannot open the tickets of this one
	// and the client simply falls back to a full handshake. A Config with
	// SessionTicketsDisabled is refused by [NewListener] alongside this
	// field: crypto/tls would send nothing, silently. One ticket per
	// connection is all crypto/tls sends, and the client, which presents a
	// ticket once (see [Dial]), gets the next one on the resumed connection
	// — for seven days from the full handshake the chain started from:
	// past that, the resumed connection issues no ticket and the client's
	// next connection checks the certificate again (RFC 9846 §4.7.1
	// recommends a bound on keying material tickets keep extending).
	SessionTickets bool

	// EarlyData lets clients send their first requests in 0-RTT packets on
	// a resumed connection, which saves them a round trip (RFC 9001 §4.6;
	// the whole design is ADR 0019). It issues tickets as SessionTickets
	// does, marked for 0-RTT, and a Config with SessionTicketsDisabled is
	// refused alongside it by [NewListener]. Off by default.
	//
	// Early data can be a replay: anyone who copies a client's first
	// flight can send it again, and the server cannot tell until the
	// client's Finished arrives. The listener accepts the 0-RTT of a
	// ticket at most once (RFC 9846 §8), and only for a ticket it issued
	// itself — another instance, or this one after a restart, resumes the
	// connection in 1-RTT — less than EarlyDataWindow ago. What remains is
	// the application's to judge: a connection with 0-RTT accepted is
	// handed to [Listener.Accept] before its handshake ends, and whatever
	// it receives before [Conn.HandshakeComplete] is closed may be a
	// replay whose handshake never completes.
	//
	// 0-RTT is sent under the parameters the client remembers from the
	// connection that gave it the ticket (RFC 9000 §7.4.1), which are the
	// listener's: [NewListener] refuses EarlyData with parameters that let
	// a client send no stream byte in 0-RTT — InitialMaxData at zero, or no
	// stream it may open with a window — since the ticket would then
	// promise a round trip saved that no request could use.
	EarlyData bool

	// EarlyDataWindow is how long after its issue a ticket may still carry
	// 0-RTT; an older one resumes in 1-RTT. Zero means 10 minutes; a
	// negative value is refused by [NewListener].
	EarlyDataWindow time.Duration

	// MaxEarlyDataTickets bounds the record of tickets whose 0-RTT was
	// accepted, each kept one EarlyDataWindow so that it is accepted once.
	// Full, the record refuses 0-RTT rather than forget an entry: the
	// refusal is made in the TLS handshake, before any 0-RTT packet is
	// read, the connection resumes in 1-RTT, and the guarantee holds
	// under any load. A ticket is entered when the resumption is checked
	// and given back if its 0-RTT is not accepted, so a resumption without
	// 0-RTT and a failed handshake hold no entry past their own end.
	// Size it as the rate of 0-RTT connections times the window; an entry
	// is a 16-byte identifier and a time, in a map and a slice. Zero means
	// 16,384; a negative value is refused by [NewListener].
	MaxEarlyDataTickets int
}

// Listener owns one [net.PacketConn] shared by the connections it serves, and
// is how this package serves at all. It demultiplexes
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
	// loop and dispatched into the same demux table; preferredV4 and
	// preferredV6 are what each handshake announces for it, one per
	// family, nil for a family not announced.
	preferred   net.PacketConn
	preferredV4 *net.UDPAddr
	preferredV6 *net.UDPAddr
	cfg         *tls.Config
	params      TransportParameters
	// lcfg is the resolved config: every default filled in, the two keys
	// included (StatelessResetKey for resetTokenFor, TokenKey for Retry).
	lcfg ListenerConfig

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
	// count) — the subset the pending table's limit bounds. An entry here
	// is always also in byDCID; the reverse is not true once a connection
	// is accepted.
	pendingConns map[*Conn]struct{}
	// connsByIP counts the connections in keysOf by the source address
	// their first Initial came from (connKey), what MaxConnsPerIP bounds;
	// ipOf remembers each connection's key so forget can give the count
	// back without the address. MaxConns itself reads len(keysOf).
	connsByIP map[string]int
	ipOf      map[*Conn]string
	// byPeer files the connections in keysOf by the remote address they
	// send to, and peerOf is each one's key: resetFor finds a stateless
	// reset's connections there, by the address §10.3.1 names, instead of
	// walking every connection and taking each one's lock. A migration
	// moves the entry (peerMoved, under the connection's lock); forget
	// removes it.
	byPeer map[peerKey][]*Conn
	peerOf map[*Conn]peerKey
	// spent is the Retry tokens that already admitted a connection, which a
	// second Initial echoing one must not open again (RFC 9000 §8.1.4).
	spent spentTokens
	// early admits 0-RTT (ListenerConfig.EarlyData), nil when off.
	early     *earlyGate
	closed    bool
	discarded uint64
	// resetWindow and resetsSent are the one-second budget maybeSendStatelessReset
	// spends, and resetsTo its per-source share of it. resetsTo only gains
	// an entry when a reset is sent, so the global budget bounds it too.
	resetWindow time.Time
	resetsSent  int
	resetsTo    map[string]int

	accepted chan *Conn
	stop     chan struct{}
	done     chan struct{} // closed when every read loop has returned
}

// Validate reports what [NewListener] would refuse in c alone — a negative
// bound or one out of range — and nil when it would accept it. A
// configuration read at deployment, from the environment or a file, is
// checked with it before any socket is opened; NewListener runs the same
// check. What depends on the rest of NewListener's arguments — a preferred
// address of the other family, session tickets the tls.Config disables —
// stays for NewListener.
func (c ListenerConfig) Validate() error {
	_, err := c.resolve()
	return err
}

// resolve applies the defaults zero stands for and returns an error naming
// every field whose value is negative or out of range: a negative bound is the caller's
// mistake, said at start rather than quietly turned into the default
// (docs/design/adr/0018).
func (c ListenerConfig) resolve() (ListenerConfig, error) {
	var err error
	if c.MaxPendingConns < 0 {
		err = errors.Join(err, fmt.Errorf("quic: ListenerConfig.MaxPendingConns %d is negative — zero means %d", c.MaxPendingConns, defaultMaxPendingConns))
	}
	if c.HandshakeTimeout < 0 {
		err = errors.Join(err, fmt.Errorf("quic: ListenerConfig.HandshakeTimeout %v is negative — zero means %v", c.HandshakeTimeout, DefaultHandshakeTimeout))
	}
	if c.MaxConns < 0 {
		err = errors.Join(err, fmt.Errorf("quic: ListenerConfig.MaxConns %d is negative — zero means no bound", c.MaxConns))
	}
	if c.MaxConnsPerIP < 0 {
		err = errors.Join(err, fmt.Errorf("quic: ListenerConfig.MaxConnsPerIP %d is negative — zero means no bound", c.MaxConnsPerIP))
	}
	err = errors.Join(err, ipkey.Check("quic: ListenerConfig.", &c.IPv4PrefixBits, &c.IPv6PrefixBits))
	if c.ConnQueueLen < 0 {
		err = errors.Join(err, fmt.Errorf("quic: ListenerConfig.ConnQueueLen %d is negative — zero means %d", c.ConnQueueLen, defaultConnQueueLen))
	}
	if c.RetryTokenTTL < 0 {
		err = errors.Join(err, fmt.Errorf("quic: ListenerConfig.RetryTokenTTL %v is negative — zero means %v", c.RetryTokenTTL, defaultRetryTokenTTL))
	}
	if c.MaxSpentRetryTokens < 0 {
		err = errors.Join(err, fmt.Errorf("quic: ListenerConfig.MaxSpentRetryTokens %d is negative — zero means %d", c.MaxSpentRetryTokens, defaultMaxSpentRetryTokens))
	}
	if c.StatelessResetsPerSecond < 0 {
		err = errors.Join(err, fmt.Errorf("quic: ListenerConfig.StatelessResetsPerSecond %d is negative — zero means %d", c.StatelessResetsPerSecond, defaultResetsPerSecond))
	}
	if c.StatelessResetsPerAddress < 0 {
		err = errors.Join(err, fmt.Errorf("quic: ListenerConfig.StatelessResetsPerAddress %d is negative — zero means %d", c.StatelessResetsPerAddress, defaultResetsPerAddress))
	}
	if c.EarlyDataWindow < 0 {
		err = errors.Join(err, fmt.Errorf("quic: ListenerConfig.EarlyDataWindow %v is negative — zero means %v", c.EarlyDataWindow, defaultEarlyDataWindow))
	}
	if c.MaxEarlyDataTickets < 0 {
		err = errors.Join(err, fmt.Errorf("quic: ListenerConfig.MaxEarlyDataTickets %d is negative — zero means %d", c.MaxEarlyDataTickets, defaultMaxEarlyDataTickets))
	}
	if c.TokenKey != ([32]byte{}) && c.TokenKey == c.StatelessResetKey {
		err = errors.Join(err, errors.New("quic: ListenerConfig.TokenKey equals StatelessResetKey — one key, two uses; give each its own"))
	}
	if c.PreferredIPv4.IsValid() {
		c.PreferredIPv4 = c.PreferredIPv4.Unmap()
		if !c.PreferredIPv4.Is4() || c.PreferredIPv4.IsUnspecified() {
			err = errors.Join(err, fmt.Errorf("quic: ListenerConfig.PreferredIPv4 %v is not an IPv4 address a client can send to", c.PreferredIPv4))
		}
	}
	if c.PreferredIPv6.IsValid() && (!c.PreferredIPv6.Is6() || c.PreferredIPv6.Is4In6() || c.PreferredIPv6.IsUnspecified()) {
		err = errors.Join(err, fmt.Errorf("quic: ListenerConfig.PreferredIPv6 %v is not an IPv6 address a client can send to", c.PreferredIPv6))
	}
	if c.PreferredAddress == nil && (c.PreferredIPv4.IsValid() || c.PreferredIPv6.IsValid()) {
		err = errors.Join(err, errors.New("quic: ListenerConfig.PreferredIPv4 or PreferredIPv6 is set without a PreferredAddress socket to announce"))
	}
	if err != nil {
		return ListenerConfig{}, err
	}
	if c.MaxPendingConns == 0 {
		c.MaxPendingConns = defaultMaxPendingConns
	}
	if c.HandshakeTimeout == 0 {
		c.HandshakeTimeout = DefaultHandshakeTimeout
	}
	if c.ConnQueueLen == 0 {
		c.ConnQueueLen = defaultConnQueueLen
	}
	if c.RetryTokenTTL == 0 {
		c.RetryTokenTTL = defaultRetryTokenTTL
	}
	if c.EarlyDataWindow == 0 {
		c.EarlyDataWindow = defaultEarlyDataWindow
	}
	if c.MaxEarlyDataTickets == 0 {
		c.MaxEarlyDataTickets = defaultMaxEarlyDataTickets
	}
	if c.MaxSpentRetryTokens == 0 {
		c.MaxSpentRetryTokens = defaultMaxSpentRetryTokens
	}
	if c.StatelessResetsPerSecond == 0 {
		c.StatelessResetsPerSecond = defaultResetsPerSecond
	}
	if c.StatelessResetsPerAddress == 0 {
		c.StatelessResetsPerAddress = defaultResetsPerAddress
	}
	// A random key per listener when none is given; both are drawn here so
	// that what the listener runs with is all in its resolved config.
	for _, k := range []*[32]byte{&c.StatelessResetKey, &c.TokenKey} {
		if *k == ([32]byte{}) {
			_, _ = rand.Read(k[:]) // never fails since Go 1.24
		}
	}
	return c, nil
}

// NewListener starts demultiplexing pc. cfg and params are what every
// accepted connection's handshake uses. pc is any [net.PacketConn]; a [net.UDPConn] is
// given the socket options RFC 9000 asks for ("The socket is the caller's"
// in the package documentation). cfg's callbacks run on each connection's
// receive goroutine, and a slow one stalls that connection alone ("The TLS
// callbacks are the caller's", same place). cfg.Certificates is refused
// when a leaf carries a Key Usage without digitalSignature or a chain
// member is signed with SHA-1 (RFC 9846 §4.5.1.2, §4.3.3): a conformant
// client would refuse the chain mid-handshake, where the cause is lost.
// What GetCertificate or GetConfigForClient return is the caller's to hold
// to the same rules, as is matching the chain to the client's
// signature_algorithms_cert, which crypto/tls does not do.
func NewListener(pc net.PacketConn, cfg *tls.Config, params TransportParameters, lcfg ListenerConfig) (*Listener, error) {
	if err := params.check(); err != nil {
		return nil, err
	}
	if err := checkTLSConfig(cfg); err != nil {
		return nil, err
	}
	if err := checkOwnCertificates(cfg); err != nil {
		return nil, err
	}
	lcfg, err := lcfg.resolve()
	if err != nil {
		return nil, err
	}
	if (lcfg.SessionTickets || lcfg.EarlyData) && cfg.SessionTicketsDisabled {
		return nil, fmt.Errorf("%w: ListenerConfig.SessionTickets or EarlyData is set and tls.Config.SessionTicketsDisabled too — "+
			"crypto/tls would send no ticket", ErrQUIC)
	}
	if lcfg.EarlyData {
		if err := params.earlyCredit(); err != nil {
			return nil, err
		}
	}
	var preferredV4, preferredV6 *net.UDPAddr
	if lcfg.PreferredAddress != nil {
		var err error
		preferredV4, preferredV6, err = preferredAnnouncements(lcfg.PreferredAddress.LocalAddr(), lcfg.PreferredIPv4, lcfg.PreferredIPv6)
		if err != nil {
			return nil, err
		}
	}
	l := &Listener{
		pc: pc, sourced: packetInfoSocket(pc), preferred: lcfg.PreferredAddress, preferredV4: preferredV4, preferredV6: preferredV6,
		cfg: cfg, params: params, lcfg: lcfg,
		spent:        spentTokens{ttl: lcfg.RetryTokenTTL, max: lcfg.MaxSpentRetryTokens},
		byDCID:       make(map[string]*Conn),
		keysOf:       make(map[*Conn][]string),
		pendingConns: make(map[*Conn]struct{}),
		connsByIP:    make(map[string]int),
		ipOf:         make(map[*Conn]string),
		byPeer:       make(map[peerKey][]*Conn),
		peerOf:       make(map[*Conn]peerKey),
		accepted:     make(chan *Conn, lcfg.MaxPendingConns),
		stop:         make(chan struct{}),
		done:         make(chan struct{}),
	}
	if lcfg.EarlyData {
		l.early = newEarlyGate(lcfg.EarlyDataWindow, lcfg.MaxEarlyDataTickets)
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
// [net.Listener.Accept]'s shape, with [Dial]'s return contract: what comes
// back is already past its handshake, not merely dialled — except under
// [ListenerConfig.EarlyData], below.
//
// Without EarlyData no application data leaves before that point: RFC 9846
// §4.5.3 lets a server send it after its first flight (0.5-RTT), to a
// client not yet authenticated, and a server here answers requests, which
// come after. With EarlyData, a connection whose 0-RTT was accepted comes
// back as soon as its 1-RTT write keys are in, before the client's
// Finished: the requests sent in 0-RTT are read, and the answers leave in
// 1-RTT as 0.5-RTT data, to a client whose handshake may never complete —
// a replay. [Conn.HandshakeComplete] says when it has; EarlyData says what
// that leaves to the application.
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

// Close stops the dispatch loop and ends every connection this listener
// routes — handshakes in progress, completed ones still queued for Accept,
// and the ones Accept already handed out — each with a CONNECTION_CLOSE
// (NO_ERROR, RFC 9000 §10.2) sent before the shared sockets close. This is
// where the [net.Listener] analogy stops: a TCP connection owns its socket
// and outlives its listener, but every connection here reads and writes
// through the listener's socket and can neither send nor receive once that
// socket is gone. Left alone, an accepted connection would hold its
// goroutines until its own idle timeout, and its peer would wait out its
// idle timeout too without ever hearing why. Close does not wait for
// anything in flight: a caller that wants its responses delivered first
// ends its connections itself before closing the listener.
func (l *Listener) Close() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	l.closed = true
	// keysOf lists every connection still routed, accepted or not; a
	// connection that ended forgot itself already. Ended after mu is
	// released: Conn.Close takes the connection's own lock.
	toClose := make([]*Conn, 0, len(l.keysOf))
	for c := range l.keysOf {
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

// Addr reports the address of the socket this listener reads: what
// [NewListener] was given, as [net.PacketConn.LocalAddr] reports it — a
// port chosen by the kernel shows up here, which is how a test or a
// supervisor learns where to dial.
func (l *Listener) Addr() net.Addr { return l.pc.LocalAddr() }

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
	oob := make([]byte, udp.PacketInfoLen)
	dfSocket(pc)           // Don't Fragment on what this socket sends (§14)
	ecnOK := ecnSocket(pc) // the kernel reports each datagram's ECN field (§13.4.1)
	var sv *sourcedVias
	if l.sourced != nil && pc == net.PacketConn(l.sourced) {
		sv = &sourcedVias{uc: l.sourced, oob: oob, byLocal: make(map[localKey]*sourcedConn)}
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
			ecn  uint8
		)
		if sv != nil {
			n, from, via, ecn, err = sv.read(buf)
		} else {
			n, from, ecn, err = readDatagram(pc, ecnOK, buf, oob)
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
		l.dispatch(append([]byte(nil), buf[:n]...), from, via, ecn)
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

func (s *sourcedVias) read(buf []byte) (n int, from net.Addr, via net.PacketConn, ecn uint8, err error) {
	var oobn int
	n, oobn, _, from, err = s.uc.ReadMsgUDP(buf, s.oob)
	if err != nil {
		return 0, nil, nil, 0, err
	}
	ecn, _ = udp.PacketECN(s.oob[:oobn])
	dst, ifindex, ok := udp.PacketDestination(s.oob[:oobn])
	if !ok || dst.IsMulticast() || dst.IsUnspecified() {
		return n, from, s.uc, ecn, nil
	}
	if !dst.Is6() || dst.Is4In6() || !dst.IsLinkLocalUnicast() {
		ifindex = 0 // let routing pick the interface, as it would anyway
	}
	k := localKey{dst, ifindex}
	v := s.byLocal[k]
	if v == nil {
		if len(s.byLocal) >= maxSourcedVias {
			return n, from, s.uc, ecn, nil
		}
		v = &sourcedConn{
			UDPConn: s.uc,
			oob:     udp.AppendPacketSource(nil, dst, ifindex),
			local:   net.UDPAddrFromAddrPort(netip.AddrPortFrom(dst.Unmap(), uint16(s.uc.LocalAddr().(*net.UDPAddr).Port))), //nolint:gosec // G115: a UDP port fits 16 bits
		}
		s.byLocal[k] = v
	}
	return n, from, v, ecn, nil
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
func (l *Listener) dispatch(datagram []byte, from net.Addr, via net.PacketConn, ecn uint8) {
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
		// A connection whose CONNECTION_CLOSE has left stays routed for
		// its closing period (RFC 9000 §10.2.1) and answers from here, on
		// this loop — its own read loop is gone — with the close again:
		// a peer that missed the first hears the code instead of the
		// stateless reset an unknown identifier would earn below.
		if c.answerClosing(datagram, from) {
			return
		}
		// Never block here: this loop serves every connection on the socket,
		// and a connection whose consumer lags (a full accept queue, a
		// stream callback applying back-pressure) legitimately stops
		// draining its channel. Waiting on it would freeze every other
		// connection and every handshake behind one slow peer. Dropping is
		// what the network would do; loss recovery retransmits, and
		// back-pressure stays per connection, through flow control.
		select {
		case c.incoming <- rawDatagram{data: datagram, from: from, via: via, ecn: ecn}:
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
		// Or the other way round: the peer of one of this listener's
		// connections lost its state and is saying so (§10.3.1). Compared
		// before the reset this end would send, within the same budget.
		if c := l.resetFor(datagram, from); c != nil {
			_ = c.handleStatelessReset()
			return
		}
		l.maybeSendStatelessReset(datagram, dcid, from, via)
		return
	}
	h, _, err := parseLongHeader(datagram)
	if err != nil || h.Type != packetInitial {
		l.noteDiscard()
		return
	}
	l.handleNewInitial(datagram, from, h, via, ecn)
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
func (l *Listener) handleNewInitial(datagram []byte, from net.Addr, h longHeader, via net.PacketConn, ecn uint8) {
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
	// cannot occupy MaxPendingConns slots for the price of sending it:
	// each slot holds TLS state and two goroutines for up to the
	// 10-second handshake deadline.
	keys, ok := initialPacketDecrypts(h)
	if !ok {
		l.noteDiscard()
		return
	}

	l.mu.Lock()
	pending := len(l.pendingConns)
	total, fromIP := len(l.keysOf), l.connsByIP[l.connKey(from)]
	l.mu.Unlock()
	// The three bounds refuse the same way; only the pending table's
	// high-water mark below turns the Retry flow on, since Retry defends
	// that table's slots, not the total.
	if pending >= l.lcfg.MaxPendingConns ||
		(l.lcfg.MaxConns > 0 && total >= l.lcfg.MaxConns) ||
		(l.lcfg.MaxConnsPerIP > 0 && fromIP >= l.lcfg.MaxConnsPerIP) {
		// RFC 9000 §5.2.2: a server with no room SHOULD say so — a
		// stateless CONNECTION_REFUSED close costs one sealed packet
		// against a decrypted ≥1200-byte Initial and spares the client its
		// whole handshake timeout. (A silent drop would pass for "no
		// different from a listen queue dropping a SYN" — but TCP has RST
		// for the loud variant, and QUIC's equivalent is this.)
		l.sendStatelessClose(from, h, &keys, transportConnectionRefused, via)
		return
	}

	// The Retry flow handles three cases: the operator asked for it on
	// every attempt; the pending table passed its high-water mark, where
	// handing out stateless Retries is how the remaining slots are kept
	// for addresses that prove themselves; and an Initial already carrying
	// a token, which only the Retry flow can verify — a token must never
	// reach the direct path unexamined, where the handshake it starts
	// would not echo retry_source_connection_id and the client would
	// refuse it.
	retry := l.lcfg.AlwaysRetry || pending >= l.lcfg.MaxPendingConns/2
	if len(h.Token) > 0 || retry {
		l.handleRetryFlow(datagram, from, h, &keys, via, ecn, retry)
		return
	}
	l.bootstrapConn(datagram, from, h, nil, &keys, via, ecn)
}

// initialKeys is what checking a client's first Initial derived: the
// extracted Initial secret, the client's direction expanded from it, and
// the opener that just authenticated the packet. Handed on so the answer —
// a stateless close or the connection's own Initial keys — does not run
// the key schedule a second time for the same identifier. The server's
// direction is not derived here: an Initial answered with a Retry, or
// refused outright, never seals anything under it, and the listener's cost
// per unauthenticated datagram is what a flood multiplies.
type initialKeys struct {
	initial []byte // hkdf.Extract's output, both directions' parent
	client  []byte
	opener  *packetOpener // the client's direction, which a server opens
}

// secrets expands the server's direction and returns both, for the paths
// that seal: a stateless close, or the connection being set up.
func (k *initialKeys) secrets() (secrets, error) {
	server, err := expandLabel(k.initial, "server in", 32)
	if err != nil {
		return secrets{}, err
	}
	return secrets{Client: k.client, Server: server}, nil
}

// initialPacketDecrypts checks an Initial against the keys its destination
// identifier derives (RFC 9001 §5.2) — the cheapest possible proof that the
// sender is a QUIC client and not noise shaped like one.
func initialPacketDecrypts(h longHeader) (initialKeys, bool) {
	initial, err := extractInitial(h.DCID)
	if err != nil {
		return initialKeys{}, false
	}
	client, err := expandLabel(initial, "client in", 32)
	if err != nil {
		return initialKeys{}, false
	}
	opener, err := newPacketOpener(client)
	if err != nil {
		return initialKeys{}, false
	}
	if _, _, err = opener.Open(h.Raw, h.PNOffset, 0); err != nil {
		// Every refusal of a first Initial is a silent drop, the reserved
		// bits of RFC 9000 §17.2 included — a connection would answer
		// those with PROTOCOL_VIOLATION, a Listener has no connection yet.
		// Chosen: Initial keys are public, so the packet can come from
		// anyone; a CONNECTION_CLOSE to an unvalidated address is an
		// unsolicited send toward a possibly forged source (§8.1), and a
		// connection created to be closed at once is the state the drop
		// exists to save. A real client that sets the bits is not
		// conformant, and its handshake timeout tells it so.
		return initialKeys{}, false
	}
	return initialKeys{initial: initial, client: client, opener: opener}, true
}

// handleRetryFlow implements RFC 9000 §8.1.2 statelessly: a first Initial
// (no token) gets a Retry and nothing else — no Conn, no table entry, no
// resource an attacker spends by sending one of these and walking away. A
// second Initial carrying a valid token proves the address and continues
// exactly like handleNewInitial would have without AlwaysRetry, except
// already validated. retry says whether an Initial that proves nothing is
// answered with a Retry or admitted on the direct path, the choice the
// dispatcher made for a tokenless one.
func (l *Listener) handleRetryFlow(datagram []byte, from net.Addr, h longHeader, keys *initialKeys, via net.PacketConn, ecn uint8, retry bool) {
	if len(h.Token) > 0 {
		rt, err := verifyRetryToken(h.Token, l.lcfg.TokenKey, l.lcfg.RetryTokenTTL, from)
		switch {
		case errors.Is(err, errForeignToken):
			// RFC 9000 §8.1.3: a token the server cannot validate — here,
			// one its key never signed, so a NEW_TOKEN from another server
			// of the same name or a token from a listener since restarted —
			// makes the client unvalidated, not wrong: it continues below
			// as if it had sent no token, "including potentially sending a
			// Retry packet". INVALID_TOKEN is kept for the token this
			// listener provably issued (§8.1.2), where the client is known
			// to hold a Retry token it will not replace.
		case err != nil:
			// RFC 9000 §8.1.2: an Initial whose Retry token cannot be
			// validated — expired, or issued to another address — SHOULD
			// be answered with an immediate INVALID_TOKEN close, so the
			// client learns now instead of eating its whole handshake
			// timeout. The answer is stateless — no Conn, no table entry —
			// and small against the ≥1200-byte datagram that provably
			// arrived (§8.1's limit allows three times that).
			l.sendStatelessClose(from, h, keys, transportInvalidToken, via)
			return
		case !bytes.Equal(h.DCID, rt.newSCID):
			// This Initial's own destination identifier must be the one
			// the Retry offered — anything else is a token replayed onto
			// an attempt it was never issued for: equally impossible to
			// validate, equally worth telling the client about now.
			l.sendStatelessClose(from, h, keys, transportInvalidToken, via)
			return
		case !l.consumeRetryToken(h.DCID):
			// RFC 9000 §8.1.4: a token admits one connection. The same one
			// echoed again — after that connection's identifiers came free,
			// since while it lives dispatch routes the replay to it — is a
			// Retry token this listener issued and will not honour twice,
			// and the client holding it accepts no other (§8.1.2).
			l.sendStatelessClose(from, h, keys, transportInvalidToken, via)
			return
		default:
			l.bootstrapConn(datagram, from, h, &rt, keys, via, ecn)
			return
		}
	}
	if !retry {
		l.bootstrapConn(datagram, from, h, nil, keys, via, ecn)
		return
	}
	newSCID, err := randomID(listenerCIDLen)
	if err != nil {
		return // nothing to send; the client's own retransmission tries again
	}
	token := appendRetryToken(nil, l.lcfg.TokenKey, from, h.DCID, newSCID)
	pkt := appendRetryPacket(nil, h.SCID, newSCID, h.DCID, token)
	_, _ = via.WriteTo(pkt, from) // best effort; a lost Retry just looks like a lost Initial to the client
}

// consumeRetryToken marks the token bound to newSCID as spent and reports
// whether it was still unspent — see spentTokens.
func (l *Listener) consumeRetryToken(newSCID []byte) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.spent.consume(newSCID, time.Now())
}

// sendStatelessClose answers one Initial with a stateless Initial-level
// CONNECTION_CLOSE carrying the given transport code — INVALID_TOKEN for a
// token that cannot be validated (§8.1.2), CONNECTION_REFUSED for a full
// pending table (§5.2.2). The keys are the ones the client derived from its
// own destination identifier (RFC 9001 §5.2), so only the endpoint that
// sent — or saw — that Initial can read the answer; the identifiers are
// echoed swapped, exactly as a server's first genuine packet would. keys is
// what the decrypt check already derived from h.DCID; the server's
// direction is expanded here, on the one refusal that seals.
func (l *Listener) sendStatelessClose(from net.Addr, h longHeader, keys *initialKeys, code uint64, via net.PacketConn) {
	l.noteDiscard() // the Initial itself is still refused, and still counted
	s, err := keys.secrets()
	if err != nil {
		return
	}
	sealer, err := newPacketSealer(s.Server)
	if err != nil {
		return
	}

	frame := varint.Append(nil, frameConnectionClose)
	frame = varint.Append(frame, code)
	frame = varint.Append(frame, 0) // the frame type being answered; none
	frame = varint.Append(frame, 0) // no reason phrase, deliberately

	const pnLen = 4
	header := []byte{0xc0 | packetInitial<<4 | pnLen - 1}
	header = append(header, 0, 0, 0, 1, byte(len(h.SCID))) //nolint:gosec // G115: a connection ID is at most 20 bytes; version 1, then the identifiers
	header = append(header, h.SCID...)
	header = append(header, byte(len(h.DCID))) //nolint:gosec // G115: a connection ID is at most 20 bytes
	header = append(header, h.DCID...)
	header = varint.Append(header, 0) // a server's Initial carries no token (§17.2.2)
	header = varint.Append(header, uint64(pnLen+len(frame)+16))
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
// standalone accept applies to its own unvalidated peer.
func (l *Listener) bootstrapConn(datagram []byte, from net.Addr, h longHeader, rt *retryToken, keys *initialKeys, via net.PacketConn, ecn uint8) {
	c, err := newServerConn(via, from, h, l.cfg, l.params, rt, keys, l.prepareConn)
	if err != nil {
		return
	}
	if rt == nil {
		// An address that has proved nothing yet pays the RFC 9000 §8.1
		// amplification limit until its first Handshake-level packet
		// decrypts. The Retry path proved itself across that round trip,
		// so no accounting is needed there. Under the connection's lock:
		// its timer loop is already running and reads the flag
		// (ptoDeadlineLocked).
		c.mu.Lock()
		c.amplActive = true
		c.mu.Unlock()
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
	ip := l.connKey(from)
	l.ipOf[c] = ip
	l.connsByIP[ip]++
	l.placePeerLocked(c, from)
	l.mu.Unlock()

	go l.runHandshake(c, datagram, from, ecn)
}

// defaultConnQueueLen is what a zero [ListenerConfig.ConnQueueLen] means;
// the field's comment has the measurement behind it.
const defaultConnQueueLen = 64

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
	c.incoming = make(chan rawDatagram, l.lcfg.ConnQueueLen)
	c.sendsTicket = l.lcfg.SessionTickets || l.lcfg.EarlyData
	c.early = l.early
	if l.preferred == nil {
		return nil
	}
	cid, err := l.addLocalCID(c)
	if err != nil {
		return err
	}
	c.params.preferredAddress = &preferredAddress{
		ipv4: l.preferredV4, ipv6: l.preferredV6,
		cid: cid, statelessResetToken: l.resetTokenFor(cid),
	}
	c.localCIDs = append(c.localCIDs, localCID{seq: 1, cid: cid})
	c.nextLocalSeq = 2
	return nil
}

// preferredAnnouncements resolves what a listener announces for its
// preferred-address socket, bound at local, from [ListenerConfig.PreferredIPv4]
// and PreferredIPv6, already checked for their family by resolve: each on
// the socket's port. With neither, the socket's own address is announced
// and must be concrete; a socket on a concrete address accepts only that
// address, since nothing else reaches it.
func preferredAnnouncements(local net.Addr, ip4, ip6 netip.Addr) (v4, v6 *net.UDPAddr, err error) {
	ua, ok := local.(*net.UDPAddr)
	if !ok || ua == nil || ua.IP == nil {
		return nil, nil, fmt.Errorf("%w: PreferredAddress must be a UDP socket, not %v", ErrQUIC, local)
	}
	if !ip4.IsValid() && !ip6.IsValid() {
		if ua.IP.IsUnspecified() {
			return nil, nil, fmt.Errorf("%w: PreferredAddress is bound to %v; name the addresses to announce in PreferredIPv4 and PreferredIPv6 — a client has to know where to send",
				ErrQUIC, local)
		}
		if ua.IP.To4() != nil {
			return ua, nil, nil
		}
		return nil, ua, nil
	}
	bound := ua.AddrPort().Addr().Unmap()
	for _, ip := range []netip.Addr{ip4, ip6} {
		if !ip.IsValid() {
			continue
		}
		if !bound.IsUnspecified() && bound != ip.WithZone(bound.Zone()) {
			return nil, nil, fmt.Errorf("%w: PreferredIPv4 or PreferredIPv6 names %v but PreferredAddress is bound to %v, which is the only address that reaches it",
				ErrQUIC, ip, ua.IP)
		}
		addr := net.UDPAddrFromAddrPort(netip.AddrPortFrom(ip, uint16(ua.Port))) //nolint:gosec // G115: a bound UDP port fits 16 bits
		if ip.Is4() {
			v4 = addr
		} else {
			v6 = addr
		}
	}
	return v4, v6, nil
}

// runHandshake drives one connection's handshake to completion on its own
// goroutine, never on the shared dispatch loop: a slow or deliberately
// stalled handshake must not head-of-line block every other connection's
// datagrams behind it.
func (l *Listener) runHandshake(c *Conn, first []byte, from net.Addr, ecn uint8) {
	accepted, watched := false, false
	defer func() {
		l.mu.Lock()
		delete(l.pendingConns, c)
		l.mu.Unlock()
		if !accepted {
			_ = c.Close()
			c.endTLS()
			// A connection that never reached Accept — its handshake
			// refused, or the peer's CONNECTION_CLOSE — keeps its closing
			// state too, routable until gone like an accepted one (below):
			// forgotten at once, the client's Initial sent again after a
			// lost close would find no route and open a fresh attempt
			// instead of the close repeated (§10.2, §10.2.1).
			if !watched {
				go func() {
					<-c.gone
					l.forget(c)
				}()
			}
		}
	}()

	if err := c.receiveGathered(rawDatagram{data: first, from: from, via: c.pc, ecn: ecn}); err != nil {
		return // an authenticated violation on its own first packet; abort already closed it
	}
	// The handshake runs under HandshakeTimeout, which the connection
	// keeps rather than a context's deadline: a client that proves its
	// address may move it once (extendHandshakeLocked).
	c.mu.Lock()
	c.handshakeBudget = l.lcfg.HandshakeTimeout
	c.mu.Unlock()
	// A connection whose 0-RTT was accepted is handed out as soon as it
	// was (ADR 0019): early data is only worth its replay risk if the
	// application can act on it before the handshake ends. It keeps its
	// pending slot until then — MaxPendingConns and HandshakeTimeout
	// still bound it — and a handshake that fails afterwards closes it
	// under its new owner.
	hsDone := make(chan error, 1)
	go func() { hsDone <- c.handshake(context.Background()) }()
	select {
	case err := <-hsDone:
		if err != nil {
			return // the handshake path closes on every failure, same as Accept's
		}
		hsDone = nil
	case <-c.earlyAccepted:
	}
	if hsDone != nil {
		defer func() { <-hsDone }() // runs before the slot is given back
	}

	// Forgotten once its closing state ends (gone), not when it closes:
	// a connection that sent a CONNECTION_CLOSE stays routable for three
	// probe timeouts so that dispatch can repeat the close to a peer that
	// missed it (§10.2.1), and only then do its identifiers — and the
	// MaxConns slot they hold — come free. §10.2 asks a server that keeps
	// its socket open not to cut this short.
	watched = true
	go func() {
		<-c.gone
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
// further use for them: its closing state over (gone), accepted or not, or
// the connection never served — its scid, the
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
	l.unplacePeerLocked(c)
	// The per-address count follows ipOf, not keysOf: a connection the
	// closing listener turned away before registering (above) has keys but
	// no address, and a second forget of the same one finds neither.
	if ip, counted := l.ipOf[c]; counted {
		delete(l.ipOf, c)
		l.connsByIP[ip]--
		if l.connsByIP[ip] <= 0 {
			delete(l.connsByIP, ip)
		}
	}
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
	mac := hmac.New(sha256.New, l.lcfg.StatelessResetKey[:])
	mac.Write(cid)
	var token [16]byte
	copy(token[:], mac.Sum(nil))
	return token
}

// defaultResetsPerSecond is ListenerConfig.StatelessResetsPerSecond when
// zero: each reset is an answer to unauthenticated bytes, and answering
// every stray at line rate would make the listener a traffic generator
// pointed wherever the source addresses claim (§10.3.3).
const defaultResetsPerSecond = 16

// defaultResetsPerAddress is ListenerConfig.StatelessResetsPerAddress when
// zero. Without a share per address, one sender spraying short headers at
// random identifiers spends the whole budget, and every genuine client
// whose connection this listener lost waits out its idle timeout instead.
const defaultResetsPerAddress = 4

// resetFor reports which live connection, if any, an unroutable short-header
// datagram is a stateless reset for: RFC 9000 §10.3.1 has the comparison
// made "when a packet cannot be associated with a connection", and on a
// server that is here, not in any connection — the datagram named an
// identifier the peer made up. The 16-byte tail is compared against the
// tokens held by the connections *on the datagram's own remote address*,
// which is the comparison §10.3.1 describes: "all stateless reset tokens
// associated with the remote address on which the datagram was received"
// (isStatelessResetFrom; used identifiers only, constant time).
//
// That address is also what bounds the cost. The per-second reset budget
// does not: it is l.resetsSent, which only a reset actually sent raises,
// and StatelessResetsPerAddress caps one source at four of the sixteen.
// Measured: 200 datagrams from a single source left it at 4, so compared
// against every live connection, every datagram would pay the whole
// table — on the one goroutine that reads the socket, with MaxConns zero
// by default.
// Scoped to the address, a flood from a source holding no connection
// compares nothing, and the budget is left to do what it is for: bounding
// the resets this end *sends*.
//
// Those connections are looked up in byPeer, not found by walking the
// table: a walk would take every live connection's lock to read its
// address, one lock per connection per datagram on the goroutine that reads
// the socket, and a flow of unroutable datagrams would keep that goroutine
// contending with every connection's receive loop. The index follows
// each migration (setPeerLocked), and isStatelessResetFrom still compares
// the address under the connection's lock, since a move may land between
// the lookup and the comparison.
//
// Long headers are not compared: §10.3 says "any packet", but a reset has
// the short-header shape by construction (§10.3), a conforming peer never
// sends a long-header one, and every Initial from a new client would
// otherwise pay a comparison per live connection on the accept path.
//
// The connections are listed under l.mu and compared outside it: the
// established order is Conn.mu then Listener.mu, and isStatelessResetFrom
// takes the connection's.
func (l *Listener) resetFor(datagram []byte, from net.Addr) *Conn {
	if len(datagram) < 21 { // §10.3: a reset is at least 21 bytes
		return nil
	}
	var room [4]*Conn
	l.mu.Lock()
	conns := append(room[:0], l.byPeer[peerKeyOf(from)]...)
	l.mu.Unlock()
	for _, c := range conns {
		if c.isStatelessResetFrom(datagram, from) {
			return c
		}
	}
	return nil
}

// peerKey is a remote address as sameAddr compares them: for a UDP address
// the IP without its IPv4-in-IPv6 mapping, and the port; for any other, its
// network and text.
type peerKey struct {
	ap   netip.AddrPort
	text string
}

func peerKeyOf(a net.Addr) peerKey {
	if u, ok := a.(*net.UDPAddr); ok {
		ap := u.AddrPort()
		return peerKey{ap: netip.AddrPortFrom(ap.Addr().Unmap(), ap.Port())}
	}
	if a == nil {
		return peerKey{}
	}
	return peerKey{text: a.Network() + " " + a.String()}
}

// placePeerLocked files c under the remote address to, out of the one it
// was filed under. Callers hold mu.
func (l *Listener) placePeerLocked(c *Conn, to net.Addr) {
	l.unplacePeerLocked(c)
	k := peerKeyOf(to)
	l.byPeer[k] = append(l.byPeer[k], c)
	l.peerOf[c] = k
}

// unplacePeerLocked takes c out of byPeer. Callers hold mu.
func (l *Listener) unplacePeerLocked(c *Conn) {
	k, ok := l.peerOf[c]
	if !ok {
		return
	}
	delete(l.peerOf, c)
	conns := l.byPeer[k]
	for i, o := range conns {
		if o == c {
			last := len(conns) - 1
			conns[i], conns[last] = conns[last], nil
			conns = conns[:last]
			break
		}
	}
	if len(conns) == 0 {
		delete(l.byPeer, k)
	} else {
		l.byPeer[k] = conns
	}
}

// peerMoved is cidRegistrar's: c, whose lock the caller holds, now sends to
// to (Conn.mu before Listener.mu). A connection already forgotten is not
// filed again.
func (l *Listener) peerMoved(c *Conn, to net.Addr) {
	l.mu.Lock()
	if _, ok := l.peerOf[c]; ok {
		l.placePeerLocked(c, to)
	}
	l.mu.Unlock()
}

// resetBudgetLocked reports whether this end may still spend a reset on src
// in the current window, rolling the window when it has run out. Callers
// hold l.mu.
func (l *Listener) resetBudgetLocked(now time.Time, src string) bool {
	if now.Sub(l.resetWindow) >= time.Second {
		l.resetWindow, l.resetsSent = now, 0
		clear(l.resetsTo)
	}
	return l.resetsSent < l.lcfg.StatelessResetsPerSecond && l.resetsTo[src] < l.lcfg.StatelessResetsPerAddress
}

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
	src := sourceIP(from)
	if !l.resetBudgetLocked(now, src) {
		l.mu.Unlock()
		l.noteDiscard()
		return
	}
	l.resetsSent++
	if l.resetsTo == nil {
		l.resetsTo = make(map[string]int, l.lcfg.StatelessResetsPerSecond)
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

// sourceIP is the per-source key of the stateless reset budget: the IP
// address without its port, an IPv4-mapped IPv6 address reduced to its IPv4
// form, or the whole address where it is not IP. The budget keys on the
// whole address, not on MaxConnsPerIP's prefix: it bounds what a forged
// source reflects to one victim, and a victim is an address.
func sourceIP(from net.Addr) string {
	if ua, ok := from.(*net.UDPAddr); ok {
		if ip, ok := netip.AddrFromSlice(ua.IP); ok {
			return string(ip.Unmap().AsSlice())
		}
	}
	return from.String()
}

// connKey is the key MaxConnsPerIP counts from under: its address's prefix
// (ipkey.Of), or the whole address where it is not IP. Every IP key of one
// listener has the same length per family, so the masked address alone —
// 4 or 16 bytes — tells two prefixes apart.
func (l *Listener) connKey(from net.Addr) string {
	if ua, ok := from.(*net.UDPAddr); ok {
		if ip, ok := netip.AddrFromSlice(ua.IP); ok {
			return string(ipkey.Of(ip, l.lcfg.IPv4PrefixBits, l.lcfg.IPv6PrefixBits).Addr().AsSlice())
		}
	}
	return from.String()
}
