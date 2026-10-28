package quic

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"fmt"
	"net"
	"slices"
	"time"

	"github.com/mlagarrigue/sluice/internal/varint"
)

// Server-generated Retry (RFC 9000 §8.1.2), the stronger of this package's
// two address-validation strategies — [ListenerConfig.AlwaysRetry] turns it
// on; the default is the anti-amplification limit alone (quic/send.go,
// quic/receive.go), which validates implicitly once a Handshake-level packet
// decrypts and costs no extra round trip.
//
// A Retry needs no connection and no server-side state to issue: everything
// a later Initial must prove is encoded into the token itself, authenticated
// with an HMAC key the issuing [Listener] holds — random per listener unless
// [ListenerConfig.TokenKey] names one. With a random key a listener restart
// simply invalidates every outstanding token, which costs an abandoned
// handshake attempt a client retries from scratch — not a correctness
// problem, the same tolerance RFC 9000 already assumes of any stateless
// Retry scheme; a key kept across restarts or shared between instances
// spares even that.

// defaultRetryTokenTTL is what a zero [ListenerConfig.RetryTokenTTL] means.
const defaultRetryTokenTTL = 10 * time.Second

// spentTokens remembers each Retry token that already admitted a connection,
// keyed by the identifier the token bound (the connection's first DCID), for
// the rest of the token's life. RFC 9000 §8.1.4 asks that replay be
// prevented or limited, and the TTL alone only limits it: while the
// connection lives a replay routes to it (byDCID), but a handshake that
// fails in a millisecond frees the identifier with most of the TTL left,
// and the same token would then open a second connection from the same
// address without the round trip that validated the first. One entry per
// Retry-born connection for at most ttl; past max the oldest is dropped, which bounds the memory at the price of a replay
// becoming possible again for a token older than the cap's worth of
// admissions — limited, as §8.1.4 allows, under a load the per-address and
// pending-table limits already cap elsewhere. Every method is called with
// the listener's mu held.
//
// The same register records the 0-RTT tickets an [earlyGate] admitted, with
// failClosed set: there a full register refuses the next entry instead of
// forgetting the oldest, since a 0-RTT refused costs the client a round
// trip and a replay accepted breaks RFC 9846 §8. That one is called with
// the gate's own mu held.
type spentTokens struct {
	byID       map[string]time.Time // identifier → when it may be forgotten
	order      []string             // byID's keys, oldest expiry first
	ttl        time.Duration        // RetryTokenTTL, or EarlyDataWindow
	max        int                  // MaxSpentRetryTokens, or MaxEarlyDataTickets
	failClosed bool                 // full: refuse, rather than forget the oldest
}

// defaultMaxSpentRetryTokens is what a zero
// [ListenerConfig.MaxSpentRetryTokens] means: an entry is a key of
// listenerCIDLen bytes and a time, about 125 bytes with the map's and the
// order slice's overhead, 2.04 MB measured at this figure — and reaching it
// takes that many Retry tokens consumed by connections within one TTL.
const defaultMaxSpentRetryTokens = 1 << 14

// consume records newSCID's token as spent until expiry and reports whether
// it was still unspent — false is a replay, or a full failClosed register. now is the caller's clock, so
// expiries stay in insertion order and the sweep below stops at the first
// live one.
func (s *spentTokens) consume(newSCID []byte, now time.Time) bool {
	if s.byID == nil {
		s.byID = make(map[string]time.Time)
	}
	id := string(newSCID)
	if exp, spent := s.byID[id]; spent && now.Before(exp) {
		return false
	}
	for len(s.order) > 0 && !now.Before(s.byID[s.order[0]]) {
		delete(s.byID, s.order[0])
		s.order = s.order[1:]
	}
	if len(s.order) >= s.max {
		if s.failClosed {
			return false
		}
		delete(s.byID, s.order[0])
		s.order = s.order[1:]
	}
	s.byID[id] = now.Add(s.ttl)
	s.order = append(s.order, id)
	return true
}

// forget drops an entry consume recorded, freeing its identifier and its
// slot: an admission that was reserved and not used (earlyGate.release).
// Linear in the register, which is bounded by max and walked once per
// release — a resumption that offered no 0-RTT, a handshake that failed —
// never per packet.
func (s *spentTokens) forget(id []byte) {
	k := string(id)
	if _, ok := s.byID[k]; !ok {
		return
	}
	delete(s.byID, k)
	if i := slices.Index(s.order, k); i >= 0 {
		s.order = slices.Delete(s.order, i, i+1)
	}
}

// AppendRetryPacket writes a Retry packet (RFC 9000 §17.2.5): dcid is the
// identifier to address it to (the client's own SCID from the Initial it
// answers), newSCID is the identifier the client should use as its DCID
// from here on, odcid is that Initial's destination identifier (needed only
// for the integrity tag, RFC 9001 §5.8 — it does not appear in the packet
// itself), and token is what AppendRetryToken produced.
func appendRetryPacket(dst, dcid, newSCID, odcid, token []byte) []byte {
	start := len(dst)
	// Long header, fixed bit set, type 11 (Retry). The four low bits are
	// unused: RFC 9000 §17.2.5 has the server set them "to an arbitrary
	// value" and the client "MUST ignore these bits". Drawn rather than
	// zero, as appendVersionNegotiation draws its own, so no client comes
	// to depend on one value.
	var unused [1]byte
	_, _ = rand.Read(unused[:])                                         // never fails since Go 1.24
	dst = append(dst, 0xf0|unused[0]&0x0f, 0, 0, 0, 1, byte(len(dcid))) //nolint:gosec // G115: a connection ID is at most 20 bytes; Version1
	dst = append(dst, dcid...)
	dst = append(dst, byte(len(newSCID))) //nolint:gosec // G115: a connection ID is at most 20 bytes
	dst = append(dst, newSCID...)
	dst = append(dst, token...)
	tag := retryTag(odcid, dst[start:])
	return append(dst, tag[:]...)
}

// newRetryTokenKey draws an HMAC-SHA256 key for Retry tokens, what a zero
// [ListenerConfig.TokenKey] stands for.
func newRetryTokenKey() ([32]byte, error) {
	var k [32]byte
	_, err := rand.Read(k[:])
	return k, err
}

// AppendRetryToken writes a token binding this Retry to the peer address and
// original destination identifier a later Initial must match, and to the
// server-chosen identifier (newSCID) that Initial's DCID must equal — the
// whole point being that verifying the token alone, with no connection
// table, recovers everything [newServerConn] needs to continue correctly as
// if this had been the client's first Initial.
func appendRetryToken(dst []byte, key [32]byte, peer net.Addr, odcid, newSCID []byte) []byte {
	return appendRetryTokenAt(dst, key, time.Now(), peer, odcid, newSCID)
}

// appendRetryTokenAt is AppendRetryToken with the issue time explicit, so a
// test can produce a genuinely encoded token that has already expired.
func appendRetryTokenAt(dst []byte, key [32]byte, issued time.Time, peer net.Addr, odcid, newSCID []byte) []byte {
	start := len(dst)
	// Milliseconds, not seconds: a TTL below one second must still mean
	// what it says, and a second-truncated issue time ages a token by up
	// to a second at birth.
	dst = varint.Append(dst, uint64(issued.UnixMilli())) //nolint:gosec // G115: milliseconds since epoch, nowhere near overflowing a varint
	addr := []byte(peer.String())
	dst = append(dst, byte(len(addr))) //nolint:gosec // G115: an IP address and port, at most 18 bytes
	dst = append(dst, addr...)
	dst = append(dst, byte(len(odcid))) //nolint:gosec // G115: a connection ID is at most 20 bytes
	dst = append(dst, odcid...)
	dst = append(dst, byte(len(newSCID))) //nolint:gosec // G115: a connection ID is at most 20 bytes
	dst = append(dst, newSCID...)
	mac := hmac.New(sha256.New, key[:])
	mac.Write(dst[start:])
	return mac.Sum(dst)
}

// retryToken is what AppendRetryToken encoded, recovered by
// VerifyRetryToken.
type retryToken struct {
	odcid, newSCID []byte
}

// errForeignToken is verifyRetryToken's answer to a token this listener's
// key did not sign: too short to carry its MAC, or a MAC under another key.
// Nothing proves it was ever a Retry token — a client may be presenting a
// NEW_TOKEN from another server of the same name, or a token from a listener
// since restarted — so RFC 9000 §8.1.3 has the server treat the client as
// unvalidated, not as an error; INVALID_TOKEN (§8.1.2) is for a token
// this listener provably issued and cannot accept.
var errForeignToken = fmt.Errorf("%w: a token this listener did not issue", ErrQUIC)

// VerifyRetryToken checks a token a client echoed on its post-Retry Initial:
// the HMAC must verify, the token must not have expired, and it must name
// the peer address it is arriving from. ttl is [ListenerConfig.RetryTokenTTL]. On success it returns the original
// destination identifier and server-chosen identifier the token bound,
// which is everything [newServerConn] needs beyond what the Initial itself
// carries. A token the HMAC does not recognise is [errForeignToken]; any
// other error is a token this listener issued and will not honour.
func verifyRetryToken(token []byte, key [32]byte, ttl time.Duration, peer net.Addr) (retryToken, error) {
	const macLen = sha256.Size
	if len(token) < macLen {
		return retryToken{}, errForeignToken
	}
	body, mac := token[:len(token)-macLen], token[len(token)-macLen:]
	want := hmac.New(sha256.New, key[:])
	want.Write(body)
	if subtle.ConstantTimeCompare(want.Sum(nil), mac) != 1 {
		return retryToken{}, errForeignToken
	}

	issued, rest, err := readVarint(body)
	if err != nil {
		return retryToken{}, err
	}
	if time.Since(time.UnixMilli(int64(issued))) > ttl { //nolint:gosec // G115: a Unix millisecond count from a clock holding this key
		return retryToken{}, fmt.Errorf("%w: an expired Retry token", ErrQUIC)
	}
	addr, rest, err := tokenField(rest)
	if err != nil {
		return retryToken{}, err
	}
	if string(addr) != peer.String() {
		return retryToken{}, fmt.Errorf("%w: a Retry token issued to a different address", ErrQUIC)
	}
	odcid, rest, err := tokenField(rest)
	if err != nil {
		return retryToken{}, err
	}
	newSCID, _, err := tokenField(rest)
	if err != nil {
		return retryToken{}, err
	}
	return retryToken{odcid: odcid, newSCID: newSCID}, nil
}

// tokenField reads one length-prefixed field off a token, the same shape
// [connectionID] reads but without its 20-byte connection-identifier cap —
// a token's address field, in particular, can run longer than that (an
// IPv6 address and port easily do).
func tokenField(b []byte) (val, rest []byte, err error) {
	if len(b) == 0 {
		return nil, nil, ErrTruncated
	}
	n := int(b[0])
	return bytesN(b[1:], n)
}
