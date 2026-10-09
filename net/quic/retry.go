package quic

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"fmt"
	"net"
	"time"
)

// Server-generated Retry (RFC 9000 §8.1.2), the stronger of this package's
// two address-validation strategies — [ListenerConfig.AlwaysRetry] turns it
// on; the default is the anti-amplification limit alone (quic/send.go,
// quic/receive.go), which validates implicitly once a Handshake-level packet
// decrypts and costs no extra round trip.
//
// A Retry needs no connection and no server-side state to issue: everything
// a later Initial must prove is encoded into the token itself, authenticated
// with an HMAC key the issuing [Listener] holds only in memory. A listener
// restart simply invalidates every outstanding token, which costs an
// abandoned handshake attempt a client retries from scratch — not a
// correctness problem, the same tolerance RFC 9000 already assumes of any
// stateless Retry scheme.

// retryTokenTTL is how long a Retry token remains acceptable. Generous for
// one round trip, short enough that a captured token is useless shortly
// after.
const retryTokenTTL = 10 * time.Second

// AppendRetryPacket writes a Retry packet (RFC 9000 §17.2.5): dcid is the
// identifier to address it to (the client's own SCID from the Initial it
// answers), newSCID is the identifier the client should use as its DCID
// from here on, odcid is that Initial's destination identifier (needed only
// for the integrity tag, RFC 9001 §5.8 — it does not appear in the packet
// itself), and token is what AppendRetryToken produced.
func appendRetryPacket(dst, dcid, newSCID, odcid, token []byte) []byte {
	start := len(dst)
	// Long header, fixed bit set, type 11 (Retry). The four low bits are
	// unused: RFC 9000 §17.2.5 lets the server set them to an arbitrary
	// value and has the client ignore them, so zero is as valid as any.
	dst = append(dst, 0xf0, 0, 0, 0, 1, byte(len(dcid))) //nolint:gosec // G115: a connection ID is at most 20 bytes; Version1
	dst = append(dst, dcid...)
	dst = append(dst, byte(len(newSCID))) //nolint:gosec // G115: a connection ID is at most 20 bytes
	dst = append(dst, newSCID...)
	dst = append(dst, token...)
	tag := retryTag(odcid, dst[start:])
	return append(dst, tag[:]...)
}

// retryTokenKey is an HMAC-SHA256 key generated fresh for one [Listener]'s
// lifetime — see the package comment above for why that's the right amount
// of statefulness.
type retryTokenKey [32]byte

func newRetryTokenKey() (retryTokenKey, error) {
	var k retryTokenKey
	_, err := rand.Read(k[:])
	return k, err
}

// AppendRetryToken writes a token binding this Retry to the peer address and
// original destination identifier a later Initial must match, and to the
// server-chosen identifier (newSCID) that Initial's DCID must equal — the
// whole point being that verifying the token alone, with no connection
// table, recovers everything [newServerConn] needs to continue correctly as
// if this had been the client's first Initial.
func appendRetryToken(dst []byte, key retryTokenKey, peer net.Addr, odcid, newSCID []byte) []byte {
	return appendRetryTokenAt(dst, key, time.Now(), peer, odcid, newSCID)
}

// appendRetryTokenAt is AppendRetryToken with the issue time explicit, so a
// test can produce a genuinely encoded token that has already expired.
func appendRetryTokenAt(dst []byte, key retryTokenKey, issued time.Time, peer net.Addr, odcid, newSCID []byte) []byte {
	start := len(dst)
	dst = AppendVarint(dst, uint64(issued.Unix())) //nolint:gosec // G115: seconds since epoch, nowhere near overflowing uint64
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

// VerifyRetryToken checks a token a client echoed on its post-Retry Initial:
// the HMAC must verify, the token must not have expired, and it must name
// the peer address it is arriving from. On success it returns the original
// destination identifier and server-chosen identifier the token bound,
// which is everything [newServerConn] needs beyond what the Initial itself
// carries.
func verifyRetryToken(token []byte, key retryTokenKey, peer net.Addr) (retryToken, error) {
	const macLen = sha256.Size
	if len(token) < macLen {
		return retryToken{}, fmt.Errorf("%w: a Retry token shorter than its own MAC", ErrQUIC)
	}
	body, mac := token[:len(token)-macLen], token[len(token)-macLen:]
	want := hmac.New(sha256.New, key[:])
	want.Write(body)
	if subtle.ConstantTimeCompare(want.Sum(nil), mac) != 1 {
		return retryToken{}, fmt.Errorf("%w: a Retry token that does not verify", ErrQUIC)
	}

	issued, rest, err := Varint(body)
	if err != nil {
		return retryToken{}, err
	}
	if time.Since(time.Unix(int64(issued), 0)) > retryTokenTTL { //nolint:gosec // G115: a Unix second count from this process's own clock
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
