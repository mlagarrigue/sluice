package quic

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/subtle"
	"crypto/tls"
	"fmt"
	"hash"

	"golang.org/x/crypto/chacha20"
	"golang.org/x/crypto/chacha20poly1305"
)

// Packet protection, RFC 9001 §5.
//
// Nothing cryptographic is written here. The key schedule is HKDF from
// [crypto/hkdf] with the labels §5.1 publishes, the packet protection is
// the negotiated suite's AEAD (AES-GCM from [crypto/cipher] or
// ChaCha20-Poly1305 from golang.org/x/crypto), and header protection is one
// block of that suite's cipher over a sample of the ciphertext. What this
// file contributes is the order
// the pieces go in, which is the part that can be got wrong and the part a
// test can check.

// initialSalt is RFC 9001 §5.2's salt for QUIC version 1. It is a published
// constant and not a secret: its job is to separate this protocol's key
// schedule from every other one that starts from a connection identifier.
var initialSalt = []byte{
	0x38, 0x76, 0x2c, 0xf7, 0xf5, 0x59, 0x34, 0xb3, 0x4d, 0x17,
	0x9a, 0xe6, 0xa4, 0xc8, 0x0c, 0xad, 0xcc, 0xbb, 0x7f, 0x0a,
}

// Secrets are one direction's Initial keys.
type secrets struct {
	Client []byte
	Server []byte
}

// InitialSecrets derives both directions' Initial secrets from the
// destination connection identifier the client chose (RFC 9001 §5.2).
//
// They need no handshake, which is what makes an Initial packet readable by
// anyone who saw the first one: Initial protection authenticates a packet
// against tampering in flight, and hides nothing from an observer. Treating
// it as confidentiality is the mistake this comment exists to prevent.
func initialSecrets(dcid []byte) (secrets, error) {
	initial, err := hkdf.Extract(sha256.New, dcid, initialSalt)
	if err != nil {
		return secrets{}, fmt.Errorf("quic: extracting the initial secret: %w", err)
	}
	client, err := expandLabel(initial, "client in", 32)
	if err != nil {
		return secrets{}, err
	}
	server, err := expandLabel(initial, "server in", 32)
	if err != nil {
		return secrets{}, err
	}
	return secrets{Client: client, Server: server}, nil
}

// expandLabel is TLS 1.3's HKDF-Expand-Label (RFC 8446 §7.1) over SHA-256,
// which the Initial keys always use (RFC 9001 §5.2) whatever suite the
// handshake goes on to pick. The "tls13 " prefix is part of the label rather
// than decoration: it is what keeps these keys from colliding with a TLS
// connection's own.
func expandLabel(secret []byte, label string, length int) ([]byte, error) {
	return expandLabelHash(sha256.New, secret, label, length)
}

func expandLabelHash(h func() hash.Hash, secret []byte, label string, length int) ([]byte, error) {
	full := "tls13 " + label
	info := make([]byte, 0, 4+len(full))
	info = append(info, byte(length>>8), byte(length), byte(len(full))) //nolint:gosec // G115: big-endian length of a short HKDF label
	info = append(info, full...)
	info = append(info, 0) // an empty context
	out, err := hkdf.Expand(h, secret, string(info), length)
	if err != nil {
		return nil, fmt.Errorf("quic: expanding %q: %w", label, err)
	}
	return out, nil
}

// suite is what a TLS 1.3 cipher suite decides about packet protection: the
// hash the key schedule runs on, the width of the packet key, how the AEAD
// and the header protector are built from their keys, and RFC 9001 §6.6's
// usage limits for that AEAD.
//
// The AES suites come whole from the standard library.
// TLS_CHACHA20_POLY1305_SHA256 cannot (crypto/tls uses the cipher
// internally without exporting it), so its AEAD and raw cipher come from
// golang.org/x/crypto — the module's one production dependency, taken for
// this and nothing else.
type suite struct {
	hash      func() hash.Hash
	keyLen    int
	aead      func(key []byte) (cipher.AEAD, error)
	hp        func(key []byte) (headerProtector, error)
	confLimit uint64 // packets sealed under one key (§6.6)
	intLimit  uint64 // failed opens tolerated under one key (§6.6)
}

// aes128Suite also protects the Initial space, whatever the handshake goes
// on to negotiate (RFC 9001 §5.2).
var aes128Suite = suite{
	hash: sha256.New, keyLen: 16, aead: aesGCM, hp: newAESProtector,
	confLimit: 1 << 23, intLimit: 1 << 52,
}

func suiteFor(id uint16) (suite, error) {
	switch id {
	case tls.TLS_AES_128_GCM_SHA256:
		return aes128Suite, nil
	case tls.TLS_AES_256_GCM_SHA384:
		return suite{
			hash: sha512.New384, keyLen: 32, aead: aesGCM, hp: newAESProtector,
			confLimit: 1 << 23, intLimit: 1 << 52,
		}, nil
	case tls.TLS_CHACHA20_POLY1305_SHA256:
		// §6.6: ChaCha20-Poly1305's confidentiality bound exceeds the 2^62-1
		// packet-number ceiling, so the packet number is the real limit; its
		// integrity limit is 2^36, far below AES-GCM's.
		return suite{
			hash: sha256.New, keyLen: 32, aead: chachaAEAD, hp: newChaChaProtector,
			confLimit: 1 << 62, intLimit: 1 << 36,
		}, nil
	}
	return suite{}, fmt.Errorf("%w: cipher suite %#x is not one this package protects packets with", ErrQUIC, id)
}

// aesGCM is the packet AEAD both AES suites share.
func aesGCM(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("quic: the packet key: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("quic: the AEAD: %w", err)
	}
	return aead, nil
}

// chachaAEAD is the transplanted ChaCha20-Poly1305 (RFC 8439) as the packet
// AEAD for TLS_CHACHA20_POLY1305_SHA256.
func chachaAEAD(key []byte) (cipher.AEAD, error) {
	aead, err := chacha20poly1305.New(key)
	if err != nil {
		return nil, fmt.Errorf("quic: the packet key: %w", err)
	}
	return aead, nil
}

// headerProtector turns the sixteen-byte sample of ciphertext into the mask
// that hides a packet's first byte and packet number (RFC 9001 §5.4). Only
// the mask's first five bytes are ever applied; the array keeps the two
// implementations interchangeable at the call sites.
type headerProtector interface {
	mask(sample []byte) [16]byte
}

// aesProtector is §5.4.3: one AES-ECB block over the sample.
type aesProtector struct{ block cipher.Block }

func newAESProtector(key []byte) (headerProtector, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("quic: the header-protection key: %w", err)
	}
	return aesProtector{block: block}, nil
}

func (p aesProtector) mask(sample []byte) [16]byte {
	var m [16]byte
	p.block.Encrypt(m[:], sample)
	return m
}

// chachaProtector is §5.4.4: the sample's first four bytes are a
// little-endian block counter, its last twelve the nonce, and the mask is
// the first five bytes of the raw ChaCha20 keystream they select.
type chachaProtector struct{ key []byte }

func newChaChaProtector(key []byte) (headerProtector, error) {
	if len(key) != chacha20.KeySize {
		return nil, fmt.Errorf("quic: a %d-byte ChaCha20 header-protection key", len(key))
	}
	return chachaProtector{key: append([]byte(nil), key...)}, nil
}

func (p chachaProtector) mask(sample []byte) [16]byte {
	var m [16]byte
	c, err := chacha20.NewUnauthenticatedCipher(p.key, sample[4:16])
	if err != nil {
		// The key length was proven at construction and the nonce length by
		// the caller's sixteen-byte sample; there is no third failure.
		panic("quic: chacha20 header protection: " + err.Error())
	}
	c.SetCounter(uint32(sample[0]) | uint32(sample[1])<<8 | uint32(sample[2])<<16 | uint32(sample[3])<<24)
	c.XORKeyStream(m[:5], m[:5])
	return m
}

// keys are one direction's packet-protection material, with the §6.6 usage
// limits of the AEAD they were built for. Confidentiality: packets sealed
// under one key — past it, an attacker's distinguishing advantage stops
// being negligible; a 1-RTT sender rotates to the next key before reaching
// it (§6), the other levels end the connection with AEAD_LIMIT_REACHED.
// Integrity: forgery attempts (failed opens) tolerated across the
// connection, which ends it with the same code.
type keys struct {
	aead      cipher.AEAD
	iv        []byte
	hp        headerProtector
	confLimit uint64
	intLimit  uint64
	// secret is what aead and iv were expanded from, kept only so the next
	// phase can be derived from it (§6.1); nil for the Initial level, which
	// never rotates.
	secret []byte
	s      suite
}

// next derives the following key phase (RFC 9001 §6.1): the new secret is
// HKDF-Expand-Label of the current one with the label "quic ku", the key
// and IV come from it as in §5.1, and the header-protection key is kept —
// the phase bit and packet number stay readable to whoever holds the old
// keys, which is what lets a receiver pick keys before it has used any.
func (k keys) next() (keys, error) {
	secret, err := expandLabelHash(k.s.hash, k.secret, "quic ku", k.s.hash().Size())
	if err != nil {
		return keys{}, err
	}
	n, err := newSuiteKeys(secret, k.s)
	if err != nil {
		return keys{}, err
	}
	n.hp = k.hp
	return n, nil
}

func newKeys(secret []byte) (keys, error) {
	return newSuiteKeys(secret, aes128Suite)
}

func newSuiteKeys(secret []byte, s suite) (keys, error) {
	key, err := expandLabelHash(s.hash, secret, "quic key", s.keyLen)
	if err != nil {
		return keys{}, err
	}
	iv, err := expandLabelHash(s.hash, secret, "quic iv", 12)
	if err != nil {
		return keys{}, err
	}
	hpKey, err := expandLabelHash(s.hash, secret, "quic hp", s.keyLen)
	if err != nil {
		return keys{}, err
	}
	aead, err := s.aead(key)
	if err != nil {
		return keys{}, err
	}
	hp, err := s.hp(hpKey)
	if err != nil {
		return keys{}, err
	}
	return keys{
		aead: aead, iv: iv, hp: hp, confLimit: s.confLimit, intLimit: s.intLimit,
		secret: append([]byte(nil), secret...), s: s,
	}, nil
}

// Sealer protects packets in one direction.
type packetSealer struct {
	k keys
	// sealed counts packets protected under this key, against
	// the key's own §6.6 confidentiality limit (keys.confLimit). Guarded by
	// the connection's lock, like
	// every call to Seal this package makes.
	sealed uint64
}

// Opener unprotects packets in one direction.
type packetOpener struct {
	k keys
	// failed counts packets that did not authenticate under this key,
	// against the key's own §6.6 integrity limit (keys.intLimit) — the one
	// place unauthenticated bytes
	// are allowed to end a connection, because §6.6 says the key is what
	// they wear out. Guarded by the connection's lock.
	failed uint64
	// phase is the 1-RTT opener's key-update state (RFC 9001 §6); nil at
	// the Initial and Handshake levels, whose keys never rotate. It is
	// immutable once built: a rotation replaces the whole opener under the
	// connection's lock, so openPhased reads it without one.
	phase *keyPhase
}

// keyPhase is what a 1-RTT receiver keeps beside its current keys to read
// across a key update (RFC 9001 §6.3, §6.5): the phase bit the current keys
// answer to, the next phase's keys derived ahead of time (so deriving them
// on demand does not leak when a rotation happened, §6.3), and the previous
// phase's keys while delayed packets may still arrive under them.
type keyPhase struct {
	bit  byte  // 0 or keyPhaseBit: what packets under the current keys carry
	next keys  // the following phase, derived ahead
	prev *keys // the previous phase, or nil once dropped (§6.5)
	// lowest is the smallest packet number opened under the current keys,
	// and hasLowest whether any was: a differing phase bit below it means
	// the previous keys, above it the next (§6.5).
	lowest    uint64
	hasLowest bool
}

// keyPhaseBit is the Key Phase bit of a short header (RFC 9000 §17.3.1),
// protected along with the packet number (RFC 9001 §5.4.1).
const keyPhaseBit = 0x04

// keyUse says which of a 1-RTT opener's three key sets authenticated a
// packet; the current keys for the other levels.
type keyUse uint8

const (
	usedCurrent keyUse = iota
	usedPrevious
	usedNext
)

// rotated is the opener that replaces o once a packet authenticated under
// the next keys at packet number pn (RFC 9001 §6.2): the current keys
// become the previous, the next the current, and a further next is derived
// so the following rotation finds it ready. The forgery count carries
// over, as §6.6 counts failures across the connection.
func (o *packetOpener) rotated(pn uint64) (*packetOpener, error) {
	cur := o.k
	next, err := o.phase.next.next()
	if err != nil {
		return nil, err
	}
	return &packetOpener{
		k:      o.phase.next,
		failed: o.failed,
		phase: &keyPhase{
			bit: o.phase.bit ^ keyPhaseBit, next: next, prev: &cur,
			lowest: pn, hasLowest: true,
		},
	}, nil
}

// withoutPrevious is o with its previous phase's keys dropped (§6.5).
func (o *packetOpener) withoutPrevious() *packetOpener {
	p := *o.phase
	p.prev = nil
	return &packetOpener{k: o.k, failed: o.failed, phase: &p}
}

// NewSealer and NewOpener build one direction's Initial protection from its
// secret. Initial keys are always AES-128-GCM under SHA-256 (RFC 9001 §5.2);
// the handshake and application levels follow the negotiated suite and are
// built internally from the suite the TLS stack reports.
func newPacketSealer(secret []byte) (*packetSealer, error) {
	k, err := newKeys(secret)
	return &packetSealer{k: k}, err
}

func newPacketOpener(secret []byte) (*packetOpener, error) {
	k, err := newKeys(secret)
	return &packetOpener{k: k}, err
}

func newSealerSuite(secret []byte, s suite) (*packetSealer, error) {
	k, err := newSuiteKeys(secret, s)
	return &packetSealer{k: k}, err
}

func newOpenerSuite(secret []byte, s suite) (*packetOpener, error) {
	k, err := newSuiteKeys(secret, s)
	return &packetOpener{k: k}, err
}

// newPhasedOpener is the 1-RTT opener: the first phase's keys with the
// next phase already derived (RFC 9001 §6.3).
func newPhasedOpener(secret []byte, s suite) (*packetOpener, error) {
	o, err := newOpenerSuite(secret, s)
	if err != nil {
		return nil, err
	}
	next, err := o.k.next()
	if err != nil {
		return nil, err
	}
	o.phase = &keyPhase{next: next}
	return o, nil
}

// Retry integrity (RFC 9001 §5.8): a Retry packet carries a sixteen-byte tag
// computed with a published key over the packet and the connection identifier
// the client first chose. The key is public, so the tag is not a secret — it
// proves the Retry's author saw the client's Initial, which an off-path
// attacker did not.
var (
	retryKey   = []byte{0xbe, 0x0c, 0x69, 0x0b, 0x9f, 0x66, 0x57, 0x5a, 0x1d, 0x76, 0x6b, 0x54, 0xe3, 0x68, 0xc8, 0x4e}
	retryNonce = []byte{0x46, 0x15, 0x99, 0xd3, 0x5d, 0x63, 0x2b, 0xf2, 0x23, 0x98, 0x25, 0xbb}
)

// retryTag computes the sixteen-byte integrity tag RFC 9001 §5.8 defines for
// a Retry packet, given the packet up to but not including its tag and the
// destination connection identifier of the Initial it answers.
//
// The same call generates and verifies: GCM-sealing an empty plaintext
// yields exactly the tag, whether this end is the one writing a Retry or the
// one checking somebody else's. Returns a zero tag (which will not match a
// real one) if odcid is too long to have been a real connection identifier.
func retryTag(odcid, packetWithoutTag []byte) [16]byte {
	var tag [16]byte
	if len(odcid) > maxConnectionIDLen {
		return tag
	}
	block, err := aes.NewCipher(retryKey)
	if err != nil {
		return tag
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return tag
	}
	pseudo := make([]byte, 0, 1+len(odcid)+len(packetWithoutTag))
	pseudo = append(pseudo, byte(len(odcid))) //nolint:gosec // G115: a connection ID is at most 20 bytes
	pseudo = append(pseudo, odcid...)
	pseudo = append(pseudo, packetWithoutTag...)
	copy(tag[:], aead.Seal(nil, retryNonce, nil, pseudo))
	return tag
}

// verifyRetryTag reports whether a Retry packet's integrity tag is the one
// RFC 9001 §5.8 computes for it, given the destination connection identifier
// of the Initial packet the Retry answers.
func verifyRetryTag(odcid, packet []byte) bool {
	if len(packet) < 16 {
		return false
	}
	tag := retryTag(odcid, packet[:len(packet)-16])
	return subtle.ConstantTimeCompare(tag[:], packet[len(packet)-16:]) == 1
}

// nonce is the IV exclusive-ored with the packet number, right-aligned
// (RFC 9001 §5.3). Reusing a nonce under one key is what breaks GCM
// completely, and this is the only thing standing between the protocol and
// that: the packet number must never repeat.
func (k keys) nonce(pn uint64) []byte {
	out := make([]byte, len(k.iv))
	copy(out, k.iv)
	for i := range 8 {
		out[len(out)-1-i] ^= byte(pn >> (8 * i)) //nolint:gosec // G115: keeps the low octet of each shift
	}
	return out
}

// Seal protects a packet in place.
//
// header is everything up to and including the packet number, and is the
// associated data: it is authenticated but not encrypted, which is what lets
// a router read a connection identifier without holding a key. pnLen and
// pnOffset say where the packet number sits, for the header protection
// applied last.
//
// The order matters and is the whole of §5.4: the payload is encrypted first,
// and the header is protected using a sample of that ciphertext. Doing it the
// other way round samples bytes the receiver cannot reproduce.
func (s *packetSealer) Seal(dst, header, payload []byte, pn uint64, pnOffset, pnLen int) ([]byte, error) {
	if pnLen < 1 || pnLen > 4 {
		return nil, fmt.Errorf("%w: a packet-number length of %d", ErrQUIC, pnLen)
	}
	if pnOffset+pnLen != len(header) {
		return nil, fmt.Errorf("%w: the header ends %d bytes past its packet number", ErrQUIC, len(header)-pnOffset-pnLen)
	}
	// dst may already hold other packets of the same datagram (RFC 9000
	// §12.2 coalescing): everything below indexes from where this one
	// starts, never from the buffer's start.
	base := len(dst)
	out := append(dst, header...)
	out = s.k.aead.Seal(out, s.k.nonce(pn), payload, header)
	s.sealed++
	pkt := out[base:]

	// The sample starts four bytes after the packet number begins, so that a
	// packet number of any length is covered by the same sixteen bytes — the
	// receiver does not know the length until it has removed the protection.
	sampleAt := pnOffset + 4
	if sampleAt+16 > len(pkt) {
		return nil, fmt.Errorf("%w: too short to sample for header protection", ErrQUIC)
	}
	mask := s.k.hp.mask(pkt[sampleAt : sampleAt+16])
	maskFirstByte(pkt, mask)
	maskPacketNumber(pkt, mask, pnOffset, pnLen)
	return out, nil
}

// Open unprotects a packet in place and returns its payload and packet
// number.
//
// packet is one whole packet, header protection still applied. dcidLen is
// needed only for short headers, where the connection identifier has no
// length on the wire. largestPN is the largest packet number already
// received in this space, which is what the truncated number on the wire is
// reconstructed against.
func (o *packetOpener) Open(packet []byte, pnOffset int, largestPN uint64) (payload []byte, pn uint64, err error) {
	payload, pn, _, err = o.openPhased(packet, pnOffset, largestPN)
	return payload, pn, err
}

// openPhased is Open that also reports which key set authenticated the
// packet. At the 1-RTT level the choice follows RFC 9001 §6.5: the Key
// Phase bit, once unprotected, names the current keys or the other phase,
// and for the other phase the packet number says whether that is the
// previous one (below everything seen under the current keys) or the next.
// The keys are chosen before any AEAD runs, and a wrong phase bit fails
// the same way as any forgery — the timing §6.3 asks for.
func (o *packetOpener) openPhased(packet []byte, pnOffset int, largestPN uint64) (payload []byte, pn uint64, used keyUse, err error) {
	sampleAt := pnOffset + 4
	if sampleAt+16 > len(packet) {
		return nil, 0, usedCurrent, ErrTruncated
	}
	// Copied rather than modified: the caller's datagram may hold other
	// packets, a failed authentication must leave it as it was (GCM's Open
	// zeroes what it wrote on failure), and the frames delivered out of the
	// payload keep referencing this buffer after the call. Measured before
	// anyone redesigns that ownership: the copy is ~12% of the receive path
	// (BenchmarkConnReceivePacket, 2026-10-05: 570ms of the 4.9s under
	// receive, once the peer's seal was moved out of the timed loop; the
	// earlier ~4% was diluted by that seal), still less than the AEAD open
	// itself (750ms).
	buf := append([]byte(nil), packet...)

	// The mask is computed once and applied in two steps, because the first
	// step is what reveals how long the second one is: the packet-number
	// length lives in the two low bits of the first byte, and those bits are
	// themselves protected.
	mask := o.k.hp.mask(buf[sampleAt : sampleAt+16])
	maskFirstByte(buf, mask)
	pnLen := int(buf[0]&0x03) + 1
	maskPacketNumber(buf, mask, pnOffset, pnLen)

	var truncated uint64
	for i := range pnLen {
		truncated = truncated<<8 | uint64(buf[pnOffset+i])
	}
	pn = decodePacketNumber(largestPN, truncated, pnLen)

	k := o.k
	if p := o.phase; p != nil && buf[0]&0x80 == 0 && buf[0]&keyPhaseBit != p.bit {
		switch {
		case p.prev != nil && p.hasLowest && pn < p.lowest:
			k, used = *p.prev, usedPrevious
		default:
			k, used = p.next, usedNext
		}
	}
	header := buf[:pnOffset+pnLen]
	body := buf[pnOffset+pnLen:]
	payload, err = k.aead.Open(body[:0], k.nonce(pn), body, header)
	if err != nil {
		return nil, 0, usedCurrent, fmt.Errorf("%w: the packet did not authenticate", ErrQUIC)
	}
	return payload, pn, used, nil
}

// maskFirstByte covers the bits of the first byte that are not needed to
// route the packet: four in a long header, five in a short one — the extra
// being the key phase, which would otherwise tell an observer when keys
// rotate. It is its own inverse.
func maskFirstByte(buf []byte, mask [16]byte) {
	if buf[0]&0x80 != 0 {
		buf[0] ^= mask[0] & 0x0f
	} else {
		buf[0] ^= mask[0] & 0x1f
	}
}

// maskPacketNumber covers exactly the bytes the packet number occupies, which
// is why it is applied after the first byte has revealed how many those are.
func maskPacketNumber(buf []byte, mask [16]byte, pnOffset, pnLen int) {
	for i := range pnLen {
		buf[pnOffset+i] ^= mask[1+i]
	}
}

// decodePacketNumber recovers the full number from the truncated one on the
// wire, choosing the candidate closest to what is expected (RFC 9000 §A.3).
//
// The wire carries one to four bytes of a sixty-two-bit number, so the
// receiver has to guess the rest — and guessing wrong is not a wrong number,
// it is a wrong nonce and a packet that will not authenticate.
func decodePacketNumber(largest, truncated uint64, pnLen int) uint64 {
	bits := uint(pnLen * 8)
	window := uint64(1) << bits
	halfWindow := window / 2
	expected := largest + 1
	candidate := (expected &^ (window - 1)) | truncated
	if candidate+halfWindow <= expected && candidate+window < 1<<62 {
		return candidate + window
	}
	if candidate > expected+halfWindow && candidate >= window {
		return candidate - window
	}
	return candidate
}
