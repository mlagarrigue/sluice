package quic

import (
	"fmt"
	"math/bits"
)

// maxCryptoBuffer bounds how far past the delivered prefix CRYPTO data may
// reach, per space. Stream reassembly is bounded by flow control; CRYPTO has
// no flow control (RFC 9000 §7.5 says bound it anyway, and this is the
// customary bound). A window rather than a count of parked bytes: it bounds
// those too — every parked byte lies inside it — and it also refuses the
// byte parked far ahead, which no handshake could ever reach and which would
// otherwise hold the connection open until its handshake timeout.
const maxCryptoBuffer = 64 << 10

// cryptoRingMin is the ring a space's first out-of-order CRYPTO byte gets:
// a TLS flight of a few packets fits, and the ring doubles up to
// maxCryptoBuffer for one that does not.
const cryptoRingMin = 4 << 10

// The CRYPTO storage of reassembly. RFC 9000 §7.5: an
// endpoint "MUST support buffering at least 4096 bytes of data received in
// out-of-order CRYPTO frames", however they are cut, and a peer may cut them
// a byte per frame. The stream reassembly keeps a sorted table of runs,
// which a byte filed in front of the others shifts by one slot each: with
// its fragment bound raised to 4096 for CRYPTO, 4096 one-byte fragments sent
// from the highest offset down moved 8 million 64-byte slots, 1.2 MB and
// 5 ms per space, on the Initial space before any address validation.
//
// So CRYPTO bytes park in place instead: a ring the size of the window
// still to fill, a byte at offset o in slot o mod its length, and a bitmap
// of the slots that hold one. Filing a frame and taking what it completes
// cost the frame's bytes, whatever is parked and however it was cut; the
// only bound is the window, which also bounds the memory — maxCryptoBuffer
// and an eighth of it for the bitmap, per space. The rules are the stream
// reassembly's: bytes at or below the delivered prefix are dropped, and the
// first bytes seen for an offset win.
//
// The ring allocates nothing until a byte arrives out of order, and is let
// go once nothing is parked in it: the Application space's, never released
// with its keys, kept up to 72 KiB per connection for a reordering the
// handshake had long since resolved.

// addThenTakeWithin files CRYPTO bytes at an offset and appends whatever is
// now contiguous with the delivered prefix to dst. Bytes reaching past
// window close the connection with CRYPTO_BUFFER_EXCEEDED, §7.5's code.
func (r *reassembly) addThenTakeWithin(window, off uint64, data, dst []byte) ([]byte, error) {
	if end := off + uint64(len(data)); end > r.consumed+window && len(data) > 0 {
		return dst, &transportError{
			code: transportCryptoBufferExceeded,
			err:  fmt.Errorf("%w: handshake data over %d bytes past what was delivered", ErrQUIC, window),
		}
	}
	off, data, ok := r.fresh(off, data)
	if !ok {
		return dst, nil
	}
	end := off + uint64(len(data))
	if off == r.consumed && r.held == 0 {
		// In order with nothing parked, the common case: no copy kept.
		r.consumed = end
		return append(dst, data...), nil
	}
	r.grow(end - r.consumed)
	r.park(off, data)
	dst = r.takeRing(dst)
	if r.held == 0 {
		r.ring, r.have = nil, nil
	}
	return dst, nil
}

// grow makes the ring hold at least need bytes past the delivered prefix.
// It grows a doubling at a time from cryptoRingMin, so a reordering moves
// its parked bytes at most four times.
func (r *reassembly) grow(need uint64) {
	if need <= uint64(len(r.ring)) {
		return
	}
	n := max(cryptoRingMin, len(r.ring))
	for uint64(n) < need {
		n *= 2
	}
	ring, have := make([]byte, n), make([]uint64, n/64)
	if r.held > 0 {
		// Both lengths are multiples of 64, so an offset has the same place
		// in its bitmap word in either ring, and the window moves a word
		// and its 64 bytes at a time; bytes under a clear bit are left
		// over, never read. Unless consumed is aligned, the window spans
		// one word more than it holds, whose bits belong to its two ends:
		// each part goes to its own word of the new ring, at least twice as
		// long as the old, so the window never folds onto itself there.
		oldLen := uint64(len(r.ring))
		lo, hi := r.consumed, r.consumed+oldLen
		for a := lo &^ 63; a < hi; a += 64 {
			m := ^uint64(0)
			if a < lo {
				m <<= lo - a
			}
			if a+64 > hi {
				m &= ^uint64(0) >> (a + 64 - hi)
			}
			oi, ni := a&(oldLen-1), a&uint64(n-1) //nolint:gosec // G115: n is a power of two, at most maxCryptoBuffer
			have[ni/64] |= r.have[oi/64] & m
			copy(ring[ni:ni+64], r.ring[oi:oi+64])
		}
	}
	r.ring, r.have = ring, have
}

// park writes the bytes of data whose slots are free, a bitmap word at a
// time; the ring already reaches past the end of data.
func (r *reassembly) park(off uint64, data []byte) {
	mask := uint64(len(r.ring) - 1) //nolint:gosec // G115: grow made the ring at least cryptoRingMin long
	for len(data) > 0 {
		i := off & mask
		// One word at most: the ring's length is a multiple of 64, so a
		// word never straddles its end.
		n := min(uint64(len(data)), 64-i%64)
		span := (^uint64(0) >> (64 - n)) << (i % 64)
		w := &r.have[i/64]
		if free := span &^ *w; free == span {
			copy(r.ring[i:i+n], data[:n])
			r.held += int(n) //nolint:gosec // G115: n is at most 64
		} else {
			for free != 0 {
				k := uint64(bits.TrailingZeros64(free)) //nolint:gosec // G115: a bit index, below 64
				free &= free - 1
				r.ring[i-i%64+k] = data[k-i%64]
				r.held++
			}
		}
		*w |= span
		off += n
		data = data[n:]
	}
}

// takeRing appends the parked bytes contiguous with the delivered prefix to dst
// and advances past them, clearing their slots.
func (r *reassembly) takeRing(dst []byte) []byte {
	mask := uint64(len(r.ring) - 1) //nolint:gosec // G115: the ring's length is a positive power of two
	for r.held > 0 {
		i := r.consumed & mask
		// The run of set bits from slot i, up to the end of its word: the
		// bits shifted in from above are zeros, which stop the count.
		n := uint64(bits.TrailingZeros64(^(r.have[i/64] >> (i % 64)))) //nolint:gosec // G115: a bit count, at most 64
		if n == 0 {
			break
		}
		dst = append(dst, r.ring[i:i+n]...)
		r.have[i/64] &^= (^uint64(0) >> (64 - n)) << (i % 64)
		r.consumed += n
		r.held -= int(n) //nolint:gosec // G115: n is at most 64
	}
	return dst
}
