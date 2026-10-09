package quic

import (
	"fmt"
	"sort"
)

// Interval reassembly for one byte stream. STREAM and CRYPTO frames arrive
// with offsets, and real peers overlap them as a matter of course: a
// retransmission covers what an ACK had not confirmed, which the original
// delivery may since have made partly stale. A reassembler that only accepts
// exact-offset chunks parks such a frame forever and the stream wedges with
// its bytes already in hand.
//
// The rules: everything at or below what was already delivered is dropped,
// overlaps merge with what is held, and the first bytes seen for an offset
// win — a peer whose retransmissions disagree with themselves is not one to
// take corrections from. Bytes held are the caller's to bound (flow control
// does exactly that for streams); the chunk *count* is bounded here, because
// a peer sending one byte per frame pays its own bandwidth but would
// otherwise choose this endpoint's bookkeeping overhead.

// maxReassemblyChunks bounds how fragmented the parked bytes may be. A
// conformant sender fragments a few ways on retransmission; hundreds of
// disjoint slivers is a peer working at it.
const maxReassemblyChunks = 256

type rchunk struct {
	off  uint64
	data []byte
}

func (c rchunk) end() uint64 { return c.off + uint64(len(c.data)) }

// reassembly holds out-of-order bytes until the gap before them fills.
// The zero value is ready to use.
type reassembly struct {
	consumed uint64 // how far the delivered prefix reaches
	chunks   []rchunk
	held     int // bytes parked, kept so callers can observe the cost
}

// addThenTake files bytes at an offset and appends whatever is now
// contiguous with the delivered prefix to dst. In the common case — bytes
// arriving in order, nothing parked — the parked copy is skipped entirely
// and the delta comes straight off the frame.
func (r *reassembly) addThenTake(off uint64, data, dst []byte) ([]byte, error) {
	end := off + uint64(len(data))
	if end <= r.consumed || len(data) == 0 {
		return dst, nil // entirely stale, or nothing
	}
	if off <= r.consumed && (len(r.chunks) == 0 || r.chunks[0].off >= end) {
		delta := data[r.consumed-off:]
		dst = append(dst, delta...)
		r.consumed = end
		// The bytes may have landed flush against a parked chunk, which is
		// now contiguous too.
		return r.takeReady(dst), nil
	}
	if err := r.add(off, data); err != nil {
		return dst, err
	}
	return r.takeReady(dst), nil
}

// add files bytes at an offset, copying what it keeps — frames borrow their
// packet's buffer, and parked bytes outlive the packet.
func (r *reassembly) add(off uint64, data []byte) error {
	end := off + uint64(len(data))
	if end <= r.consumed || len(data) == 0 {
		return nil
	}
	if off < r.consumed {
		data = data[r.consumed-off:]
		off = r.consumed
	}

	// The window [i, j) of chunks the new bytes touch or overlap. Touching
	// counts: adjacent runs merge, which is what keeps the chunk count a
	// measure of gaps rather than of arrivals. Chunks are sorted, disjoint
	// and non-touching, so both bounds are binary searches.
	i := sort.Search(len(r.chunks), func(k int) bool { return r.chunks[k].end() >= off })
	j := i + sort.Search(len(r.chunks)-i, func(k int) bool { return r.chunks[i+k].off > end })

	switch {
	case i == j: // touches nothing: a new hole's worth of bytes
		r.chunks = append(r.chunks, rchunk{})
		copy(r.chunks[i+1:], r.chunks[i:])
		r.chunks[i] = rchunk{off: off, data: append([]byte(nil), data...)}
		r.held += len(data)
	case j == i+1 && off >= r.chunks[i].off:
		// One chunk, extended (if at all) only to the right: the shape of
		// in-order bytes queuing behind a hole. Growing it in place is
		// amortised O(len(data)); rebuilding it per frame was a full copy
		// of everything parked, quadratic in the bytes behind the gap.
		c := &r.chunks[i]
		if ce := c.end(); end > ce {
			c.data = append(c.data, data[ce-off:]...)
			r.held += int(end - ce) //nolint:gosec // G115: bounded by len(data)
		}
	default:
		lo := min(off, r.chunks[i].off)
		hi := max(end, r.chunks[j-1].end())
		merged := make([]byte, hi-lo)
		copy(merged[off-lo:], data)
		for _, c := range r.chunks[i:j] {
			// Held bytes win over the new ones where they disagree; see the
			// package comment on peers that contradict themselves.
			copy(merged[c.off-lo:], c.data)
			r.held -= len(c.data)
		}
		r.chunks[i] = rchunk{off: lo, data: merged}
		n := len(r.chunks)
		r.chunks = append(r.chunks[:i+1], r.chunks[j:]...)
		// The tail beyond the new length still holds the merged chunks'
		// data pointers in the backing array — retention, same reasoning
		// as ring.pop.
		clear(r.chunks[len(r.chunks):n])
		r.held += len(merged)
	}

	if len(r.chunks) > maxReassemblyChunks {
		// The chunk above is already inserted — r.chunks and r.held both
		// reflect it — before this is checked. Harmless today: every caller
		// closes the connection on this error, so the state this leaves
		// behind is never read again. Reusing a reassembly instance past a
		// returned error would need this re-examined.
		return fmt.Errorf("%w: stream bytes parked in over %d fragments", ErrQUIC, maxReassemblyChunks)
	}
	return nil
}

// takeReady appends the newly contiguous prefix to dst and advances past it.
func (r *reassembly) takeReady(dst []byte) []byte {
	for len(r.chunks) > 0 && r.chunks[0].off <= r.consumed {
		c := r.chunks[0]
		if c.end() > r.consumed {
			dst = append(dst, c.data[r.consumed-c.off:]...)
			r.consumed = c.end()
		}
		r.held -= len(c.data)
		// Zero the popped slot: the backing array would otherwise keep the
		// chunk's data reachable — ring.pop and compact clear theirs.
		r.chunks[0] = rchunk{}
		r.chunks = r.chunks[1:]
	}
	return dst
}
