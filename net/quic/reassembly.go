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

// The chunk bound grows with the bytes held. A conformant sender leaves a
// hole per packet lost ahead of packets that arrived, and a drop-tail queue
// that overflows under a window-doubling sender loses every other packet of
// a round: measured on the DeepBuffer path under Reno, a 1.4 MB flight left
// 257 holes, each between full packets — a fixed bound of 256 closed a
// healthy connection. So the bound is one chunk per reassemblyChunkBytes
// held, below the smallest datagram a path carries (RFC 9000 §14: 1200),
// with a floor for a stream that holds little — hundreds of disjoint
// slivers is a peer working at it — and a ceiling, because every insert
// moves the slice's tail: the ceiling caps that move at half a megabyte.
const (
	reassemblyChunkFloor   = 256
	reassemblyChunkCeiling = 1 << 14
	reassemblyChunkBytes   = 1024
)

type rchunk struct {
	off  uint64
	data []byte
	// buf is data's allocation from its start, and head the room in it
	// before data[0]: data is buf[head:]. The room is zero until bytes land
	// in front of the chunk (see prepend).
	buf  []byte
	head int
}

func (c rchunk) end() uint64 { return c.off + uint64(len(c.data)) }

// newChunk parks a copy of p at off: frames borrow their packet's buffer,
// and parked bytes outlive the packet.
func newChunk(off uint64, p []byte) rchunk {
	b := append([]byte(nil), p...)
	return rchunk{off: off, data: b, buf: b}
}

// appendRight extends c with p, amortised O(len(p)) as append is.
func (c *rchunk) appendRight(p []byte) {
	c.buf = append(c.buf, p...)
	c.data = c.buf[c.head:]
}

// prepend extends c leftward with p, into the room kept in front of its
// data. With too little room the chunk moves to an allocation with as much
// room in front as it will then hold, so frames walking backwards from a run
// cost amortised O(len(p)) each, as appendRight's growth does on the right;
// rebuilding the run around each one was a copy of everything parked.
func (c *rchunk) prepend(p []byte) {
	if c.head < len(p) {
		room := len(p) + len(c.data)
		nb := make([]byte, room+len(c.data))
		copy(nb[room:], c.data)
		c.buf, c.head = nb, room
	}
	c.head -= len(p)
	copy(c.buf[c.head:], p)
	c.data = c.buf[c.head:]
	c.off -= uint64(len(p))
}

// reassembly holds out-of-order bytes until the gap before them fills.
// The zero value is ready to use.
type reassembly struct {
	consumed uint64 // how far the delivered prefix reaches
	chunks   []rchunk
	held     int // bytes parked, kept so callers can observe the cost
}

// chunkLimit is how many chunks r may hold at its current size.
func (r *reassembly) chunkLimit() int {
	return min(max(r.held/reassemblyChunkBytes, reassemblyChunkFloor), reassemblyChunkCeiling)
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

	if i == j { // touches nothing: a new hole's worth of bytes
		r.chunks = append(r.chunks, rchunk{})
		copy(r.chunks[i+1:], r.chunks[i:])
		r.chunks[i] = newChunk(off, data)
		r.held += len(data)
	} else if c := &r.chunks[i]; j == i+1 && off >= c.off {
		// One chunk, extended (if at all) only to the right: the shape of
		// in-order bytes queuing behind a hole, kept off merge's general
		// path. Growing it in place is amortised O(len(data)); rebuilding it
		// per frame was a full copy of everything parked, quadratic in the
		// bytes behind the gap.
		if ce := c.end(); end > ce {
			c.appendRight(data[ce-off:])
			r.held += int(end - ce) //nolint:gosec // G115: bounded by len(data)
		}
	} else {
		r.merge(i, j, off, data)
	}

	if limit := r.chunkLimit(); len(r.chunks) > limit {
		// The chunk above is already inserted — r.chunks and r.held both
		// reflect it — before this is checked. Harmless today: every caller
		// closes the connection on this error, so the state this leaves
		// behind is never read again. Reusing a reassembly instance past a
		// returned error would need this re-examined.
		// Closed with INTERNAL_ERROR, stated rather than defaulted to: the
		// bound is this end refusing a bookkeeping cost the peer imposes,
		// not a violation on the peer's part — fragmenting is legal — and
		// RFC 9000 §20.1 offers no other code for "cannot continue". The
		// reason travels with it, for the peer's log. Written up in
		// docs/guide/limits.md and ADR 0015.
		return &transportError{
			code: transportInternalError,
			err:  fmt.Errorf("%w: stream bytes parked in over %d fragments", ErrQUIC, limit),
		}
	}
	return nil
}

// merge files data at off into the chunks [i, j) it touches or overlaps, which
// become one. The result grows out of the largest of them, in place —
// prepend on the left, appendRight on the right — so the work is the size of
// everything but that chunk: rebuilding the run around each frame was a copy
// of all of it, which a peer could ask for with one byte in front of a
// parked run, or with a one-byte hole opened before it and filled, two
// frames at a time. Held bytes win over the new ones where they disagree;
// see the package comment on peers that contradict themselves.
func (r *reassembly) merge(i, j int, off uint64, data []byte) {
	end := off + uint64(len(data))
	big := i
	for k := i + 1; k < j; k++ {
		if len(r.chunks[k].data) > len(r.chunks[big].data) {
			big = k
		}
	}
	lo, hi := min(off, r.chunks[i].off), max(end, r.chunks[j-1].end())
	b := r.chunks[big]
	bOff, bEnd := b.off, b.end()
	for _, c := range r.chunks[i:j] {
		r.held -= len(c.data)
	}
	if lo < bOff {
		var left []byte
		if j == i+1 {
			left = data[:bOff-off] // nothing else lies in front of the one chunk
		} else {
			left = make([]byte, bOff-lo)
			if off < bOff {
				copy(left[off-lo:], data[:min(end, bOff)-off])
			}
			for _, c := range r.chunks[i:big] {
				copy(left[c.off-lo:], c.data)
			}
		}
		b.prepend(left)
	}
	if hi > bEnd {
		var right []byte
		if j == i+1 {
			right = data[bEnd-off:] // in-order bytes queuing behind a hole
		} else {
			right = make([]byte, hi-bEnd)
			if end > bEnd {
				from := max(off, bEnd)
				copy(right[from-bEnd:], data[from-off:])
			}
			for _, c := range r.chunks[big+1 : j] {
				copy(right[c.off-bEnd:], c.data)
			}
		}
		b.appendRight(right)
	}
	r.chunks[i] = b
	if j > i+1 {
		n := len(r.chunks)
		r.chunks = append(r.chunks[:i+1], r.chunks[j:]...)
		// The tail beyond the new length still holds the merged chunks'
		// data pointers in the backing array — retention, same reasoning as
		// ring.pop.
		clear(r.chunks[len(r.chunks):n])
	}
	r.held += int(hi - lo) //nolint:gosec // G115: bounded by the bytes held and the frame
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
