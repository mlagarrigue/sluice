package httpstream

import (
	"bytes"
	"errors"
	"fmt"
)

// HPACK (RFC 7541), the header compression HTTP/2 requires.
//
// It is a decoder and a deliberately minimal encoder. Decoding must be
// complete — a peer chooses the representation, so all of them have to be
// understood — while encoding may use the simplest representation the format
// allows and still be conformant, which is what the encoder here does.
//
// # The two ways this format goes wrong
//
// **It is stateful.** The dynamic table is built from the headers that have
// already been decoded, so a decoder that mis-decodes one block mis-decodes
// every block after it on that connection, and the damage is silent. There is
// no resynchronising: [ErrHPACK] ends the connection.
//
// **It amplifies.** A small block can decode to a large header list, and a
// table update can be asked for repeatedly. Both are bounded here by numbers
// the caller sets, and the bound is checked as the list grows rather than
// after.

// ErrHPACK reports a header block this package will not decode.
//
// Naming: this package's sentinels for a peer that broke the rules are
// [ErrHPACK], [ErrH2Protocol] and [ErrH3Protocol], one per rule set — the
// HPACK format (RFC 7541), the HTTP/2 protocol (RFC 9113) and the HTTP/3
// protocol (RFC 9114). The split is what the wire needs: HTTP/2 answers an
// HPACK failure with COMPRESSION_ERROR and a framing failure with
// PROTOCOL_ERROR, and [codeFor] tells them apart with errors.Is. The names
// follow the RFCs' own words, which is why HPACK, a format, carries no
// "Protocol" suffix while the two protocol versions do. Decided before v1
// and not to be revisited: renaming any of the three is an API break.
var ErrHPACK = errors.New("httpstream: HPACK decoding failed")

// hpackEntrySize is the per-entry overhead RFC 7541 §4.1 defines, so that an
// entry's cost is its name plus its value plus this. It is an accounting
// constant rather than a real allocation, and both peers must agree on it or
// their tables evict differently.
const hpackEntrySize = 32

// hpackDecoder decodes header blocks for one connection, carrying the dynamic
// table between them.
//
// It is not safe for concurrent use, and it is per connection rather than per
// stream: HTTP/2 shares one compression context across a connection's streams,
// which is why a header block cannot be interleaved with another's.
type hpackDecoder struct {
	// dynamic is a ring of n entries starting at the oldest, first: an
	// insertion writes one slot and an eviction clears one. The index space
	// numbers entries the other way round (most recent first, §2.3.3), which
	// entry translates. A most-recent-first slice made every insertion a
	// memmove of the whole table — 1 MiB of it per field under
	// NewHPACKDecoder(1<<20, ...) — and an oldest-first one resliced from
	// the front reallocated it every few dozen insertions.
	dynamic []hpackEntry
	first   int
	n       int
	size    int // the table's accounted size, not its byte count
	maxSize int

	// advertised is SETTINGS_HEADER_TABLE_SIZE: the ceiling a peer's table
	// size update may not pass. It is kept apart from maxSize because maxSize
	// is what the peer last asked for and moves under resize — checking an
	// update against it refuses the legal sequence "shrink to 0, then restore
	// to 4096" and kills the connection of a client doing nothing wrong.
	advertised int

	// maxListSize bounds one decoded header list, which is the amplification
	// bound: a block of a few hundred bytes can name a few hundred entries
	// that are large.
	maxListSize int

	scratch []byte
}

// hpackEntry is one dynamic-table field. Its bytes are written once, when the
// entry is decoded, and never again: eviction drops the reference rather than
// reusing the storage. That is what lets a field resolved from the table be
// handed out as a view of the entry instead of a copy.
type hpackEntry struct{ Name, Value []byte }

func (e hpackEntry) size() int { return len(e.Name) + len(e.Value) + hpackEntrySize }

// staticTableBytes is staticTable's entries as []byte, built once so a
// static-table hit decodes to a view of these package-lifetime, never-written
// bytes instead of cloning name and value per field — two allocations per
// field on the request hot path, for entries that never change.
//
// Each slice is capped at its own length: []byte(s) may round its capacity up
// to a size class, and an append to a decoded field would otherwise write
// into the spare room of an entry every connection shares.
var staticTableBytes = func() (t [len(staticTable)]Header) {
	for i, e := range staticTable {
		t[i] = Header{Name: capped([]byte(e.Name)), Value: capped([]byte(e.Value))}
	}
	return t
}()

// capped is b with its capacity cut to its length, so that appending to it
// reallocates instead of writing past it into bytes something else views.
func capped(b []byte) []byte { return b[:len(b):len(b)] }

// cloneHeaders deep-copies hs into one allocation of bytes, every name and
// value a capped view of it.
func cloneHeaders(hs []Header) []Header {
	n := 0
	for _, h := range hs {
		n += len(h.Name) + len(h.Value)
	}
	buf := make([]byte, 0, n)
	out := make([]Header, len(hs))
	for i, h := range hs {
		at := len(buf)
		buf = append(buf, h.Name...)
		mid := len(buf)
		buf = append(buf, h.Value...)
		out[i] = Header{Name: buf[at:mid:mid], Value: buf[mid:len(buf):len(buf)]}
	}
	return out
}

// appendFieldString decodes one string literal's octets — Huffman-coded or
// not — onto the end of *arena and returns them as a view capped at their
// own length.
//
// The arena is the header block's: every literal a block decodes that only
// the header list will hold lands in it, so a block costs one allocation
// where it cost one per string. It is allocated on the first string, sized
// from the octets the block has left (remaining), capped by the list-size
// budget left. Encoders code a block's strings one way throughout, so the
// first string's coding sizes it: plain octets decode to at most themselves,
// and Huffman ones to at most 8/5 of themselves, the shortest code being
// five bits. Sizing every block for Huffman cost a plain block 60% more
// bytes than its strings; a block that mixes codings after a plain first
// string may regrow once instead. Regrowing breaks nothing: earlier views
// keep the old array alive, and nothing writes there again. The arena is
// only ever appended to, and a fresh one is started per block, so a view
// handed out is never written after.
func appendFieldString(arena *[]byte, raw []byte, huff bool, remaining, budget int) ([]byte, error) {
	a := *arena
	if cap(a) == 0 {
		bound := remaining
		if huff {
			bound = remaining*8/5 + 1
		}
		a = make([]byte, 0, max(0, min(bound, budget)))
	}
	start := len(a)
	var err error
	if huff {
		a, err = huffmanDecode(a, raw)
	} else {
		a = append(a, raw...)
	}
	*arena = a
	if err != nil {
		return nil, err
	}
	return a[start:len(a):len(a)], nil
}

// newHPACKDecoder returns a decoder whose dynamic table may reach maxSize
// bytes and whose decoded lists may reach maxListSize.
//
// Both default to modest values, and neither has one that suits every
// deployment: maxSize is what this end advertises as SETTINGS_HEADER_TABLE_SIZE
// and a peer may not exceed it, while maxListSize is what stops a small block
// from decoding into a large list.
func newHPACKDecoder(maxSize, maxListSize int) *hpackDecoder {
	if maxSize <= 0 {
		maxSize = 4096 // the protocol's own initial value
	}
	if maxListSize <= 0 {
		maxListSize = 64 << 10
	}
	return &hpackDecoder{maxSize: maxSize, advertised: maxSize, maxListSize: maxListSize}
}

// Decode appends the fields of one header block to dst.
//
// The block must be complete: HEADERS plus every CONTINUATION that followed
// it, which is what [h2Frames] tracks and bounds. Names and values never alias
// the block. A field resolved from a table — the static one, or an entry of
// the dynamic one — is a view of that entry's bytes, which are never written
// after they are decoded: a later eviction drops the table's reference and
// leaves the field intact. A literal the table does not keep is a view of
// one buffer this call allocates for the block's literals and never writes
// after returning. Callers treat decoded fields as read-only, which they owe
// every Header anyway.
func (d *hpackDecoder) Decode(dst []Header, block []byte) ([]Header, error) {
	listSize := 0
	var arena []byte // the block's unindexed literals; see appendFieldString
	// fields records that a field representation has been decoded, after
	// which a table size update is out of place (RFC 7541 §4.2).
	fields := false
	for len(block) > 0 {
		b := block[0]
		var (
			name, value []byte
			err         error
		)
		switch {
		case b&0x80 != 0: // §6.1 indexed field
			var idx uint64
			if idx, block, err = hpackInt(block, 7); err != nil {
				return dst, err
			}
			if idx == 0 {
				return dst, fmt.Errorf("%w: index 0 is not a field", ErrHPACK)
			}
			if idx <= uint64(len(staticTable)) {
				// A static hit appends the immutable package copy as-is: no
				// clone, where this case used to cost two per field.
				e := staticTableBytes[idx-1]
				listSize += len(e.Name) + len(e.Value) + hpackEntrySize
				if listSize > d.maxListSize {
					return dst, fmt.Errorf("%w: the header list passed %d bytes", ErrHPACK, d.maxListSize)
				}
				dst = append(dst, e)
				fields = true
				continue
			}
			if name, value, err = d.at(idx); err != nil {
				return dst, err
			}

		case b&0xc0 == 0x40: // §6.2.1 literal, incremental indexing
			// Not the block's arena: the entry outlives the block in the
			// table, and a view of the arena would keep the whole block's
			// literals alive for as long as this one field stays indexed.
			if name, value, block, err = d.literalBytes(block, 6, nil, 0); err != nil {
				return dst, err
			}
			d.add(hpackEntry{name, value})

		case b&0xe0 == 0x20: // §6.3 dynamic table size update
			// §4.2: an update opens a header block, before its first field.
			// One after a field would resize the table mid-block, between
			// fields that were indexed against different sizes; nghttp2
			// refuses it, and so does this.
			if fields {
				return dst, fmt.Errorf("%w: a table size update after a field", ErrHPACK)
			}
			var size uint64
			if size, block, err = hpackInt(block, 5); err != nil {
				return dst, err
			}
			if int(size) > d.advertised { //nolint:gosec // G115: hpackInt refuses anything past 28 bits
				return dst, fmt.Errorf("%w: a table update asks for %d bytes, over the %d advertised",
					ErrHPACK, size, d.advertised)
			}
			d.resize(int(size)) //nolint:gosec // G115: hpackInt refuses anything past 28 bits
			continue

		default: // §6.2.2 and §6.2.3: literal, without or never indexed
			// Never added to the dynamic table, so its strings go onto the
			// block's arena — one allocation for every such literal.
			var nameBytes, valueBytes []byte
			if nameBytes, valueBytes, block, err = d.literalBytes(block, 4, &arena, d.maxListSize-listSize); err != nil {
				return dst, err
			}
			listSize += len(nameBytes) + len(valueBytes) + hpackEntrySize
			if listSize > d.maxListSize {
				return dst, fmt.Errorf("%w: the header list passed %d bytes", ErrHPACK, d.maxListSize)
			}
			dst = append(dst, Header{Name: nameBytes, Value: valueBytes})
			fields = true
			continue
		}

		listSize += len(name) + len(value) + hpackEntrySize
		if listSize > d.maxListSize {
			return dst, fmt.Errorf("%w: the header list passed %d bytes", ErrHPACK, d.maxListSize)
		}
		// A view of the table entry, not a copy: entries are immutable.
		dst = append(dst, Header{Name: name, Value: value})
		fields = true
	}
	return dst, nil
}

// literalBytes decodes a literal representation whose index prefix is n bits:
// the name is either an index or a string, and the value is always a string.
// An indexed name is a view of the table entry that holds it.
//
// With a non-nil arena the strings are appended to it (see
// appendFieldString), budget being what the list may still spend. With a nil
// one they are decoded into the scratch buffer and copied out in one
// allocation the field owns outright — a name and value bound for the dynamic
// table share it, rather than taking one each.
func (d *hpackDecoder) literalBytes(block []byte, n uint8, arena *[]byte, budget int) (name, value, rest []byte, err error) {
	idx, rest, err := hpackInt(block, n)
	if err != nil {
		return nil, nil, nil, err
	}
	switch {
	case idx == 0:
	case idx <= uint64(len(staticTable)):
		// A static-table name is a view of the immutable package copy — the
		// shape this package's own encoder emits for every response field.
		name = staticTableBytes[idx-1].Name
	default:
		if name, _, err = d.at(idx); err != nil {
			return nil, nil, nil, err
		}
	}
	if arena != nil {
		if idx == 0 {
			if name, rest, err = d.strArena(rest, arena, budget); err != nil {
				return nil, nil, nil, err
			}
		}
		if value, rest, err = d.strArena(rest, arena, budget); err != nil {
			return nil, nil, nil, err
		}
		return name, value, rest, nil
	}

	buf := d.scratch[:0]
	if idx == 0 {
		if buf, rest, err = d.appendStr(buf, rest); err != nil {
			return nil, nil, nil, err
		}
	}
	mid := len(buf)
	if buf, rest, err = d.appendStr(buf, rest); err != nil {
		return nil, nil, nil, err
	}
	d.scratch = buf
	owned := bytes.Clone(buf)
	if owned == nil {
		// An empty name and value with no scratch yet: present-but-empty
		// is not absent, as the h2 :authority check reads it.
		owned = []byte{}
	}
	if idx == 0 {
		name = owned[:mid:mid]
	}
	return name, owned[mid:], rest, nil
}

// strArena decodes a string literal onto the block's arena.
func (d *hpackDecoder) strArena(block []byte, arena *[]byte, budget int) (b, rest []byte, err error) {
	raw, huff, rest, err := d.strRaw(block)
	if err != nil {
		return nil, nil, err
	}
	if b, err = appendFieldString(arena, raw, huff, len(block), budget); err != nil {
		return nil, nil, err
	}
	return b, rest, nil
}

// appendStr decodes a string literal onto the end of dst.
func (d *hpackDecoder) appendStr(dst, block []byte) (out, rest []byte, err error) {
	raw, huff, rest, err := d.strRaw(block)
	if err != nil {
		return dst, nil, err
	}
	if !huff {
		return append(dst, raw...), rest, nil
	}
	if dst, err = huffmanDecode(dst, raw); err != nil {
		return dst, nil, err
	}
	return dst, rest, nil
}

// at resolves an index over the static table then the dynamic one, which
// share one one-based space (RFC 7541 §2.3.3).
func (d *hpackDecoder) at(idx uint64) (name, value []byte, err error) {
	if idx <= uint64(len(staticTable)) {
		e := staticTableBytes[idx-1]
		return e.Name, e.Value, nil
	}
	i := idx - uint64(len(staticTable)) - 1
	if i >= uint64(d.n) { //nolint:gosec // G115: d.n counts entries, never negative
		return nil, nil, fmt.Errorf("%w: index %d names nothing; the table holds %d entries",
			ErrHPACK, idx, len(staticTable)+d.n)
	}
	e := d.entry(int(i)) //nolint:gosec // G115: below d.n
	return e.Name, e.Value, nil
}

// entry is the dynamic table's i-th entry, 0 being the most recent.
func (d *hpackDecoder) entry(i int) hpackEntry {
	return d.dynamic[(d.first+d.n-1-i)%len(d.dynamic)]
}

// add inserts the newest entry and evicts the oldest until it fits.
//
// An entry larger than the whole table empties it and is not stored, which
// RFC 7541 §4.4 requires rather than treats as an error.
func (d *hpackDecoder) add(e hpackEntry) {
	if e.size() > d.maxSize {
		clear(d.dynamic)
		d.first, d.n, d.size = 0, 0, 0
		return
	}
	for d.size+e.size() > d.maxSize {
		d.evictOldest()
	}
	if d.n == len(d.dynamic) {
		// Full: grow, laying the entries out oldest-first from slot 0. The
		// ring only ever grows to the most entries the table has held at
		// once, which the 32-byte overhead bounds to maxSize/32.
		grown := make([]hpackEntry, max(8, 2*d.n))
		for i := range d.n {
			grown[i] = d.dynamic[(d.first+i)%len(d.dynamic)]
		}
		d.dynamic, d.first = grown, 0
	}
	d.dynamic[(d.first+d.n)%len(d.dynamic)] = e
	d.n++
	d.size += e.size()
}

// evictOldest drops the oldest entry. The slot is zeroed so the ring does not
// keep an evicted entry's bytes alive until the slot is reused.
func (d *hpackDecoder) evictOldest() {
	d.size -= d.dynamic[d.first].size()
	d.dynamic[d.first] = hpackEntry{}
	d.first = (d.first + 1) % len(d.dynamic)
	d.n--
}

func (d *hpackDecoder) resize(size int) {
	d.maxSize = size
	for d.size > d.maxSize && d.n > 0 {
		d.evictOldest()
	}
}

// strRaw decodes a string literal's length and Huffman flag and slices its
// octets out of block, without copying — the caller decides where they land.
func (d *hpackDecoder) strRaw(block []byte) (raw []byte, huff bool, rest []byte, err error) {
	if len(block) == 0 {
		return nil, false, nil, fmt.Errorf("%w: a string literal with no length", ErrHPACK)
	}
	huff = block[0]&0x80 != 0
	n, rest, err := hpackInt(block, 7)
	if err != nil {
		return nil, false, nil, err
	}
	if n > uint64(len(rest)) {
		return nil, false, nil, fmt.Errorf("%w: a string declares %d bytes, %d remain", ErrHPACK, n, len(rest))
	}
	return rest[:n], huff, rest[n:], nil
}

// hpackInt decodes RFC 7541 §5.1's prefixed integer: n bits in the first
// octet, then continuation octets seven bits at a time.
//
// The width is bounded rather than left to overflow: the format can express
// arbitrarily large integers, and every use of one here is a length or an
// index that a peer could otherwise make enormous with a few bytes.
func hpackInt(block []byte, n uint8) (v uint64, rest []byte, err error) {
	if len(block) == 0 {
		return 0, nil, fmt.Errorf("%w: an integer with no first octet", ErrHPACK)
	}
	mask := uint64(1)<<n - 1
	v = uint64(block[0]) & mask
	block = block[1:]
	if v < mask {
		return v, block, nil
	}
	for shift := uint(0); ; shift += 7 {
		if len(block) == 0 {
			return 0, nil, fmt.Errorf("%w: an integer runs off the end of the block", ErrHPACK)
		}
		if shift > 21 {
			// Four continuation octets carry more than any length or index
			// this package will act on, and refusing here is what keeps a
			// five-byte integer from becoming a huge allocation downstream.
			return 0, nil, fmt.Errorf("%w: an integer wider than this decoder will act on", ErrHPACK)
		}
		b := block[0]
		block = block[1:]
		v += uint64(b&0x7f) << shift
		if b&0x80 == 0 {
			return v, block, nil
		}
	}
}
