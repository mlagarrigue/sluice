package httpstream

import (
	"errors"
	"fmt"
	"math/rand/v2"

	"github.com/mlagarrigue/sluice/net/quic"
)

// HTTP/3 (RFC 9114) over the QUIC stream layer, and QPACK (RFC 9204) as far
// as it can honestly go here.
//
// # Where the batch comes from, for the third time
//
// HTTP/1.1 gives a batch when a client pipelines, which almost none do.
// HTTP/2 gives one because it multiplexes over a single connection. HTTP/3
// gives one because QUIC does: a datagram carries frames for several streams
// at once, so the requests are already grouped when they arrive and this
// layer only has to assemble them. It is the same conclusion the other two
// reached by different routes, and here it needs no cooperation at all.
//
// # What is complete, and what is not
//
// The frame layer is complete: RFC 9114 §7's types, bounded, refusing rather
// than guessing. Request assembly is complete. QPACK is complete over the
// static table this package now ships; the dynamic table is declined by
// SETTINGS rather than missing — see [QPACKDecoder].

// HTTP/3 frame types (RFC 9114 §7.2). The type is a variable-length integer,
// so the reserved and greasing types are large numbers rather than a fixed
// byte.
const (
	h3Data        = 0x00
	h3Headers     = 0x01
	h3CancelPush  = 0x03
	h3Settings    = 0x04
	h3PushPromise = 0x05
	h3GoAway      = 0x07
	h3MaxPushID   = 0x0d
)

// h3MaxServerGoAway is the identifier of a server's first GOAWAY in a
// graceful shutdown (RFC 9114 §5.2): the highest client-initiated
// bidirectional stream identifier QUIC has, 2^62−4, which stops new
// requests without yet naming the last one taken.
const h3MaxServerGoAway = 1<<62 - 4

// ErrH3Protocol reports an HTTP/3 frame or request this package will not
// accept. Named as [ErrHPACK] explains.
var ErrH3Protocol = errors.New("httpstream: HTTP/3 protocol error")

// h3Frame is one frame off a QUIC stream. Payload borrows the stream buffer.
type h3Frame struct {
	Type    uint64
	Payload []byte
}

// parseH3Frames decodes the frames in a stream's buffered bytes, returning
// what it could not yet complete.
//
// A QUIC stream is a byte stream, so a frame may be split across datagrams:
// the remainder comes back rather than being an error, and the caller keeps
// it for the next arrival. That is the difference from the HTTP/2 layer,
// where a frame is whole or the connection is lost.
//
// A frame declared over maxFrame is refused on its declaration, with the
// frames ahead of it returned and rest starting at it: a request stream
// answers an oversized DATA frame with a 413 once the frames before it — its
// HEADERS — are read, rather than with the protocol error.
//
// rest is always a suffix of buf — buf itself when the very first frame is
// still incomplete, nil when everything was consumed — so the caller can tell
// from its length alone whether anything was consumed; see keepRest.
//
// A frame of a type this layer does not know is skipped, not kept: RFC 9114
// §7.2.8 has peers send reserved (GREASE) types precisely so that a receiver
// which refuses the unknown is found out early, and refusing them is how a
// protocol ossifies. One whose payload has not all arrived is skipped as far
// as it goes, and skip says how many of its bytes are still to come — the
// caller discards that many off the front of its next deliveries (skipAhead)
// before parsing again. Nothing of it is buffered and no bound applies to its
// length: the work it costs is one pass over the bytes the peer actually
// sent, and the stream's own bound (rawStreamBound) still holds what is kept.
// Until 2026-10-21 an unknown frame longer than maxFrame was refused like a
// DATA frame of that size; a peer that greases with a long frame is
// conforming, and §7.2.8 says to ignore it.
func parseH3Frames(dst []h3Frame, buf []byte, maxFrame int) (frames []h3Frame, rest []byte, skip uint64, err error) {
	for len(buf) > 0 {
		typ, after, err := quic.Varint(buf)
		if err != nil {
			return dst, buf, 0, nil //nolint:nilerr // not yet a whole varint: the rest waits for more bytes
		}
		length, after, err := quic.Varint(after)
		if err != nil {
			return dst, buf, 0, nil //nolint:nilerr // not yet a whole varint: the rest waits for more bytes
		}
		if h3ReservedH2(typ) {
			// RFC 9114 §7.2.8: the HTTP/2 frame types with no HTTP/3
			// counterpart — PRIORITY, PING, WINDOW_UPDATE, CONTINUATION —
			// "MUST NOT be sent, and their receipt MUST be treated as a
			// connection error of type H3_FRAME_UNEXPECTED". Refused on the
			// type alone, before the payload is waited for: there is nothing
			// in it this layer would read. They used to be skipped with the
			// GREASE types, which the same section wants ignored.
			return dst, nil, 0, fmt.Errorf("%w: frame type %#x is an HTTP/2 type with no HTTP/3 meaning",
				errH3FrameUnexpected, typ)
		}
		if !h3Known(typ) {
			// The one exception is the four above, which §7.2.8 reserves by
			// name.
			if length > uint64(len(after)) {
				return dst, nil, length - uint64(len(after)), nil
			}
			buf = after[length:]
			continue
		}
		if maxFrame > 0 && length > uint64(maxFrame) {
			// ErrTooLarge as well as ErrH3Protocol: a frame too long to read is the
			// peer asking for more than it may, which a request stream answers
			// with H3_EXCESSIVE_LOAD rather than H3_MESSAGE_ERROR.
			return dst, buf, 0, fmt.Errorf("%w: %w: a frame of %d bytes, over the %d bound", ErrH3Protocol, ErrTooLarge, length, maxFrame)
		}
		if length > uint64(len(after)) {
			return dst, buf, 0, nil // the payload has not all arrived
		}
		dst = append(dst, h3Frame{Type: typ, Payload: after[:length]})
		buf = after[length:]
	}
	return dst, nil, 0, nil
}

// skipAhead discards, off the front of a delivery, what is left of an
// unknown frame parseH3Frames started skipping: as many bytes as the frame
// still owes, or the whole delivery when it owes more. It returns the bytes
// after the skip, which are the start of the next frame, and what the frame
// still owes after them. The discarded bytes are consumed like any other —
// released to flow control by the caller's "delivered minus retained"
// arithmetic — and nothing of them is kept.
func skipAhead(data []byte, skip uint64) (after []byte, owed uint64) {
	if skip == 0 {
		return data, 0
	}
	if skip >= uint64(len(data)) {
		return data[len(data):], skip - uint64(len(data))
	}
	return data[skip:], 0
}

// h3Identifier reads a payload made of exactly one variable-length integer —
// all a GOAWAY, MAX_PUSH_ID or CANCEL_PUSH frame carries (RFC 9114 §7.2.3,
// §7.2.6, §7.2.7). Anything short or long is the frame's layout at fault,
// H3_FRAME_ERROR (§7.1, §10.8), as a SETTINGS pair cut short is.
func h3Identifier(payload []byte) (uint64, error) {
	v, rest, err := quic.Varint(payload)
	if err != nil {
		return 0, fmt.Errorf("%w: an identifier cut short", errH3FrameLayout)
	}
	if len(rest) != 0 {
		return 0, fmt.Errorf("%w: %d bytes after the identifier", errH3FrameLayout, len(rest))
	}
	return v, nil
}

// keepRest moves what parseH3Frames left unconsumed to the front of its
// buffer, for the next delivery to append to. rest is a suffix of buf; when
// it is the whole of buf nothing was consumed and the buffer is returned as
// it is. Copying it onto itself is not free: runtime.memmove has no shortcut
// for dst == src, and a DATA frame declared at MaxBodyBytes parks a buffer
// that grows to rawStreamBound while its payload trickles in — fed one byte
// per datagram, each byte used to cost a copy of the whole buffer
// (RFC 9114 §10.5: the work a peer can cause must stay bounded by what it
// sends).
func keepRest(buf, rest []byte) []byte {
	if len(rest) == len(buf) {
		return buf
	}
	return append(buf[:0], rest...)
}

func h3Known(typ uint64) bool {
	switch typ {
	case h3Data, h3Headers, h3CancelPush, h3Settings, h3GoAway, h3MaxPushID:
		return true
	case h3PushPromise:
		return true
	}
	return false
}

// h3ReservedH2 reports a frame type RFC 9114 §11.2.1 registers as "Reserved"
// because HTTP/2 used it for a frame HTTP/3 has no equivalent of: 0x02
// PRIORITY, 0x06 PING, 0x08 WINDOW_UPDATE, 0x09 CONTINUATION. §7.2.8 makes
// their receipt H3_FRAME_UNEXPECTED, as §7.2.4.1 does for the matching
// setting identifiers walkH3Settings refuses.
func h3ReservedH2(typ uint64) bool {
	switch typ {
	case 0x02, 0x06, 0x08, 0x09:
		return true
	}
	return false
}

// appendH3Frame writes a frame: a variable-length type, a variable-length
// length, then the payload.
func appendH3Frame(dst []byte, typ uint64, payload []byte) []byte {
	dst = quic.AppendVarint(dst, typ)
	dst = quic.AppendVarint(dst, uint64(len(payload)))
	return append(dst, payload...)
}

// qpackDecoder decodes HTTP/3 field sections.
//
// # The static table ships, and stays a seam
//
// QPACK's static table is ninety-nine entries published in RFC 9204
// Appendix A, and this package carries them — see qpack_static.go. It once
// did not, on the argument that a lookup table has no property a
// transcription error would break, the way HPACK's Huffman code breaks the
// Kraft equality. That argument put the transcription on every caller
// instead, all of whom would type it with less checking than one shared copy
// gets: the shipped table is pinned by the RFC's own encoded example, by
// structural checks, and by a fixture of what a real client sends. What the
// argument was right about is recorded in ADR 0013.
//
// The parameter remains as the override [join.StreamTableWith] would
// be: pass a table to replace the shipped one, or an empty non-nil table to
// refuse indexed field lines entirely.
//
// The dynamic table is a different matter and needs no seam. Advertising
// SETTINGS_QPACK_MAX_TABLE_CAPACITY of zero — which [h3Settings] does — means
// a peer must not use dynamic references at all, so refusing them is
// conformant rather than incomplete.
type qpackDecoder struct {
	static      []Header
	maxListSize int
}

// newQPACKDecoder returns a decoder over a static table, bounding one decoded
// field section.
//
// A nil static means RFC 9204 Appendix A, which the wire indexes from zero —
// the first entry is index 0, unlike HPACK's table. A non-nil table replaces
// it; an empty non-nil table refuses every indexed field line with
// [errQPACKIndexed].
func newQPACKDecoder(static []Header, maxListSize int) *qpackDecoder {
	if maxListSize <= 0 {
		maxListSize = 64 << 10
	}
	if static == nil {
		static = qpackStaticHeaders
	} else {
		// A caller's table is copied once, here, so a static hit can hand out
		// a view of it the way the shipped table's are handed out: decoded
		// fields never alias bytes the caller still holds and may rewrite.
		static = cloneHeaders(static)
	}
	return &qpackDecoder{static: static, maxListSize: maxListSize}
}

// staticAt resolves an index into the static table in use.
func (d *qpackDecoder) staticAt(i uint64) (Header, error) {
	if i >= uint64(len(d.static)) {
		return Header{}, fmt.Errorf("%w: index %d in a table of %d entries", errQPACKIndexed, i, len(d.static))
	}
	return d.static[i], nil
}

// errQPACKIndexed reports an indexed field line naming no entry of the static
// table in use — an index past RFC 9204 Appendix A's ninety-nine, or any
// index at all when [Config.QPACKStatic] was set to an empty table to refuse
// them. Refused rather than guessed, as everywhere else in this package.
var errQPACKIndexed = fmt.Errorf("%w: an indexed field line names no entry of the static table in use", ErrH3Protocol)

// Decode appends the fields of one field section to dst.
//
// Names and values never alias the section. A static-table hit is a view of
// the table's entry, which is never written; the literal strings of the
// section are decoded into one buffer allocated for this call alone, which
// the fields are views of and nothing writes after they are returned — the
// ownership [hpackDecoder.Decode] gives its fields, at one allocation per
// section rather than up to four per field.
func (d *qpackDecoder) Decode(dst []Header, block []byte) ([]Header, error) {
	// The section prefix (RFC 9204 §4.5.1): the Required Insert Count is an
	// 8-bit-prefix integer and the Delta Base a sign bit over a 7-bit-prefix
	// one — RFC 7541's prefixed integers, not QUIC varints. The two encodings
	// agree on a single byte up to 0x3F, which is how parsing them as varints
	// survived every section that encoded zero: a conformant prefix with a
	// Delta Base of 0x40 or more was eaten a byte short and the field lines
	// after it misread.
	// Both wrap the decoder's own error: an integer past 62 bits here is
	// the stream's fault, not the connection's (RFC 9204 §7.4), and
	// sectionRefused tells the two apart by errIntegerWide.
	ric, block, err := hpackInt(block, 8)
	if err != nil {
		return dst, fmt.Errorf("%w: a field section prefix's required insert count: %w", ErrH3Protocol, err)
	}
	if ric != 0 {
		return dst, fmt.Errorf("%w: a required insert count of %d, but no dynamic table was offered", ErrH3Protocol, ric)
	}
	if len(block) == 0 {
		return dst, fmt.Errorf("%w: a field section prefix with no base", ErrH3Protocol)
	}
	// With a Required Insert Count of zero the sign must be clear: S=1 says
	// Base = RIC − Delta Base − 1, which is negative here whatever the Delta
	// Base carries (§4.5.1.2), and no conformant encoder writes it.
	if block[0]&0x80 != 0 {
		return dst, fmt.Errorf("%w: a negative base against an insert count of zero", ErrH3Protocol)
	}
	if _, block, err = hpackInt(block, 7); err != nil {
		return dst, fmt.Errorf("%w: a field section prefix's delta base: %w", ErrH3Protocol, err)
	}

	listSize := 0
	var arena []byte // the section's decoded literals; see appendFieldString
	for len(block) > 0 {
		b := block[0]
		budget := d.maxListSize - listSize
		var name, value []byte
		switch {
		case b&0x80 != 0: // §4.5.2 indexed field line
			if b&0x40 == 0 {
				// The T bit is clear: a dynamic-table reference, which a peer
				// told the table capacity is zero may not make.
				return dst, fmt.Errorf("%w: a dynamic-table reference, but a capacity of zero was advertised", ErrH3Protocol)
			}
			var idx uint64
			if idx, block, err = hpackInt(block, 6); err != nil {
				return dst, err
			}
			h, err := d.staticAt(idx)
			if err != nil {
				return dst, err
			}
			name, value = h.Name, h.Value

		case b&0xc0 == 0x40: // §4.5.4 literal with a name reference
			if b&0x10 == 0 {
				return dst, fmt.Errorf("%w: a dynamic-table name reference, but a capacity of zero was advertised", ErrH3Protocol)
			}
			var idx uint64
			if idx, block, err = hpackInt(block, 4); err != nil {
				return dst, err
			}
			h, err := d.staticAt(idx)
			if err != nil {
				return dst, err
			}
			name = h.Name
			if value, block, err = qpackString(d, block, 7, budget, &arena); err != nil {
				return dst, err
			}

		case b&0xe0 == 0x20: // §4.5.6 literal with a literal name
			// The pattern is 001NHxxx: masking with 0xf0 used to drop the
			// N=1 half (0x30–0x3F), which is exactly what encoders set on
			// sensitive fields like authorization — a never-indexed literal
			// MUST decode like any other (RFC 9204 §4.5.6).
			if name, block, err = qpackString(d, block, 3, budget, &arena); err != nil {
				return dst, err
			}
			if value, block, err = qpackString(d, block, 7, budget, &arena); err != nil {
				return dst, err
			}

		case b&0xf0 == 0x10: // §4.5.3 indexed field line with a post-base index
			return dst, fmt.Errorf("%w: a post-base reference, which needs a dynamic table", ErrH3Protocol)

		default:
			return dst, fmt.Errorf("%w: a field line representation of %#x", ErrH3Protocol, b)
		}

		listSize += len(name) + len(value) + 32
		if listSize > d.maxListSize {
			// The request's fault, not the connection's: RFC 9114 §4.2.2
			// answers it 431 on the stream, which [errHeadersTooLarge] maps
			// to. No dynamic table means nothing past this field has to be
			// read for the next section to decode (the HPACK decoder's
			// decodeOver has no twin here).
			return dst, fmt.Errorf("%w: a field section past %d bytes", errHeadersTooLarge, d.maxListSize)
		}
		dst = append(dst, Header{Name: name, Value: value})
	}
	return dst, nil
}

// qpackString decodes a string with an n-bit length prefix and a Huffman flag
// in the bit above it — the same shape HPACK uses, over the same code, which
// is why [huffmanDecode] serves both.
//
// budget is what the section's list-size bound may still spend. The check it
// feeds runs on the *declared* length, before any Huffman decoding: the
// list-size check after the fact already refuses the section, but by then a
// peer had bought a full decode of the oversized literal — CPU and scratch
// growth both well past the advertised header budget. The floor used cannot
// refuse a conformant string: a plain literal costs exactly its length, and a
// Huffman one decodes to at least 8/30 of it, the longest code being 30 bits.
func qpackString(d *qpackDecoder, block []byte, n uint8, budget int, arena *[]byte) (s, rest []byte, err error) {
	if len(block) == 0 {
		return nil, nil, fmt.Errorf("%w: a string with no length", ErrH3Protocol)
	}
	huff := block[0]&(1<<n) != 0
	length, rest, err := hpackInt(block, n)
	if err != nil {
		return nil, nil, err
	}
	if length > uint64(len(rest)) {
		return nil, nil, fmt.Errorf("%w: a string of %d bytes, %d remain", ErrH3Protocol, length, len(rest))
	}
	floor := int(length) //nolint:gosec // G115: bounded by len(rest) just above
	if huff {
		floor = floor * 8 / 30
	}
	if floor > budget {
		return nil, nil, fmt.Errorf("%w: a field section past %d bytes", errHeadersTooLarge, d.maxListSize)
	}
	raw, rest := rest[:length], rest[length:]
	if s, err = appendFieldString(arena, raw, huff, len(block), budget); err != nil {
		return nil, nil, err
	}
	return s, rest, nil
}

// appendQPACK writes a field section using literal representations only,
// which needs no table on either side and is what this package can both write
// and read.
//
// set-cookie, authorization and proxy-authorization carry the N bit (RFC 9204
// §4.5.6, §7.1.3), as the HPACK writer marks them: no table here would index
// them, but an intermediary re-encoding the section must not either.
func appendQPACK(dst []byte, fields []Header) []byte {
	// The prefix (§4.5.1): Required Insert Count, an 8-bit prefix integer,
	// then Delta Base, a 7-bit one under the sign bit. Both are zero, since
	// nothing is dynamic.
	dst = appendHPACKInt(dst, 0, 8, 0x00)
	dst = appendHPACKInt(dst, 0, 7, 0x00)
	for _, h := range fields {
		// 0x20 marks a literal with a literal name, 0x10 within it the N bit.
		flags := byte(0x20)
		if sensitiveField(h.Name) {
			flags |= 0x10
		}
		dst = appendHPACKInt(dst, uint64(len(h.Name)), 3, flags)
		dst = append(dst, h.Name...)
		dst = appendHPACKInt(dst, uint64(len(h.Value)), 7, 0x00)
		dst = append(dst, h.Value...)
	}
	return dst
}

// Unidirectional stream types (RFC 9114 §6.2). The type is a variable-length
// integer sent as the stream's first bytes and never repeated, which is why a
// receiver has to remember what each stream turned out to be.
const (
	h3StreamControl     = 0x00
	h3StreamPush        = 0x01
	h3StreamQPACKEncode = 0x02
	h3StreamQPACKDecode = 0x03
)

// SETTINGS identifiers this package sends and understands (RFC 9114 §7.2.4.1
// and RFC 9204 §5).
const (
	h3SettingMaxFieldSection       = 0x06
	h3SettingQPACKMaxTableCapacity = 0x01
	h3SettingQPACKBlockedStreams   = 0x07
)

// appendH3Control writes what a control stream opens with: its type, then the
// SETTINGS frame RFC 9114 §6.2.1 requires be the first thing on it.
//
// The QPACK capacity is zero and the blocked-stream count is zero, and both
// are deliberate rather than unfilled. A capacity of zero tells the peer it
// may not make dynamic-table references — which turns this package's lack of
// a dynamic table from an incompleteness into a stated limit the peer is
// bound by.
func appendH3Control(dst []byte, maxFieldSection uint64) []byte {
	dst = quic.AppendVarint(dst, h3StreamControl)

	var settings []byte
	settings = quic.AppendVarint(settings, h3SettingQPACKMaxTableCapacity)
	settings = quic.AppendVarint(settings, 0)
	settings = quic.AppendVarint(settings, h3SettingQPACKBlockedStreams)
	settings = quic.AppendVarint(settings, 0)
	settings = quic.AppendVarint(settings, h3SettingMaxFieldSection)
	settings = quic.AppendVarint(settings, maxFieldSection)
	// One setting from the reserved 0x1f*N+0x21 space, with a meaningless
	// value — RFC 9114 §7.2.4.1's SHOULD. A peer that chokes on an unknown
	// identifier is found out now, not when the protocol grows a real one.
	// This setting, and the reserved close code of h3GracefulCode, are the
	// only grease this end sends. The reserved frames and reserved streams
	// of §4.1, §7.2.8 and §6.2.3 are read and ignored but never sent: a
	// MAY that would cost a peer bytes to skip and this end a code path to
	// keep, for a grease no peer of this package has shown to need.
	settings = quic.AppendVarint(settings, 0x21+0x1f*rand.Uint64N(1<<16)) //nolint:gosec // G404: grease only has to vary, not be secret
	settings = quic.AppendVarint(settings, rand.Uint64N(1<<16))           //nolint:gosec // G404: grease only has to vary, not be secret
	return appendH3Frame(dst, h3Settings, settings)
}

// parseH3Settings reads a SETTINGS frame's payload into pairs.
//
// Every identifier is returned, known or not: this is a parser, and acting on
// a setting is the caller's business. §7.2.4.1 requires an unknown identifier
// to be ignored, not refused — the space is extensible, and a receiver that
// refuses the unknown is how a protocol stops being able to change. A
// repeated identifier is refused, because the two values would disagree and
// nothing says which wins. HTTP/2's setting identifiers with no HTTP/3
// meaning (0x00 and 0x02–0x05) are refused too: §7.2.4.1 reserves them and
// makes their receipt H3_SETTINGS_ERROR rather than something to pass over.
//
// [h3Assembler] does not keep this map: it holds the one value a server acts
// on for the connection's life, rather than every identifier a peer chose to
// send.
func parseH3Settings(payload []byte) (map[uint64]uint64, error) {
	out := map[uint64]uint64{}
	err := walkH3Settings(payload, func(id, v uint64) error {
		if _, seen := out[id]; seen {
			return fmt.Errorf("%w: setting %#x appears twice", ErrH3Protocol, id)
		}
		out[id] = v
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// errH3FrameLayout marks a frame payload that ends inside one of its fields —
// RFC 9114 §7.1's H3_FRAME_ERROR, distinct from what the fields say once they
// are whole. walkH3Settings raises it so the control stream can tell a
// truncated SETTINGS (a layout fault) from a reserved one (H3_SETTINGS_ERROR).
var errH3FrameLayout = fmt.Errorf("%w: a frame payload that ends inside a field", ErrH3Protocol)

// walkH3Settings calls each for every pair of a SETTINGS payload, refusing a
// truncated pair and the reserved HTTP/2 identifiers on the way.
func walkH3Settings(payload []byte, each func(id, v uint64) error) error {
	for len(payload) > 0 {
		id, rest, err := quic.Varint(payload)
		if err != nil {
			return fmt.Errorf("%w: a truncated setting", errH3FrameLayout)
		}
		switch id {
		case 0x00, 0x02, 0x03, 0x04, 0x05:
			return fmt.Errorf("%w: reserved HTTP/2 setting %#x", ErrH3Protocol, id)
		}
		v, rest, err := quic.Varint(rest)
		if err != nil {
			return fmt.Errorf("%w: setting %#x has no value", errH3FrameLayout, id)
		}
		if err := each(id, v); err != nil {
			return err
		}
		payload = rest
	}
	return nil
}
