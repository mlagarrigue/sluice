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
func parseH3Frames(dst []h3Frame, buf []byte, maxFrame int) (frames []h3Frame, rest []byte, err error) {
	for len(buf) > 0 {
		typ, after, err := quic.Varint(buf)
		if err != nil {
			return dst, buf, nil //nolint:nilerr // not yet a whole varint: the rest waits for more bytes
		}
		length, after, err := quic.Varint(after)
		if err != nil {
			return dst, buf, nil //nolint:nilerr // not yet a whole varint: the rest waits for more bytes
		}
		if maxFrame > 0 && length > uint64(maxFrame) {
			// ErrTooLarge as well as ErrH3Protocol: a frame too long to read is the
			// peer asking for more than it may, whatever its type — a GREASE
			// frame included — which a request stream answers with
			// H3_EXCESSIVE_LOAD rather than H3_MESSAGE_ERROR.
			return dst, nil, fmt.Errorf("%w: %w: a frame of %d bytes, over the %d bound", ErrH3Protocol, ErrTooLarge, length, maxFrame)
		}
		if length > uint64(len(after)) {
			return dst, buf, nil // the payload has not all arrived
		}
		// Reserved types exist to be ignored: RFC 9114 §7.2.8 has peers send
		// them precisely so that a receiver which refuses the unknown is
		// found out early. Refusing them is how a protocol ossifies.
		if !h3Known(typ) {
			buf = after[length:]
			continue
		}
		dst = append(dst, h3Frame{Type: typ, Payload: after[:length]})
		buf = after[length:]
	}
	return dst, nil, nil
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
	ric, block, err := hpackInt(block, 8)
	if err != nil {
		return dst, fmt.Errorf("%w: a field section with a truncated prefix", ErrH3Protocol)
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
		return dst, fmt.Errorf("%w: a field section prefix with a truncated delta base", ErrH3Protocol)
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
			return dst, fmt.Errorf("%w: a field section past %d bytes", ErrH3Protocol, d.maxListSize)
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
		return nil, nil, fmt.Errorf("%w: a field section past %d bytes", ErrH3Protocol, d.maxListSize)
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

// walkH3Settings calls each for every pair of a SETTINGS payload, refusing a
// truncated pair and the reserved HTTP/2 identifiers on the way.
func walkH3Settings(payload []byte, each func(id, v uint64) error) error {
	for len(payload) > 0 {
		id, rest, err := quic.Varint(payload)
		if err != nil {
			return fmt.Errorf("%w: a truncated setting", ErrH3Protocol)
		}
		switch id {
		case 0x00, 0x02, 0x03, 0x04, 0x05:
			return fmt.Errorf("%w: reserved HTTP/2 setting %#x", ErrH3Protocol, id)
		}
		v, rest, err := quic.Varint(rest)
		if err != nil {
			return fmt.Errorf("%w: setting %#x has no value", ErrH3Protocol, id)
		}
		if err := each(id, v); err != nil {
			return err
		}
		payload = rest
	}
	return nil
}
