package httpstream

import (
	"errors"
	"fmt"

	"github.com/mlagarrigue/sluice/internal/varint"
	"github.com/mlagarrigue/sluice/net/quic"
)

// uni folds one frame of a client unidirectional stream (RFC 9114 §6.2): the
// type varint first, then the stream's own content: the control stream's
// frames, and the QPACK pair's instructions, checked against the dynamic
// table neither side uses. Push streams are a client opening what only
// servers may, and unknown types are discarded, as §6.2 permits.
func (a *h3Assembler) uni(f quic.Frame) error {
	u := a.unis[f.StreamID]
	if u == nil {
		if len(a.unis) >= maxUniStreams {
			return nil // far past the transport's own stream admission
		}
		u = &h3Uni{kind: -1}
		a.unis[f.StreamID] = u
	}
	switch u.kind {
	case -1:
		// The whole delivery is appended, but only an incomplete type varint
		// — under 8 bytes — stays parked: once it parses, the buffer keeps
		// just what follows it.
		u.buf = append(u.buf, f.Data...)
		typ, rest, err := varint.Read(u.buf)
		if err != nil {
			// Not yet a whole varint. A stream closed before its type byte
			// is tolerated (§6.2), not an error.
			if f.Fin {
				delete(a.unis, f.StreamID)
			}
			return nil
		}
		u.buf = append(u.buf[:0], rest...)
		switch typ {
		case h3StreamControl:
			if a.hasControl {
				return h3ConnectionError{Code: h3StreamCreationError, Err: fmt.Errorf(
					"%w: a second control stream", ErrH3Protocol)}
			}
			a.hasControl = true
			u.kind = h3StreamControl
		case h3StreamPush:
			// §6.2.2: push streams are server-initiated by definition.
			return h3ConnectionError{Code: h3StreamCreationError, Err: fmt.Errorf(
				"%w: a client opened a push stream", ErrH3Protocol)}
		case h3StreamQPACKEncode:
			if a.hasEncoder {
				return h3ConnectionError{Code: h3StreamCreationError, Err: fmt.Errorf(
					"%w: a second QPACK encoder stream", ErrH3Protocol)}
			}
			a.hasEncoder = true
			u.kind = h3StreamQPACKEncode
			// The bytes after the type varint are instructions, kept: a
			// capacity of zero was advertised, so almost any of them is the
			// peer misusing a table it was told does not exist.
		case h3StreamQPACKDecode:
			if a.hasDecoder {
				return h3ConnectionError{Code: h3StreamCreationError, Err: fmt.Errorf(
					"%w: a second QPACK decoder stream", ErrH3Protocol)}
			}
			a.hasDecoder = true
			u.kind = h3StreamQPACKDecode
			// Like the encoder stream's, the bytes after the type are
			// instructions and are read (decoderInstructions).
		default:
			u.kind = uniKindUnknown
			u.buf = nil
		}
	case h3StreamControl:
		// What is left of an unknown frame goes by unread (§7.2.8). The
		// carry buffer holds at most one incomplete frame this server
		// reads, under MaxHeaderBytes plus its header: parseH3Frames
		// refuses a longer known frame on its declaration and skips an
		// unknown one unbuffered, so what controlFrames keeps is bounded
		// without a check of its own. Checked on arrival, the bound
		// would refuse a long GREASE frame the section says to ignore.
		data, skip := skipAhead(f.Data, u.skip)
		u.skip = skip
		u.buf = append(u.buf, data...)
	case h3StreamQPACKEncode, h3StreamQPACKDecode:
		// Each instruction is consumed as soon as it is whole, so what parks
		// here is at most one integer's few bytes.
		u.buf = append(u.buf, f.Data...)
	}
	switch u.kind {
	case h3StreamControl:
		if err := a.controlFrames(u); err != nil {
			return err
		}
	case h3StreamQPACKEncode:
		if err := a.encoderInstructions(u); err != nil {
			return err
		}
	case h3StreamQPACKDecode:
		if err := a.decoderInstructions(u); err != nil {
			return err
		}
	}
	if f.Fin {
		kind := u.kind
		delete(a.unis, f.StreamID)
		return a.criticalGone(kind)
	}
	return nil
}

// controlFrames consumes whole frames off the control stream's carry buffer:
// SETTINGS first and once (§6.2.1, §7.2.4), nothing that belongs on a
// request stream, and the frames a non-pushing server has no action for
// skipped.
func (a *h3Assembler) controlFrames(u *h3Uni) error {
	if !a.controlSettings && len(u.buf) > 0 {
		// §6.2.1: SETTINGS must be the control stream's very first frame.
		// ParseH3Frames below skips GREASE frame types, as §7.2.8 wants
		// everywhere else — so the first frame's type is read here, before
		// the skip can hide a GREASE frame standing where SETTINGS has to be.
		if typ, _, err := varint.Read(u.buf); err == nil && typ != h3Settings {
			return h3ConnectionError{Code: h3MissingSettings, Err: fmt.Errorf(
				"%w: the control stream opened with frame %#x, not SETTINGS", ErrH3Protocol, typ)}
		}
	}
	frames, rest, skip, err := parseH3Frames(a.frames[:0], u.buf, a.cfg.MaxHeaderBytes)
	// Added, not set: a skip still owed emptied the buffer before the parse,
	// which then started none; a parse that started one found none owed.
	a.frames, u.skip = keepFrames(frames), u.skip+skip
	if err != nil {
		// A frame too long is a layout fault (H3_FRAME_ERROR); an HTTP/2
		// frame type is §7.2.8's H3_FRAME_UNEXPECTED.
		code := uint64(h3FrameError)
		if errors.Is(err, errH3FrameUnexpected) {
			code = h3FrameUnexpected
		}
		return h3ConnectionError{Code: code, Err: err}
	}
	for _, fr := range frames {
		if !a.controlSettings {
			if fr.Type != h3Settings {
				return h3ConnectionError{Code: h3MissingSettings, Err: fmt.Errorf(
					"%w: the control stream opened with frame %#x, not SETTINGS", ErrH3Protocol, fr.Type)}
			}
			if err := a.settings(fr.Payload); err != nil {
				// A payload that ends inside a pair fails the frame's layout,
				// which §7.1 makes H3_FRAME_ERROR; what the pairs say is
				// §7.2.4's H3_SETTINGS_ERROR.
				code := uint64(h3SettingsError)
				if errors.Is(err, errH3FrameLayout) {
					code = h3FrameError
				}
				return h3ConnectionError{Code: code, Err: err}
			}
			a.controlSettings = true
			continue
		}
		switch fr.Type {
		case h3Settings:
			return h3ConnectionError{Code: h3FrameUnexpected, Err: fmt.Errorf(
				"%w: a second SETTINGS on the control stream", ErrH3Protocol)}
		case h3Data, h3Headers, h3PushPromise:
			return h3ConnectionError{Code: h3FrameUnexpected, Err: fmt.Errorf(
				"%w: frame type %#x on the control stream", ErrH3Protocol, fr.Type)}
		case h3GoAway, h3MaxPushID, h3CancelPush:
			// Legal here, and inert: this server never pushes and announces
			// its own shutdown by closing, so there is nothing to act on.
			// Their payloads are still held to their layout (§7.1, §10.8:
			// exactly one identifier) and to the identifier rules of §5.2,
			// §7.2.3 and §7.2.7 — each a MUST for a connection error, and
			// each a peer whose framer or push state is wrong.
			if err := a.controlIdentifier(fr); err != nil {
				return err
			}
		}
	}
	u.buf = keepRest(u.buf, rest)
	return nil
}

// controlIdentifier checks the one-identifier frames of the control stream:
// a GOAWAY carrying a push ID larger than the previous one (§5.2), a
// MAX_PUSH_ID smaller than the previous one (§7.2.7), and any CANCEL_PUSH
// at all — it names a push ID "not yet mentioned by a PUSH_PROMISE frame"
// (§7.2.3), since this server promises none — are connection errors of type
// H3_ID_ERROR; a payload that is not exactly one varint is H3_FRAME_ERROR.
func (a *h3Assembler) controlIdentifier(fr h3Frame) error {
	id, err := h3Identifier(fr.Payload)
	if err != nil {
		return h3ConnectionError{Code: h3FrameError, Err: fmt.Errorf("frame %#x: %w", fr.Type, err)}
	}
	switch fr.Type {
	case h3GoAway:
		if id > a.peerGoAway {
			return h3ConnectionError{Code: h3IDError, Err: fmt.Errorf(
				"%w: GOAWAY(%d) after GOAWAY(%d)", ErrH3Protocol, id, a.peerGoAway)}
		}
		a.peerGoAway = id
	case h3MaxPushID:
		if id < a.peerMaxPush {
			return h3ConnectionError{Code: h3IDError, Err: fmt.Errorf(
				"%w: MAX_PUSH_ID(%d) after MAX_PUSH_ID(%d)", ErrH3Protocol, id, a.peerMaxPush)}
		}
		a.peerMaxPush = id
	case h3CancelPush:
		return h3ConnectionError{Code: h3IDError, Err: fmt.Errorf(
			"%w: CANCEL_PUSH(%d) for a push never promised", ErrH3Protocol, id)}
	}
	return nil
}

// encoderInstructions consumes QPACK encoder-stream instructions against the
// dynamic-table capacity of zero this server advertised. RFC 9204 §3.2.3
// binds the peer that accepted it to send no instruction that uses the table,
// so there is almost nothing legal to arrive here: the one shape tolerated is
// Set Dynamic Table Capacity (§4.3.1) carrying zero, which changes nothing
// and which §4.3.1 gives no ground to refuse. An insert, a duplicate, or a
// capacity above the advertised zero is the connection error
// QPACK_ENCODER_STREAM_ERROR (§4.2, §8.3).
func (a *h3Assembler) encoderInstructions(u *h3Uni) error {
	// One pass over the buffer, one compaction at the end (keepRest):
	// moving the rest to the front after each instruction made a delivery of
	// one-byte instructions quadratic.
	buf := u.buf
	defer func() { u.buf = keepRest(u.buf, buf) }()
	for len(buf) > 0 {
		if buf[0]&0xe0 != 0x20 {
			// Insert With Name Reference (1xxxxxxx), Insert With Literal
			// Name (01xxxxxx) and Duplicate (000xxxxx) all grow or read a
			// table whose capacity cannot leave zero.
			return h3ConnectionError{Code: qpackEncoderStreamError, Err: fmt.Errorf(
				"%w: encoder instruction %#x against a dynamic-table capacity of zero", ErrH3Protocol, buf[0])}
		}
		capacity, rest, err := hpackInt(buf, 5)
		if err != nil {
			// An integer that runs off the end has not all arrived yet; one
			// wider than 62 bits (RFC 9204 §4.1.1) never will be legal.
			if errors.Is(err, errIntegerShort) {
				return nil
			}
			return h3ConnectionError{Code: qpackEncoderStreamError, Err: fmt.Errorf(
				"%w: a dynamic-table capacity wider than 62 bits", ErrH3Protocol)}
		}
		if capacity != 0 {
			return h3ConnectionError{Code: qpackEncoderStreamError, Err: fmt.Errorf(
				"%w: a dynamic-table capacity of %d, but zero was advertised", ErrH3Protocol, capacity)}
		}
		buf = rest
	}
	return nil
}

// decoderInstructions consumes the client's QPACK decoder stream (RFC 9204
// §4.4), which reports on this server's encoder's dynamic table. That table
// is never used: every field section goes out with a Required Insert Count of
// zero and nothing is ever inserted. So Stream Cancellation (§4.4.2), which a
// decoder may send for any reset stream, is the one instruction that can
// legitimately arrive; a Section Acknowledgment (§4.4.1) names a section
// with nothing to acknowledge and an Insert Count Increment (§4.4.3) counts
// inserts never sent, and both are QPACK_DECODER_STREAM_ERROR. Discarding the
// stream unread, as before, accepted either without a word.
func (a *h3Assembler) decoderInstructions(u *h3Uni) error {
	buf := u.buf // one pass, one compaction, as in encoderInstructions
	defer func() { u.buf = keepRest(u.buf, buf) }()
	for len(buf) > 0 {
		switch b := buf[0]; {
		case b&0x80 != 0:
			return h3ConnectionError{Code: qpackDecoderStreamError, Err: fmt.Errorf(
				"%w: a Section Acknowledgment, but no field section used the dynamic table", ErrH3Protocol)}
		case b&0x40 == 0:
			return h3ConnectionError{Code: qpackDecoderStreamError, Err: fmt.Errorf(
				"%w: an Insert Count Increment, but nothing was ever inserted", ErrH3Protocol)}
		}
		// Stream Cancellation: a stream identifier with a 6-bit prefix, and
		// nothing for an encoder without a table to do about it. The
		// identifier is a QUIC stream ID, 62 bits wide (RFC 9000 §2.1), and
		// hpackInt admits the whole width: a decoder that stopped at 28 bits
		// would close the connection over the cancellation of stream
		// 2^28 — a legal instruction a long-lived client reaches, since
		// HTTP/3 caps no request count.
		_, rest, err := hpackInt(buf, 6)
		if err != nil {
			// As in encoderInstructions: an integer that runs off the end
			// has not all arrived yet.
			if errors.Is(err, errIntegerShort) {
				return nil
			}
			return h3ConnectionError{Code: qpackDecoderStreamError, Err: fmt.Errorf(
				"%w: a Stream Cancellation naming a stream wider than 62 bits", ErrH3Protocol)}
		}
		buf = rest
	}
	return nil
}

// peerMaxFieldSection is the MAX_FIELD_SECTION_SIZE the client advertised, or
// h3NoFieldSectionLimit when none has arrived — RFC 9114 §4.2.2 binds a
// sender only once the parameter has been received, and an advertised zero
// binds it to nothing at all.
func (a *h3Assembler) peerMaxFieldSection() uint64 {
	return a.peerFieldSection
}

// settings takes the client's SETTINGS, keeping what this server acts on.
//
// A repeated identifier is refused among the low ones, where every setting
// RFC 9114 and RFC 9204 define lives; above them a repeat is two values of a
// setting this server ignores either way, which §7.2.4 lets it pass over
// (refusing is a MAY) without remembering every identifier to find out.
func (a *h3Assembler) settings(payload []byte) error {
	var seen uint64 // a bit per identifier below 64
	return walkH3Settings(payload, func(id, v uint64) error {
		if id < 64 {
			if seen&(1<<id) != 0 {
				return fmt.Errorf("%w: setting %#x appears twice", ErrH3Protocol, id)
			}
			seen |= 1 << id
		}
		if id == h3SettingMaxFieldSection {
			a.peerFieldSection = v
		}
		return nil
	})
}

// criticalGone is the connection error a closed critical stream is
// (RFC 9114 §6.2.1, RFC 9204 §4.2), and nil for every stream whose closure
// is the peer's own business.
func (a *h3Assembler) criticalGone(kind int) error {
	switch kind {
	case h3StreamControl:
		return h3ConnectionError{Code: h3ClosedCriticalStream, Err: fmt.Errorf(
			"%w: the control stream closed", ErrH3Protocol)}
	case h3StreamQPACKEncode, h3StreamQPACKDecode:
		return h3ConnectionError{Code: h3ClosedCriticalStream, Err: fmt.Errorf(
			"%w: a QPACK stream closed", ErrH3Protocol)}
	}
	return nil
}
