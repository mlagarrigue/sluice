package quic

import "fmt"

// Frame types, RFC 9000 §19. The ranges are types that carry flags in their
// low bits — STREAM is eight types that differ by which fields follow.
const (
	framePadding            = 0x00
	framePing               = 0x01
	frameACK                = 0x02 // 0x02..0x03, the high bit adding ECN counts
	FrameResetStream        = 0x04
	frameStopSending        = 0x05
	frameCrypto             = 0x06
	frameNewToken           = 0x07
	FrameStream             = 0x08 // 0x08..0x0f, the low bits being OFF/LEN/FIN
	frameMaxData            = 0x10
	frameMaxStreamData      = 0x11
	frameMaxStreams         = 0x12 // 0x12..0x13
	frameDataBlocked        = 0x14
	frameStreamDataBlocked  = 0x15
	frameStreamsBlocked     = 0x16 // 0x16..0x17
	frameNewConnectionID    = 0x18
	frameRetireConnectionID = 0x19
	framePathChallenge      = 0x1a
	framePathResponse       = 0x1b
	frameConnectionClose    = 0x1c // 0x1c..0x1d
	frameHandshakeDone      = 0x1e
)

// The bits STREAM's type carries (RFC 9000 §19.8).
const (
	streamFIN = 0x01
	streamLEN = 0x02
	streamOFF = 0x04
)

// Frame is one decoded frame. The fields that mean nothing for a given type
// are zero, and Data borrows the packet's payload — the batch contract, on a
// datagram.
//
// A caller meets frames through [Conn.OnStreamFrames], which delivers the
// stream frames of a packet together: Type is then [FrameStream] (test it
// with [Frame.IsStream]) or [FrameResetStream], and StreamID, Data and Fin
// are what it carries. The transport's own frames are parsed into the same
// type but never leave it, and their fields are not exported.
type Frame struct {
	Type byte

	// StreamID is the stream a STREAM, RESET_STREAM, STOP_SENDING or
	// MAX_STREAM_DATA frame is about.
	StreamID uint64

	// offset is where Data belongs in the stream, for STREAM and CRYPTO —
	// and, reusing the slot, NEW_CONNECTION_ID's Retire Prior To threshold.
	// Stream data reaches a caller as ordered deltas, so the offset is the
	// transport's business, not the caller's.
	offset uint64

	// Data is the payload of a STREAM, CRYPTO, NEW_TOKEN, PATH_CHALLENGE,
	// PATH_RESPONSE or CONNECTION_CLOSE frame. For NEW_CONNECTION_ID it is
	// the identifier with its sixteen-byte stateless reset token appended.
	Data []byte

	// Fin reports a STREAM frame that ends its stream.
	Fin bool

	// value carries the single number a MAX_DATA, MAX_STREAMS,
	// DATA_BLOCKED, STREAMS_BLOCKED or RETIRE_CONNECTION_ID frame is, and
	// NEW_CONNECTION_ID's sequence number.
	value uint64

	// largest and delay are an ACK's first two fields. The ranges are not
	// expanded here: Data holds their encoded form, starting at the range
	// count, and [decodeAckRanges] walks them without allocating — loss
	// detection reads every ACK on the hot path, and a parser that built a
	// slice per ACK would pay for it on each one.
	largest uint64
	delay   uint64
}

// IsStream reports whether the frame carries stream data.
func (f Frame) IsStream() bool { return f.Type&0xf8 == FrameStream }

// ParseFrames decodes every frame in a packet payload, appending to dst.
//
// This is where the batch comes from. A QUIC datagram carries whatever frames
// the sender had ready, which for a multiplexing peer means frames for
// several streams at once — so one read yields a batch by construction,
// without pipelining, without waiting, and without the peer doing anything
// unusual. HTTP/1.1 needed a rare client for this and HTTP/2 needed a
// protocol built around it; here it is the transport's own shape.
//
// Frames borrow the payload, so they are valid for as long as it is.
func parseFrames(dst []Frame, payload []byte) ([]Frame, error) {
	for len(payload) > 0 {
		var (
			f   Frame
			err error
		)
		f, payload, err = parseFrame(payload)
		if err != nil {
			return dst, err
		}
		if f.Type == framePadding {
			continue // padding is not a frame worth reporting, only bytes
		}
		dst = append(dst, f)
	}
	return dst, nil
}

func parseFrame(b []byte) (Frame, []byte, error) {
	// The type is itself a variable-length integer, though every type this
	// package knows fits in one byte.
	typ, rest, err := Varint(b)
	if err != nil {
		return Frame{}, nil, err
	}
	if typ > 0xff {
		return Frame{}, nil, fmt.Errorf("%w: frame type %#x", ErrQUIC, typ)
	}
	f := Frame{Type: byte(typ)}

	switch {
	case f.Type == framePadding, f.Type == framePing, f.Type == frameHandshakeDone:
		return f, rest, nil

	case f.Type&0xf8 == FrameStream:
		return parseStream(f, rest)

	case f.Type == frameCrypto:
		if f.offset, rest, err = Varint(rest); err != nil {
			return f, nil, err
		}
		return takeLengthPrefixed(f, rest)

	case f.Type == frameACK || f.Type == frameACK|1:
		return parseACK(f, rest)

	case f.Type == FrameResetStream:
		rest, err = takeVarints(rest, &f.StreamID, &f.value, &f.offset)
		return f, rest, err

	case f.Type == frameStopSending:
		rest, err = takeVarints(rest, &f.StreamID, &f.value)
		return f, rest, err

	case f.Type == frameNewToken, f.Type == framePathChallenge, f.Type == framePathResponse:
		if f.Type == frameNewToken {
			f, rest, err = takeLengthPrefixed(f, rest)
			if err == nil && len(f.Data) == 0 {
				// §19.7: a zero-length token is FRAME_ENCODING_ERROR, which is
				// the code the caller maps every parse failure here to.
				return f, nil, fmt.Errorf("%w: a NEW_TOKEN frame with an empty token", ErrQUIC)
			}
			return f, rest, err
		}
		// PATH_CHALLENGE and PATH_RESPONSE carry exactly eight bytes, which
		// is the one place the protocol writes a length nowhere.
		f.Data, rest, err = bytesN(rest, 8)
		return f, rest, err

	case f.Type == frameMaxData, f.Type == frameDataBlocked,
		f.Type == frameMaxStreams, f.Type == frameMaxStreams|1,
		f.Type == frameStreamsBlocked, f.Type == frameStreamsBlocked|1,
		f.Type == frameRetireConnectionID:
		rest, err = takeVarints(rest, &f.value)
		return f, rest, err

	case f.Type == frameMaxStreamData, f.Type == frameStreamDataBlocked:
		rest, err = takeVarints(rest, &f.StreamID, &f.value)
		return f, rest, err

	case f.Type == frameNewConnectionID:
		return parseNewConnectionID(f, rest)

	case f.Type == frameConnectionClose, f.Type == frameConnectionClose|1:
		return parseConnectionClose(f, rest)
	}
	return f, nil, fmt.Errorf("%w: unknown frame type %#x", ErrQUIC, f.Type)
}

func parseStream(f Frame, b []byte) (Frame, []byte, error) {
	var err error
	if f.StreamID, b, err = Varint(b); err != nil {
		return f, nil, err
	}
	if f.Type&streamOFF != 0 {
		if f.offset, b, err = Varint(b); err != nil {
			return f, nil, err
		}
	}
	f.Fin = f.Type&streamFIN != 0
	if f.Type&streamLEN == 0 {
		// No length: the frame runs to the end of the packet, which is why
		// such a frame may only be the last one.
		f.Data, b = b, nil
		return f, b, nil
	}
	return takeLengthPrefixed(f, b)
}

// takeLengthPrefixed reads a varint length and that many bytes.
func takeLengthPrefixed(f Frame, b []byte) (Frame, []byte, error) {
	n, b, err := Varint(b)
	if err != nil {
		return f, nil, err
	}
	if n > uint64(len(b)) {
		return f, nil, ErrTruncated
	}
	f.Data, b, err = bytesN(b, int(n)) //nolint:gosec // G115: checked against len(b) above
	return f, b, err
}

// takeVarints decodes consecutive variable-length integers into the fields
// pointed at, and returns what is left.
//
// It takes the destinations and nothing else, deliberately: a version that
// took the frame by value and returned it would hand every caller the copy
// made *before* the fields were written, so every field decoded would be
// silently discarded — RESET_STREAM naming stream zero with code zero
// whatever the peer sent — and nothing would notice until something read
// those fields.
func takeVarints(b []byte, into ...*uint64) ([]byte, error) {
	var err error
	for _, p := range into {
		if *p, b, err = Varint(b); err != nil {
			return nil, err
		}
	}
	return b, nil
}

func parseACK(f Frame, b []byte) (Frame, []byte, error) {
	var count, first uint64
	var err error
	if b, err = takeVarints(b, &f.largest, &f.delay); err != nil {
		return f, nil, err
	}
	ranges := b // from the range count on, kept in Data for decodeAckRanges
	if b, err = takeVarints(b, &count, &first); err != nil {
		return f, nil, err
	}
	// The walk finds where the frame ends and proves the ranges decode; the
	// values are read again by loss detection, from Data, without allocating.
	// The walk is bounded — count is a peer's number.
	if count > uint64(len(b)) {
		return f, nil, fmt.Errorf("%w: an ACK declares %d ranges in %d bytes", ErrQUIC, count, len(b))
	}
	if first > f.largest {
		return f, nil, fmt.Errorf("%w: an ACK whose first range runs below packet zero", ErrQUIC)
	}
	low := f.largest - first
	for range count {
		var gap, length uint64
		if gap, b, err = Varint(b); err != nil {
			return f, nil, err
		}
		if length, b, err = Varint(b); err != nil {
			return f, nil, err
		}
		// Each following range sits gap+2 below the previous one's low end
		// (RFC 9000 §19.3.1), and one that underflows is malformed.
		if gap+2 > low || length > low-gap-2 {
			return f, nil, fmt.Errorf("%w: an ACK range runs below packet zero", ErrQUIC)
		}
		low = low - gap - 2 - length
	}
	f.Data = ranges[:len(ranges)-len(b)]
	if f.Type&1 != 0 { // ECN counts
		var ect0, ect1, ce uint64
		if b, err = takeVarints(b, &ect0, &ect1, &ce); err != nil {
			return f, nil, err
		}
	}
	return f, b, nil
}

// decodeAckRanges walks an ACK's ranges — Data as parseACK stored it — and
// calls visit with each acknowledged range, highest first. The bounds were
// checked when the frame was parsed, so the walk here cannot underflow.
func decodeAckRanges(largest uint64, data []byte, visit func(lo, hi uint64)) error {
	count, b, err := Varint(data)
	if err != nil {
		return err
	}
	var first uint64
	if first, b, err = Varint(b); err != nil {
		return err
	}
	hi, lo := largest, largest-first
	visit(lo, hi)
	for range count {
		var gap, length uint64
		if gap, b, err = Varint(b); err != nil {
			return err
		}
		if length, b, err = Varint(b); err != nil {
			return err
		}
		hi = lo - gap - 2
		lo = hi - length
		visit(lo, hi)
	}
	return nil
}

// appendAckFrame writes an ACK for the given received ranges, highest first,
// as [ackTracker.ranges] maintains them. delay is in microseconds and is
// scaled by this endpoint's ack_delay_exponent, which this package leaves at
// the protocol default of 3.
func appendAckFrame(dst []byte, ranges []pnRange, delayMicros uint64) []byte {
	dst = AppendVarint(dst, frameACK)
	top := ranges[0]
	dst = AppendVarint(dst, top.hi)
	dst = AppendVarint(dst, delayMicros>>3)
	dst = AppendVarint(dst, uint64(len(ranges)-1)) //nolint:gosec // G115: ranges[0] above already requires one range
	dst = AppendVarint(dst, top.hi-top.lo)
	prev := top.lo
	for _, r := range ranges[1:] {
		dst = AppendVarint(dst, prev-r.hi-2)
		dst = AppendVarint(dst, r.hi-r.lo)
		prev = r.lo
	}
	return dst
}

// appendFrame writes a frame that is nothing but its type and a run of
// variable-length integers, which is most of the control frames this package
// sends.
func appendFrame(dst []byte, typ uint64, fields ...uint64) []byte {
	dst = AppendVarint(dst, typ)
	for _, v := range fields {
		dst = AppendVarint(dst, v)
	}
	return dst
}

func parseNewConnectionID(f Frame, b []byte) (Frame, []byte, error) {
	var seq, retire uint64
	var err error
	if b, err = takeVarints(b, &seq, &retire); err != nil {
		return f, nil, err
	}
	if retire > seq {
		return f, nil, fmt.Errorf("%w: NEW_CONNECTION_ID retires %d but is %d", ErrQUIC, retire, seq)
	}
	if len(b) == 0 {
		return f, nil, ErrTruncated
	}
	n := int(b[0])
	if n < 1 || n > maxConnectionIDLen {
		return f, nil, fmt.Errorf("%w: a connection identifier of %d bytes", ErrQUIC, n)
	}
	// The identifier and its sixteen-byte stateless reset token are
	// adjacent on the wire and kept together: Data's last sixteen bytes are
	// the token, what precedes them the identifier. Value carries the
	// sequence number and Offset the Retire Prior To threshold.
	if f.Data, b, err = bytesN(b[1:], n+16); err != nil {
		return f, nil, err
	}
	f.value = seq
	f.offset = retire
	return f, b, nil
}

func parseConnectionClose(f Frame, b []byte) (Frame, []byte, error) {
	var err error
	if b, err = takeVarints(b, &f.value); err != nil {
		return f, nil, err
	}
	if f.Type == frameConnectionClose { // the transport form names a frame type
		var frameType uint64
		if b, err = takeVarints(b, &frameType); err != nil {
			return f, nil, err
		}
	}
	return takeLengthPrefixed(f, b)
}
