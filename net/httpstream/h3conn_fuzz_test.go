package httpstream

import (
	"errors"
	"testing"

	"github.com/mlagarrigue/sluice/net/quic"
)

// fuzzFrameBatches turns fuzz-provided bytes into a sequence of []quic.Frame
// batches — what a sequence of datagrams would hand H3Assembler.Frames.
//
// testing.F only accepts []byte/string/numeric/bool seeds, not a slice of
// structs, so this is the hand-rolled decoder the fuzz function drives: each
// iteration reads a stream id, a length, a FIN bit and that many payload
// bytes off the front of data, and a zero length byte starts a new batch —
// which is what lets one fuzz input exercise several datagrams' worth of
// frames, split and FIN placement included, against one assembler's carry
// buffer.
func fuzzFrameBatches(data []byte) [][]quic.Frame {
	var batches [][]quic.Frame
	var cur []quic.Frame
	for len(data) >= 3 {
		idByte, lenByte, finByte := data[0], data[1], data[2]
		data = data[3:]

		if lenByte == 0 {
			if len(cur) > 0 {
				batches = append(batches, cur)
				cur = nil
			}
			continue
		}
		// A handful of stream ids, including both client-bidi (low two bits
		// clear) and the unidirectional/server-initiated ones Frames must
		// skip, so both paths get exercised.
		id := uint64(idByte % 12)

		n := min(int(lenByte), len(data))
		chunk := data[:n]
		data = data[n:]

		cur = append(cur, quic.Frame{
			Type:     quic.FrameStream,
			StreamID: id,
			Data:     chunk,
			Fin:      finByte&1 == 1,
		})
	}
	if len(cur) > 0 {
		batches = append(batches, cur)
	}
	return batches
}

// isWellFormedH3Error reports whether err is one of this package's own named
// errors — ErrH3Protocol for the frame and request-shape refusals H3Assembler makes
// itself, ErrTooLarge for the bounds it enforces, or ErrHPACK, because QPACK
// string decoding (qpackString, in h3.go) reuses HPACK's integer and Huffman
// code and its errors with it. Anything else would mean a plain, unclassified
// error reached a caller that has no way to tell a protocol violation from a
// bug.
func isWellFormedH3Error(err error) bool {
	return errors.Is(err, ErrH3Protocol) || errors.Is(err, ErrTooLarge) || errors.Is(err, ErrHPACK)
}

// FuzzH3Assembler drives H3Assembler.Frames with synthetic QUIC stream
// frames, split across datagram-like batches and with varied FIN placement —
// exactly what drain's carry buffer (h3conn.go's h3Stream.buf, folded in
// around Frames/drain) exists to reassemble, and nothing today fuzzes that
// path directly.
//
// The assertions: no panic ever, any error returned is one of this package's
// own well-formed errors rather than something a caller cannot classify, and
// the carry buffer a malformed or slow-trickling peer can grow never exceeds
// the per-stream raw bound — Frames checks len(st.buf)+len(f.Data) against
// it before appending, so an assembler that is doing its job holds that
// invariant for every input.
//
// Credit is wired the way ServeH3 wires it under manual flow control, and the
// credit balance is checked after every batch: every byte delivered is
// either released, still retained, or handed off in a request body. A path
// that forgets to release starves a real peer's window — silently, and only
// on that path.
func FuzzH3Assembler(f *testing.F) {
	// A whole, well-formed request in one frame.
	f.Add(append([]byte{0, byte(len(h3Request("GET", "/x"))), 1}, h3Request("GET", "/x")...))
	// The same request split into two frames on the same stream, no FIN on
	// the first — the ordinary split-across-datagrams case.
	whole := h3Request("POST", "/split")
	half := len(whole) / 2
	var split []byte
	split = append(split, 0, byte(half), 0)
	split = append(split, whole[:half]...)
	split = append(split, 0, byte(len(whole)-half), 1)
	split = append(split, whole[half:]...)
	f.Add(split)
	// A batch separator (a zero length byte) between two requests on
	// different streams.
	var twoBatches []byte
	twoBatches = append(twoBatches, 0, byte(len(h3Request("GET", "/a"))), 1)
	twoBatches = append(twoBatches, h3Request("GET", "/a")...)
	twoBatches = append(twoBatches, 5, 0, 0) // batch separator
	twoBatches = append(twoBatches, 4, byte(len(h3Request("GET", "/b"))), 1)
	twoBatches = append(twoBatches, h3Request("GET", "/b")...)
	f.Add(twoBatches)
	// Junk on a unidirectional stream id, which Frames must ignore rather
	// than misread as a request.
	f.Add([]byte{2, 4, 0, 0xff, 0xff, 0xff, 0xff})
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0})

	f.Fuzz(func(t *testing.T, data []byte) {
		cfg := Config{MaxBodyBytes: 4096, MaxHeaderBytes: 4096, MaxConcurrentStreams: 16}.withDefaults()
		a := newH3Assembler(cfg)
		var released, delivered, handedOff uint64
		balanced := true
		a.Credit = func(_, n uint64) { released += n }

		batches := fuzzFrameBatches(data)
		for _, frames := range batches {
			for _, f := range frames {
				delivered += uint64(len(f.Data))
			}
			batch, ids, failed, err := a.Frames(frames)
			for _, r := range batch.Items {
				handedOff += uint64(len(r.Body))
			}
			// A connection error abandons the rest of its batch unread, so
			// the balance is only defined up to the first one; the
			// connection it ended would carry no more credit anyway.
			if err != nil {
				balanced = false
			}
			retained := a.assembling
			for _, u := range a.unis {
				retained += len(u.buf)
			}
			if balanced && released+uint64(retained)+handedOff != delivered { //nolint:gosec // G115: test sums
				t.Fatalf("delivered %d ≠ released %d + retained %d + handed off %d",
					delivered, released, retained, handedOff)
			}
			if err != nil {
				if !isWellFormedH3Error(err) {
					t.Fatalf("Frames returned an error not wrapping a known package error: %v", err)
				}
				continue
			}
			if batch.Len() != len(ids) {
				t.Fatalf("%d requests but %d stream ids", batch.Len(), len(ids))
			}
			for _, se := range failed {
				if !isWellFormedH3Error(se.Err) {
					t.Errorf("stream %d failed with %v, not wrapping a known package error", se.StreamID, se.Err)
				}
			}
			// The carry buffer invariant: whatever is still parked per
			// stream never exceeds the raw bound, because Frames checks
			// before it appends.
			for id, st := range a.streams {
				if len(st.buf) > a.rawStreamBound() {
					t.Fatalf("stream %d's carry buffer holds %d bytes, over the %d bound",
						id, len(st.buf), a.rawStreamBound())
				}
				if len(st.req.Body) > cfg.MaxBodyBytes {
					t.Fatalf("stream %d's body holds %d bytes, over the %d bound",
						id, len(st.req.Body), cfg.MaxBodyBytes)
				}
			}
			if len(a.streams) > cfg.MaxConcurrentStreams {
				t.Fatalf("%d concurrently open streams, over the %d bound", len(a.streams), cfg.MaxConcurrentStreams)
			}
		}
	})
}
