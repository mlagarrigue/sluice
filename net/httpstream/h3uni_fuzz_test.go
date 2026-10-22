package httpstream

import (
	"errors"
	"testing"

	"github.com/mlagarrigue/sluice/net/quic"
)

// FuzzH3Uni drives the client's unidirectional streams alone — control,
// QPACK encoder and decoder, push, unknown types — split across deliveries
// and closed anywhere. Two properties:
//
//   - every error is the connection's (h3ConnectionError): RFC 9114 §6.2.1
//     and §8 make a fault on a critical stream a connection error, and a
//     unidirectional stream has no response to refuse it on, so nothing of
//     it may come back as a stream error;
//   - what each stream parks between deliveries is bounded whatever the
//     peer sends: under eight bytes before the type varint has arrived,
//     nothing for a type that is discarded, one QPACK integer on the QPACK
//     pair, and one frame of at most MaxHeaderBytes with its header on the
//     control stream (parseH3Frames refuses a longer one on its
//     declaration and skips an unknown one unbuffered).
//
// The input decodes like FuzzH3Assembler's — stream, length, FIN, then the
// bytes, a zero length starting a new delivery — onto the four client
// unidirectional ids 2, 6, 10 and 14.
func FuzzH3Uni(f *testing.F) {
	frame := func(id, fin byte, data ...byte) []byte {
		return append([]byte{id, byte(len(data)), fin}, data...)
	}
	settings := []byte{h3StreamControl, h3Settings, 0}
	f.Add(frame(0, 0, settings...))
	f.Add(append(frame(0, 0, h3StreamControl, h3Settings), frame(0, 0, 0)...))         // split inside SETTINGS
	f.Add(append(frame(0, 0, settings...), frame(1, 0, h3StreamQPACKEncode, 0x20)...)) // capacity 0
	f.Add(append(frame(0, 0, settings...), frame(2, 0, h3StreamQPACKDecode, 0x01)...)) // insert count increment
	f.Add(frame(1, 0, h3StreamPush, 0))                                                // a client push stream
	f.Add(append(frame(0, 0, settings...), frame(1, 0, h3StreamControl)...))           // a second control stream
	f.Add(frame(0, 0, h3StreamControl, 0x07, 1, 0))                                    // GOAWAY before SETTINGS
	f.Add(frame(0, 1, settings...))                                                    // the control stream closed
	f.Add(frame(3, 0, 0x40, 0x21, 0xff, 0xff))                                         // a reserved type, discarded
	f.Add(frame(0, 0, 0x40))                                                           // a type varint cut short
	f.Add(append(frame(0, 0, settings...), frame(0, 0, 0x21, 0x50, 0x00, 1, 2)...))    // a long GREASE frame

	// A known frame declared past MaxHeaderBytes, its bytes then arriving:
	// refused on the declaration, never parked.
	long := append(frame(0, 0, settings...), frame(0, 0, 0x07, 0x48, 0x00)...)
	for range 5 {
		long = append(long, frame(0, 0, make([]byte, 250)...)...)
	}
	f.Add(long)

	f.Fuzz(func(t *testing.T, data []byte) {
		cfg := Config{MaxHeaderBytes: 1024}.mustResolve()
		a := testAssembler(cfg)
		for _, frames := range uniFrameBatches(data) {
			_, _, failed, err := a.Frames(frames)
			for _, se := range failed {
				if se.StreamID%4 == 2 {
					t.Fatalf("unidirectional stream %d failed alone with %v; its errors are the connection's", se.StreamID, se.Err)
				}
			}
			if err != nil {
				if _, ok := errors.AsType[h3ConnectionError](err); !ok {
					t.Fatalf("a unidirectional stream ended with %v (%T), not a connection error", err, err)
				}
				return // the connection is over; nothing more is read
			}
			for id, u := range a.unis {
				var bound int
				switch u.kind {
				case -1:
					bound = 7
				case uniKindUnknown:
					bound = 0
				case h3StreamQPACKEncode, h3StreamQPACKDecode:
					bound = 16
				case h3StreamControl:
					bound = cfg.MaxHeaderBytes + 16
				}
				if len(u.buf) > bound {
					t.Fatalf("unidirectional stream %d (kind %d) parks %d bytes, over %d", id, u.kind, len(u.buf), bound)
				}
			}
		}
	})
}

// uniFrameBatches is fuzzFrameBatches onto the client unidirectional ids.
func uniFrameBatches(data []byte) [][]quic.Frame {
	batches := fuzzFrameBatches(data)
	for _, b := range batches {
		for i := range b {
			b[i].StreamID = 2 + 4*(b[i].StreamID%4)
		}
	}
	return batches
}
