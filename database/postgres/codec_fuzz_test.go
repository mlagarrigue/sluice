package postgres

import (
	"bytes"
	"errors"
	"testing"
)

// The codecs read whatever the server sent. The framing layer guarantees the
// length of a value and nothing about its content, so every decoder below is
// reachable with arbitrary bytes by anything that answered the socket.
//
// What is asserted is the contract, not a value: no panic, every failure
// classified as [ErrCodec], and — where the type allows it — an accepted value
// that re-encodes to the bytes it came from. That last property is not a
// correctness check; correctness is pinned by the server's own vectors next
// door. It is a check that the decoder read *all* of the input rather than a
// prefix it found agreeable, which is the way a decoder silently discards
// information.

func FuzzDecodeInet(f *testing.F) {
	f.Add([]byte{2, 32, 0, 4, 192, 168, 1, 1})
	f.Add([]byte{2, 24, 0, 4, 192, 168, 1, 1})
	f.Add([]byte{3, 128, 0, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1})
	f.Add([]byte{2, 8, 1, 4, 10, 0, 0, 0}) // cidr
	f.Add([]byte{})
	f.Add([]byte{2, 255, 0, 4, 1, 2, 3, 4}) // a netmask wider than the address
	f.Add([]byte{9, 32, 0, 4, 1, 2, 3, 4})  // an unknown family

	f.Fuzz(func(t *testing.T, b []byte) {
		p, err := DecodeInet(b)
		if err != nil {
			if !errors.Is(err, ErrCodec) {
				t.Fatalf("unclassified error %v for %x", err, b)
			}
			return
		}
		if !p.IsValid() {
			t.Fatalf("DecodeInet(%x) returned an invalid prefix and no error", b)
		}
		// The is_cidr flag is the one bit a prefix cannot carry, so the
		// re-encoding is only expected to match when it was clear.
		if b[2] == 0 {
			if got := AppendInet(nil, p); !bytes.Equal(got, b) {
				t.Fatalf("DecodeInet(%x) -> %v -> %x: the value did not survive", b, p, got)
			}
		}
	})
}

func FuzzDecodeInterval(f *testing.F) {
	f.Add(make([]byte, intervalSize))
	f.Add([]byte{0, 0, 0, 0, 0, 15, 66, 64, 0, 0, 0, 0, 0, 0, 0, 0})
	f.Add(bytes.Repeat([]byte{255}, intervalSize))
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, b []byte) {
		iv, err := DecodeInterval(b)
		if err != nil {
			if !errors.Is(err, ErrCodec) {
				t.Fatalf("unclassified error %v for %x", err, b)
			}
			return
		}
		if got := AppendInterval(nil, iv); !bytes.Equal(got, b) {
			t.Fatalf("DecodeInterval(%x) -> %v -> %x: the value did not survive", b, iv, got)
		}
		// Duration must never hand back a number it could not compute. The
		// wrap it used to allow turned a positive interval into a negative
		// one, which is worse than refusing.
		if d, ok := iv.Duration(); ok {
			if iv.Months != 0 {
				t.Fatalf("Duration converted an interval carrying %d months", iv.Months)
			}
			if (iv.Days > 0 && iv.Micros >= 0 && d < 0) || (iv.Days < 0 && iv.Micros <= 0 && d > 0) {
				t.Fatalf("Duration(%v) = %v, which has the wrong sign", iv, d)
			}
		}
		// AddTo must not panic whatever the fields hold.
		_ = iv.AddTo(pgEpoch)
	})
}

func FuzzDecodeJSONB(f *testing.F) {
	f.Add([]byte{1, '{', '}'})
	f.Add([]byte{1})
	f.Add([]byte{2, '{', '}'})
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, b []byte) {
		j, err := DecodeJSONB(b)
		if err != nil {
			if !errors.Is(err, ErrCodec) {
				t.Fatalf("unclassified error %v for %x", err, b)
			}
			return
		}
		if got := AppendJSONB(nil, j); !bytes.Equal(got, b) {
			t.Fatalf("DecodeJSONB(%x) -> %q -> %x: the value did not survive", b, j, got)
		}
	})
}

func FuzzDecodeUUIDArray(f *testing.F) {
	f.Add(AppendUUIDArray(nil, [][16]byte{{1}, {2}}))
	f.Add(AppendUUIDArray(nil, nil))
	f.Add(make([]byte, 12))
	f.Add(AppendInt8Array(nil, []int64{1}))
	// A header claiming far more elements than the body can hold: the loop
	// must stop at the bytes, not at the count.
	f.Add([]byte{0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 11, 134, 127, 255, 255, 255, 0, 0, 0, 1})

	f.Fuzz(func(t *testing.T, b []byte) {
		ids, err := DecodeUUIDArray(nil, b)
		if err != nil {
			if !errors.Is(err, ErrCodec) {
				t.Fatalf("unclassified error %v for %x", err, b)
			}
			return
		}
		if len(ids) == 0 {
			return // the empty array, which is zero dimensions rather than an error
		}
		// Idempotence rather than byte equality: the header carries a flags
		// word this decoder has no use for — PostgreSQL sets bit 0 when the
		// array may hold NULLs, which the per-element check already refuses —
		// so a re-encoding is allowed to normalise it. What must not change is
		// the values, and decoding twice is what proves that without also
		// asserting a header detail the type does not depend on.
		again, err := DecodeUUIDArray(nil, AppendUUIDArray(nil, ids))
		if err != nil {
			t.Fatalf("DecodeUUIDArray(%x) -> %x, which then failed to decode: %v", b, ids, err)
		}
		if len(again) != len(ids) {
			t.Fatalf("DecodeUUIDArray(%x) is not idempotent: %d elements then %d", b, len(ids), len(again))
		}
		for i := range ids {
			if again[i] != ids[i] {
				t.Fatalf("DecodeUUIDArray(%x): element %d changed on a second pass", b, i)
			}
		}
	})
}

// The eight bytes shared by timestamp and timestamptz, over their whole range
// rather than over the ±292 years a time.Duration reaches. The wrap that used
// to live here returned a date centuries off with no error, so the property to
// hold is that every accepted value comes back out unchanged.
func FuzzDecodeTimestamp(f *testing.F) {
	f.Add(make([]byte, 8))
	f.Add(AppendInt8(nil, 9467107200000000))  // 2300-01-01
	f.Add(AppendInt8(nil, -9467020800000000)) // 1700-01-01
	f.Add(AppendInt8(nil, 9223371331200000000))
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, b []byte) {
		ts, err := DecodeTimestamp(b)
		if err != nil {
			if !errors.Is(err, ErrCodec) {
				t.Fatalf("unclassified error %v for %x", err, b)
			}
			return
		}
		if got := AppendTimestamp(nil, ts); !bytes.Equal(got, b) {
			t.Fatalf("DecodeTimestamp(%x) -> %v -> %x: the value did not survive", b, ts.UTC(), got)
		}
	})
}
