package postgres

import (
	"encoding/binary"
	"errors"
	"math"
	"testing"
	"time"
)

// The wire forms below are built from the protocol's definitions rather than
// captured from a server, because this machine has no credentials for one. The
// integration suite settles them against a real backend the same way the other
// codecs are settled — `SELECT $1 = <literal>`, so the type's own comparison
// operator decides — and that is where they stop being this package's opinion.

func TestTimeRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name  string
		clock TimeOfDay
		want  string
	}{
		{"midnight", TimeOfDay{}, "00:00:00"},
		{"noon", TimeOfDay{Micros: 12 * 3_600_000_000}, "12:00:00"},
		{"with microseconds", TimeOfDay{Micros: 3_723_000_456}, "01:02:03.000456"},
		{"end of day", TimeOfDay{Micros: microsPerDay}, "24:00:00"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := DecodeTime(AppendTime(nil, tc.clock))
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.clock {
				t.Errorf("round trip gave %v, want %v", got, tc.clock)
			}
			if s := tc.clock.String(); s != tc.want {
				t.Errorf("String() = %q, want %q", s, tc.want)
			}
		})
	}
}

// '24:00:00' is a value the server produces, so normalising it to midnight
// here would turn the end of one day into the start of it.
func TestTimeKeepsEndOfDay(t *testing.T) {
	end := TimeOfDay{Micros: microsPerDay}
	got, err := DecodeTime(AppendTime(nil, end))
	if err != nil {
		t.Fatal(err)
	}
	if got.Micros != microsPerDay {
		t.Errorf("'24:00:00' came back as %v", got)
	}
	if !end.InRange() {
		t.Error("InRange refuses a value the server accepts")
	}
	if (TimeOfDay{Micros: microsPerDay + 1}).InRange() {
		t.Error("InRange accepts a value past the end of the day")
	}
}

func TestTimeOfDayFromDiscardsTheDateAndTruncates(t *testing.T) {
	// A wall time whose nanoseconds are not a whole microsecond.
	when := time.Date(2026, 8, 21, 13, 45, 30, 123_456_789, time.FixedZone("x", 3600))
	got := TimeOfDayFrom(when)
	want := int64(13)*3_600_000_000 + 45*60_000_000 + 30*1_000_000 + 123_456
	if got.Micros != want {
		t.Errorf("Micros = %d, want %d", got.Micros, want)
	}
	if got.String() != "13:45:30.123456" {
		t.Errorf("String() = %q", got.String())
	}
}

func TestTimeRejectsAWrongLength(t *testing.T) {
	for _, n := range []int{0, 4, 7, 9} {
		if _, err := DecodeTime(make([]byte, n)); !errors.Is(err, ErrCodec) {
			t.Errorf("DecodeTime(%d bytes) = %v, want ErrCodec", n, err)
		}
	}
}

// The sign of a timetz offset is the trap: the wire carries seconds to
// *subtract*, which is the opposite of every other place an offset appears.
// A codec that flipped it would still decode everything, two hours out.
func TestTimeTZOffsetSignIsTheWireConvention(t *testing.T) {
	// 09:00 at UTC+2 — the wire says -7200.
	v := TimeTZ{Micros: 9 * 3_600_000_000, OffsetSeconds: -7200}

	raw := AppendTimeTZ(nil, v)
	if len(raw) != 12 {
		t.Fatalf("timetz encoded to %d bytes, want 12", len(raw))
	}
	if got := int32(binary.BigEndian.Uint32(raw[8:12])); got != -7200 { //nolint:gosec // G115: the protocol field is int32
		t.Errorf("offset on the wire = %d, want -7200 for UTC+2", got)
	}

	got, err := DecodeTimeTZ(raw)
	if err != nil {
		t.Fatal(err)
	}
	if got != v {
		t.Errorf("round trip gave %+v, want %+v", got, v)
	}
	// And Zone() states it the way Go does: seconds east of UTC.
	if got.Zone() != 7200 {
		t.Errorf("Zone() = %d, want 7200 — the sign was not flipped for the Go convention", got.Zone())
	}
	if _, offset := time.Now().In(time.FixedZone("UTC+2", 7200)).Zone(); offset != got.Zone() {
		t.Errorf("Zone() = %d disagrees with time.FixedZone(7200)", got.Zone())
	}
}

func TestTimeTZRejectsAWrongLength(t *testing.T) {
	for _, n := range []int{0, 8, 11, 13} {
		if _, err := DecodeTimeTZ(make([]byte, n)); !errors.Is(err, ErrCodec) {
			t.Errorf("DecodeTimeTZ(%d bytes) = %v, want ErrCodec", n, err)
		}
	}
}

func TestMacaddrRoundTrip(t *testing.T) {
	mac := [6]byte{0x08, 0x00, 0x2b, 0x01, 0x02, 0x03}
	got, err := DecodeMacaddr(AppendMacaddr(nil, mac))
	if err != nil {
		t.Fatal(err)
	}
	if got != mac {
		t.Errorf("round trip gave % x, want % x", got, mac)
	}
	for _, n := range []int{0, 5, 7, 8} {
		if _, err := DecodeMacaddr(make([]byte, n)); !errors.Is(err, ErrCodec) {
			t.Errorf("DecodeMacaddr(%d bytes) = %v, want ErrCodec", n, err)
		}
	}
}

// macaddr8 is a different type, not a wider one: eight bytes read as six, or
// six padded to eight, are both wrong in a way that still looks like a MAC.
func TestMacaddr8IsNotAWiderMacaddr(t *testing.T) {
	mac8 := [8]byte{0x08, 0x00, 0x2b, 0xff, 0xfe, 0x01, 0x02, 0x03}
	got, err := DecodeMacaddr8(AppendMacaddr8(nil, mac8))
	if err != nil {
		t.Fatal(err)
	}
	if got != mac8 {
		t.Errorf("round trip gave % x, want % x", got, mac8)
	}
	// The six-byte decoder must refuse eight bytes rather than take the first six.
	if _, err := DecodeMacaddr(AppendMacaddr8(nil, mac8)); !errors.Is(err, ErrCodec) {
		t.Error("DecodeMacaddr accepted a macaddr8 and would have returned its first six bytes")
	}
	if _, err := DecodeMacaddr8(AppendMacaddr(nil, [6]byte{1, 2, 3, 4, 5, 6})); !errors.Is(err, ErrCodec) {
		t.Error("DecodeMacaddr8 accepted a six-byte macaddr")
	}
}

// The bit count is in bits and the payload is in bytes. B'1' and B'10000000'
// are the same byte and different values, which is the whole type.
func TestBitsLengthIsInBitsNotBytes(t *testing.T) {
	one := Bits{Len: 1, Bytes: []byte{0x80}}
	eight := Bits{Len: 8, Bytes: []byte{0x80}}

	rawOne, rawEight := AppendBits(nil, one), AppendBits(nil, eight)
	if len(rawOne) != len(rawEight) {
		t.Fatalf("the two encode to %d and %d bytes; they should differ only in the count", len(rawOne), len(rawEight))
	}

	gotOne, err := DecodeBits(rawOne)
	if err != nil {
		t.Fatal(err)
	}
	gotEight, err := DecodeBits(rawEight)
	if err != nil {
		t.Fatal(err)
	}
	if gotOne.Len == gotEight.Len {
		t.Fatal("B'1' and B'10000000' decoded to the same length")
	}
	if gotOne.String() != "1" {
		t.Errorf("B'1' rendered as %q", gotOne.String())
	}
	if gotEight.String() != "10000000" {
		t.Errorf("B'10000000' rendered as %q", gotEight.String())
	}
}

func TestBitsAtCountsFromTheMostSignificantBit(t *testing.T) {
	// 1011 0001, ten bits: the last two are padding and out of range.
	b := Bits{Len: 10, Bytes: []byte{0xb1, 0x40}}
	want := []bool{true, false, true, true, false, false, false, true, false, true}
	for i, w := range want {
		got, err := b.At(i)
		if err != nil {
			t.Fatalf("At(%d): %v", i, err)
		}
		if got != w {
			t.Errorf("At(%d) = %v, want %v", i, got, w)
		}
	}
	if _, err := b.At(10); !errors.Is(err, ErrCodec) {
		t.Error("At accepted an index past the declared length")
	}
	if _, err := b.At(-1); !errors.Is(err, ErrCodec) {
		t.Error("At accepted a negative index")
	}
	if s := b.String(); s != "1011000101" {
		t.Errorf("String() = %q", s)
	}
}

// The declared count is checked against what arrived. Trusting it would let
// At read past the slice, or silently drop the tail.
func TestBitsRejectsACountThatDisagreesWithTheBytes(t *testing.T) {
	tooMany := binary.BigEndian.AppendUint32(nil, 32)
	tooMany = append(tooMany, 0x80) // one byte for thirty-two bits
	if _, err := DecodeBits(tooMany); !errors.Is(err, ErrCodec) {
		t.Errorf("error = %v, want ErrCodec", err)
	}

	tooFew := binary.BigEndian.AppendUint32(nil, 1)
	tooFew = append(tooFew, 0x80, 0x00, 0x00) // three bytes for one bit
	if _, err := DecodeBits(tooFew); !errors.Is(err, ErrCodec) {
		t.Errorf("error = %v, want ErrCodec", err)
	}

	negative := binary.BigEndian.AppendUint32(nil, ^uint32(0)) // -1
	if _, err := DecodeBits(negative); !errors.Is(err, ErrCodec) {
		t.Errorf("error = %v, want ErrCodec", err)
	}

	if _, err := DecodeBits([]byte{0, 0}); !errors.Is(err, ErrCodec) {
		t.Errorf("error = %v, want ErrCodec", err)
	}
}

// The decoded bytes must not alias the wire buffer: it is reused per batch, so
// a borrowed Bits would change under whoever kept it.
func TestBitsCopiesOutOfTheWireBuffer(t *testing.T) {
	raw := AppendBits(nil, Bits{Len: 8, Bytes: []byte{0xff}})
	got, err := DecodeBits(raw)
	if err != nil {
		t.Fatal(err)
	}
	raw[4] = 0x00 // the batch moves on and reuses the buffer
	if got.Bytes[0] != 0xff {
		t.Error("the decoded bits changed when the wire buffer was reused")
	}
}

func TestBitsEmpty(t *testing.T) {
	got, err := DecodeBits(AppendBits(nil, Bits{Len: 0}))
	if err != nil {
		t.Fatal(err)
	}
	if got.Len != 0 || got.String() != "" {
		t.Errorf("empty bit string decoded to %+v", got)
	}
}

// A reading outside the day is not one the server sends, but String is what a
// failing test prints, so it must stay legible at the extremes. Negating
// math.MinInt64 overflows to itself, and the old form printed every field
// with its own sign: "--2562047788:00:-54.-775808".
func TestTimeOfDayStringAtTheExtremes(t *testing.T) {
	for _, tc := range []struct {
		micros int64
		want   string
	}{
		{math.MinInt64, "-2562047788:00:54.775808"},
		{math.MaxInt64, "2562047788:00:54.775807"},
		{-1, "-00:00:00.000001"},
	} {
		if got := (TimeOfDay{Micros: tc.micros}).String(); got != tc.want {
			t.Errorf("TimeOfDay{%d}.String() = %q, want %q", tc.micros, got, tc.want)
		}
	}
}
