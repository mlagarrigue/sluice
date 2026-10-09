package postgres

import (
	"encoding/binary"
	"fmt"
	"time"
)

// Type OIDs for the types this file encodes.
const (
	OIDTime     uint32 = 1083
	OIDTimeTZ   uint32 = 1266
	OIDMacaddr  uint32 = 829
	OIDMacaddr8 uint32 = 774
	OIDBit      uint32 = 1560
	OIDVarbit   uint32 = 1562
)

// TimeOfDay is a clock reading with no date and no zone: PostgreSQL's `time`.
//
// It is not a [time.Duration] and not a [time.Time], because it is neither. A
// Duration would invite arithmetic that wraps past midnight into a value the
// server cannot store; a Time would need a date, and the whole point of the
// type is that there is not one. This is microseconds since midnight, which is
// exactly what the wire holds.
type TimeOfDay struct {
	// Micros is microseconds since midnight, in [0, 86_400_000_000).
	//
	// The upper bound is not quite closed on the server: PostgreSQL accepts
	// '24:00:00' as an end-of-day marker. That value round-trips through here
	// unchanged rather than being normalised to zero, because normalising it
	// would turn "the end of Monday" into "the start of Monday".
	Micros int64
}

const microsPerDay = 24 * 60 * 60 * 1_000_000

// InRange reports whether the reading is one the server would accept:
// midnight through '24:00:00' inclusive.
//
// The upper end is inclusive because PostgreSQL's `time` accepts '24:00:00' as
// an end-of-day marker, and it is a value a column can genuinely hold. A
// checker that rejected it would refuse data the server produced.
func (t TimeOfDay) InRange() bool { return t.Micros >= 0 && t.Micros <= microsPerDay }

// String renders the clock reading as PostgreSQL prints it.
func (t TimeOfDay) String() string {
	// The magnitude is taken unsigned: -math.MinInt64 is itself, and a
	// negative remainder would print every field with its own minus sign.
	us := uint64(t.Micros) //nolint:gosec // G115: two's complement, negated below when negative
	neg := ""
	if t.Micros < 0 {
		neg, us = "-", -us
	}
	h, us := us/3_600_000_000, us%3_600_000_000
	m, us := us/60_000_000, us%60_000_000
	s, frac := us/1_000_000, us%1_000_000
	if frac == 0 {
		return fmt.Sprintf("%s%02d:%02d:%02d", neg, h, m, s)
	}
	return fmt.Sprintf("%s%02d:%02d:%02d.%06d", neg, h, m, s, frac)
}

// TimeOfDayFrom builds a clock reading from a wall time, discarding the date
// and the zone. Nanoseconds are truncated to microseconds, which is the
// server's resolution — stated because truncation of a monotonic-looking value
// is the sort of thing that is discovered by a failing round trip.
func TimeOfDayFrom(t time.Time) TimeOfDay {
	h, m, s := t.Clock()
	return TimeOfDay{Micros: int64(h)*3_600_000_000 + int64(m)*60_000_000 +
		int64(s)*1_000_000 + int64(t.Nanosecond())/1000}
}

// AppendTime appends a `time`: microseconds since midnight, as an int8.
func AppendTime(dst []byte, t TimeOfDay) []byte {
	return AppendInt8(dst, t.Micros)
}

// DecodeTime decodes a `time`.
func DecodeTime(b []byte) (TimeOfDay, error) {
	us, err := DecodeInt8(b)
	if err != nil {
		return TimeOfDay{}, fmt.Errorf("%w: time is %d bytes, want 8", ErrCodec, len(b))
	}
	return TimeOfDay{Micros: us}, nil
}

// TimeTZ is a clock reading with a UTC offset and still no date: PostgreSQL's
// `timetz`.
//
// # Use it only when you have to
//
// PostgreSQL's own documentation calls this type of questionable usefulness,
// and it is right: an offset without a date cannot answer what the offset will
// be, because the answer depends on a date through daylight saving. `09:00+02`
// is a different instant in January than in July for the same nominal zone.
// The type is here because columns of it exist and a connector that cannot
// read a column is not a connector — not because anything should be stored in
// it.
type TimeTZ struct {
	// Micros is microseconds since midnight in the stated offset.
	Micros int64

	// OffsetSeconds is the zone offset **as the wire carries it**, which is
	// the number of seconds to *subtract* to reach UTC — the opposite sign
	// from [time.Time.Zone] and from an ISO 8601 suffix. UTC+2 travels as
	// -7200.
	//
	// It is stored in the wire's convention rather than silently flipped,
	// because a sign error here is invisible: every value still decodes, and
	// every one is two hours out in the direction nobody checks. Use
	// [TimeTZ.Zone] to read it the way Go states offsets.
	OffsetSeconds int32
}

// Zone returns the offset the way [time.Time.Zone] does — seconds *east* of
// UTC — which is the opposite sign from [TimeTZ.OffsetSeconds].
func (t TimeTZ) Zone() int { return -int(t.OffsetSeconds) }

// AppendTimeTZ appends a `timetz`: the microseconds, then the offset.
func AppendTimeTZ(dst []byte, t TimeTZ) []byte {
	dst = AppendInt8(dst, t.Micros)
	return AppendInt4(dst, t.OffsetSeconds)
}

// DecodeTimeTZ decodes a `timetz`.
func DecodeTimeTZ(b []byte) (TimeTZ, error) {
	if len(b) != 12 {
		return TimeTZ{}, fmt.Errorf("%w: timetz is %d bytes, want 12", ErrCodec, len(b))
	}
	return TimeTZ{
		Micros:        int64(binary.BigEndian.Uint64(b[0:8])),  //nolint:gosec // G115: the protocol field is int64
		OffsetSeconds: int32(binary.BigEndian.Uint32(b[8:12])), //nolint:gosec // G115: the protocol field is int32
	}, nil
}

// AppendMacaddr appends a `macaddr`: six bytes, in order.
func AppendMacaddr(dst []byte, mac [6]byte) []byte { return append(dst, mac[:]...) }

// DecodeMacaddr decodes a `macaddr`.
//
// The six bytes are the address as written, most significant first — there is
// no endianness to get wrong here, which is worth saying because every other
// fixed-width type in this package has one.
func DecodeMacaddr(b []byte) ([6]byte, error) {
	var mac [6]byte
	if len(b) != 6 {
		return mac, fmt.Errorf("%w: macaddr is %d bytes, want 6", ErrCodec, len(b))
	}
	copy(mac[:], b)
	return mac, nil
}

// AppendMacaddr8 appends a `macaddr8`: eight bytes, the EUI-64 form.
func AppendMacaddr8(dst []byte, mac [8]byte) []byte { return append(dst, mac[:]...) }

// DecodeMacaddr8 decodes a `macaddr8`.
//
// It is a distinct type from `macaddr`, not a wider one: the server converts
// between them by inserting `ff:fe` in the middle, so eight bytes read as six
// or six padded to eight are both wrong in a way that still looks like a MAC
// address.
func DecodeMacaddr8(b []byte) ([8]byte, error) {
	var mac [8]byte
	if len(b) != 8 {
		return mac, fmt.Errorf("%w: macaddr8 is %d bytes, want 8", ErrCodec, len(b))
	}
	copy(mac[:], b)
	return mac, nil
}

// Bits is a string of bits: PostgreSQL's `bit(n)` and `bit varying`.
//
// The length is in **bits**, not bytes, and the two disagree whenever the
// count is not a multiple of eight. That is the whole difficulty of the type:
// `B'1'` and `B'10000000'` occupy the same byte on the wire and are different
// values, so the count has to travel beside the bytes and be believed.
type Bits struct {
	// Len is how many bits are meaningful, counted from the most significant
	// bit of Bytes[0].
	Len int

	// Bytes holds them, padded to a whole byte. The padding bits are zero on
	// anything the server sends.
	Bytes []byte
}

// At reports the bit at index i, counting from the most significant bit of the
// first byte — which is how the server numbers them and the opposite of how a
// Go programmer reaching for `1 << i` would.
func (b Bits) At(i int) (bool, error) {
	if i < 0 || i >= b.Len {
		return false, fmt.Errorf("%w: bit %d of a %d-bit string", ErrCodec, i, b.Len)
	}
	return b.Bytes[i/8]&(0x80>>(i%8)) != 0, nil
}

// String renders the bits the way PostgreSQL prints them.
func (b Bits) String() string {
	out := make([]byte, b.Len)
	for i := range b.Len {
		if b.Bytes[i/8]&(0x80>>(i%8)) != 0 {
			out[i] = '1'
		} else {
			out[i] = '0'
		}
	}
	return string(out)
}

// AppendBits appends a `bit` or `bit varying`: the bit count, then the bytes.
func AppendBits(dst []byte, b Bits) []byte {
	dst = AppendInt4(dst, int32(b.Len)) //nolint:gosec // G115: a caller's bit count
	return append(dst, b.Bytes...)
}

// DecodeBits decodes a `bit` or `bit varying`.
//
// The declared bit count is checked against the bytes that follow rather than
// trusted: a count that claims more bits than arrived would make [Bits.At] read
// past the slice, and a count that claims fewer would silently drop the tail.
func DecodeBits(b []byte) (Bits, error) {
	if len(b) < 4 {
		return Bits{}, fmt.Errorf("%w: bit string is %d bytes, want at least 4", ErrCodec, len(b))
	}
	n := int(int32(binary.BigEndian.Uint32(b[0:4]))) //nolint:gosec // G115: the protocol field is int32
	if n < 0 {
		return Bits{}, fmt.Errorf("%w: bit string declares %d bits", ErrCodec, n)
	}
	body := b[4:]
	if want := (n + 7) / 8; len(body) != want {
		return Bits{}, fmt.Errorf("%w: bit string declares %d bits, which needs %d bytes, and %d arrived", ErrCodec, n, want, len(body))
	}
	// Copied rather than borrowed: the wire buffer is reused per batch, and a
	// Bits handed back pointing into it would change under its owner.
	out := make([]byte, len(body))
	copy(out, body)
	return Bits{Len: n, Bytes: out}, nil
}
