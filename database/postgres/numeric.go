package postgres

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// OIDNumeric is PostgreSQL's arbitrary-precision decimal.
const OIDNumeric uint32 = 1700

// NumericSign says what kind of value a [Numeric] holds.
type NumericSign uint16

const (
	// NumericPositive and NumericNegative are ordinary numbers.
	NumericPositive NumericSign = 0x0000
	NumericNegative NumericSign = 0x4000

	// NumericNaN is the value numeric can hold that no float can be trusted
	// to round-trip.
	NumericNaN NumericSign = 0xC000

	// NumericPosInf and NumericNegInf exist from PostgreSQL 14. Older servers
	// never send them; decoding one from an older server would mean the
	// stream is being misread, which is why they are named rather than
	// treated as unknown.
	NumericPosInf NumericSign = 0xD000
	NumericNegInf NumericSign = 0xF000
)

// numericBase is the base the wire format counts in: four decimal digits per
// stored digit.
const numericBase = 10000

// Numeric is an exact decimal, held the way the wire holds it.
//
// # Why this is not a float64
//
// A numeric column is chosen precisely when a float would lose what matters —
// a monetary amount, a measured quantity, a rate with a mandated number of
// decimals. Decoding it into float64 would make this package the thing that
// destroys the property the column exists for, and it would do so silently,
// which is the failure this project ranks below a crash. So the value is kept
// exactly, and every lossy reading is a method the caller has to name:
// [Numeric.Float64] says what it does in its own signature.
//
// # Why it is not big.Rat either
//
// A rational would be exact and would lose something else: the **display
// scale**. `10.00` and `10` are the same rational and different numerics, and
// the difference is not decoration — it is what a schema declares with
// `numeric(12,2)` and what an invoice prints. Keeping the wire's own shape
// keeps that.
//
// The zero Numeric is a valid zero: no digits, positive, scale zero.
type Numeric struct {
	// Sign is the value's kind — including the ones that are not numbers.
	Sign NumericSign

	// Weight is the position of the first digit, counted in powers of 10000:
	// the value is Digits[0]·10000^Weight + Digits[1]·10000^(Weight−1) + …
	Weight int16

	// Dscale is how many decimal digits follow the point. It is what
	// distinguishes 10.00 from 10, and it is carried rather than derived.
	Dscale int16

	// Digits holds the value in base 10000, most significant first. Each is
	// in [0, 9999].
	Digits []int16
}

// IsNaN reports whether the value is NaN.
func (n Numeric) IsNaN() bool { return n.Sign == NumericNaN }

// IsInf reports whether the value is an infinity, and its direction.
func (n Numeric) IsInf() (inf, positive bool) {
	switch n.Sign {
	case NumericPosInf:
		return true, true
	case NumericNegInf:
		return true, false
	}
	return false, false
}

// numericHeaderSize is the digit count, weight, sign and display scale — four
// int16s, and the entire value when the sign is NaN or an infinity.
const numericHeaderSize = 8

// numericMaxDscale is PostgreSQL's maximum display scale. A value claiming more
// decimal places than this is not one the server can hold, so it did not come
// from one.
const numericMaxDscale = 16383

// DecodeNumeric decodes a numeric.
func DecodeNumeric(b []byte) (Numeric, error) {
	if len(b) < numericHeaderSize {
		return Numeric{}, fmt.Errorf("%w: numeric header is %d bytes, want at least %d", ErrCodec, len(b), numericHeaderSize)
	}
	ndigits := int(int16(binary.BigEndian.Uint16(b[0:2]))) //nolint:gosec // G115: the protocol field is int16
	n := Numeric{
		Weight: int16(binary.BigEndian.Uint16(b[2:4])), //nolint:gosec // G115: the protocol field is int16
		Sign:   NumericSign(binary.BigEndian.Uint16(b[4:6])),
		Dscale: int16(binary.BigEndian.Uint16(b[6:8])), //nolint:gosec // G115: the protocol field is int16
	}

	switch n.Sign {
	case NumericPositive, NumericNegative:
	case NumericNaN, NumericPosInf, NumericNegInf:
		// These carry no digits, and a server sending some would mean the
		// stream is being read at the wrong offset.
		//
		// Weight and Dscale are kept as sent rather than zeroed, and must not
		// be refused for being non-zero: PostgreSQL sends **dscale = 32** for
		// both infinities — `numeric_send('Infinity')` is `00000000d0000020` —
		// while NaN gets zero. Refusing a non-zero dscale here reads like the
		// obvious tightening and would reject every infinity a real server
		// sends.
		if ndigits != 0 {
			return Numeric{}, fmt.Errorf("%w: numeric %#04x carries %d digits", ErrCodec, uint16(n.Sign), ndigits)
		}
		// The header is the whole value, so anything after it means the stream
		// is being read at the wrong offset. This branch used to return before
		// the length check below and so accepted any amount of trailing
		// padding — the one place in this package that took a prefix it found
		// agreeable and ignored the rest.
		if len(b) != numericHeaderSize {
			return Numeric{}, fmt.Errorf("%w: numeric %#04x is %d bytes, want %d", ErrCodec, uint16(n.Sign), len(b), numericHeaderSize)
		}
		return n, nil
	default:
		return Numeric{}, fmt.Errorf("%w: numeric sign %#04x is not one this package knows", ErrCodec, uint16(n.Sign))
	}

	if ndigits < 0 {
		return Numeric{}, fmt.Errorf("%w: numeric declares %d digits", ErrCodec, ndigits)
	}
	// The display scale bounds how much [Numeric.String] renders after the
	// point, so a scale nothing could have produced is both a wrong value and
	// a rendering this process pays for. A negative one is not a value at all.
	if n.Dscale < 0 || n.Dscale > numericMaxDscale {
		return Numeric{}, fmt.Errorf("%w: numeric display scale is %d, want 0..%d", ErrCodec, n.Dscale, numericMaxDscale)
	}
	// A value with no digits is zero, and zero has weight zero — the server
	// sends that for `0`, for `0.00` and for twenty-four decimal places of it.
	// Weight is the count of base-10000 groups [Numeric.String] walks before
	// the point, so a header claiming none of them exist while placing the
	// first one 12336 groups up renders fifty thousand characters of zeros
	// from eight bytes of input. That is a wrong value first and an
	// amplification second, and this check refuses both — for the
	// digitless case only. A value with digits may sit at any int16 weight:
	// the server allows 131072 digits before the point, so ten bytes can
	// still render that many, and that bound is the type's, not a defect.
	if ndigits == 0 && n.Weight != 0 {
		return Numeric{}, fmt.Errorf("%w: numeric has no digits but a weight of %d, and zero has weight zero", ErrCodec, n.Weight)
	}
	if want := numericHeaderSize + 2*ndigits; len(b) != want {
		return Numeric{}, fmt.Errorf("%w: numeric is %d bytes for %d digits, want %d", ErrCodec, len(b), ndigits, want)
	}
	if ndigits > 0 {
		// The first group is never zero in a value the server produced: it
		// strips leading zero groups, so `0.0001` arrives as one digit with a
		// weight of −1 rather than as a run of zeros. Zeros in the *middle*
		// are ordinary — `10000.0001` is 1, 0, 1 — and only the first is
		// judged here.
		//
		// It is refused rather than tolerated because [Numeric.String] writes
		// the leading group unpadded and the rest four-wide, so a zero there
		// renders "08240000…" — text that parses back to a different number.
		// A misread stream would otherwise become a plausible value instead of
		// an error.
		if first := int16(binary.BigEndian.Uint16(b[8:])); first == 0 { //nolint:gosec // G115: the protocol field is int16
			return Numeric{}, fmt.Errorf("%w: numeric's first digit group is zero, which the server strips", ErrCodec)
		}
		n.Digits = make([]int16, ndigits)
		for i := range ndigits {
			d := int16(binary.BigEndian.Uint16(b[8+2*i:])) //nolint:gosec // G115: the protocol field is int16
			if d < 0 || d >= numericBase {
				return Numeric{}, fmt.Errorf("%w: numeric digit %d is %d, want 0..9999", ErrCodec, i, d)
			}
			n.Digits[i] = d
		}
	}
	return n, nil
}

// AppendNumeric appends a numeric.
func AppendNumeric(dst []byte, n Numeric) []byte {
	switch n.Sign {
	case NumericNaN, NumericPosInf, NumericNegInf:
		// No digits, but Weight and Dscale travel as they were decoded rather
		// than as zeros. The server tolerates zeros on input — it compares
		// equal either way — so this is not about being understood. It is
		// about a value read from a column and written back producing the
		// bytes it arrived as, which is what lets the round trip be asserted
		// without an exception, and an assertion with an exception in it is
		// one a real defect can hide behind.
		dst = binary.BigEndian.AppendUint16(dst, 0)                // no digits
		dst = binary.BigEndian.AppendUint16(dst, uint16(n.Weight)) //nolint:gosec // G115: two's complement
		dst = binary.BigEndian.AppendUint16(dst, uint16(n.Sign))
		return binary.BigEndian.AppendUint16(dst, uint16(n.Dscale)) //nolint:gosec // G115: two's complement
	}
	dst = binary.BigEndian.AppendUint16(dst, uint16(len(n.Digits))) //nolint:gosec // G115: a value's digit count
	dst = binary.BigEndian.AppendUint16(dst, uint16(n.Weight))      //nolint:gosec // G115: two's complement
	dst = binary.BigEndian.AppendUint16(dst, uint16(n.Sign))
	dst = binary.BigEndian.AppendUint16(dst, uint16(n.Dscale)) //nolint:gosec // G115: two's complement
	for _, d := range n.Digits {
		dst = binary.BigEndian.AppendUint16(dst, uint16(d)) //nolint:gosec // G115: bounded to 0..9999
	}
	return dst
}

// ParseNumeric reads an exact decimal from its text form: an optional sign,
// digits, an optional point and more digits. "NaN", "Infinity" and
// "-Infinity" are accepted, case-insensitively, as PostgreSQL writes them.
//
// This is how a caller gets a value in without going through a float: the
// text of a decimal is exact, and parsing it here means the library never
// holds an approximation it would then have to justify.
func ParseNumeric(s string) (Numeric, error) {
	t := strings.TrimSpace(s)
	switch strings.ToLower(t) {
	case "nan":
		return Numeric{Sign: NumericNaN}, nil
	case "infinity", "+infinity", "inf", "+inf":
		return Numeric{Sign: NumericPosInf}, nil
	case "-infinity", "-inf":
		return Numeric{Sign: NumericNegInf}, nil
	}

	n := Numeric{Sign: NumericPositive}
	switch {
	case strings.HasPrefix(t, "-"):
		n.Sign, t = NumericNegative, t[1:]
	case strings.HasPrefix(t, "+"):
		t = t[1:]
	}

	intPart, fracPart, _ := strings.Cut(t, ".")
	if intPart == "" && fracPart == "" {
		return Numeric{}, fmt.Errorf("%w: %q is not a decimal", ErrCodec, s)
	}
	for _, part := range []string{intPart, fracPart} {
		for _, c := range part {
			if c < '0' || c > '9' {
				return Numeric{}, fmt.Errorf("%w: %q is not a decimal", ErrCodec, s)
			}
		}
	}
	if len(fracPart) > math.MaxInt16 {
		return Numeric{}, fmt.Errorf("%w: %d decimal places is past what numeric carries", ErrCodec, len(fracPart))
	}
	n.Dscale = int16(len(fracPart)) //nolint:gosec // G115: checked above

	// Group into base-10000 digits: the integer part from the point
	// leftwards, the fraction from the point rightwards, each padded so the
	// groups align on the point rather than on the ends of the string.
	if pad := len(intPart) % 4; pad != 0 {
		intPart = strings.Repeat("0", 4-pad) + intPart
	}
	if pad := len(fracPart) % 4; pad != 0 {
		fracPart += strings.Repeat("0", 4-pad)
	}

	digits := make([]int16, 0, (len(intPart)+len(fracPart))/4)
	for i := 0; i < len(intPart); i += 4 {
		digits = append(digits, groupValue(intPart[i:i+4]))
	}
	intGroups := len(digits)
	for i := 0; i < len(fracPart); i += 4 {
		digits = append(digits, groupValue(fracPart[i:i+4]))
	}

	// The weight counts from the last integer group, so it is one less than
	// the number of them; leading zero groups shift it down as they go.
	//
	// The shift is not stopped at zero. A negative weight is how the format
	// expresses a value below 1 — the server sends `0.0001` as one digit at
	// weight −1 and `0.00001` as one digit at weight −2 — so a guard keeping
	// the weight non-negative would leave a run of zero groups in front of
	// every small value. It did: `0.000000000000000000001` went out as six
	// digit groups where the server sends one, ten wasted bytes on a type this
	// package exists to move by the thousand. The value was right and the
	// shape was not, which is why nothing failed.
	weight := intGroups - 1
	lead := 0
	for lead < len(digits) && digits[lead] == 0 {
		lead++
	}
	digits = digits[lead:]
	weight -= lead

	// Trailing zero groups carry nothing: the scale already says how many
	// decimals to print.
	for len(digits) > 0 && digits[len(digits)-1] == 0 {
		digits = digits[:len(digits)-1]
	}
	if len(digits) == 0 {
		return Numeric{Sign: n.Sign, Dscale: n.Dscale}, nil
	}
	if weight < math.MinInt16 || weight > math.MaxInt16 {
		return Numeric{}, fmt.Errorf("%w: %q is past the range numeric carries", ErrCodec, s)
	}
	n.Weight = int16(weight) //nolint:gosec // G115: checked above
	n.Digits = digits
	return n, nil
}

func groupValue(s string) int16 {
	v := 0
	for i := range 4 {
		v = v*10 + int(s[i]-'0')
	}
	return int16(v) //nolint:gosec // G115: four decimal digits fit in int16
}

// String renders the value exactly, with the number of decimals its scale
// declares — so a numeric(12,2) holding ten prints "10.00", which is the
// difference the type exists to keep.
func (n Numeric) String() string {
	switch n.Sign {
	case NumericNaN:
		return "NaN"
	case NumericPosInf:
		return "Infinity"
	case NumericNegInf:
		return "-Infinity"
	}

	var b strings.Builder
	if n.Sign == NumericNegative {
		b.WriteByte('-')
	}

	// The integer part: every group from the weight down to zero, whether or
	// not the value carries a digit for it.
	if n.Weight < 0 {
		b.WriteByte('0')
	} else {
		for i := 0; i <= int(n.Weight); i++ {
			d := n.digitAt(i)
			if i == 0 {
				b.WriteString(strconv.Itoa(int(d)))
				continue
			}
			fmt.Fprintf(&b, "%04d", d)
		}
	}

	if n.Dscale <= 0 {
		return b.String()
	}
	b.WriteByte('.')
	// The fraction: groups after the point, cut to exactly Dscale digits.
	var frac strings.Builder
	for i := int(n.Weight) + 1; frac.Len() < int(n.Dscale); i++ {
		fmt.Fprintf(&frac, "%04d", n.digitAt(i))
	}
	b.WriteString(frac.String()[:n.Dscale])
	return b.String()
}

// digitAt returns the base-10000 digit at a position, or zero where the value
// carries none — a numeric is stored without its leading and trailing zero
// groups.
func (n Numeric) digitAt(i int) int16 {
	if i < 0 || i >= len(n.Digits) {
		return 0
	}
	return n.Digits[i]
}

// Float64 converts to a float64, losing whatever a float64 cannot hold.
//
// The name is the warning, and it is the only way out of this type that is
// allowed to be approximate: a caller that writes Float64 has said so. NaN and
// the infinities convert to their float counterparts.
//
// Approximate, but correctly rounded: the result is the float64 nearest the
// exact value, as [strconv.ParseFloat] gives for the same digits. A value
// beyond the float64 range saturates to an infinity or to zero.
func (n Numeric) Float64() float64 {
	switch n.Sign {
	case NumericNaN:
		return math.NaN()
	case NumericPosInf:
		return math.Inf(1)
	case NumericNegInf:
		return math.Inf(-1)
	}
	// The digits are written out as "<decimal digits>e<exponent>" and parsed
	// once. Summing digit × 10000^k in float64 instead rounds at every term
	// and misses the nearest float on about one value in five; the decimal
	// form lets strconv round exactly once. The buffer stays on the stack for
	// the common sizes.
	buf := make([]byte, 0, 64)
	if n.Sign == NumericNegative {
		buf = append(buf, '-')
	}
	buf = append(buf, '0')
	for _, d := range n.Digits {
		if d < 0 || d >= numericBase {
			// Not from the wire — DecodeNumeric refuses it — but Digits is
			// an exported field, so a hand-built value still converts.
			return n.float64Sum()
		}
		buf = append(buf, byte('0'+d/1000), byte('0'+d/100%10), byte('0'+d/10%10), byte('0'+d%10)) //nolint:gosec // G115: d is in [0, 9999]
	}
	buf = append(buf, 'e')
	buf = strconv.AppendInt(buf, 4*(int64(n.Weight)+1-int64(len(n.Digits))), 10)
	// The only error left is ErrRange, and the value returned with it is the
	// saturation the doc promises.
	f, _ := strconv.ParseFloat(string(buf), 64)
	return f
}

// float64Sum is the term-by-term conversion, kept for digits outside the
// base the decimal form can spell.
func (n Numeric) float64Sum() float64 {
	v := 0.0
	for i, d := range n.Digits {
		v += float64(d) * math.Pow(numericBase, float64(int(n.Weight)-i))
	}
	if n.Sign == NumericNegative {
		return -v
	}
	return v
}
