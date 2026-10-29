package pgtype

import (
	"encoding/binary"
	"fmt"
	"math"
	"time"
)

// intervalSize is microseconds, days, months — in that order on the wire.
const intervalSize = 16

// Interval is a PostgreSQL interval, kept as the three independent fields the
// server stores rather than collapsed into one.
//
// # Why this is not a time.Duration
//
// A [time.Duration] is a fixed count of nanoseconds. Two of these three fields
// are not fixed counts of anything:
//
//   - A month is 28, 29, 30 or 31 days depending on which month, so
//     "+1 month" from 31 January is 28 February, and from 31 March is 30
//     April. There is no number of nanoseconds that does this.
//   - A day is 24 hours except across a daylight-saving change, where it is 23
//     or 25. PostgreSQL keeps days separate from microseconds for exactly this
//     reason, and a client that merges them has silently chosen the wrong
//     answer twice a year.
//
// Collapsing them would produce a value that is right most of the time, which
// is the failure mode this project ranks below a crash. So the fields stay
// separate, and [Interval.AddTo] does the arithmetic against a real calendar.
//
// The zero value is a zero interval and is usable.
type Interval struct {
	// Months counts calendar months. PostgreSQL normalises years into it: an
	// interval of '1 year 2 mons' arrives as 14.
	Months int32

	// Days counts calendar days, which are 24 hours except where a time zone
	// says otherwise.
	Days int32

	// Micros counts microseconds, which is the resolution the server stores
	// and therefore the resolution beyond which a round trip loses.
	Micros int64
}

// AddTo returns t advanced by the interval, in t's own location, as the
// server computes timestamp + interval.
//
// The order is the one PostgreSQL uses: months, then days, then the
// microseconds. It matters — adding a month to 31 January and then a day is 1
// March, while adding a day and then a month is 28 February. Anything doing
// this arithmetic by hand has to pick an order too, and picking a different
// one from the server is a difference that appears only on month ends.
//
// The months move the calendar month and keep the day of the month, cut to
// the last day of the month reached: 31 January plus one month is 29 February
// in a leap year and 28 February otherwise, 31 March plus one month is 30
// April — the server's rule, not [time.Time.AddDate]'s, which carries the
// surplus into the next month (2 March). The days then move the date on the
// calendar, keeping the wall-clock time, so a daylight-saving change in t's
// location is handled by the standard library rather than by a constant here.
func (iv Interval) AddTo(t time.Time) time.Time {
	if iv.Months != 0 {
		y, m, d := t.Date()
		months := y*12 + int(m) - 1 + int(iv.Months)
		y, m = months/12, time.Month(months%12)
		if m < 0 {
			y, m = y-1, m+12
		}
		m++
		// Day 0 of the next month is the last day of this one.
		d = min(d, time.Date(y, m+1, 0, 0, 0, 0, 0, time.UTC).Day())
		hh, mm, ss := t.Clock()
		t = time.Date(y, m, d, hh, mm, ss, t.Nanosecond(), t.Location())
	}
	t = t.AddDate(0, 0, int(iv.Days))

	// The microseconds are added through Unix seconds rather than as a
	// [time.Duration], which counts nanoseconds in an int64 and therefore
	// spans only about ±292 years. An interval of a hundred million hours is
	// a value PostgreSQL stores happily and Duration wraps silently.
	secs, usec := iv.Micros/microsPerSecond, iv.Micros%microsPerSecond
	return time.Unix(t.Unix()+secs, int64(t.Nanosecond())+usec*1000).In(t.Location())
}

// Duration converts the interval to a [time.Duration], reporting whether that
// conversion means anything.
//
// It is exact and ok is true only when Months is zero, because a month has no
// length until you say which one. Days are taken as 24 hours, which is what
// they are outside a daylight-saving change — use [Interval.AddTo] where that
// distinction matters, which is any time the result lands near one.
//
// This is the shape it is so that the common case — a timeout or a retry delay
// read out of a configuration table as `interval '30 seconds'` — is one call,
// while the case that cannot work says so instead of returning a number.
func (iv Interval) Duration() (d time.Duration, ok bool) {
	if iv.Months != 0 {
		return 0, false
	}
	// A Duration counts nanoseconds in an int64 and so spans about ±292 years,
	// which is far less than an interval can hold. Every step is checked
	// rather than left to wrap: a duration that came back as its own negation
	// would be worse than one that came back as "no".
	const nsPerDay = 24 * 60 * 60 * int64(time.Second)
	days := int64(iv.Days)
	if days > math.MaxInt64/nsPerDay || days < math.MinInt64/nsPerDay {
		return 0, false
	}
	if iv.Micros > math.MaxInt64/1000 || iv.Micros < math.MinInt64/1000 {
		return 0, false
	}
	fromDays, fromMicros := days*nsPerDay, iv.Micros*1000
	sum := fromDays + fromMicros
	if (fromDays > 0 && fromMicros > 0 && sum < 0) || (fromDays < 0 && fromMicros < 0 && sum > 0) {
		return 0, false
	}
	return time.Duration(sum), true
}

// String renders the interval's three fields as they are stored — months,
// days, microseconds, as in "14mon 3d 14706789000us" — which is what makes a
// failing test readable. It is not PostgreSQL's IntervalStyle output
// ("1 year 2 mons 3 days 04:05:06.789") for the same value.
func (iv Interval) String() string {
	return fmt.Sprintf("%dmon %dd %dus", iv.Months, iv.Days, iv.Micros)
}

// AppendInterval appends an interval in binary format.
//
// The wire order is microseconds, days, months — narrowest unit first, which
// is the opposite of how the type is read aloud and of how the struct is
// written. That inversion is the mistake to make here, so the three writes are
// left explicit rather than looped.
func AppendInterval(dst []byte, iv Interval) []byte {
	dst = binary.BigEndian.AppendUint64(dst, uint64(iv.Micros))  //nolint:gosec // G115: the protocol field is int64
	dst = binary.BigEndian.AppendUint32(dst, uint32(iv.Days))    //nolint:gosec // G115: the protocol field is int32
	return binary.BigEndian.AppendUint32(dst, uint32(iv.Months)) //nolint:gosec // G115: the protocol field is int32
}

// DecodeInterval decodes an interval.
func DecodeInterval(b []byte) (Interval, error) {
	if len(b) != intervalSize {
		return Interval{}, fmt.Errorf("%w: interval is %d bytes, want %d", ErrCodec, len(b), intervalSize)
	}
	return Interval{
		Micros: int64(binary.BigEndian.Uint64(b[0:8])),   //nolint:gosec // G115: the protocol field is int64
		Days:   int32(binary.BigEndian.Uint32(b[8:12])),  //nolint:gosec // G115: the protocol field is int32
		Months: int32(binary.BigEndian.Uint32(b[12:16])), //nolint:gosec // G115: the protocol field is int32
	}, nil
}
