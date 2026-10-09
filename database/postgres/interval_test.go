package postgres

import (
	"bytes"
	"testing"
	"time"
)

// # interval

func TestIntervalAgainstServerVectors(t *testing.T) {
	tests := []struct {
		sql string
		iv  Interval
		hex string
	}{
		{"1 mon", Interval{Months: 1}, "00000000000000000000000000000001"},
		{"1 day", Interval{Days: 1}, "00000000000000000000000100000000"},
		{"00:00:01", Interval{Micros: 1_000_000}, "00000000000f42400000000000000000"},
		{
			"-1 mon -1 day -00:00:01",
			Interval{Months: -1, Days: -1, Micros: -1_000_000},
			"fffffffffff0bdc0ffffffffffffffff",
		},
		{
			// Years are normalised into months by the server: 1 year 2 mons
			// arrives as 14, and a client that kept a Years field would have
			// to invent it back.
			"1 year 2 mons 3 days 04:05:06.789",
			Interval{Months: 14, Days: 3, Micros: 14_706_789_000},
			"000000036c97ca88000000030000000e",
		},
	}
	for _, tc := range tests {
		t.Run(tc.sql, func(t *testing.T) {
			want := mustHex(t, tc.hex)
			if got := AppendInterval(nil, tc.iv); !bytes.Equal(got, want) {
				t.Errorf("AppendInterval = %x, want %x", got, want)
			}
			back, err := DecodeInterval(want)
			if err != nil {
				t.Fatalf("DecodeInterval: %v", err)
			}
			if back != tc.iv {
				t.Errorf("DecodeInterval = %v, want %v", back, tc.iv)
			}
		})
	}
}

// The arithmetic a Duration cannot do. These are the cases that make the three
// fields worth keeping apart.
func TestIntervalAddTo(t *testing.T) {
	tests := []struct {
		name string
		from time.Time
		iv   Interval
		want time.Time
	}{
		{
			"a month from the 31st lands on the end of a shorter month",
			time.Date(2024, 1, 31, 0, 0, 0, 0, time.UTC),
			Interval{Months: 1},
			time.Date(2024, 3, 2, 0, 0, 0, 0, time.UTC), // Go normalises 31 Feb
		},
		{
			"a year of months, which is what the server sends",
			time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
			Interval{Months: 14},
			time.Date(2025, 3, 15, 0, 0, 0, 0, time.UTC),
		},
		{
			"months, then days, then microseconds — in that order",
			time.Date(2024, 1, 31, 10, 0, 0, 0, time.UTC),
			Interval{Months: 1, Days: 1, Micros: 3_600_000_000},
			time.Date(2024, 3, 3, 11, 0, 0, 0, time.UTC),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.iv.AddTo(tc.from); !got.Equal(tc.want) {
				t.Errorf("AddTo = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestIntervalDuration(t *testing.T) {
	if d, ok := (Interval{Micros: 30_000_000}).Duration(); !ok || d != 30*time.Second {
		t.Errorf("Duration = %v, %v; want 30s, true", d, ok)
	}
	if d, ok := (Interval{Days: 2}).Duration(); !ok || d != 48*time.Hour {
		t.Errorf("Duration = %v, %v; want 48h, true", d, ok)
	}
	// A month has no length until you say which one, so this must refuse
	// rather than pick 30 days and be wrong eleven times a year.
	if _, ok := (Interval{Months: 1}).Duration(); ok {
		t.Error("Duration converted a month, which has no fixed length")
	}
}

func TestDecodeIntervalRefusesTheWrongWidth(t *testing.T) {
	if _, err := DecodeInterval(make([]byte, 12)); err == nil {
		t.Error("DecodeInterval accepted twelve bytes")
	}
}

// An interval far larger than a time.Duration can hold. PostgreSQL stores it
// happily — `'100000000 hours'::interval` renders as 100000000:00:00 — and a
// Duration, counting nanoseconds in an int64, spans about ±292 years.
//
// Vector from the server: SELECT encode(interval_send('100000000 hours'), 'hex').
func TestIntervalBeyondADuration(t *testing.T) {
	want := mustHex(t, "04fefa17b72400000000000000000000")

	iv, err := DecodeInterval(want)
	if err != nil {
		t.Fatalf("DecodeInterval: %v", err)
	}
	const hours = 100_000_000
	if iv.Micros != hours*3600*1_000_000 {
		t.Fatalf("micros = %d, want %d", iv.Micros, int64(hours)*3600*1_000_000)
	}
	if !bytes.Equal(AppendInterval(nil, iv), want) {
		t.Errorf("AppendInterval did not reproduce the server's bytes")
	}

	// Duration must refuse rather than wrap. A value that came back as its own
	// negation would be worse than one that came back as "no".
	if d, ok := iv.Duration(); ok {
		t.Errorf("Duration = %v, true; want it refused as out of range", d)
	}

	// AddTo must still be exact, because it does not go through a Duration.
	from := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	got := iv.AddTo(from)
	want2 := time.Unix(from.Unix()+hours*3600, 0).UTC()
	if !got.Equal(want2) {
		t.Errorf("AddTo = %v, want %v", got, want2)
	}
}

// The days field has its own overflow: 24 hours each, in nanoseconds.
func TestIntervalDurationRefusesTooManyDays(t *testing.T) {
	if d, ok := (Interval{Days: 1 << 30}).Duration(); ok {
		t.Errorf("Duration = %v, true; want it refused", d)
	}
	// And the sum of two terms that each fit but together do not.
	big := Interval{Days: 100_000, Micros: 9_000_000_000_000_000}
	if d, ok := big.Duration(); ok {
		t.Errorf("Duration = %v, true; want the sum refused", d)
	}
}

// String is what a failing test and a log line reach for, so it is the one
// method whose absence from the coverage report is worth acting on: a
// diagnostic that has never run is a diagnostic that will run for the first
// time during an incident.
func TestIntervalString(t *testing.T) {
	tests := []struct {
		iv   Interval
		want string
	}{
		{Interval{}, "0mon 0d 0us"},
		{Interval{Months: 14, Days: 3, Micros: 14_706_789_000}, "14mon 3d 14706789000us"},
		{Interval{Months: -1, Days: -1, Micros: -1_000_000}, "-1mon -1d -1000000us"},
	}
	for _, tc := range tests {
		if got := tc.iv.String(); got != tc.want {
			t.Errorf("String() = %q, want %q", got, tc.want)
		}
	}

	// The three fields must all be visible: a rendering that dropped one would
	// make two different intervals print the same, which is the opposite of
	// what a diagnostic is for.
	a := Interval{Months: 1}
	b := Interval{Days: 1}
	c := Interval{Micros: 1}
	if a.String() == b.String() || b.String() == c.String() || a.String() == c.String() {
		t.Errorf("three different intervals render alike: %q %q %q", a, b, c)
	}
}
