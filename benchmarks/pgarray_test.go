// Package benchmarks compares this library against the libraries people
// actually use, on work that is the same on both sides.
//
// The rule the whole directory is written under: a comparison this project
// wins everywhere is a comparison nobody believes. Every case here has to be
// able to lose, the competitor is written the way its own documentation writes
// it, and what is *not* measured is stated beside what is.
package benchmarks

import (
	"slices"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/mlagarrigue/sluice/database/postgres"
)

// The claim the connector is built around, measured against the driver the
// ecosystem uses: a thousand keys leave as one binary parameter and a thousand
// values come back out of one array.
//
// # What this measures, and what it does not
//
// Only the codec. No socket, no server, no round trip — which is deliberate
// and is also why this number must never be quoted as "faster than pgx" on its
// own. Against ~500 µs of round trip, everything here is noise; the figure
// matters because a `= ANY($1)` batch encodes one array per query and decodes
// one per result, so it sits on the path every batched read takes, once.
//
// pgx is used through pgtype.Map, which is how pgx encodes a Go slice into a
// binary array parameter and how its own Rows.Scan reaches an array column.
const arrayLen = 1000

func keys() []int64 {
	out := make([]int64, arrayLen)
	for i := range out {
		out[i] = int64(i) * 7
	}
	return out
}

func BenchmarkPGEncodeInt8ArraySluice(b *testing.B) {
	src := keys()
	buf := make([]byte, 0, arrayLen*12+64)
	b.ReportAllocs()
	for b.Loop() {
		buf = postgres.AppendInt8Array(buf[:0], src)
	}
	sink = len(buf)
}

func BenchmarkPGEncodeInt8ArrayPgx(b *testing.B) {
	src := keys()
	m := pgtype.NewMap()
	var buf []byte
	b.ReportAllocs()
	for b.Loop() {
		out, err := m.Encode(pgtype.Int8ArrayOID, pgtype.BinaryFormatCode, src, buf[:0])
		if err != nil {
			b.Fatal(err)
		}
		buf = out
	}
	sink = len(buf)
}

func BenchmarkPGDecodeInt8ArraySluice(b *testing.B) {
	wire := postgres.AppendInt8Array(nil, keys())
	dst := make([]int64, 0, arrayLen)
	b.ReportAllocs()
	for b.Loop() {
		out, err := postgres.DecodeInt8Array(dst[:0], wire)
		if err != nil {
			b.Fatal(err)
		}
		dst = out
	}
	sink = len(dst)
}

func BenchmarkPGDecodeInt8ArrayPgx(b *testing.B) {
	wire := postgres.AppendInt8Array(nil, keys())
	m := pgtype.NewMap()
	var dst []int64
	b.ReportAllocs()
	for b.Loop() {
		if err := m.Scan(pgtype.Int8ArrayOID, pgtype.BinaryFormatCode, wire, &dst); err != nil {
			b.Fatal(err)
		}
	}
	sink = len(dst)
}

// sink keeps the compiler from deleting the work above.
var sink int

// The comparison is worthless unless both sides produce the same bytes and
// read each other's. This is not a nicety: an encoder that is faster because
// it emits something subtly different is not faster, it is wrong — and a
// benchmark cannot tell the difference on its own.
//
// It also makes pgx a real oracle for the codec, which is what the root
// module's zero-dependency rule forbids there and this module exists to allow.
func TestInt8ArrayWireCompatibility(t *testing.T) {
	src := keys()
	m := pgtype.NewMap()

	ours := postgres.AppendInt8Array(nil, src)
	theirs, err := m.Encode(pgtype.Int8ArrayOID, pgtype.BinaryFormatCode, src, nil)
	if err != nil {
		t.Fatalf("pgx failed to encode: %v", err)
	}
	if !slices.Equal(ours, theirs) {
		t.Fatalf("the two encoders disagree:\n ours %d bytes\npgx   %d bytes", len(ours), len(theirs))
	}

	// And each decodes the other's bytes back to the input.
	back, err := postgres.DecodeInt8Array(nil, theirs)
	if err != nil {
		t.Fatalf("decoding pgx's bytes: %v", err)
	}
	if !slices.Equal(back, src) {
		t.Error("this package did not read pgx's array back")
	}

	var pgxBack []int64
	if err := m.Scan(pgtype.Int8ArrayOID, pgtype.BinaryFormatCode, ours, &pgxBack); err != nil {
		t.Fatalf("pgx decoding this package's bytes: %v", err)
	}
	if !slices.Equal(pgxBack, src) {
		t.Error("pgx did not read this package's array back")
	}
}

// Fairness check for the encode figure above. Passing a plain []int64 to
// pgtype.Map is what a pgx caller writes, but pgx also offers FlatArray, which
// is the type its own documentation reaches for when the shape is known. If
// the gap is an artefact of the wrong entry point, it shows up here.
func BenchmarkPGEncodeInt8ArrayPgxFlat(b *testing.B) {
	src := pgtype.FlatArray[int64](keys())
	m := pgtype.NewMap()
	var buf []byte
	b.ReportAllocs()
	for b.Loop() {
		out, err := m.Encode(pgtype.Int8ArrayOID, pgtype.BinaryFormatCode, src, buf[:0])
		if err != nil {
			b.Fatal(err)
		}
		buf = out
	}
	sink = len(buf)
}

// And the same for decoding, into the type pgx prefers.
func BenchmarkPGDecodeInt8ArrayPgxFlat(b *testing.B) {
	wire := postgres.AppendInt8Array(nil, keys())
	m := pgtype.NewMap()
	var dst pgtype.FlatArray[int64]
	b.ReportAllocs()
	for b.Loop() {
		if err := m.Scan(pgtype.Int8ArrayOID, pgtype.BinaryFormatCode, wire, &dst); err != nil {
			b.Fatal(err)
		}
	}
	sink = len(dst)
}
