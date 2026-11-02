package pgtype

import (
	"bytes"
	"encoding/hex"
	"errors"
	"slices"
	"testing"
)

// # geometry and money

// The layouts of geo_ops.c's send functions, written out by hand: float8
// big-endian, in the order the server sends them. The integration suite
// compares the same encoders with the server's own output.
func TestGeometryAgainstHandWrittenVectors(t *testing.T) {
	const one, two, three, four = "3ff0000000000000", "4000000000000000", "4008000000000000", "4010000000000000"
	tests := []struct {
		name string
		got  []byte
		want string
	}{
		{"point (1,2)", AppendPoint(nil, Point{1, 2}), one + two},
		{"line {1,2,3}", AppendLine(nil, Line{1, 2, 3}), one + two + three},
		{"lseg [(1,2),(3,4)]", AppendLSeg(nil, LSeg{P: [2]Point{{1, 2}, {3, 4}}}), one + two + three + four},
		{"box (3,4),(1,2)", AppendBox(nil, Box{High: Point{3, 4}, Low: Point{1, 2}}), three + four + one + two},
		{"circle <(1,2),3>", AppendCircle(nil, Circle{Center: Point{1, 2}, Radius: 3}), one + two + three},
		{"open path [(1,2)]", AppendPath(nil, Path{Points: []Point{{1, 2}}}), "00" + "00000001" + one + two},
		{"closed path ((1,2))", AppendPath(nil, Path{Closed: true, Points: []Point{{1, 2}}}), "01" + "00000001" + one + two},
		{"polygon ((1,2),(3,4))", AppendPolygon(nil, []Point{{1, 2}, {3, 4}}), "00000002" + one + two + three + four},
		{"money 12.34 at two digits", AppendMoney(nil, 1234), "00000000000004d2"},
	}
	for _, tt := range tests {
		if got := hex.EncodeToString(tt.got); got != tt.want {
			t.Errorf("%s = %s, want %s", tt.name, got, tt.want)
		}
	}
}

func TestGeometryRoundTrips(t *testing.T) {
	p := Point{-1.5, 2.25}
	if got, err := DecodePoint(AppendPoint(nil, p)); err != nil || got != p {
		t.Errorf("point = %v, %v", got, err)
	}
	l := Line{1, -1, 0.5}
	if got, err := DecodeLine(AppendLine(nil, l)); err != nil || got != l {
		t.Errorf("line = %v, %v", got, err)
	}
	s := LSeg{P: [2]Point{{0, 0}, {3, 4}}}
	if got, err := DecodeLSeg(AppendLSeg(nil, s)); err != nil || got != s {
		t.Errorf("lseg = %v, %v", got, err)
	}
	bx := Box{High: Point{3, 4}, Low: Point{1, 2}}
	if got, err := DecodeBox(AppendBox(nil, bx)); err != nil || got != bx {
		t.Errorf("box = %v, %v", got, err)
	}
	c := Circle{Center: Point{1, 1}, Radius: 2}
	if got, err := DecodeCircle(AppendCircle(nil, c)); err != nil || got != c {
		t.Errorf("circle = %v, %v", got, err)
	}
	path := Path{Closed: true, Points: []Point{{0, 0}, {1, 0}, {1, 1}}}
	if got, err := DecodePath(nil, AppendPath(nil, path)); err != nil || got.Closed != path.Closed || !slices.Equal(got.Points, path.Points) {
		t.Errorf("path = %v, %v", got, err)
	}
	poly := []Point{{0, 0}, {1, 0}, {0, 1}}
	if got, err := DecodePolygon(nil, AppendPolygon(nil, poly)); err != nil || !slices.Equal(got, poly) {
		t.Errorf("polygon = %v, %v", got, err)
	}
	if got, err := DecodeMoney(AppendMoney(nil, -1234)); err != nil || got != -1234 {
		t.Errorf("money = %d, %v", got, err)
	}
}

// Values the server never sends are refused with ErrCodec, before any
// allocation a count asks for: a point count the bytes do not hold, no
// point at all, a closed flag that is neither 0 nor 1, a fixed size off by
// one.
func TestGeometryRefusesMalformedValues(t *testing.T) {
	huge := append([]byte{0xff, 0xff, 0xff, 0xff}, make([]byte, 16)...)
	tests := []struct {
		name string
		err  error
	}{
		{"point short", func() error { _, err := DecodePoint(make([]byte, 15)); return err }()},
		{"line long", func() error { _, err := DecodeLine(make([]byte, 25)); return err }()},
		{"lseg short", func() error { _, err := DecodeLSeg(make([]byte, 31)); return err }()},
		{"box short", func() error { _, err := DecodeBox(nil); return err }()},
		{"circle short", func() error { _, err := DecodeCircle(make([]byte, 23)); return err }()},
		{"money short", func() error { _, err := DecodeMoney(make([]byte, 7)); return err }()},
		{"polygon of no point", func() error { _, err := DecodePolygon(nil, make([]byte, 4)); return err }()},
		{"polygon count past its bytes", func() error { _, err := DecodePolygon(nil, huge); return err }()},
		{"path empty", func() error { _, err := DecodePath(nil, nil); return err }()},
		{"path closed flag 2", func() error {
			_, err := DecodePath(nil, append([]byte{2}, AppendPolygon(nil, []Point{{}})...))
			return err
		}()},
	}
	for _, tt := range tests {
		if !errors.Is(tt.err, ErrCodec) {
			t.Errorf("%s: %v, want ErrCodec", tt.name, tt.err)
		}
	}
}

func FuzzRoundTripPoint(f *testing.F) {
	fuzzRoundTrip(f, DecodePoint, AppendPoint, make([]byte, 16), bytes.Repeat([]byte{0xff}, 16))
}

func FuzzRoundTripBox(f *testing.F) {
	fuzzRoundTrip(f, DecodeBox, AppendBox, make([]byte, 32))
}

func FuzzRoundTripCircle(f *testing.F) {
	fuzzRoundTrip(f, DecodeCircle, AppendCircle, make([]byte, 24))
}

func FuzzRoundTripPath(f *testing.F) {
	f.Add(AppendPath(nil, Path{Closed: true, Points: []Point{{1, 2}, {3, 4}}}))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		p, err := DecodePath(nil, b)
		if err != nil {
			if !errors.Is(err, ErrCodec) {
				t.Fatalf("unclassified error %v for %x", err, b)
			}
			return
		}
		if got := AppendPath(nil, p); !bytes.Equal(got, b) {
			t.Fatalf("decode(%x) -> %v -> encode = %x: the value did not survive", b, p, got)
		}
	})
}
