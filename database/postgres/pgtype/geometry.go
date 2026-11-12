package pgtype

import (
	"encoding/binary"
	"fmt"
	"math"
)

// The geometric types are float8 values back to back, in the order the
// server's own send functions write them (src/backend/utils/adt/geo_ops.c):
// fixed-size for point, line, lseg, box and circle; a count of points and
// then the points for path and polygon. Nothing is normalised on the way:
// the server checks and normalises what it receives — a box's corners put
// back in order, a line whose A and B are both zero refused — and a client
// that did it too would be a second place to be wrong.

// pointSize is a point's two float8 coordinates.
const pointSize = 16

// Point is a PostgreSQL point, (X, Y).
type Point struct{ X, Y float64 }

// Line is a PostgreSQL line, the infinite line A·x + B·y + C = 0. The server
// refuses A and B both zero.
type Line struct{ A, B, C float64 }

// LSeg is a PostgreSQL lseg, the segment between two points.
type LSeg struct{ P [2]Point }

// Box is a PostgreSQL box. The server keeps the upper-right corner first,
// whatever order the corners were given in, and sends them so: High holds
// the larger coordinates, Low the smaller. A box written with them the other
// way round is put back in order by the server.
type Box struct{ High, Low Point }

// Path is a PostgreSQL path: points joined in order, the last back to the
// first when Closed. A path has at least one point.
type Path struct {
	Closed bool
	Points []Point
}

// Circle is a PostgreSQL circle: its center and a radius the server
// refuses below zero.
type Circle struct {
	Center Point
	Radius float64
}

func appendPoint(dst []byte, p Point) []byte {
	dst = binary.BigEndian.AppendUint64(dst, math.Float64bits(p.X))
	return binary.BigEndian.AppendUint64(dst, math.Float64bits(p.Y))
}

// decodeFloats reads n float8 values from b, which must hold exactly that
// many, n at most four. They come back in an array, by value: a slice
// escaped to the heap, an allocation per point, line, segment, box or circle
// decoded — one per element of a point[].
func decodeFloats(b []byte, n int, name string) ([4]float64, error) {
	var out [4]float64
	if len(b) != 8*n {
		return out, fmt.Errorf("%w: %s is %d bytes, want %d", ErrCodec, name, len(b), 8*n)
	}
	for i := range n {
		out[i] = math.Float64frombits(binary.BigEndian.Uint64(b[8*i:]))
	}
	return out, nil
}

// AppendPoint appends a point.
func AppendPoint(dst []byte, p Point) []byte { return appendPoint(dst, p) }

// DecodePoint decodes a point.
func DecodePoint(b []byte) (Point, error) {
	f, err := decodeFloats(b, 2, "point")
	if err != nil {
		return Point{}, err
	}
	return Point{f[0], f[1]}, nil
}

// AppendLine appends a line.
func AppendLine(dst []byte, l Line) []byte {
	dst = binary.BigEndian.AppendUint64(dst, math.Float64bits(l.A))
	dst = binary.BigEndian.AppendUint64(dst, math.Float64bits(l.B))
	return binary.BigEndian.AppendUint64(dst, math.Float64bits(l.C))
}

// DecodeLine decodes a line.
func DecodeLine(b []byte) (Line, error) {
	f, err := decodeFloats(b, 3, "line")
	if err != nil {
		return Line{}, err
	}
	return Line{f[0], f[1], f[2]}, nil
}

// AppendLSeg appends an lseg.
func AppendLSeg(dst []byte, s LSeg) []byte {
	return appendPoint(appendPoint(dst, s.P[0]), s.P[1])
}

// DecodeLSeg decodes an lseg.
func DecodeLSeg(b []byte) (LSeg, error) {
	f, err := decodeFloats(b, 4, "lseg")
	if err != nil {
		return LSeg{}, err
	}
	return LSeg{P: [2]Point{{f[0], f[1]}, {f[2], f[3]}}}, nil
}

// AppendBox appends a box, High first as the server sends it.
func AppendBox(dst []byte, b Box) []byte {
	return appendPoint(appendPoint(dst, b.High), b.Low)
}

// DecodeBox decodes a box.
func DecodeBox(b []byte) (Box, error) {
	f, err := decodeFloats(b, 4, "box")
	if err != nil {
		return Box{}, err
	}
	return Box{High: Point{f[0], f[1]}, Low: Point{f[2], f[3]}}, nil
}

// AppendCircle appends a circle.
func AppendCircle(dst []byte, c Circle) []byte {
	dst = appendPoint(dst, c.Center)
	return binary.BigEndian.AppendUint64(dst, math.Float64bits(c.Radius))
}

// DecodeCircle decodes a circle.
func DecodeCircle(b []byte) (Circle, error) {
	f, err := decodeFloats(b, 3, "circle")
	if err != nil {
		return Circle{}, err
	}
	return Circle{Center: Point{f[0], f[1]}, Radius: f[2]}, nil
}

// AppendPath appends a path: whether it is closed, the count of its points,
// and the points. A path of no point is written as one, for the server to
// refuse ("invalid number of points").
func AppendPath(dst []byte, p Path) []byte {
	closed := byte(0)
	if p.Closed {
		closed = 1
	}
	return appendPoints(append(dst, closed), p.Points)
}

// DecodePath decodes a path, its points appended to dst.
func DecodePath(dst []Point, b []byte) (Path, error) {
	if len(b) < 1 {
		return Path{}, fmt.Errorf("%w: path is %d bytes, want at least 1", ErrCodec, len(b))
	}
	// The server sends 0 or 1 (path_send); another byte is no value it
	// writes, and read as closed it would not encode back to itself.
	if b[0] > 1 {
		return Path{}, fmt.Errorf("%w: path closed flag %d", ErrCodec, b[0])
	}
	pts, err := decodePoints(dst, b[1:], "path")
	if err != nil {
		return Path{}, err
	}
	return Path{Closed: b[0] != 0, Points: pts}, nil
}

// AppendPolygon appends a polygon: the count of its points, and the points.
// A polygon of no point is written as one, for the server to refuse.
func AppendPolygon(dst []byte, points []Point) []byte { return appendPoints(dst, points) }

// DecodePolygon decodes a polygon, its points appended to dst.
func DecodePolygon(dst []Point, b []byte) ([]Point, error) {
	return decodePoints(dst, b, "polygon")
}

func appendPoints(dst []byte, points []Point) []byte {
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(points))) //nolint:gosec // G115: a point count, bounded by memory well under 2^31
	for _, p := range points {
		dst = appendPoint(dst, p)
	}
	return dst
}

// decodePoints reads a count and as many points, refusing a count the bytes
// do not hold before anything is allocated for it: the count is the
// server's, but the length check costs nothing and keeps a damaged value
// from asking for memory it does not carry.
func decodePoints(dst []Point, b []byte, name string) ([]Point, error) {
	if len(b) < 4 {
		return dst, fmt.Errorf("%w: %s is %d bytes, want at least 4", ErrCodec, name, len(b))
	}
	n := binary.BigEndian.Uint32(b)
	b = b[4:]
	if n == 0 || uint64(len(b)) != uint64(n)*pointSize {
		return dst, fmt.Errorf("%w: %s declares %d points in %d bytes", ErrCodec, name, n, len(b))
	}
	for i := 0; i < len(b); i += pointSize {
		dst = append(dst, Point{
			X: math.Float64frombits(binary.BigEndian.Uint64(b[i:])),
			Y: math.Float64frombits(binary.BigEndian.Uint64(b[i+8:])),
		})
	}
	return dst, nil
}
