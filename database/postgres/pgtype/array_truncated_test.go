package pgtype

import (
	"encoding/binary"
	"testing"
)

// truncatedArray is a one-dimensional array header declaring n elements of
// type oid, followed by body.
func truncatedArray(oid uint32, n int, body ...byte) []byte {
	b := binary.BigEndian.AppendUint32(nil, 1)
	b = binary.BigEndian.AppendUint32(b, 0)
	b = binary.BigEndian.AppendUint32(b, oid)
	b = binary.BigEndian.AppendUint32(b, uint32(n)) //nolint:gosec // G115: a test's small count
	b = binary.BigEndian.AppendUint32(b, 1)
	return append(b, body...)
}

// An array whose body stops short of its last element is refused. The body
// passes the four-bytes-per-element bound DecodeArray checks first, so the
// fast element loops of text, bytea and numeric stand guard with their
// len(body) < 4 test: inverted, it read the missing element's length out of
// an empty slice and panicked on what the server sent.
func TestATruncatedArrayBodyIsRefused(t *testing.T) {
	for name, decode := range map[string]func() error{
		"text": func() error {
			_, err := DecodeArray(Array[string]{}, truncatedArray(OIDText, 2, 0, 0, 0, 4, 'a', 'b', 'c', 'd'), TextCodec())
			return err
		},
		"bytea": func() error {
			_, err := DecodeArray(Array[[]byte]{}, truncatedArray(OIDBytea, 2, 0, 0, 0, 4, 1, 2, 3, 4), ByteaCodec())
			return err
		},
		"numeric": func() error {
			_, err := DecodeArray(Array[Numeric]{}, truncatedArray(OIDNumeric, 3, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0), NumericCodec())
			return err
		},
	} {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("%s: panicked on a truncated body: %v", name, r)
				}
			}()
			if err := decode(); err == nil {
				t.Errorf("%s: no error for a missing element", name)
			}
		}()
	}
}
