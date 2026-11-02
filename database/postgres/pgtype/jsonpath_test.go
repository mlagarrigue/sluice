package pgtype

import (
	"errors"
	"testing"
)

// A jsonpath is its version byte, 1, then its text; any other first byte
// is refused, as the server refuses it.
func TestJSONPath(t *testing.T) {
	b := AppendJSONPath(nil, `$."a"`)
	if string(b) != "\x01$.\"a\"" {
		t.Fatalf("AppendJSONPath = %q", b)
	}
	if p, err := DecodeJSONPath(b); err != nil || p != `$."a"` {
		t.Errorf("DecodeJSONPath = %q, %v", p, err)
	}
	for _, bad := range [][]byte{nil, []byte("\x02$")} {
		if _, err := DecodeJSONPath(bad); !errors.Is(err, ErrCodec) {
			t.Errorf("DecodeJSONPath(%q) = %v, want ErrCodec", bad, err)
		}
	}
}

func FuzzRoundTripJSONPath(f *testing.F) {
	fuzzRoundTrip(f, DecodeJSONPath, AppendJSONPath, []byte("\x01$"), []byte{2})
}
