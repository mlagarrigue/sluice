package httpstream

import (
	"bytes"
	"testing"
)

// An error that quotes a value the peer sent goes through cite, cut to
// citeBytes and quoted, so that the peer chooses neither the size nor the
// bytes of the text the error carries to the log. Three refusals of a
// pseudo-header name and the asterisk-form refusal printed the name or the
// method whole and raw: a name of 40,000 bytes holding line feeds came out
// as is.
func TestRefusalsCiteThePeersValues(t *testing.T) {
	const bound = 4*citeBytes + 256 // a quoted cut and the sentence around it
	long := func(prefix string) []byte {
		return append([]byte(prefix), bytes.Repeat([]byte("a\n"), 20000)...)
	}
	base := func() []Header {
		return []Header{
			{Name: []byte(":method"), Value: []byte("GET")},
			{Name: []byte(":scheme"), Value: []byte("https")},
			{Name: []byte(":path"), Value: []byte("/")},
		}
	}
	for name, err := range map[string]error{
		"an unknown pseudo-header": func() error {
			_, _, err := buildRequest(append(base(), Header{Name: long(":"), Value: []byte("v")}), ErrH2Protocol)
			return err
		}(),
		"a pseudo-header after a regular field": func() error {
			fields := append(base(), Header{Name: []byte("x"), Value: []byte("v")}, Header{Name: long(":"), Value: []byte("v")})
			_, _, err := buildRequest(fields, ErrH2Protocol)
			return err
		}(),
		"the asterisk form on another method": checkTarget(long("M"), []byte("*"), ErrH2Protocol),
	} {
		if err == nil {
			t.Errorf("%s: accepted", name)
			continue
		}
		if msg := err.Error(); len(msg) > bound || bytes.IndexByte([]byte(msg), '\n') >= 0 {
			t.Errorf("%s: an error of %d bytes, raw line feed %v; want at most %d, quoted", name, len(msg), bytes.IndexByte([]byte(msg), '\n') >= 0, bound)
		}
	}
}
