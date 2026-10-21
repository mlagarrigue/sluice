package httpstream

import (
	"cmp"
	"errors"
	"testing"

	"github.com/mlagarrigue/sluice/net/quic"
)

// The authority rule and the target rule are one function for HTTP/2 and
// HTTP/3, so every case below is run against both: a request the one refuses
// and the other delivers is the divergence that function exists to make
// impossible (RFC 9113 §8.3.1, RFC 9114 §4.3.1, RFC 3986 §3.2).
//
// A nil authority or host means the field is absent; an empty string means
// it is sent empty. The scheme is https unless the case names one.
func TestRequestValidationIsTheSameOnH2AndH3(t *testing.T) {
	type testCase struct {
		name            string
		method, path    string
		authority, host *string
		accepted        bool
		scheme          string
	}
	tests := []testCase{
		{"a plain authority", "GET", "/x", ptr("h"), nil, true, ""},
		{"an empty :authority", "GET", "/x", ptr(""), nil, false, ""},
		{"an empty host", "GET", "/x", nil, ptr(""), false, ""},
		{"userinfo in :authority", "GET", "/x", ptr("u@h"), nil, false, ""},
		{"userinfo in host", "GET", "/x", nil, ptr("u@h"), false, ""},
		{"a path in :authority", "GET", "/x", ptr("h/x"), nil, false, ""},
		{"a name with a port", "GET", "/x", ptr("h:8080"), nil, true, ""},
		{"an empty port", "GET", "/x", ptr("h:"), nil, true, ""},
		{"a port that is not digits", "GET", "/x", ptr("h:8a"), nil, false, ""},
		{"an IPv6 literal with a port", "GET", "/x", ptr("[::1]:8443"), nil, true, ""},
		{"an unclosed IPv6 literal", "GET", "/x", ptr("[::1"), nil, false, ""},
		// RFC 3986 §3.2.2 whole: '%' starts two hex digits, brackets hold an
		// IPv6 address or an IPvFuture, and a zone identifier is refused
		// (RFC 9844 §3: no longer URI syntax).
		{"a percent-encoded reg-name", "GET", "/x", ptr("h%41"), nil, true, ""},
		{"a percent not followed by hex", "GET", "/x", ptr("h%zz"), nil, false, ""},
		{"a truncated percent-encoding", "GET", "/x", ptr("h%"), nil, false, ""},
		{"a percent-encoding cut by the port", "GET", "/x", ptr("h%4:80"), nil, false, ""},
		{"an IPv6 literal", "GET", "/x", ptr("[::1]"), nil, true, ""},
		{"brackets holding no address", "GET", "/x", ptr("[zz]"), nil, false, ""},
		{"empty brackets", "GET", "/x", ptr("[]"), nil, false, ""},
		{"an IPv4 address in brackets", "GET", "/x", ptr("[1.2.3.4]"), nil, false, ""},
		{"an IPv6 literal with a zone", "GET", "/x", ptr("[fe80::1%25eth0]"), nil, false, ""},
		{"an IPv6 literal with a raw zone", "GET", "/x", ptr("[fe80::1%eth0]"), nil, false, ""},
		{"an IPvFuture", "GET", "/x", ptr("[v1.x]"), nil, true, ""},
		{"an IPvFuture with a port", "GET", "/x", ptr("[v1F.a:b]:80"), nil, true, ""},
		{"an IPvFuture without a version", "GET", "/x", ptr("[v.x]"), nil, false, ""},
		{"an IPvFuture without an address", "GET", "/x", ptr("[v1.]"), nil, false, ""},
		{"an IPvFuture with a slash", "GET", "/x", ptr("[v1.a/b]"), nil, false, ""},
		{"a colon in a reg-name", "GET", "/x", ptr("a:b:80"), nil, false, ""},
		{"host agreeing with :authority", "GET", "/x", ptr("h"), ptr("h"), true, ""},
		{"host disagreeing with :authority", "GET", "/x", ptr("a"), ptr("b"), false, ""},
		{"OPTIONS *", "OPTIONS", "*", ptr("h"), nil, true, ""},
		{"GET *", "GET", "*", ptr("h"), nil, false, ""},
		// RFC 9110 §4.2.1, RFC 9114 §4.3.1: http and https have a mandatory
		// authority component, so a request naming none is malformed, as
		// HTTP/1.1 answers 400 to a missing Host; a scheme without one is
		// delivered as it came.
		{"no authority at all", "GET", "/x", nil, nil, false, ""},
		{"no authority at all over http", "GET", "/x", nil, nil, false, "http"},
		{"host alone", "GET", "/x", nil, ptr("h"), true, ""},
		{"no authority for a scheme that needs none", "GET", "/x", nil, nil, true, "file"},
	}
	fieldsOf := func(tt testCase) []Header {
		scheme := cmp.Or(tt.scheme, "https")
		fields := []Header{
			{Name: []byte(":method"), Value: []byte(tt.method)},
			{Name: []byte(":path"), Value: []byte(tt.path)},
			{Name: []byte(":scheme"), Value: []byte(scheme)},
		}
		if tt.authority != nil {
			fields = append(fields, Header{Name: []byte(":authority"), Value: []byte(*tt.authority)})
		}
		if tt.host != nil {
			fields = append(fields, Header{Name: []byte("host"), Value: []byte(*tt.host)})
		}
		return fields
	}

	for _, tt := range tests {
		t.Run("h2/"+tt.name, func(t *testing.T) {
			addr, sizes := serveTest(t, Config{}, echoTarget)
			c := dialH2(t, addr)
			var block []byte
			for _, h := range fieldsOf(tt) {
				block = literal(block, string(h.Name), string(h.Value))
			}
			c.frame(h2Frame{
				Type: frameHeaders, Flags: flagEndHeaders | flagEndStream,
				StreamID: 1, Payload: block,
			})
			c.flush()
			if tt.accepted {
				if got := c.bodies(1)[1]; got != tt.path {
					t.Errorf("answered %q, want the target %q echoed", got, tt.path)
				}
				return
			}
			if code := c.waitReset(1); code != 0x1 {
				t.Errorf("reset code = %#x, want 0x1 PROTOCOL_ERROR", code)
			}
			if c.sawGoAway {
				t.Error("one malformed request ended the whole connection")
			}
			if got := sizes(); len(got) > 0 {
				t.Errorf("a malformed request reached the handler: %v", got)
			}
		})
		t.Run("h3/"+tt.name, func(t *testing.T) {
			a := testAssembler(Config{})
			stream := appendH3Frame(nil, h3Headers, appendQPACK(nil, fieldsOf(tt)))
			batch, _, failed, err := a.Frames([]quic.Frame{streamFrame(0, stream, true)})
			if err != nil {
				t.Fatalf("one malformed request failed the connection: %v", err)
			}
			if tt.accepted {
				if batch.Len() != 1 || len(failed) != 0 {
					t.Fatalf("%d requests and %v, want one request delivered", batch.Len(), failed)
				}
				if got := string(batch.Items[0].Target); got != tt.path {
					t.Errorf("target %q, want %q", got, tt.path)
				}
				return
			}
			if batch.Len() != 0 || len(failed) != 1 || !errors.Is(failed[0].Err, ErrH3Protocol) {
				t.Errorf("%d requests and %v, want one stream error wrapping ErrH3Protocol", batch.Len(), failed)
			}
		})
	}
}

func ptr(s string) *string { return &s }
