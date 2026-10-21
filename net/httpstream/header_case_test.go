package httpstream

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/net/quic"
)

// caseHandler answers every request 200 with the fields an HTTP/1.1 handler
// would spell: a capitalised Content-Type and a mixed-case custom name.
func caseHandler(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
	out := make([]Response, 0, b.Len())
	for range b.Items {
		out = append(out, Response{Status: 200, Headers: []Header{
			{Name: []byte("Content-Type"), Value: []byte("text/plain")},
			{Name: []byte("X-Request-ID"), Value: []byte("7")},
		}, Body: []byte("ok")})
	}
	return sluice.Batch[Response]{Items: out}
}

// One handler, three protocols: a field name spelled the HTTP/1.1 way goes
// out as spelled on HTTP/1.1, and lowercase on HTTP/2 and HTTP/3 (RFC 9113
// §8.2.1, RFC 9114 §4.2), as the response the handler meant, not a 500.
func TestResponseHeaderCaseOnH1(t *testing.T) {
	addr, _ := serveTest(t, Config{}, caseHandler)
	res := readH1(t, addr, "GET / HTTP/1.1\r\nHost: h\r\n\r\n")
	if res.StatusCode != 200 {
		t.Fatalf("status %d, want 200", res.StatusCode)
	}
	if got := res.Header.Get("Content-Type"); got != "text/plain" {
		t.Errorf("content-type %q, want text/plain", got)
	}
}

func TestResponseHeaderCaseOnH2(t *testing.T) {
	addr, _ := serveTest(t, Config{}, caseHandler)
	c := dialH2(t, addr)
	c.request(1, "GET", "/")
	c.flush()
	c.bodies(1)
	checkLowercaseFields(t, h2Fields(t, c.heads[1]))
}

func TestResponseHeaderCaseOnH3(t *testing.T) {
	conn, _, _ := startH3(t, quic.DefaultParameters(), caseHandler)
	checkLowercaseFields(t, readH3Fields(t, askH3(t, conn, h3Request("GET", "/"))))
}

func checkLowercaseFields(t *testing.T, f map[string][]string) {
	t.Helper()
	if got := f[":status"]; len(got) != 1 || got[0] != "200" {
		t.Fatalf(":status %q, want 200", got)
	}
	for name, want := range map[string]string{"content-type": "text/plain", "x-request-id": "7"} {
		if got := f[name]; len(got) != 1 || got[0] != want {
			t.Errorf("%s: on the wire %q, want %q once", name, got, want)
		}
	}
	for name := range f {
		if !lowerASCII(name) {
			t.Errorf("field %q went out with an uppercase letter", name)
		}
	}
}

// The lowercased name is what the rest of the encoder sees: Content-Type
// still finds its static-table entry (31) instead of going literal, a
// handler's own Date is recognised and not doubled, and the refusals — a
// connection-specific field, content-length — hold whatever the case.
func TestLowercasedNameFeedsTheEncoder(t *testing.T) {
	s := newBenchH2Server(Config{})
	s.date = testDate
	res := Response{Status: 200, Headers: []Header{
		{Name: []byte("Content-Type"), Value: []byte("text/plain")},
		{Name: []byte("Date"), Value: testDate},
	}}
	block, err := s.appendHead(nil, 1, []byte("GET"), res, defaultMaxFrameSize)
	if err != nil {
		t.Fatal(err)
	}
	// After the 9-byte frame header and :status (0x88): index 31 under a
	// 4-bit prefix is 0x0f then 31-15.
	if head := block[9:]; !bytes.HasPrefix(head, []byte{0x88, 0x0f, 0x10}) {
		t.Errorf("Content-Type did not take static index 31: block starts %x", head[:4])
	}
	if got := h2Fields(t, block[9:])["date"]; len(got) != 1 {
		t.Errorf("the handler's Date was not recognised: %q", got)
	}
	if !bytes.Equal(res.Headers[0].Name, []byte("Content-Type")) {
		t.Error("the handler's bytes were written")
	}
	for _, name := range []string{"Connection", "Content-Length", "Transfer-Encoding"} {
		bad := Response{Status: 200, Headers: []Header{{Name: []byte(name), Value: []byte("1")}}}
		if _, err := s.appendHead(nil, 1, []byte("GET"), bad, defaultMaxFrameSize); !errors.Is(err, ErrH2Protocol) {
			t.Errorf("h2 %s: got %v, want ErrH2Protocol", name, err)
		}
		if _, err := appendH3Response(nil, bad, []byte("GET"), h3NoFieldSectionLimit, &h3Scratch{}, testDate); !errors.Is(err, ErrH3Protocol) {
			t.Errorf("h3 %s: got %v, want ErrH3Protocol", name, err)
		}
	}
	// Two mixed-case names in one HTTP/3 response share the scratch and
	// both survive to the block.
	sc := &h3Scratch{}
	two := Response{Status: 200, Headers: []Header{
		{Name: []byte("X-One"), Value: []byte("1")},
		{Name: []byte("X-Two"), Value: []byte("2")},
	}}
	if _, err := appendH3Response(nil, two, []byte("GET"), h3NoFieldSectionLimit, sc, testDate); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"x-one", "x-two"} {
		if !bytes.Contains(sc.qpack, []byte(want)) {
			t.Errorf("%s is not in the QPACK block %q", want, sc.qpack)
		}
	}
}
