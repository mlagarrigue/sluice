package httpstream

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/net/quic"
)

// checkDate asserts that values is exactly one Date field, parsable as
// RFC 9110 §5.6.7's IMF-fixdate and within two seconds of the clock.
func checkDate(t *testing.T, what string, values []string) {
	t.Helper()
	if len(values) != 1 {
		t.Fatalf("%s: Date = %q, want exactly one", what, values)
	}
	at, err := http.ParseTime(values[0])
	if err != nil {
		t.Fatalf("%s: Date %q does not parse: %v", what, values[0], err)
	}
	if d := time.Since(at); d < -2*time.Second || d > 2*time.Second {
		t.Errorf("%s: Date %q is %v from now", what, values[0], d)
	}
}

// handlerDate is the value a handler puts in its own Date field in the
// tests below: a fixed instant, so the wire can be checked for it and for
// the absence of a second Date beside it.
const handlerDate = "Mon, 01 Jan 2024 00:00:00 GMT"

// testDate is the batch's Date value the encoder tests hand the encoders.
var testDate = []byte(handlerDate)

// dateHandler answers / with a 200, /missing with a 404, /dated with a 200
// carrying its own Date, and panics on /boom so the layer answers 500.
func dateHandler(b sluice.Batch[Request]) sluice.Batch[Response] {
	out := make([]Response, 0, b.Len())
	for i := range b.Items {
		r := &b.Items[i]
		switch string(r.Target) {
		case "/missing":
			out = append(out, Response{Status: 404})
		case "/dated":
			out = append(out, Response{Status: 200, Headers: []Header{{Name: []byte("date"), Value: []byte(handlerDate)}}})
		case "/boom":
			panic("handler bug")
		default:
			out = append(out, Response{Status: 200, Body: []byte("ok")})
		}
	}
	return sluice.Batch[Response]{Items: out}
}

// readH1 sends one request on a fresh connection and returns the response
// as net/http parses it.
func readH1(t *testing.T, addr, raw string) *http.Response {
	t.Helper()
	c := dial(t, addr)
	fmt.Fprint(c, raw)
	res, err := http.ReadResponse(bufio.NewReader(c), nil)
	if err != nil {
		t.Fatalf("%q: %v", raw, err)
	}
	_, _ = io.Copy(io.Discard, res.Body)
	_ = res.Body.Close()
	return res
}

// Every HTTP/1.1 response carries a Date: the handler's 200 and 404, and the
// 400 the layer writes itself for a request with no Host. A handler that set
// its own Date is not doubled.
func TestDateHeaderOnH1(t *testing.T) {
	addr, _ := serveTest(t, Config{}, dateHandler)
	for _, tt := range []struct {
		raw  string
		want int
	}{
		{"GET / HTTP/1.1\r\nHost: h\r\n\r\n", 200},
		{"GET /missing HTTP/1.1\r\nHost: h\r\n\r\n", 404},
		{"GET / HTTP/1.1\r\n\r\n", 400},
	} {
		res := readH1(t, addr, tt.raw)
		if res.StatusCode != tt.want {
			t.Fatalf("%q: status %d, want %d", tt.raw, res.StatusCode, tt.want)
		}
		checkDate(t, fmt.Sprintf("h1 %d", tt.want), res.Header.Values("Date"))
	}
	res := readH1(t, addr, "GET /dated HTTP/1.1\r\nHost: h\r\n\r\n")
	if got := res.Header.Values("Date"); len(got) != 1 || got[0] != handlerDate {
		t.Errorf("h1 handler's Date: on the wire %q, want %q once", got, handlerDate)
	}
}

// h2Fields decodes a response's header block, as the peer would.
func h2Fields(t *testing.T, block []byte) map[string][]string {
	t.Helper()
	fields, err := newHPACKDecoder(4096, 0).Decode(nil, block)
	if err != nil {
		t.Fatalf("the block does not decode: %v", err)
	}
	out := map[string][]string{}
	for _, f := range fields {
		out[string(f.Name)] = append(out[string(f.Name)], string(f.Value))
	}
	return out
}

// Every HTTP/2 response carries a date: the handler's 200 and 404, and the
// 500 the layer answers a panicking handler with. A handler's own date is
// not doubled.
func TestDateHeaderOnH2(t *testing.T) {
	addr, _ := serveTest(t, Config{}, dateHandler)
	c := dialH2(t, addr)
	c.request(1, "GET", "/")
	c.request(3, "GET", "/missing")
	c.request(7, "GET", "/dated")
	c.flush()
	c.bodies(3)
	// On its own batch: a panic answers every request of the batch 500.
	c.request(9, "GET", "/boom")
	c.flush()
	c.bodies(4)
	for id, want := range map[uint32]string{1: "200", 3: "404", 9: "500"} {
		f := h2Fields(t, c.heads[id])
		if got := f[":status"]; len(got) != 1 || got[0] != want {
			t.Fatalf("stream %d: :status %q, want %s", id, got, want)
		}
		checkDate(t, "h2 "+want, f["date"])
	}
	if got := h2Fields(t, c.heads[7])["date"]; len(got) != 1 || got[0] != handlerDate {
		t.Errorf("h2 handler's date: on the wire %q, want %q once", got, handlerDate)
	}
}

// readH3Fields reads a response stream to its end and returns its HEADERS
// fields, decoded as the peer would.
func readH3Fields(t *testing.T, s *quic.Stream) map[string][]string {
	t.Helper()
	_ = s.SetReadDeadline(time.Now().Add(10 * time.Second))
	raw, err := io.ReadAll(s)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("the response stream ended with %v", err)
	}
	frames, _, err := parseH3Frames(nil, raw, 1<<24)
	if err != nil {
		t.Fatalf("the response did not parse: %v", err)
	}
	out := map[string][]string{}
	d := newQPACKDecoder(nil, 0)
	for _, f := range frames {
		if f.Type != h3Headers {
			continue
		}
		fields, err := d.Decode(nil, f.Payload)
		if err != nil {
			t.Fatal(err)
		}
		for _, h := range fields {
			out[string(h.Name)] = append(out[string(h.Name)], string(h.Value))
		}
	}
	return out
}

// Every HTTP/3 response carries a date: the handler's 200 and 404, and the
// 500 the layer answers a panicking handler with. A handler's own date is
// not doubled.
func TestDateHeaderOnH3(t *testing.T) {
	conn, _, _ := startH3(t, quic.DefaultParameters(), dateHandler)
	for _, tt := range []struct {
		path string
		want string
	}{{"/", "200"}, {"/missing", "404"}, {"/boom", "500"}} {
		f := readH3Fields(t, askH3(t, conn, h3Request("GET", tt.path)))
		if got := f[":status"]; len(got) != 1 || got[0] != tt.want {
			t.Fatalf("%s: :status %q, want %s", tt.path, got, tt.want)
		}
		checkDate(t, "h3 "+tt.want, f["date"])
	}
	if got := readH3Fields(t, askH3(t, conn, h3Request("GET", "/dated")))["date"]; len(got) != 1 || got[0] != handlerDate {
		t.Errorf("h3 handler's date: on the wire %q, want %q once", got, handlerDate)
	}
}

// The index the h2 writer spells for date is the static table's.
func TestHPACKDateIndex(t *testing.T) {
	if got := staticNameIndex("date"); got != hpackDateIndex {
		t.Errorf("staticNameIndex(date) = %d, hpackDateIndex = %d", got, hpackDateIndex)
	}
}

// The value is formatted once per second and shared: two calls within the
// same second return the same bytes, in GMT, the layout's length; the next
// second gets its own.
func TestDateValueIsCachedPerSecond(t *testing.T) {
	now := time.Date(2026, time.October, 15, 12, 0, 0, 500e6, time.FixedZone("CEST", 2*3600))
	a := dateFor(now)
	if string(a) != "Thu, 15 Oct 2026 10:00:00 GMT" || len(a) != len(dateLayout) {
		t.Fatalf("Date value %q", a)
	}
	if _, err := http.ParseTime(string(a)); err != nil {
		t.Fatal(err)
	}
	if b := dateFor(now.Add(400 * time.Millisecond)); !bytes.Equal(a, b) {
		t.Errorf("two values within one second differ: %q, %q", a, b)
	}
	if b := dateFor(now.Add(time.Second)); string(b) != "Thu, 15 Oct 2026 10:00:01 GMT" {
		t.Errorf("the next second: %q", b)
	}
}
