package httpstream

import (
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/net/quic"
)

// cookieEcho answers every request with the cookie it carries, and records
// what the handler saw: the joined value, and how many cookie fields there
// were — the contract says one.
type cookieEcho struct {
	mu     sync.Mutex
	seen   []string
	fields []int
}

func (e *cookieEcho) handle(b sluice.Batch[Request]) sluice.Batch[Response] {
	e.mu.Lock()
	defer e.mu.Unlock()
	res := make([]Response, 0, b.Len())
	for i := range b.Items {
		r := &b.Items[i]
		n := 0
		for _, h := range r.Headers {
			if string(h.Name) == "cookie" {
				n++
			}
		}
		e.seen = append(e.seen, string(r.Get("cookie")))
		e.fields = append(e.fields, n)
		res = append(res, Response{Status: 200, Body: append([]byte(nil), r.Get("cookie")...)})
	}
	return sluice.Batch[Response]{Items: res}
}

// crumbs is a cookie of n pairs, one field per pair — the split RFC 9113
// §8.2.3 and RFC 9114 §4.2.1 allow and net/http's HTTP/2 transport makes.
func crumbs(n int) (fields [][2]string, joined string) {
	pairs := make([]string, 0, n)
	for i := range n {
		pair := "c" + strconv.Itoa(i) + "=v" + strconv.Itoa(i)
		fields = append(fields, [2]string{"cookie", pair})
		pairs = append(pairs, pair)
	}
	return fields, strings.Join(pairs, "; ")
}

// A cookie split into crumbs must reach the handler as the one field HTTP/1.1
// would deliver, joined by "; " (RFC 9113 §8.2.3 MUST). It used to arrive as
// separate fields: Request.Get returned the first pair alone, so a session
// cookie behind a theme cookie went missing on HTTP/2 only — and the 70-pair
// cookie a browser may send was 70 fields, over MaxHeaders, where HTTP/1.1
// counted one.
func TestH2CookieCrumbsAreJoined(t *testing.T) {
	echo := &cookieEcho{}
	addr, _ := serveTest(t, Config{}, echo.handle)
	c := dialH2(t, addr)

	c.request(1, "GET", "/", [2]string{"cookie", "theme=dark"}, [2]string{"cookie", "session=SECRET"})
	seventy, joined := crumbs(70)
	c.request(3, "GET", "/", seventy...)
	c.flush()

	bodies := c.bodies(2)
	if got := bodies[1]; got != "theme=dark; session=SECRET" {
		t.Errorf("two crumbs reached the handler as %q", got)
	}
	if got := bodies[3]; got != joined {
		t.Errorf("70 crumbs: body %q, want the 70 pairs joined (200, not ENHANCE_YOUR_CALM)", got)
	}
	echo.mu.Lock()
	defer echo.mu.Unlock()
	for i, n := range echo.fields {
		if n != 1 {
			t.Errorf("request %d: %d cookie fields reached the handler, want 1", i, n)
		}
	}
}

// The same rule on HTTP/3 (RFC 9114 §4.2.1 MUST), at the assembler, byte for
// byte: two cookie field lines in the QPACK block, one field out.
func TestH3CookieCrumbsAreJoined(t *testing.T) {
	echo := &cookieEcho{}
	a := testAssembler(Config{})
	seventy, joined := crumbs(70)
	frames := []quic.Frame{
		streamFrame(0, h3Request("GET", "/", [2]string{"cookie", "theme=dark"}, [2]string{"cookie", "session=SECRET"}), true),
		streamFrame(4, h3Request("GET", "/", seventy...), true),
	}
	heads, _, failed, err := serveH3Datagram(a, frames, echo.handle)
	if err != nil {
		t.Fatalf("serveH3Datagram returned %v", err)
	}
	if len(failed) != 0 {
		t.Fatalf("%d streams failed: %+v", len(failed), failed)
	}
	if len(heads) != 2 {
		t.Fatalf("%d streams answered, want 2", len(heads))
	}
	echo.mu.Lock()
	defer echo.mu.Unlock()
	if len(echo.seen) != 2 || echo.seen[0] != "theme=dark; session=SECRET" || echo.seen[1] != joined {
		t.Errorf("the handler saw %q, want the two crumbs joined then the 70 pairs joined", echo.seen)
	}
	for i, n := range echo.fields {
		if n != 1 {
			t.Errorf("request %d: %d cookie fields reached the handler, want 1", i, n)
		}
	}
}
