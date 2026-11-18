package httpstream

import (
	"context"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
)

// A HEAD refused on its request line — too long (414), a version not
// served (505), a fourth field (400) — gets its refusal's head and no body,
// as a HEAD refused after its line does (RFC 9110 §9.3.2). The refusal came
// before the method was read, and the body went out behind the head.
func TestAHeadRefusedOnItsLineGetsNoBody(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = Serve(ctx, ln, Config{}, func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
			out := make([]Response, b.Len())
			for i := range out {
				out[i] = Response{Status: 200, Body: []byte("ok")}
			}
			return sluice.Batch[Response]{Items: out}
		})
	}()
	t.Cleanup(func() { cancel(); <-done })

	for _, tc := range []struct{ name, req, status string }{
		{"a line too long", "HEAD /" + strings.Repeat("a", 20000) + " HTTP/1.1\r\nHost: x\r\n\r\n", "414"},
		{"a version not served", "HEAD / HTTP/2.0\r\nHost: x\r\n\r\n", "505"},
		{"a fourth field", "HEAD / HTTP/1.1 extra\r\nHost: x\r\n\r\n", "400"},
		{"behind the empty line RFC 9112 §2.2 allows", "\r\nHEAD / HTTP/2.0\r\nHost: x\r\n\r\n", "505"},
		{"refused after its line (the control)", "HEAD / HTTP/1.1\r\n\r\n", "400"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := net.Dial("tcp", ln.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			_ = c.SetDeadline(time.Now().Add(20 * time.Second))
			if _, err := io.WriteString(c, tc.req); err != nil {
				t.Fatal(err)
			}
			raw, _ := io.ReadAll(c)
			got := string(raw)
			if !strings.HasPrefix(got, "HTTP/1.1 "+tc.status+" ") {
				t.Fatalf("want status %s: %q", tc.status, got)
			}
			head, body, ok := strings.Cut(got, "\r\n\r\n")
			if !ok || body != "" {
				t.Fatalf("a HEAD's refusal carries a body %q behind %q", body, head)
			}
		})
	}
}
