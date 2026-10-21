package httpstream

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/net/quic"
)

// lendingHandler answers every request with the same buffer, as a pipeline
// that recycles one does: the first call lends it as is, every later call
// overwrites it for its own answer first. That is the reuse the Response
// documentation allows once the earlier answer has been written — here the
// client has read it whole before it sends the next request — and the first
// answer must reach the client unchanged.
func lendingHandler() Handler {
	buf := []byte("first answer")
	calls := 0
	return func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
		out := make([]Response, len(b.Items))
		for i := range out {
			if calls > 0 {
				copy(buf, "second-reply")
			}
			calls++
			out[i] = Response{Body: buf}
		}
		return sluice.Batch[Response]{Items: out}
	}
}

// A Response stays valid until it is written, and no longer: a buffer lent
// to one answer is the pipeline's again once that answer is on the wire, and
// rewriting it for the next one changes nothing the client already has. What
// is not guaranteed — rewriting it before the write is over, which on HTTP/2
// and HTTP/3 outlasts the handler's return — is the caller's bug the
// documentation names, and produces a corrupted body; nothing here relies on
// it.
func TestResponseBufferIsFreeOnceWritten(t *testing.T) {
	const first, second = "first answer", "second-reply"

	t.Run("h1", func(t *testing.T) {
		addr, _ := serveTest(t, Config{}, lendingHandler())
		c := dial(t, addr)
		r := bufio.NewReader(c)
		var got []string
		for range 2 {
			fmt.Fprint(c, "GET / HTTP/1.1\r\nHost: h\r\n\r\n")
			res, err := http.ReadResponse(r, nil)
			if err != nil {
				t.Fatal(err)
			}
			body, err := io.ReadAll(res.Body)
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, string(body))
		}
		if got[0] != first || got[1] != second {
			t.Errorf("bodies %q, want %q then %q", got, first, second)
		}
	})

	t.Run("h2", func(t *testing.T) {
		addr, _ := serveTest(t, Config{}, lendingHandler())
		c := dialH2(t, addr)
		c.request(1, "GET", "/")
		c.flush()
		c.bodies(1)
		c.request(3, "GET", "/")
		c.flush()
		got := c.bodies(2)
		if got[1] != first || got[3] != second {
			t.Errorf("bodies %q then %q, want %q then %q", got[1], got[3], first, second)
		}
	})

	t.Run("h3", func(t *testing.T) {
		conn, _, _ := startH3(t, quic.DefaultParameters(), lendingHandler())
		a, _ := readH3Body(t, askH3(t, conn, h3Request("GET", "/")))
		b, _ := readH3Body(t, askH3(t, conn, h3Request("GET", "/")))
		if a != first || b != second {
			t.Errorf("bodies %q then %q, want %q then %q", a, b, first, second)
		}
	})
}
