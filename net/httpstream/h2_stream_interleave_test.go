package httpstream

import (
	"context"
	"testing"

	"github.com/mlagarrigue/sluice"
)

// interleaveTarget answers /hold with a streamed body that yields "a0", then
// waits for release before yielding "a1"; /stream with a streamed body that
// yields "c0" and "c1" at once; anything else with the buffered body "b".
func interleaveTarget(release <-chan struct{}) Handler {
	return func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
		out := make([]Response, 0, b.Len())
		for i := range b.Items {
			switch string(b.Items[i].Target) {
			case "/hold":
				out = append(out, Response{Status: 200, Stream: func(yield func(sluice.Batch[[]byte]) bool) {
					if !yield(sluice.Batch[[]byte]{Items: [][]byte{[]byte("a0")}}) {
						return
					}
					<-release
					yield(sluice.Batch[[]byte]{Items: [][]byte{[]byte("a1")}})
				}})
			case "/stream":
				out = append(out, Response{Status: 200, Stream: func(yield func(sluice.Batch[[]byte]) bool) {
					if yield(sluice.Batch[[]byte]{Items: [][]byte{[]byte("c0")}}) {
						yield(sluice.Batch[[]byte]{Items: [][]byte{[]byte("c1")}})
					}
				}})
			default:
				out = append(out, Response{Status: 200, Body: []byte("b")})
			}
		}
		return sluice.Batch[Response]{Items: out}
	}
}

// A streamed response that lasts holds no other response of its connection:
// one the handler answers later, buffered or streamed, and one of the same
// handler batch. A browser has one HTTP/2 connection per origin, so a
// streamed answer that held the others would hold the whole page.
func TestH2StreamedResponseHoldsNoOther(t *testing.T) {
	cases := []struct {
		name string
		// together sends the second request in the first one's frame batch,
		// so the handler answers both in one call.
		together bool
		path     string
		want     string
	}{
		{"buffered after", false, "/buffered", "b"},
		{"streamed after", false, "/stream", "c0c1"},
		{"streamed in the same batch", true, "/stream", "c0c1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			release := make(chan struct{})
			addr, sizes := serveTest(t, Config{}, interleaveTarget(release))
			released := false
			t.Cleanup(func() {
				if !released {
					close(release)
				}
			})

			c := dialH2(t, addr)
			c.request(1, "GET", "/hold")
			if tc.together {
				c.request(3, "GET", tc.path)
			} else {
				c.flush()
				c.pump(func() bool { return c.got[1] == "a0" })
				if c.got[1] != "a0" {
					t.Fatalf("stream 1 carried %q before the hold, want %q", c.got[1], "a0")
				}
				c.request(3, "GET", tc.path)
			}
			c.flush()

			c.pump(func() bool { return c.ended[3] })
			if !c.ended[3] || c.got[3] != tc.want {
				t.Fatalf("stream 3 carried %q (ended %v) while stream 1 was held, want %q whole",
					c.got[3], c.ended[3], tc.want)
			}
			if c.ended[1] {
				t.Fatal("stream 1 ended before its producer was released")
			}
			if tc.together {
				if got := sizes(); len(got) != 1 || got[0] != 2 {
					t.Logf("the two requests reached the handler as batches %v, not one of two", got)
				}
			}

			close(release)
			released = true
			c.pump(func() bool { return c.ended[1] })
			if !c.ended[1] || c.got[1] != "a0a1" {
				t.Fatalf("stream 1 carried %q (ended %v) once released, want %q whole", c.got[1], c.ended[1], "a0a1")
			}
		})
	}
}
