package httpstream

import (
	"net"
	"testing"
	"time"
)

// A request still assembling when ReadTimeout runs out has its stream reset
// even when the peer then says nothing at all: the frame reader wakes for the
// earliest assembly deadline, not only when a frame arrives. The connection
// is not lost for it — a request that follows is served. Two shapes: a
// header block left open (HEADERS without END_HEADERS), whose remaining
// fragments must still reach the HPACK decoder once the peer resumes, and
// a head whole with its body still awaited, under BodyWhole.
func TestH2SilentAssemblyIsResetAtReadTimeout(t *testing.T) {
	cases := []struct {
		name string
		// stall opens stream 1 and leaves it assembling; resume lets the
		// peer finish what the stall left open, if anything, so that the
		// connection can carry another request.
		stall  func(c *h2client)
		resume func(c *h2client)
	}{
		{
			name: "header block left open",
			stall: func(c *h2client) {
				c.frame(h2Frame{Type: frameHeaders, StreamID: 1, Payload: headBlock("GET", "/one")})
			},
			resume: func(c *h2client) {
				// §6.10: nothing but the block's CONTINUATION may follow.
				c.frame(h2Frame{Type: frameContinuation, Flags: flagEndHeaders, StreamID: 1})
			},
		},
		{
			name: "body awaited",
			stall: func(c *h2client) {
				c.frame(h2Frame{Type: frameHeaders, Flags: flagEndHeaders, StreamID: 1, Payload: headBlock("POST", "/one")})
			},
			resume: func(*h2client) {},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = ln.Close() })
			go func() {
				c, err := ln.Accept()
				if err != nil {
					return
				}
				cfg := Config{
					Logger: discardLogger(), BodyDelivery: BodyWhole,
					ReadTimeout: 300 * time.Millisecond, IdleTimeout: time.Minute,
				}
				_ = ServeH2Conn(t.Context(), c, cfg, echoTarget)
			}()
			conn, err := net.Dial("tcp", ln.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = conn.Close() })
			c := h2clientOn(t, conn)

			start := time.Now()
			tc.stall(c)
			c.flush()
			c.pump(func() bool { return c.resetCount[1] > 0 || c.sawGoAway })
			waited := time.Since(start)
			if c.resetCount[1] == 0 || c.sawGoAway {
				t.Fatalf("after %v of silence under a ReadTimeout of 300ms: %d RST_STREAM on the stream, GOAWAY %v; want the stream reset and the connection kept",
					waited.Round(time.Millisecond), c.resetCount[1], c.sawGoAway)
			}
			if waited > 2*time.Second {
				t.Fatalf("the stream was reset after %v, over six ReadTimeouts", waited.Round(time.Millisecond))
			}
			if code := c.resets[1]; code != errCancel {
				t.Errorf("RST_STREAM code %d, want CANCEL (%d)", code, errCancel)
			}

			tc.resume(c)
			c.request(3, "GET", "/three")
			c.flush()
			c.pump(func() bool { return c.ended[3] || c.sawGoAway })
			if c.got[3] != "/three" || c.sawGoAway {
				t.Fatalf("the request after the reset carried %q (GOAWAY %v), want it served", c.got[3], c.sawGoAway)
			}
		})
	}
}
