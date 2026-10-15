package httpstream

import (
	"context"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/net/quic"
)

// A client that never grants credit for a response holds its writer for
// Config.WriteTimeout and no longer: the write is given up on, the stream
// reset with H3_REQUEST_CANCELLED, and a streamed producer told at its next
// yield. The client's request stream opens with 64 bytes of credit for the
// response and is never read, so nothing past those bytes may leave; the
// connection's idle timeout, at its 30s default, is far past the bound the
// test holds the server to, which is what makes the write deadline — not the
// idle timeout — the thing that ends the wait.
func TestH3WriteTimeoutResetsAStarvedResponse(t *testing.T) {
	for _, mode := range []struct {
		name string
		body func(yielded chan<- bool) Response
	}{
		{"head", func(chan<- bool) Response {
			return Response{Status: 200, Body: make([]byte, 4096)}
		}},
		{"streamed", func(yielded chan<- bool) Response {
			return Response{Status: 200, Stream: func(yield func(sluice.Batch[[]byte]) bool) {
				yielded <- yield(sluice.Batch[[]byte]{Items: [][]byte{make([]byte, 4096)}})
			}}
		}},
	} {
		t.Run(mode.name, func(t *testing.T) {
			clientParams := quic.DefaultParameters()
			clientParams.InitialMaxStreamDataBidiLocal = 64
			yielded := make(chan bool, 1)
			conn, _, _ := startH3With(t, Config{WriteTimeout: 200 * time.Millisecond}, quic.DefaultParameters(), clientParams,
				func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
					res := make([]Response, b.Len())
					for i := range res {
						res[i] = mode.body(yielded)
					}
					return sluice.Batch[Response]{Items: res}
				})

			start := time.Now()
			s := askH3(t, conn, h3Request("GET", "/large"))
			deadline := time.After(time.Second)
			for {
				if code, reset := s.WasReset(); reset {
					if code != h3RequestCancelled {
						t.Fatalf("the starved stream was reset with 0x%x, want H3_REQUEST_CANCELLED", code)
					}
					break
				}
				select {
				case <-deadline:
					t.Fatal("the starved response was not given up on within 1s: WriteTimeout does not bound it")
				case <-time.After(10 * time.Millisecond):
				}
			}
			if took := time.Since(start); took < 200*time.Millisecond {
				t.Fatalf("the stream was reset after %v, before the 200ms write timeout", took)
			}
			if mode.name == "streamed" {
				select {
				case ok := <-yielded:
					if ok {
						t.Error("the producer's yield returned true after its write was given up on")
					}
				case <-time.After(time.Second):
					t.Fatal("the producer's yield never returned")
				}
			}
		})
	}
}
