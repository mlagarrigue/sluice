package httpstream

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/net/quic"
)

// A client that never grants credit for a response holds its writer for
// Config.WriteTimeout and no longer: the write is given up on, the stream
// reset with H3_REQUEST_CANCELLED, and a streamed producer told at its next
// yield. The client's request stream opens with 64 bytes of credit for the
// response and is never read, so nothing past those bytes may leave. The
// idle timeout cannot end the wait instead, whatever its value: the starved
// writer re-announces STREAM_DATA_BLOCKED every third of it (RFC 9000 §4.1),
// and each announcement and its ACK restart both ends' idle clocks — set
// under the write timeout, it leaves this test green.
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
			clientParams := quic.DefaultConfig()
			clientParams.InitialMaxStreamDataBidiLocal = 64
			yielded := make(chan bool, 1)
			conn, _, _ := startH3With(t, Config{WriteTimeout: 200 * time.Millisecond}, quic.DefaultConfig(), clientParams,
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

// slowH3Client reads a response step bytes at a time, one step every pace,
// until the stream ends, is reset or bound passes. The client's transport
// grants the server credit as the application reads (net/quic credits
// consumption, not arrival), so the pace of the reads is the pace of the
// credit — slow, and known, which is what a test of the write timeout needs.
func slowH3Client(t *testing.T, s *quic.Stream, step int, pace, bound time.Duration) (got []byte, ended bool, code uint64, reset bool) {
	t.Helper()
	buf := make([]byte, step)
	start := time.Now()
	for time.Since(start) < bound {
		if c, was := s.WasReset(); was {
			return got, false, c, true
		}
		_ = s.SetReadDeadline(time.Now().Add(pace))
		// A step is read whole, however the transport chunks it, so that the
		// credit it earns does not depend on the chunking.
		for want := step; want > 0; {
			n, err := s.Read(buf[:want])
			got = append(got, buf[:n]...)
			want -= n
			if errors.Is(err, io.EOF) {
				return got, true, 0, false
			}
			if err != nil {
				break
			}
		}
		if c, was := s.WasReset(); was {
			return got, false, c, true
		}
		time.Sleep(pace)
	}
	return got, false, 0, false
}

// A client that reads slowly but steadily is served to the end over HTTP/3:
// WriteTimeout is a time without progress, so a write that times out having
// put Config.WriteProgressBytes on the wire gets the timeout again from where
// it stopped. The client's stream window is 4 KiB and it reads 3000 bytes
// every 150ms, which re-grants 3000 bytes of credit at that pace — over the
// 1024-byte floor in every timeout, so the 24 KiB answer takes four timeouts
// and no single deadline from the start of the write would let it finish.
func TestH3SlowReaderAboveTheProgressFloorIsServedToTheEnd(t *testing.T) {
	const (
		timeout = 300 * time.Millisecond
		bodyLen = 24 << 10
	)
	clientParams := quic.DefaultConfig()
	clientParams.InitialMaxStreamDataBidiLocal = 4096
	conn, _, _ := startH3With(t, Config{WriteTimeout: timeout, WriteProgressBytes: 1024}, quic.DefaultConfig(), clientParams,
		func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
			res := make([]Response, b.Len())
			for i := range res {
				res[i] = Response{Status: 200, Body: make([]byte, bodyLen)}
			}
			return sluice.Batch[Response]{Items: res}
		})

	start := time.Now()
	s := askH3(t, conn, h3Request("GET", "/slow"))
	got, ended, code, reset := slowH3Client(t, s, 3000, 150*time.Millisecond, 30*timeout)
	took := time.Since(start)
	if reset {
		t.Fatalf("a client reading 3000 bytes per 150ms had its stream reset with %#x after %v and %d bytes: "+
			"the write timeout does not follow progress", code, took.Round(time.Millisecond), len(got))
	}
	if !ended {
		t.Fatalf("the response had not ended after %v, with %d bytes", took.Round(time.Millisecond), len(got))
	}
	frames, _, _, err := parseH3Frames(nil, got, 1<<24)
	if err != nil {
		t.Fatalf("the response did not parse: %v", err)
	}
	var body int
	for _, f := range frames {
		if f.Type == h3Data {
			body += len(f.Payload)
		}
	}
	if body != bodyLen {
		t.Errorf("%d of %d payload bytes arrived", body, bodyLen)
	}
	if took < 3*timeout {
		t.Errorf("the answer took %v, under the three timeouts that make it a slow one", took.Round(time.Millisecond))
	}
}

// A client that reads less than the progress floor per timeout has its stream
// reset with H3_REQUEST_CANCELLED at about the timeout, however steadily it
// reads. The client's window is 1 KiB and it reads 200 bytes every two thirds
// of a timeout, which re-grants a few hundred bytes of credit per timeout —
// far under the 4 KiB default — so the deadline never moves.
func TestH3DribbleReaderIsResetWithinTheTimeout(t *testing.T) {
	const timeout = 300 * time.Millisecond
	clientParams := quic.DefaultConfig()
	clientParams.InitialMaxStreamDataBidiLocal = 1024
	conn, _, _ := startH3With(t, Config{WriteTimeout: timeout}, quic.DefaultConfig(), clientParams,
		func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
			res := make([]Response, b.Len())
			for i := range res {
				res[i] = Response{Status: 200, Body: make([]byte, 24<<10)}
			}
			return sluice.Batch[Response]{Items: res}
		})

	start := time.Now()
	s := askH3(t, conn, h3Request("GET", "/drip"))
	got, ended, code, reset := slowH3Client(t, s, 200, 2*timeout/3, 10*timeout)
	took := time.Since(start)
	if ended {
		t.Fatalf("the whole response was served to a client under the progress floor, %d bytes in %v",
			len(got), took.Round(time.Millisecond))
	}
	if !reset {
		t.Fatalf("after %v of 200-byte reads the stream was still open with %d bytes taken: "+
			"every grant moved the write deadline", took.Round(time.Millisecond), len(got))
	}
	if code != h3RequestCancelled {
		t.Errorf("the dribbled stream was reset with %#x, want H3_REQUEST_CANCELLED (%#x)", code, uint64(h3RequestCancelled))
	}
	if took > 3*timeout {
		t.Errorf("the reset took %v, want under %v", took.Round(time.Millisecond), 3*timeout)
	}
}

// A streamed response without trailers whose producer stays silent past
// WriteTimeout after its last batch, then ends, still ends its stream: the
// FIN takes a deadline of its own, as the trailers' write does. Under the
// last batch's deadline, long past, the transport refused the FIN and
// nothing reset the stream, so the client read a body that never ended.
// The case with no pause checks the client reads EOF at all.
func TestH3StreamedFinAfterASilenceLongerThanWriteTimeout(t *testing.T) {
	for _, tc := range []struct {
		name  string
		pause time.Duration
	}{
		{"no pause", 0},
		{"silence past WriteTimeout", 400 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, client := connPair(t, quic.DefaultConfig())
			s := openedStream(t, server, client)
			cfg := Config{WriteTimeout: 50 * time.Millisecond}.mustResolve()
			producer := sluice.Stream[[]byte](func(yield func(sluice.Batch[[]byte]) bool) {
				if !yield(sluice.Batch[[]byte]{Items: [][]byte{[]byte("abc")}}) {
					return
				}
				time.Sleep(tc.pause) // a silence, not a measurement
			})
			r := Response{Status: 200, Stream: producer}
			whole := writeH3Response(server, s.ID(), r, []byte("GET"), h3NoFieldSectionLimit, writeBoundOf(cfg), dateFor(time.Now()), func() {})
			_ = s.SetReadDeadline(time.Now().Add(5 * time.Second))
			_, err := io.ReadAll(s)
			if !whole {
				t.Error("writeH3Response reported the response not whole")
			}
			if err != nil {
				t.Errorf("the client did not read the body to its end: %v", err)
			}
		})
	}
}
