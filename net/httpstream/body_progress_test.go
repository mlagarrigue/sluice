package httpstream

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
)

// A streamed body has ReadTimeout of waiting to deliver ReadProgressBytes,
// and each step of progress refills the time: the body twin of
// WriteTimeout. Each read used to re-arm the whole ReadTimeout, so a
// client that sent one byte per fraction of it held the request — its
// connection, its goroutine, its buffer and the handler — for as long as it
// kept dripping (RP-20261025-h1h2-01). The tests assert an outcome, not a
// duration: the drip runs until the test ends.
func TestAStreamedBodyThatDripsIsCut(t *testing.T) {
	addr, _ := serveTest(t, Config{
		ReadTimeout:          300 * time.Millisecond,
		MaxBodyBytes:         1024,
		MaxStreamedBodyBytes: 1 << 20,
	}, readAllHandler)
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Write([]byte("POST /up HTTP/1.1\r\nHost: x\r\nContent-Length: 100000\r\n\r\n")); err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	var dripping sync.WaitGroup
	defer func() { close(stop); dripping.Wait() }()
	dripping.Go(func() {
		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()
		for {
			if _, err := c.Write([]byte("a")); err != nil {
				return // the server ended the request
			}
			select {
			case <-stop:
				return
			case <-tick.C:
			}
		}
	})
	if err := c.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 128)
	if n, err := c.Read(buf); n == 0 {
		t.Fatalf("the dripped body was never cut: nothing came back (%v)", err)
	}
}

// The other side of the rule: a body that moves by ReadProgressBytes within
// each ReadTimeout is served to the end, however long it takes in all.
func TestAStreamedBodyThatMovesIsServed(t *testing.T) {
	addr, _ := serveTest(t, Config{
		ReadTimeout:          300 * time.Millisecond,
		MaxBodyBytes:         1024,
		MaxStreamedBodyBytes: 1 << 20,
	}, readAllHandler)
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.SetDeadline(time.Now().Add(20 * time.Second)); err != nil {
		t.Fatal(err)
	}
	const blocks, block = 8, 4 << 10
	if _, err := c.Write(fmt.Appendf(nil, "POST /up HTTP/1.1\r\nHost: x\r\nContent-Length: %d\r\n\r\n", blocks*block)); err != nil {
		t.Fatal(err)
	}
	go func() {
		for range blocks {
			if _, err := c.Write(bytes.Repeat([]byte("a"), block)); err != nil {
				return
			}
			time.Sleep(150 * time.Millisecond) // half ReadTimeout: 1.2 s in all, four ReadTimeouts
		}
	}()
	status, body := readResponse(t, bufio.NewReader(c))
	if want := fmt.Sprintf("%d a a", blocks*block); !strings.Contains(status, "200") || body != want {
		t.Fatalf("a body moving by a block per half ReadTimeout: %s %q, want 200 %q", status, body, want)
	}
}

// The HTTP/2 arm (bodyQueue serves HTTP/3 the same way): a DATA frame of
// one byte per half ReadTimeout is cut, one of ReadProgressBytes per half
// ReadTimeout is read to the end.
func TestAnH2StreamedBodyMustMove(t *testing.T) {
	for _, tc := range []struct {
		name  string
		chunk int
		count int
		cut   bool
	}{
		{"drip", 1, 1 << 20, true},
		{"moving", 4 << 10, 8, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := h2StreamCfg()
			cfg.ReadTimeout = 200 * time.Millisecond
			type result struct {
				n   int
				err error
			}
			done := make(chan result, 1)
			addr, _ := serveH2(t, cfg, func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
				body, err := b.Items[0].ReadAll(nil, 1<<20)
				done <- result{len(body), err}
				return sluice.Batch[Response]{Items: []Response{{Status: 200}}}
			})
			c := dialH2(t, addr)
			if err := c.conn.SetDeadline(time.Now().Add(60 * time.Second)); err != nil {
				t.Fatal(err)
			}
			c.frame(h2Frame{Type: frameHeaders, Flags: flagEndHeaders, StreamID: 1, Payload: headBlock("POST", "/")})
			c.flush()
			stop := make(chan struct{})
			var sending sync.WaitGroup
			defer func() { close(stop); sending.Wait() }()
			sending.Go(func() {
				for i := range tc.count {
					last := i == tc.count-1
					var flags byte
					if last {
						flags = flagEndStream
					}
					f, err := appendH2Frame(nil, h2Frame{Type: frameData, Flags: flags, StreamID: 1, Payload: bytes.Repeat([]byte("a"), tc.chunk)})
					if err != nil {
						return
					}
					if _, err := c.conn.Write(f); err != nil {
						return
					}
					select {
					case <-stop:
						return
					case <-time.After(100 * time.Millisecond):
					}
				}
			})
			select {
			case r := <-done:
				switch {
				case tc.cut && !errors.Is(r.err, os.ErrDeadlineExceeded):
					t.Errorf("a dripped body read %d bytes and ended with %v, want os.ErrDeadlineExceeded", r.n, r.err)
				case !tc.cut && (r.err != nil || r.n != tc.chunk*tc.count):
					t.Errorf("a moving body read %d bytes and ended with %v, want %d and nil", r.n, r.err, tc.chunk*tc.count)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("the handler is still waiting on the body")
			}
		})
	}
}
