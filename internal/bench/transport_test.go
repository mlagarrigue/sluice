package bench

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/gateway"
	"github.com/mlagarrigue/sluice/net/httpstream"
)

// Where a transport batch actually pays, and where it does not.
//
// The library has two ways to turn many requests into one backend round
// trip, and they are not the same mechanism:
//
//   - the **gateway** batches across connections, by waiting: a call is held
//     up to Config.Within for company, and that wait is latency added on
//     purpose.
//   - **httpstream** batches within a connection, by not needing to wait: a
//     client that pipelines has already put its requests on the wire, so the
//     batch is whatever a single read returned.
//
// The question this answers is whether either survives a real backend, where
// one round trip costs three orders of magnitude more than the transport. The
// unit is therefore **round trips**, not nanoseconds: it is exact, it needs no
// quiet machine, and it is the thing that costs.

// backend is the shared cost model: one call per batch, priced the same
// whatever the batch holds. That is what `= ANY($1)` buys, and without it
// batching anything would be pointless.
type backend struct {
	roundTrip time.Duration
	calls     atomic.Int64
	rows      atomic.Int64
}

func (b *backend) load(ids []int64) {
	b.calls.Add(1)
	b.rows.Add(int64(len(ids)))
	// Slept rather than spun: a round trip parks the goroutine and lets the
	// others run, which is exactly what makes batching visible.
	time.Sleep(b.roundTrip)
}

// serveHTTPGateway is the supported vertical: net/http in front, a gateway
// forming batches out of concurrent callers.
func serveHTTPGateway(t *testing.T, be *backend, within time.Duration) string {
	t.Helper()
	gw := gateway.New(gateway.Config{Size: 64, Within: within},
		func(calls sluice.Stream[*gateway.Call[int64, int64]]) {
			calls(func(b sluice.Batch[*gateway.Call[int64, int64]]) bool {
				ids := make([]int64, 0, b.Len())
				for _, c := range b.Items {
					ids = append(ids, c.In)
				}
				be.load(ids)
				for _, c := range b.Items {
					c.Reply(c.In)
				}
				return true
			})
		})
	t.Cleanup(func() { _ = gw.Close() })

	mux := http.NewServeMux()
	mux.HandleFunc("GET /o/{id}", func(w http.ResponseWriter, r *http.Request) {
		var id int64
		_, _ = fmt.Sscanf(r.PathValue("id"), "%d", &id)
		out, err := gw.Do(r.Context(), id)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		fmt.Fprintf(w, "%d", out)
	})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return ln.Addr().String()
}

// serveStream is the experiment: the transport hands over whatever arrived
// together, and the batch goes straight to the backend with nothing waited
// for.
func serveStream(t *testing.T, be *backend) string {
	t.Helper()
	return startStream(t, func(b sluice.Batch[httpstream.Request]) sluice.Batch[httpstream.Response] {
		ids := make([]int64, 0, b.Len())
		for _, r := range b.Items {
			ids = append(ids, targetID(r.Target))
		}
		be.load(ids)
		out := make([]httpstream.Response, 0, b.Len())
		for _, id := range ids {
			out = append(out, httpstream.Response{Status: 200, Body: fmt.Append(nil, id)})
		}
		return sluice.Batch[httpstream.Response]{Items: out}
	})
}

// serveStreamGateway is the two mechanisms composed: the connection's batch
// goes into a gateway, which can still gather callers from other connections.
func serveStreamGateway(t *testing.T, be *backend, within time.Duration) string {
	t.Helper()
	gw := gateway.New(gateway.Config{Size: 64, Within: within},
		func(calls sluice.Stream[*gateway.Call[int64, int64]]) {
			calls(func(b sluice.Batch[*gateway.Call[int64, int64]]) bool {
				ids := make([]int64, 0, b.Len())
				for _, c := range b.Items {
					ids = append(ids, c.In)
				}
				be.load(ids)
				for _, c := range b.Items {
					c.Reply(c.In)
				}
				return true
			})
		})
	t.Cleanup(func() { _ = gw.Close() })

	return startStream(t, func(b sluice.Batch[httpstream.Request]) sluice.Batch[httpstream.Response] {
		// One submission for the connection's batch: the gateway sees it as
		// company that has already arrived, and callers on other connections
		// can still join the same window. This used to be a goroutine per
		// element around Do; DoBatch is that composition as an API.
		ids := make([]int64, b.Len())
		for i, r := range b.Items {
			ids[i] = targetID(r.Target)
		}
		out := make([]httpstream.Response, b.Len())
		for i, o := range gw.DoBatch(context.Background(), ids) {
			if o.Err != nil {
				out[i] = httpstream.Response{Status: 500}
				continue
			}
			out[i] = httpstream.Response{Status: 200, Body: fmt.Append(nil, o.Out)}
		}
		return sluice.Batch[httpstream.Response]{Items: out}
	})
}

func startStream(t *testing.T, h httpstream.Handler) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = httpstream.Serve(ctx, ln, httpstream.Config{}, h) }()
	t.Cleanup(func() { cancel(); <-done })
	return ln.Addr().String()
}

func targetID(target []byte) int64 {
	var id int64
	_, _ = fmt.Sscanf(string(target), "/o/%d", &id)
	return id
}

// pipelinedClient writes n requests on one connection before reading any
// answer — what HTTP/1.1 allows and what a batching transport can exploit.
func pipelinedClient(t *testing.T, addr string, n int) {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	_ = c.SetDeadline(time.Now().Add(30 * time.Second))

	var out strings.Builder
	for i := range n {
		fmt.Fprintf(&out, "GET /o/%d HTTP/1.1\r\nHost: h\r\n\r\n", i)
	}
	if _, err := c.Write([]byte(out.String())); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(c)
	for range n {
		if err := drainOneResponse(br); err != nil {
			t.Fatalf("reading a response: %v", err)
		}
	}
}

// concurrentClient opens n connections at once, one request each — the shape
// the gateway was built for.
func concurrentClient(t *testing.T, addr string, n int) {
	t.Helper()
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			c, err := net.Dial("tcp", addr)
			if err != nil {
				// Surfaced, not swallowed: a failed dial would otherwise show
				// up only as a short call count in the caller's assertion.
				t.Errorf("connection %d: dial: %v", i, err)
				return
			}
			defer func() { _ = c.Close() }()
			_ = c.SetDeadline(time.Now().Add(30 * time.Second))
			if _, err := fmt.Fprintf(c, "GET /o/%d HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n", i); err != nil {
				t.Errorf("connection %d: write: %v", i, err)
				return
			}
			if err := drainOneResponse(bufio.NewReader(c)); err != nil {
				t.Errorf("connection %d: reading the response: %v", i, err)
			}
		})
	}
	wg.Wait()
}

func drainOneResponse(br *bufio.Reader) error {
	length := -1
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return err
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			break
		}
		if name, value, ok := strings.Cut(line, ": "); ok && strings.EqualFold(name, "content-length") {
			if _, err := fmt.Sscanf(value, "%d", &length); err != nil {
				return err
			}
		}
	}
	if length <= 0 {
		return nil
	}
	buf := make([]byte, length)
	for read := 0; read < length; {
		n, err := br.Read(buf[read:])
		read += n
		if err != nil {
			return err
		}
	}
	return nil
}

// TestTransportBatchingMatrix runs the same clients against the same backend
// over three servers, and reports the round trips each shape costs.
//
// It is a test rather than a benchmark because the number that matters —
// how many times the backend was called — is exact and needs no quiet
// machine. The wall clock is logged beside it as context, not as a claim.
func TestTransportBatchingMatrix(t *testing.T) {
	if testing.Short() {
		t.Skip("starts servers and sleeps a simulated round trip per batch")
	}
	const (
		requests  = 32
		roundTrip = 500 * time.Microsecond // the figure the docs use
		within    = 2 * time.Millisecond   // the gateway's advertised ceiling
	)

	servers := []struct {
		name  string
		start func(*testing.T, *backend) string
	}{
		{"net/http + gateway", func(t *testing.T, be *backend) string { t.Helper(); return serveHTTPGateway(t, be, within) }},
		{"httpstream", serveStream},
		{"httpstream + gateway", func(t *testing.T, be *backend) string { t.Helper(); return serveStreamGateway(t, be, within) }},
	}
	clients := []struct {
		name string
		run  func(*testing.T, string, int)
	}{
		{"pipelined (one connection)", pipelinedClient},
		{"concurrent (32 connections)", concurrentClient},
	}

	type cell struct {
		calls, rows int64
		wall        time.Duration
	}
	got := map[string]cell{}

	for _, srv := range servers {
		for _, cl := range clients {
			name := srv.name + " / " + cl.name
			t.Run(name, func(t *testing.T) {
				be := &backend{roundTrip: roundTrip}
				addr := srv.start(t, be)

				start := time.Now()
				cl.run(t, addr, requests)
				wall := time.Since(start)

				c := cell{be.calls.Load(), be.rows.Load(), wall}
				got[name] = c
				t.Logf("%-24s %-28s  %3d round trips for %2d rows  (%v)",
					srv.name, cl.name, c.calls, c.rows, wall.Round(time.Millisecond))
			})
		}
	}

	// What the matrix has to show, stated as assertions so it cannot quietly
	// stop being true. The bounds are loose: the exact count depends on how
	// the scheduler interleaves, and the claim is about orders of magnitude.
	check := func(name string, want func(int64) bool, why string) {
		t.Helper()
		c, ok := got[name]
		if !ok {
			t.Fatalf("no result for %q", name)
		}
		if !want(c.calls) {
			t.Errorf("%s: %d round trips — %s", name, c.calls, why)
		}
	}

	// The transport batch: the requests were already on the wire, so one read
	// serves all of them and nothing was waited for.
	check("httpstream / pipelined (one connection)",
		func(n int64) bool { return n <= 2 },
		"a pipelined connection should cost about one round trip")

	// And the case it does nothing for: one request per connection is one
	// batch of one, however good the transport is.
	check("httpstream / concurrent (32 connections)",
		func(n int64) bool { return n >= requests/2 },
		"separate connections cannot batch at the transport, and should not appear to")

	// The gateway's own shape, which the transport cannot help with and does
	// not need to.
	check("net/http + gateway / concurrent (32 connections)",
		func(n int64) bool { return n <= requests/4 },
		"concurrent callers are what the gateway batches")

	// The pairing that covers both: the connection's batch is submitted
	// together, so the gateway finds its company already there.
	check("httpstream + gateway / pipelined (one connection)",
		func(n int64) bool { return n <= 2 },
		"a pipelined batch should reach the gateway as company that already arrived")
	check("httpstream + gateway / concurrent (32 connections)",
		func(n int64) bool { return n <= requests/4 },
		"the gateway still batches across connections behind the stream transport")
}
