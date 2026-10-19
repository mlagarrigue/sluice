package benchmarks

import (
	"bytes"
	"context"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/database/postgres"
	"github.com/mlagarrigue/sluice/pushdown"
)

// TestPGDelayProxy is not a test: it is the far end of the remote-read
// benchmarks. With SLUICE_PG_PROXY set to a listen address, it forwards every
// connection to SLUICE_PG and holds each segment SLUICE_PG_RTT/2 in each
// direction, until it is killed. The benchmarks then run with SLUICE_PG
// pointing at the proxy, and every figure pays the round trip a server that
// far away would cost.
//
// The proxy runs in a process of its own so that it never shares the
// benchmark's core: in the same process under GOMAXPROCS=1, a request
// written while the consumer works would leave only when the consumer
// blocks, which no network does.
//
// What it emulates is latency alone. Segments are queued with their due time
// and written in order, so the link carries as many bytes as arrive — the
// bandwidth is loopback's — and the window is the kernel's on each side.
//
//	SLUICE_PG_PROXY=127.0.0.1:15432 SLUICE_PG_RTT=10ms \
//	  go test -run TestPGDelayProxy -timeout 0 &
//	SLUICE_PG=127.0.0.1:15432 go test -run '^$' -bench PGWide
func TestPGDelayProxy(t *testing.T) {
	listen := os.Getenv("SLUICE_PG_PROXY")
	if listen == "" {
		t.Skip("SLUICE_PG_PROXY is not set")
	}
	target := os.Getenv("SLUICE_PG")
	rtt, err := time.ParseDuration(os.Getenv("SLUICE_PG_RTT"))
	if err != nil || target == "" {
		t.Fatalf("SLUICE_PG_RTT (%q) and SLUICE_PG (%q) must be set", os.Getenv("SLUICE_PG_RTT"), target)
	}
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%s -> %s, %v round trip", ln.Addr(), target, rtt)
	for {
		c, err := ln.Accept()
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			up, err := net.Dial("tcp", target)
			if err != nil {
				_ = c.Close()
				return
			}
			var wg sync.WaitGroup
			wg.Go(func() { delayLine(up.(*net.TCPConn), c.(*net.TCPConn), rtt/2) })
			wg.Go(func() { delayLine(c.(*net.TCPConn), up.(*net.TCPConn), rtt/2) })
			wg.Wait()
			_ = c.Close()
			_ = up.Close()
		}()
	}
}

// delayLine copies src to dst, each segment written d after it was read. The
// reader never waits for the writer, so a segment's delay is d plus the
// writer's lateness, never d plus the segments before it.
func delayLine(dst, src *net.TCPConn, d time.Duration) {
	type segment struct {
		b   []byte
		due time.Time
	}
	q := make(chan segment, 1<<16)
	go func() {
		defer close(q)
		buf := make([]byte, 64<<10)
		for {
			n, err := src.Read(buf)
			if n > 0 {
				q <- segment{bytes.Clone(buf[:n]), time.Now().Add(d)}
			}
			if err != nil {
				return
			}
		}
	}()
	for s := range q {
		waitUntil(s.due)
		if _, err := dst.Write(s.b); err != nil {
			_ = src.Close() // ends the reader, which closes q
			for range q {
			}
			return
		}
	}
	_ = dst.CloseWrite()
}

// waitUntil returns at t, give or take a few microseconds. A timer alone wakes
// up to a millisecond late here — a 1 ms round trip measured 2.5 — so it
// sleeps to within 2 ms and spins the rest; the proxy has cores to spare.
func waitUntil(t time.Time) {
	if d := time.Until(t) - 2*time.Millisecond; d > 0 {
		time.Sleep(d)
	}
	for time.Now().Before(t) {
	}
}

// The wide read under a demand nothing advances: one Execute per batch, sent
// after the batch is delivered, because the request depends on what the
// consumer says once it has it. Against BenchmarkPGWideSluice this is the
// price of not asking ahead.
func BenchmarkPGWideSluiceDemand(b *testing.B) {
	benchWideSluice(b, postgres.QueryConfig{BatchRows: 1024, Demand: new(pushdown.Demand)})
}

// stopSQL is a result nobody reads to the end: ten million rows, which the
// server builds before sending the first, so ns/op is mostly that; stop-ns
// is the figure.
const stopSQL = `SELECT n FROM generate_series(1, 10000000) n`

// A consumer that stops at the first batch of a result asked for whole. The
// rest is cancelled — a second connection carries the cancel request — and
// the query returns once the server has said so; stop-ns is that wait, from
// the consumer's false to the stream's return.
func BenchmarkPGStopSluice(b *testing.B) {
	conn := sluiceConn(b)
	var stops []time.Duration
	b.ReportAllocs()
	for b.Loop() {
		var stoppedAt time.Time
		src := conn.Query(b.Context(), stopSQL, nil, postgres.QueryConfig{BatchRows: 1024, Fetch: postgres.FetchAll})
		src.Stream()(func(sluice.Batch[postgres.Row]) bool {
			stoppedAt = time.Now()
			return false
		})
		if err := src.Err(); err != nil {
			b.Fatal(err)
		}
		stops = append(stops, time.Since(stoppedAt))
	}
	reportPercentiles(b, "stop", stops)
	// The connection must be good for the next query.
	if err := conn.Exec(context.Background(), "SELECT 1"); err != nil {
		b.Fatal(err)
	}
}
