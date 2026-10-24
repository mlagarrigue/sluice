package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// A job is one request and the instant it was due. Every delay of the
// report is measured from `due`, so a request that waited in the queue
// because no connection was free carries that wait as latency: that is the
// coordinated omission corrected (§3.1), and what makes the queue a
// published guard (§3.5) rather than a hidden one.
type job struct {
	req   request
	due   time.Time
	warm  bool // due before the clock started: sent, checked, never counted
	fresh bool // opens its own connection and closes it (§3.6)
}

// run opens the connections the budget lacks, then fires one request per
// interval for the warm-up and the duration and collects every outcome of
// the duration. The dispatcher never blocks: the jobs channel holds the
// whole stage, so a slow server lengthens the queue, never the interval.
// It returns once every request has been answered: the drain of §7.6
// starts from there.
func (st stage) run(ctx context.Context, cl *client) (report, error) {
	draw := st.scenario.draw(st.seed, st.keyspace)
	fresh := st.freshDraw()
	if err := cl.warm(ctx, st.conns); err != nil {
		return report{}, fmt.Errorf("opening the connections: %w", err)
	}

	n, warm := st.requests(), st.warmRequests()
	jobs := make(chan job, n)
	// load is how many requests are due and not yet answered; over the
	// number of workers, the excess is the queue: requests due with no
	// connection free (§3.5). A job posted but not yet taken by an idle
	// worker is not queued, only late by a scheduling tick.
	var load, queueMax, queued atomic.Int64
	perConn := st.inFlightPerConn()
	workers := st.conns * perConn
	results := make([][]result, workers)
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			for j := range jobs {
				r := cl.send(ctx, w/perConn, j)
				load.Add(-1)
				if !j.warm {
					results[w] = append(results[w], r)
				}
			}
		})
	}

	start := time.Now()
	interval := st.interval()
	late := make([]time.Duration, 0, n-warm)
	var clock time.Time
	// What the server did to the budget, counted from the clock start.
	var dials, closes int64
	for i := range n {
		due := start.Add(time.Duration(i) * interval)
		if !sleepUntil(ctx, due) {
			close(jobs)
			wg.Wait()
			return report{}, errors.New("interrupted")
		}
		if i == warm {
			// The clock starts: what the warm-up queued is the warm-up's.
			clock = due
			queueMax.Store(0)
			queued.Store(0)
			dials, closes = cl.dials.Load(), cl.closes.Load()
		}
		if i >= warm {
			late = append(late, time.Since(due))
		}
		if excess := load.Add(1) - int64(workers); excess > 0 {
			queued.Add(1)
			for {
				m := queueMax.Load()
				if excess <= m || queueMax.CompareAndSwap(m, excess) {
					break
				}
			}
		}
		jobs <- job{req: draw(i), due: due, warm: i < warm, fresh: fresh(i)}
	}
	close(jobs)
	wg.Wait()
	elapsed := time.Since(clock)

	var all []result
	for _, r := range results {
		all = append(all, r...)
	}
	rep := st.report(all, elapsed, queueMax.Load(), queued.Load())
	rep.Late = summarise(late)
	rep.Reconnects = int(cl.dials.Load() - dials)
	rep.Closed = int(cl.closes.Load() - closes)
	rep.TLSVersion, rep.TLSSuite, rep.ALPN = cl.tlsSettings()
	return rep, nil
}

// sleepSlack is how early the dispatcher wakes to spin until the due
// instant. The runtime's timers wake late by up to a millisecond here
// (measured: p50 0.6 ms, max 1.1 ms on a 6.18 kernel), and that lateness
// would be added to every delay since the clock starts at the due instant.
// Spinning the last stretch costs one core at most, which a tester has.
const sleepSlack = 2 * time.Millisecond

// sleepUntil returns false when the context ended first.
func sleepUntil(ctx context.Context, due time.Time) bool {
	if d := time.Until(due) - sleepSlack; d > 0 {
		select {
		case <-time.After(d):
		case <-ctx.Done():
			return false
		}
	}
	for time.Now().Before(due) {
		if ctx.Err() != nil {
			return false
		}
	}
	return true
}
