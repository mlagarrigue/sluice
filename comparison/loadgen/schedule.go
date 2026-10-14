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
	req request
	due time.Time
}

// run opens the connections, then fires one request per interval for the
// duration and collects every outcome. The dispatcher never blocks: the
// jobs channel holds the whole stage, so a slow server lengthens the queue,
// never the interval.
func (st stage) run(ctx context.Context) (report, error) {
	cl, err := st.newClient()
	if err != nil {
		return report{}, err
	}
	defer cl.close()
	draw := st.scenario.draw(st.seed, st.keyspace)
	if err := cl.warm(ctx, st.conns); err != nil {
		return report{}, fmt.Errorf("opening the connections: %w", err)
	}

	n := st.requests()
	jobs := make(chan job, n)
	var depth, queueMax, queued atomic.Int64
	workers := st.conns * inFlightPerConn
	results := make([][]result, workers)
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			for j := range jobs {
				depth.Add(-1)
				results[w] = append(results[w], cl.do(ctx, j))
			}
		})
	}

	start := time.Now()
	interval := st.interval()
	late := make([]time.Duration, 0, n)
	for i := range n {
		due := start.Add(time.Duration(i) * interval)
		if !sleepUntil(ctx, due) {
			close(jobs)
			wg.Wait()
			return report{}, errors.New("interrupted")
		}
		late = append(late, time.Since(due))
		// Due jobs not yet taken by a worker, this one included: more than
		// one means no connection was free when this request was due.
		d := depth.Add(1)
		if d > 1 {
			queued.Add(1)
		}
		for {
			m := queueMax.Load()
			if d <= m || queueMax.CompareAndSwap(m, d) {
				break
			}
		}
		jobs <- job{draw(i), due}
	}
	close(jobs)
	wg.Wait()
	elapsed := time.Since(start)

	var all []result
	for _, r := range results {
		all = append(all, r...)
	}
	rep := st.report(all, elapsed, queueMax.Load(), queued.Load())
	rep.Late = summarise(late)
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
