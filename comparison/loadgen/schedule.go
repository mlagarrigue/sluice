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
	win   int  // the window it falls in, -1 for none
}

// run opens the connections the budget lacks, then fires the requests of
// each phase at its constant interval — the warm-up, then the measured
// phases — and collects every outcome after the warm-up. The dispatcher
// never blocks while the queue holds under maxQueue requests, so a slow
// server lengthens the queue, never the interval. It returns once every
// request has been answered: the drain of §7.6 starts from there. With a
// window, the measured requests are also reported window by window, by
// the instant they were due.
func (st stage) run(ctx context.Context, cl *client) (whole report, perWindow []report, err error) {
	draw := st.scenario.draw(st.seed, st.keyspace)
	fresh := st.freshDraw()
	conns, perConn := st.layout()
	if err := cl.warm(ctx, conns); err != nil {
		return report{}, nil, fmt.Errorf("opening the connections: %w", err)
	}

	phases := st.phases()
	n, warm := st.requests(), st.warmRequests()
	jobs := make(chan job, min(n, maxQueue))
	// load is how many requests are due and not yet answered; over the
	// number of workers, the excess is the queue: requests due with no
	// connection free (§3.5). A job posted but not yet taken by an idle
	// worker is not queued, only late by a scheduling tick.
	var load atomic.Int64
	workers := conns * perConn
	results := make([][]result, workers)
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			for j := range jobs {
				r := cl.send(ctx, w/perConn, j)
				r.win = j.win
				load.Add(-1)
				if !j.warm {
					results[w] = append(results[w], r)
				}
			}
		})
	}

	// The queue and the dispatcher's lateness, for the stage and per
	// window: only the dispatcher writes them.
	windows := st.windows()
	queueMax, queued := make([]int64, windows+1), make([]int, windows+1)
	late := make([][]time.Duration, windows+1)
	start := time.Now()
	var clock time.Time
	// What the server did to the budget, counted from the clock start.
	var dials, closes int64
	i := 0
	var offset time.Duration
	for _, ph := range phases {
		interval := time.Second / time.Duration(ph.rate)
		for k := range ph.n {
			due := start.Add(offset + time.Duration(k)*interval)
			if !sleepUntil(ctx, due) {
				close(jobs)
				wg.Wait()
				return report{}, nil, errors.New("interrupted")
			}
			if i == warm {
				// The clock starts: what the warm-up queued is the warm-up's.
				clock = due
				dials, closes = cl.dials.Load(), cl.closes.Load()
			}
			win := -1
			if i >= warm {
				win = st.windowOf(due.Sub(clock))
				late[windows] = append(late[windows], time.Since(due))
				if win >= 0 {
					late[win] = append(late[win], time.Since(due))
				}
			}
			if excess := load.Add(1) - int64(workers); excess > 0 && i >= warm {
				for _, at := range []int{windows, win} {
					if at >= 0 {
						queued[at]++
						queueMax[at] = max(queueMax[at], excess)
					}
				}
			}
			jobs <- job{req: draw(i), due: due, warm: i < warm, fresh: fresh(i), win: win}
			i++
		}
		offset += time.Duration(ph.n) * interval
	}
	close(jobs)
	wg.Wait()
	elapsed := time.Since(clock)

	var all []result
	for _, r := range results {
		all = append(all, r...)
	}
	version, suite, alpn := cl.tlsSettings()
	rep := st.report(all, elapsed, queueMax[windows], int64(queued[windows]))
	rep.Late = summarise(late[windows])
	rep.Reconnects = int(cl.dials.Load() - dials)
	rep.Closed = int(cl.closes.Load() - closes)
	rep.TLSVersion, rep.TLSSuite, rep.ALPN = version, suite, alpn
	rep.Start = clock.UTC().Format(time.RFC3339Nano)
	if windows == 0 {
		return rep, nil, nil
	}

	// One line per window, in the order of the clock.
	byWin := make([][]result, windows)
	for _, r := range all {
		if r.win >= 0 {
			byWin[r.win] = append(byWin[r.win], r)
		}
	}
	lines := make([]report, windows)
	for w := range windows {
		line := st.report(byWin[w], st.window, queueMax[w], int64(queued[w]))
		line.Late = summarise(late[w])
		line.Window = &w
		line.Phase, line.Rate = st.phaseAt(time.Duration(w) * st.window)
		line.Duration = st.window.Seconds()
		line.TLSVersion, line.TLSSuite, line.ALPN = version, suite, alpn
		line.Start = clock.Add(time.Duration(w) * st.window).UTC().Format(time.RFC3339Nano)
		lines[w] = line
	}
	return rep, lines, nil
}

// maxQueue bounds the requests due and not yet taken: a stage of minutes
// would otherwise hold millions of jobs. Past it the dispatcher waits,
// which late_ms shows; a queue that deep is past every deadline anyway.
const maxQueue = 1 << 18

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
