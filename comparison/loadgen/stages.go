package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/mlagarrigue/sluice/comparison/loadgen/stats"
)

// budget is C = 2 R (RTT + X) of §3.5, the connections a rate needs so that
// the generator's queue never forms while the server holds its SLO: the
// factor 2 is the margin for latency peaks. It depends on the scenario and
// the rate only, never on the contender, so every contender gets the same C
// at the same rate. The tester's ceiling caps it when there is one.
func (c campaign) budget(rate int) int {
	n := int(math.Ceil(2 * float64(rate) * (c.rtt + c.slo).Seconds()))
	n = max(n, 1)
	if c.maxConns > 0 {
		n = min(n, c.maxConns)
	}
	return n
}

// gridFrom yields the study's common grid 100 × 1.05^k (§3.3, §7.6), from
// the first point at or above `from`, rounded to the unit, k negative under
// 100 req/s: every contender is measured at the same rates, whichever stage
// it breaks at.
func gridFrom(from int) func(yield func(int) bool) {
	return func(yield func(int) bool) {
		prev := 0
		for k := stats.MinStep; ; k++ {
			r := int(stats.Grid(k))
			if r <= prev || r < from {
				continue
			}
			prev = r
			if !yield(r) {
				return
			}
		}
	}
}

// run writes one JSON line per stage and says whether what it ran is a
// measurement: a single stage that passed every check, or a ramp whose
// stages are all measurements, the last one broken by the SLO alone. A ramp
// stops at the first stage that breaks, whatever the cause, and before a
// stage its keyspace could not hold.
func (c campaign) run(ctx context.Context, out io.Writer) (bool, error) {
	cl, err := c.newClient()
	if err != nil {
		return false, err
	}
	defer cl.close()
	enc := json.NewEncoder(out)

	if !c.ramp && len(c.rates) == 0 {
		rep, windows, err := c.stage.run(ctx, cl)
		if err != nil {
			return false, err
		}
		for w := range windows {
			if err := enc.Encode(&windows[w]); err != nil {
				return false, err
			}
		}
		return !rep.Failed(), enc.Encode(rep)
	}

	rates := c.rates
	if c.ramp {
		rates = nil
		for r := range gridFrom(c.rate) {
			rates = append(rates, r)
			if st := c.at(r); st.fits() != nil {
				break
			}
		}
	}
	first := true
	for _, r := range rates {
		st := c.at(r)
		if err := st.fits(); err != nil {
			return false, fmt.Errorf("the ramp stops before %d req/s: %w", r, err)
		}
		if !first && !sleepUntil(ctx, time.Now().Add(c.drain)) {
			return false, ctx.Err()
		}
		first = false
		rep, _, err := st.run(ctx, cl)
		if err != nil {
			return false, err
		}
		if err := enc.Encode(rep); err != nil {
			return false, err
		}
		if !rep.Held {
			return rep.Cause == causeSLO, nil
		}
	}
	return true, nil
}

// at is the stage of a ramp at one rate, with its own budget.
func (c campaign) at(rate int) stage {
	st := c.stage
	st.rate = rate
	st.conns = c.budget(rate)
	return st
}
