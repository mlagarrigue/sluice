package main

import (
	"slices"
	"time"
)

// report is one stage's line: what was asked, what came back and the two
// delays as quantiles. Delays are milliseconds; a quantile is read over the
// answers that passed every check. Errors are counted, never averaged in.
type report struct {
	URL      string  `json:"url"`
	Scenario string  `json:"scenario"`
	Rate     int     `json:"rate"`
	Duration float64 `json:"duration_s"`
	Conns    int     `json:"conns"`
	Seed     uint64  `json:"seed"`

	Sent     int     `json:"sent"`
	OK       int     `json:"ok"`
	Achieved float64 `json:"achieved_rps"` // sent over the time the stage took, drain included

	Errors   int `json:"errors"`    // every request that is not ok
	BadKey   int `json:"bad_key"`   // the response contract broken (§4): one is a failed stage
	Connect  int `json:"connect"`   // failed connects and handshakes (§3.6): one is an invalid stage
	QueueMax int `json:"queue_max"` // most requests due at once with no connection free (§3.5)
	Queued   int `json:"queued"`    // requests that were due while another was still waiting

	Late quantiles `json:"late_ms"` // due → dispatched: the generator's own lateness, its noise floor
	Wait quantiles `json:"wait_ms"` // due → taken by a worker: the queue seen from one request
	TTFB quantiles `json:"ttfb_ms"`
	TTLB quantiles `json:"ttlb_ms"`
}

type quantiles struct {
	P50  float64 `json:"p50"`
	P90  float64 `json:"p90"`
	P99  float64 `json:"p99"`
	P999 float64 `json:"p999"`
	Max  float64 `json:"max"`
}

// Failed says whether the stage is not a measurement: an error of any
// kind, or a connect that failed.
func (r report) Failed() bool { return r.Errors > 0 || r.Connect > 0 }

func (st stage) report(all []result, elapsed time.Duration, queueMax, queued int64) report {
	rep := report{
		URL: st.url, Scenario: st.name, Rate: st.rate, Duration: st.duration.Seconds(),
		Conns: st.conns, Seed: st.seed, Sent: len(all),
		QueueMax: int(queueMax), Queued: int(queued),
	}
	if elapsed > 0 {
		rep.Achieved = float64(len(all)) / elapsed.Seconds()
	}
	var ttfb, ttlb, wait []time.Duration
	for _, r := range all {
		wait = append(wait, r.wait)
		if r.err == nil {
			rep.OK++
			ttfb = append(ttfb, r.ttfb)
			ttlb = append(ttlb, r.ttlb)
			continue
		}
		rep.Errors++
		if r.badKey {
			rep.BadKey++
		}
		if r.connect {
			rep.Connect++
		}
	}
	rep.Wait = summarise(wait)
	rep.TTFB = summarise(ttfb)
	rep.TTLB = summarise(ttlb)
	return rep
}

func summarise(ds []time.Duration) quantiles {
	if len(ds) == 0 {
		return quantiles{}
	}
	slices.Sort(ds)
	at := func(q float64) float64 {
		i := int(q * float64(len(ds)-1))
		return float64(ds[i]) / float64(time.Millisecond)
	}
	return quantiles{P50: at(0.5), P90: at(0.9), P99: at(0.99), P999: at(0.999), Max: at(1)}
}
