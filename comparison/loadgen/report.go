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
	Proto    string  `json:"proto"`
	Scenario string  `json:"scenario"`
	Rate     int     `json:"rate"`
	Duration float64 `json:"duration_s"`
	Conns    int     `json:"conns"`
	Seed     uint64  `json:"seed"`
	SLO      float64 `json:"slo_ms"` // X: the p99 to hold (§3.2)
	RTT      float64 `json:"rtt_ms"` // the round trip injected, what C was computed with (§3.5)
	NewConns int     `json:"new_conns_pct"`

	Held  bool   `json:"held"`  // the SLO held and the stage is a measurement
	Cause string `json:"cause"` // why not, by precedence: key, connect, queue, slo

	Sent     int     `json:"sent"`
	OK       int     `json:"ok"`
	Achieved float64 `json:"achieved_rps"` // sent over the time the stage took, drain included

	// What became of the requests not ok (§1.2), one nature each:
	// sent = ok + bad_key + timed_out + errors + tester.
	BadKey         int     `json:"bad_key"`     // the response contract broken (§4): one is a failed stage
	TimedOut       int     `json:"timed_out"`   // no complete answer by deadline_ms after the due instant
	Deadline       float64 `json:"deadline_ms"` // 10 X + 4 RTT (§1.2)
	Errors         int     `json:"errors"`      // the contender's errors, the sum of the six below
	Status4xx      int     `json:"status_4xx"`
	Status5xx      int     `json:"status_5xx"`
	Refused        int     `json:"refused"`         // ECONNREFUSED
	Reset          int     `json:"reset"`           // the connection ended under a request sent: ECONNRESET, EOF, a stream reset
	ConnectTimeout int     `json:"connect_timeout"` // the deadline passed while the connection was opening
	Other          int     `json:"other"`           // a failed handshake, a status out of 200, 4xx and 5xx, anything else
	Tester         int     `json:"tester"`          // the tester could not open a connection, no port or descriptor (§4): one is an invalid stage

	// What the server did to the budget's connections (§1.2): a closure
	// past zero means the stage no longer holds "0 % fresh connections"
	// (§3.6).
	Closed     int `json:"closed"`     // connections of the budget the server ended: Connection: close, GOAWAY, FIN or RST
	Reconnects int `json:"reconnects"` // connections of the budget opened again: after a closure, or a request given up on under HTTP/1.1

	QueueMax   int `json:"queue_max"`  // most requests due at once with no connection free (§3.5)
	Queued     int `json:"queued"`     // requests that were due with no connection free
	Handshakes int `json:"handshakes"` // TLS handshakes inside the measurement: the fresh connections (§3.6), the reconnections

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

// Failed says whether the stage is not a measurement: a request that is
// not ok, whatever became of it.
func (r report) Failed() bool { return r.OK < r.Sent }

// The causes of a stage not held, by precedence. A wrong key is a bug of
// the server, which fails the stage with no extrapolation (§4); a
// connection the tester could not open or a queue under X is the
// apparatus limiting, which invalidates it (§3.5, §4); only then is the
// SLO the verdict. A refusal, a reset or a timeout is the contender's,
// and counts against the SLO (§4). The queue counts
// only under X: over X, the wait in the queue is the server's latency
// seen from the due instant, and C = 2 R (RTT + X) makes a queue form
// only once the server is past 2X anyway.
const (
	causeKey    = "key"
	causeTester = "tester"
	causeQueue  = "queue"
	causeSLO    = "slo"
)

// verdict applies the SLO of §3.2: p99 ≤ X and errors and timeouts under
// 0.1 % of what was sent, with the guards above.
func (r *report) verdict() {
	underX := r.TTLB.P99 <= r.SLO
	switch {
	case r.BadKey > 0:
		r.Cause = causeKey
	case r.Tester > 0:
		r.Cause = causeTester
	case r.Queued > 0 && underX:
		r.Cause = causeQueue
	case !underX || r.Sent == 0 || float64(r.Errors+r.TimedOut)*1000 >= float64(r.Sent):
		r.Cause = causeSLO
	default:
		r.Held = true
	}
}

func (st stage) report(all []result, elapsed time.Duration, queueMax, queued int64) report {
	rep := report{
		URL: st.url, Proto: st.proto, Scenario: st.name, Rate: st.rate, Duration: st.duration.Seconds(),
		Conns: st.conns, Seed: st.seed, Sent: len(all),
		SLO: ms(st.slo), RTT: ms(st.rtt), Deadline: ms(st.deadline()), NewConns: st.newConns,
		QueueMax: int(queueMax), Queued: int(queued),
	}
	if elapsed > 0 {
		rep.Achieved = float64(len(all)) / elapsed.Seconds()
	}
	var ttfb, ttlb, wait []time.Duration
	for _, r := range all {
		wait = append(wait, r.wait)
		if r.handshake {
			rep.Handshakes++
		}
		switch r.nature {
		case natureOK:
			rep.OK++
			ttfb = append(ttfb, r.ttfb)
			ttlb = append(ttlb, r.ttlb)
		case natureBadKey:
			rep.BadKey++
		case natureTimedOut:
			rep.TimedOut++
		case natureTester:
			rep.Tester++
		case nature4xx:
			rep.Status4xx++
		case nature5xx:
			rep.Status5xx++
		case natureRefused:
			rep.Refused++
		case natureReset:
			rep.Reset++
		case natureConnectTimeout:
			rep.ConnectTimeout++
		case natureOther:
			rep.Other++
		}
	}
	rep.Errors = rep.Status4xx + rep.Status5xx + rep.Refused + rep.Reset + rep.ConnectTimeout + rep.Other
	rep.Wait = summarise(wait)
	rep.TTFB = summarise(ttfb)
	rep.TTLB = summarise(ttlb)
	rep.verdict()
	return rep
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

func summarise(ds []time.Duration) quantiles {
	if len(ds) == 0 {
		return quantiles{}
	}
	slices.Sort(ds)
	at := func(q float64) float64 {
		i := int(q * float64(len(ds)-1))
		return ms(ds[i])
	}
	return quantiles{P50: at(0.5), P90: at(0.9), P99: at(0.99), P999: at(0.999), Max: at(1)}
}
