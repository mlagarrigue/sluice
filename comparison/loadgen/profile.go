package main

import (
	"fmt"
	"math"
	"time"
)

// The profiles of §3.3 a single run can take. A stage is one rate for
// -duration; a plateau is the same, long, read window by window for drift;
// a peak is the base rate for -duration, -peak-factor times it for
// -peak-for, then the base rate again for -duration (absorption and
// recovery), with C sized on the peak (§3.5).
const (
	profileStage   = "stage"
	profilePlateau = "plateau"
	profilePeak    = "peak"
)

// The traffic shapes of §8.4. An API client is one connection with one
// request in flight under HTTP/1.1, at most 8 under HTTP/2 and HTTP/3. A
// browser opens 6 connections under HTTP/1.1, and one under HTTP/2 and
// HTTP/3 that carries up to 50 requests at once.
const (
	shapeAPI     = "api"
	shapeBrowser = "browser"

	browserConnsH1   = 6
	browserStreamsH2 = 50
)

// layout is the connections a stage opens and the requests each carries at
// once. The budget C is in API connections: the browser shape keeps the
// same requests in flight, grouped as a browser groups them — under
// HTTP/1.1 the same connections, six to a client; under HTTP/2 and HTTP/3,
// 8 C streams on connections of 50, fewer connections each carrying more,
// which is where HTTP/2's head-of-line blocking shows (§8.4).
func (st stage) layout() (conns, perConn int) {
	switch {
	case st.proto == protoH1 && st.shape == shapeBrowser:
		return browserConnsH1 * ceilDiv(st.conns, browserConnsH1), 1
	case st.proto == protoH1:
		return st.conns, 1
	case st.shape == shapeBrowser:
		return ceilDiv(st.conns*streamsPerConn, browserStreamsH2), browserStreamsH2
	}
	return st.conns, streamsPerConn
}

// clients is how many virtual clients the layout makes.
func (st stage) clients() int {
	conns, _ := st.layout()
	if st.proto == protoH1 && st.shape == shapeBrowser {
		return conns / browserConnsH1
	}
	return conns
}

func ceilDiv(a, b int) int { return (a + b - 1) / b }

// phase is a stretch of a stage at one constant rate.
type phase struct {
	name string // warmup, or the profile's: "" for a stage or a plateau, base, peak, back
	rate int
	n    int // requests in it, one per interval
}

func requestsIn(d time.Duration, rate int) int {
	return int(d / (time.Second / time.Duration(rate)))
}

// phases is the stage's schedule: the warm-up at the base rate, never
// counted, then the measured phases.
func (st stage) phases() []phase {
	ps := []phase{{name: "warmup", rate: st.rate, n: requestsIn(st.warmup, st.rate)}}
	if st.profile != profilePeak {
		return append(ps, phase{rate: st.rate, n: requestsIn(st.duration, st.rate)})
	}
	peak := st.peakRate()
	return append(ps,
		phase{name: "base", rate: st.rate, n: requestsIn(st.duration, st.rate)},
		phase{name: "peak", rate: peak, n: requestsIn(st.peakFor, peak)},
		phase{name: "back", rate: st.rate, n: requestsIn(st.duration, st.rate)},
	)
}

func (st stage) peakRate() int {
	if st.profile != profilePeak {
		return 0
	}
	return st.rate * st.peakFactor
}

// measured is how long the measured phases last.
func (st stage) measured() time.Duration {
	if st.profile == profilePeak {
		return 2*st.duration + st.peakFor
	}
	return st.duration
}

// requests is how many requests the stage sends, warm-up included.
func (st stage) requests() int {
	n := 0
	for _, ph := range st.phases() {
		n += ph.n
	}
	return n
}

// warmRequests is how many of them fall before the clock starts.
func (st stage) warmRequests() int { return st.phases()[0].n }

// windows is how many windows the measured phases are cut into, 0 for none.
func (st stage) windows() int {
	if st.window <= 0 {
		return 0
	}
	return int(st.measured() / st.window)
}

// windowOf is the window a request due `since` after the clock falls in.
func (st stage) windowOf(since time.Duration) int {
	if st.window <= 0 {
		return -1
	}
	return min(int(since/st.window), st.windows()-1)
}

// phaseAt is the phase, and its rate, at `since` after the clock.
func (st stage) phaseAt(since time.Duration) (name string, rate int) {
	if st.profile != profilePeak {
		return "", st.rate
	}
	switch {
	case since < st.duration:
		return "base", st.rate
	case since < st.duration+st.peakFor:
		return "peak", st.peakRate()
	}
	return "back", st.rate
}

// wraps says whether the draw may pass over the keyspace again: a plateau
// or a peak lasts minutes, more requests than a table holds, and only reads
// may repeat a key — each pass in an order of its own, so that no two
// passes ask in the same sequence. A write never repeats its key.
func (st stage) wraps() bool {
	return (st.profile == profilePlateau || st.profile == profilePeak) && (st.name == "static" || st.name == "unit" || st.name == "list")
}

// passes is how many times the stage walks the keyspace.
func (st stage) passes() int {
	return int(math.Ceil(float64(st.requests()) / float64(st.keyspace)))
}

// fits refuses a stage that would repeat a key it may not (§2.1): the
// warm-up draws from the same permutation as the measurement.
func (st stage) fits() error {
	if n := st.requests(); n > st.keyspace && !st.wraps() {
		return fmt.Errorf("%d requests at %d req/s for %s (warm-up %s), but the keyspace holds %d keys: a stage never repeats a key (§2.1); only a plateau or a peak of reads passes over it again", n, st.rate, st.measured(), st.warmup, st.keyspace)
	}
	return nil
}
