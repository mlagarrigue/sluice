// Command loadgen is the study's own load generator: a constant-rate client
// that measures every request from the instant it was due, not the instant
// it left (the correction of the coordinated omission, §3.1 of the study),
// checks the key in every answer (§2.1, §4) and publishes TTFB and TTLB
// apart, one JSON line per stage.
//
// It speaks HTTP/1.1, HTTP/2 or HTTP/3 over TLS, one connection per virtual
// client and at most 8 streams on it (§3.5), and runs one stage or a ramp of them: stages
// on the study's geometric grid, a drain between two, the connection budget
// recomputed per stage and opened before the clock starts, a share of fresh
// connections, and the stop at the first stage that breaks, with its cause.
// Nothing it prints is a result of the study:
// it is the apparatus.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime/pprof"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func main() { os.Exit(run()) }

// run is main with an exit status: 2 for a bad flag, 1 for a stage that is
// not a measurement or an error, 0 otherwise. Deferred work (the CPU
// profile) runs before the process exits, which os.Exit in main would skip.
func run() int {
	c, err := parse(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if c.cpuprofile != "" {
		f, err := os.Create(c.cpuprofile)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 2
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 2
		}
		defer func() { pprof.StopCPUProfile(); _ = f.Close() }()
	}
	if c.trivial != "" {
		if err := runTrivial(ctx, c.trivial, c.keyspace, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		return 0
	}
	ok, err := c.run(ctx, os.Stdout)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if !ok {
		return 1
	}
	return 0
}

// stage is one run at one rate: what the flags say, and what a campaign
// varies from stage to stage (rate and budget).
type stage struct {
	url      string
	proto    string // h1, h2 or h3
	rate     int    // requests per second, at a constant interval
	duration time.Duration
	warmup   time.Duration // sent at the rate before the clock starts, never counted
	conns    int           // the connection budget C of §3.5
	name     string
	scenario scenario
	keyspace int
	seed     uint64
	ca       string
	slo      time.Duration // X: the p99 the scenario must hold (§3.2)
	rtt      time.Duration // the RTT injected on the wire, for C only (§3.5)
	newConns int           // percent of requests that open a fresh connection (§3.6)
	encoding string        // the content coding asked for, "" for identity (§2.3 bis)
}

// campaign is what the command runs: one stage, or a ramp of stages that
// stops at the first broken one.
type campaign struct {
	stage
	ramp     bool
	rates    []int
	drain    time.Duration
	maxConns int // the tester's ceiling on C, 0 for none

	trivial    string // run the trivial server of §4 on this address instead
	cpuprofile string // write the generator's CPU profile here
}

// defaultSLO is X by scenario (§3.2, orders of magnitude before the local
// survey confirms them); a write holds the unit read's.
var defaultSLO = map[string]time.Duration{
	"static": 10 * time.Millisecond,
	"unit":   25 * time.Millisecond,
	"list":   100 * time.Millisecond,
	"write":  25 * time.Millisecond,
	"mixed":  50 * time.Millisecond,
}

func parse(args []string) (campaign, error) {
	fs := flag.NewFlagSet("loadgen", flag.ContinueOnError)
	var c campaign
	st := &c.stage
	var name, rates string
	fs.StringVar(&st.url, "url", "https://127.0.0.1:8443", "the server under test, scheme and host")
	fs.StringVar(&st.proto, "proto", protoH1, "h1, h2 or h3: HTTP/1.1, HTTP/2 or HTTP/3, one connection per virtual client, at most 8 streams on it")
	fs.IntVar(&st.rate, "rate", 100, "requests per second, sent at a constant interval; with -ramp, where the grid starts")
	fs.DurationVar(&st.duration, "duration", 10*time.Second, "length of the measurement of a stage")
	fs.DurationVar(&st.warmup, "warmup", 5*time.Second, "sent at the stage's rate before its clock starts, never counted")
	fs.IntVar(&st.conns, "conns", 64, "connections kept open: the budget C, one request in flight each under HTTP/1.1, up to 8 under HTTP/2 and HTTP/3 (a single stage; a ramp computes it)")
	fs.StringVar(&name, "scenario", "unit", "static, unit, list, write or mixed (80 % unit, 15 % list, 5 % write)")
	fs.IntVar(&st.keyspace, "keyspace", 100000, "ids the table holds, 1..keyspace; every request draws a different one")
	fs.Uint64Var(&st.seed, "seed", 1, "seed of the draw, the same for every contender of a round (§2.1)")
	fs.StringVar(&st.ca, "ca", "", "PEM file of the CA that signed the server's certificate; empty trusts any certificate")
	fs.DurationVar(&st.slo, "slo", 0, "X: the p99 to hold; 0 takes the scenario's (static 10ms, unit and write 25ms, list 100ms, mixed 50ms). A request unanswered 10 X + 4 RTT after it was due timed out")
	fs.DurationVar(&st.rtt, "rtt", 0, "the round trip injected on the wire, for the budget C = 2 R (RTT + X) of a ramp and the deadline 10 X + 4 RTT of a request")
	fs.StringVar(&st.encoding, "encoding", "identity", "the content coding asked for in Accept-Encoding: identity, gzip or deflate (br and zstd once their decoders are in go.mod)")
	fs.IntVar(&st.newConns, "new-conns", 0, "percent of requests that open a fresh connection and close it, 0 or 10 (§3.6)")
	fs.BoolVar(&c.ramp, "ramp", false, "run the stages of the grid 100 × 1.05^k from -rate up, until one breaks")
	fs.StringVar(&rates, "rates", "", "run these stages instead of the grid, comma-separated req/s, until one breaks")
	fs.DurationVar(&c.drain, "drain", 5*time.Second, "pause between two stages, once every request of the first has been answered")
	fs.IntVar(&c.maxConns, "max-conns", 0, "the tester's ceiling on the budget C of a ramp; 0 for none")
	fs.StringVar(&c.trivial, "trivial", "", "serve the trivial server of §4 on this address instead (h1, h2 and h3 at once, -keyspace rows), to prove the generator is not the bottleneck")
	fs.StringVar(&c.cpuprofile, "cpuprofile", "", "write the generator's CPU profile to this file, or the trivial server's under -trivial (written when it is stopped)")
	if err := fs.Parse(args); err != nil {
		return c, err
	}
	if c.trivial != "" {
		if st.keyspace <= listLimit {
			return c, fmt.Errorf("-keyspace must exceed the list limit %d", listLimit)
		}
		return c, nil
	}
	sc, ok := scenarios[name]
	if !ok {
		return c, fmt.Errorf("-scenario %q: want static, unit, list, write or mixed", name)
	}
	st.name, st.scenario = name, sc
	if st.proto != protoH1 && st.proto != protoH2 && st.proto != protoH3 {
		return c, fmt.Errorf("-proto %q: want h1, h2 or h3", st.proto)
	}
	if st.slo == 0 {
		st.slo = defaultSLO[name]
	}
	switch st.encoding {
	case "identity":
		st.encoding = ""
	case codingGzip, codingDeflate:
	case "br", "zstd":
		return c, fmt.Errorf("-encoding %s: its decoder is not in loadgen/go.mod yet; identity, gzip or deflate", st.encoding)
	default:
		return c, fmt.Errorf("-encoding %q: want identity, gzip or deflate", st.encoding)
	}
	if rates != "" {
		for part := range strings.SplitSeq(rates, ",") {
			r, err := strconv.Atoi(strings.TrimSpace(part))
			if err != nil || r <= 0 {
				return c, fmt.Errorf("-rates %q: want positive req/s, comma-separated", rates)
			}
			c.rates = append(c.rates, r)
		}
	}
	switch {
	case st.rate <= 0:
		return c, errors.New("-rate must be positive")
	case st.duration <= 0:
		return c, errors.New("-duration must be positive")
	case st.warmup < 0 || c.drain < 0:
		return c, errors.New("-warmup and -drain cannot be negative")
	case st.conns <= 0:
		return c, errors.New("-conns must be positive")
	case c.maxConns < 0:
		return c, errors.New("-max-conns cannot be negative")
	case st.keyspace <= listLimit:
		return c, fmt.Errorf("-keyspace must exceed the list limit %d", listLimit)
	case st.slo <= 0 || st.rtt < 0:
		return c, errors.New("-slo must be positive and -rtt cannot be negative")
	case st.newConns < 0 || st.newConns > 100:
		return c, errors.New("-new-conns is a percentage")
	case c.ramp && len(c.rates) > 0:
		return c, errors.New("-ramp and -rates exclude each other")
	}
	if !c.ramp && len(c.rates) == 0 {
		if err := st.fits(); err != nil {
			return c, err
		}
	}
	return c, nil
}

// fits refuses a stage that would repeat a key (§2.1): the warm-up draws
// from the same permutation as the measurement.
func (st stage) fits() error {
	if n := st.requests(); n > st.keyspace {
		return fmt.Errorf("%d requests at %d req/s for %s (warm-up %s), but the keyspace holds %d keys: a stage never repeats a key (§2.1)", n, st.rate, st.duration, st.warmup, st.keyspace)
	}
	return nil
}

// requests is how many requests the stage sends, warm-up included: one per
// interval.
func (st stage) requests() int {
	return int((st.warmup + st.duration) / st.interval())
}

// warmRequests is how many of them fall before the clock starts.
func (st stage) warmRequests() int {
	return int(st.warmup / st.interval())
}

// deadline is how long a request has, from its due instant, to be answered
// to the end before it counts as timed out (§1.2): ten times X, and four
// round trips for a fresh connection's TCP and TLS 1.3 handshakes, the
// request and a margin.
func (st stage) deadline() time.Duration {
	return 10*st.slo + 4*st.rtt
}

func (st stage) interval() time.Duration {
	return time.Second / time.Duration(st.rate)
}
