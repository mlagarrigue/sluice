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
	"encoding/json"
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
	if c.reference != "" {
		rep, err := conformance{stage: c.stage, reference: c.reference, n: c.checks}.run(ctx)
		if err == nil {
			err = json.NewEncoder(os.Stdout).Encode(rep)
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		if !rep.Conformant {
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
	resume   bool          // keep a client session cache, so fresh connections resume (§4 "TLS fixed")

	profile    string        // stage, plateau or peak (§3.3)
	window     time.Duration // report the measurement window by window, 0 for one line
	peakFactor int           // the peak's rate over the base rate
	peakFor    time.Duration // how long the peak lasts
	shape      string        // api or browser (§8.4)
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
	reference  string // check -url against this server instead of loading it (§4 "same answer")
	checks     int    // how many requests the check sends
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
	fs.StringVar(&st.url, "url", "https://127.0.0.1:8443", "the server under test, scheme and host; http:// is HTTP/1.1 in clear (TLS off, level 2)")
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
	fs.StringVar(&st.profile, "profile", profileStage, "stage (one rate for -duration), plateau (the same, long, read by -window) or peak (-duration at -rate, -peak-for at -peak-factor times it, -duration at -rate again)")
	fs.DurationVar(&st.window, "window", 0, "also write one line per window of this length; a plateau or a peak defaults to 10s")
	fs.IntVar(&st.peakFactor, "peak-factor", 5, "the peak's rate over -rate, under -profile peak")
	fs.DurationVar(&st.peakFor, "peak-for", 30*time.Second, "how long the peak lasts, under -profile peak")
	fs.StringVar(&st.shape, "shape", shapeAPI, "api (one connection per virtual client, at most 8 streams on it) or browser (6 connections per client under HTTP/1.1, one carrying up to 50 under HTTP/2 and HTTP/3)")
	fs.BoolVar(&st.resume, "tls-resume", false, "keep a TLS session cache: fresh connections resume a session (the study's R does not)")
	fs.IntVar(&st.newConns, "new-conns", 0, "percent of requests that open a fresh connection and close it, 0 or 10 (§3.6)")
	fs.BoolVar(&c.ramp, "ramp", false, "run the stages of the grid 100 × 1.05^k from -rate up, until one breaks")
	fs.StringVar(&rates, "rates", "", "run these stages instead of the grid, comma-separated req/s, until one breaks")
	fs.DurationVar(&c.drain, "drain", 5*time.Second, "pause between two stages, once every request of the first has been answered")
	fs.IntVar(&c.maxConns, "max-conns", 0, "the tester's ceiling on the budget C of a ramp; 0 for none")
	fs.StringVar(&c.trivial, "trivial", "", "serve the trivial server of §4 on this address instead (h1, h2 and h3 at once, -keyspace rows), to prove the generator is not the bottleneck")
	fs.StringVar(&c.reference, "conformance", "", "check -url against the server at this URL instead of loading it: the same requests to both, the answers compared after parsing (§4 \"same answer\"); exit status 1 at the first difference")
	fs.IntVar(&c.checks, "checks", 200, "how many requests -conformance sends to each server")
	fs.StringVar(&c.cpuprofile, "cpuprofile", "", "write the generator's CPU profile to this file, or the trivial server's under -trivial (written when it is stopped)")
	if err := fs.Parse(args); err != nil {
		return c, err
	}
	connsSet := false
	fs.Visit(func(f *flag.Flag) { connsSet = connsSet || f.Name == "conns" })
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
	// In clear, HTTP/1.1 only: h2c is no configuration of the study, as the
	// server refuses it too, and QUIC is always TLS.
	switch {
	case strings.HasPrefix(st.url, "http://") && st.proto != protoH1:
		return c, fmt.Errorf("-url %s: in clear, -proto h1 only (h2c is not a configuration of the study, QUIC is always TLS)", st.url)
	case !strings.HasPrefix(st.url, "http://") && !strings.HasPrefix(st.url, "https://"):
		return c, fmt.Errorf("-url %s: want http:// or https://", st.url)
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
	case c.reference != "" && (c.checks <= 0 || c.checks > st.keyspace):
		return c, errors.New("-checks must be positive and at most -keyspace")
	case st.shape != shapeAPI && st.shape != shapeBrowser:
		return c, fmt.Errorf("-shape %q: want api or browser", st.shape)
	case st.profile != profileStage && st.profile != profilePlateau && st.profile != profilePeak:
		return c, fmt.Errorf("-profile %q: want stage, plateau or peak", st.profile)
	case st.profile != profileStage && (c.ramp || len(c.rates) > 0):
		return c, errors.New("-profile plateau and peak are one run at one rate: no -ramp, no -rates")
	case st.window < 0 || st.window > 0 && (c.ramp || len(c.rates) > 0):
		return c, errors.New("-window cuts one run: no -ramp, no -rates")
	case st.profile == profilePeak && (st.peakFactor < 2 || st.peakFor <= 0):
		return c, errors.New("-peak-factor must be 2 or more and -peak-for positive")
	}
	if st.profile != profileStage && st.window == 0 {
		st.window = 10 * time.Second
	}
	if st.window > 0 && (st.duration%st.window != 0 || st.profile == profilePeak && st.peakFor%st.window != 0) {
		return c, fmt.Errorf("-window %s must divide -duration (and -peak-for under a peak): no window straddles two phases", st.window)
	}
	if st.profile == profilePeak && !connsSet {
		// C is sized on the peak (§3.5): otherwise the generator's
		// connections limit during it, not the server.
		st.conns = c.budget(st.peakRate())
	}
	if !c.ramp && len(c.rates) == 0 {
		if err := st.fits(); err != nil {
			return c, err
		}
	}
	return c, nil
}

// deadline is how long a request has, from its due instant, to be answered
// to the end before it counts as timed out (§1.2): ten times X, and four
// round trips for a fresh connection's TCP and TLS 1.3 handshakes, the
// request and a margin.
func (st stage) deadline() time.Duration {
	return 10*st.slo + 4*st.rtt
}
