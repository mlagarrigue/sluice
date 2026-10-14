// Command loadgen is the study's own load generator: a constant-rate client
// that measures every request from the instant it was due, not the instant
// it left (the correction of the coordinated omission, §3.1 of the study),
// checks the key in every answer (§2.1, §4) and publishes TTFB and TTLB
// apart, one JSON line per stage.
//
// Today it speaks HTTP/1.1 over TLS and runs one stage; HTTP/2 and HTTP/3,
// the geometric stages, the drain between them and the connection budget
// recomputed per stage come next. Nothing it prints is a result of the
// study: it is the apparatus.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"
)

func main() {
	st, err := parseStage(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	rep, err := st.run(ctx)
	stop()
	if err == nil {
		err = json.NewEncoder(os.Stdout).Encode(rep)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if rep.Failed() {
		os.Exit(1)
	}
}

// stage is one run at one rate: what the flags say, and what a campaign
// will vary from stage to stage.
type stage struct {
	url      string
	rate     int // requests per second, at a constant interval
	duration time.Duration
	conns    int // the connection budget C of §3.5
	name     string
	scenario scenario
	keyspace int
	seed     uint64
	timeout  time.Duration
	ca       string
}

func parseStage(args []string) (stage, error) {
	fs := flag.NewFlagSet("loadgen", flag.ContinueOnError)
	var st stage
	var name string
	fs.StringVar(&st.url, "url", "https://127.0.0.1:8443", "the server under test, scheme and host")
	fs.IntVar(&st.rate, "rate", 100, "requests per second, sent at a constant interval")
	fs.DurationVar(&st.duration, "duration", 10*time.Second, "length of the stage")
	fs.IntVar(&st.conns, "conns", 64, "connections kept open: the budget C, one request in flight each under HTTP/1.1")
	fs.StringVar(&name, "scenario", "unit", "static, unit, list, write or mixed (80 % unit, 15 % list, 5 % write)")
	fs.IntVar(&st.keyspace, "keyspace", 100000, "ids the table holds, 1..keyspace; every request draws a different one")
	fs.Uint64Var(&st.seed, "seed", 1, "seed of the draw, the same for every contender of a round (§2.1)")
	fs.DurationVar(&st.timeout, "timeout", 30*time.Second, "give up on a request after this long")
	fs.StringVar(&st.ca, "ca", "", "PEM file of the CA that signed the server's certificate; empty trusts any certificate")
	if err := fs.Parse(args); err != nil {
		return st, err
	}
	sc, ok := scenarios[name]
	if !ok {
		return st, fmt.Errorf("-scenario %q: want static, unit, list, write or mixed", name)
	}
	st.name, st.scenario = name, sc
	switch {
	case st.rate <= 0:
		return st, errors.New("-rate must be positive")
	case st.duration <= 0:
		return st, errors.New("-duration must be positive")
	case st.conns <= 0:
		return st, errors.New("-conns must be positive")
	case st.keyspace <= listLimit:
		return st, fmt.Errorf("-keyspace must exceed the list limit %d", listLimit)
	}
	if n := st.requests(); n > st.keyspace {
		return st, fmt.Errorf("%d requests at %d req/s for %s, but the keyspace holds %d keys: a stage never repeats a key (§2.1)", n, st.rate, st.duration, st.keyspace)
	}
	return st, nil
}

// requests is how many requests the stage sends: one per interval.
func (st stage) requests() int {
	return int(st.duration / st.interval())
}

func (st stage) interval() time.Duration {
	return time.Second / time.Duration(st.rate)
}
