package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

// sleepServer answers every path after exactly `d` — a sleep that wakes
// early, then a spin to the instant, since time.Sleep alone overshoots by
// ~0.2 ms here and the calibration wants the reference exact — with a document that
// honours the response contract for the key the path carries, so that the
// generator's checks all pass and only the delay is under test. A key
// listed in `wrong` is answered with another key: the contract broken on
// purpose.
func sleepServer(tb testing.TB, d time.Duration, wrong map[int64]bool) *httptest.Server {
	tb.Helper()
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sleepExactly(d)
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/static":
			fmt.Fprint(w, `{"status":"ok"}`)
		case strings.HasPrefix(r.URL.Path, "/orders/"):
			var id int64
			id, _ = strconv.ParseInt(strings.TrimPrefix(r.URL.Path, "/orders/"), 10, 64)
			if wrong[id] {
				id++
			}
			fmt.Fprintf(w, `{"id":%d,"total":%d,"label":"order-%d"}`, id, id*7, id)
		default:
			http.Error(w, "no such route", http.StatusNotFound)
		}
	}))
	srv.TLS = &tls.Config{MinVersion: tls.VersionTLS13, NextProtos: []string{"http/1.1"}}
	srv.StartTLS()
	tb.Cleanup(srv.Close)
	return srv
}

func sleepExactly(d time.Duration) {
	until := time.Now().Add(d)
	if d > time.Millisecond {
		time.Sleep(d - time.Millisecond)
	}
	for time.Now().Before(until) { //nolint:revive // the spin is the point
	}
}

func testStage(tb testing.TB, url string, args ...string) stage {
	tb.Helper()
	st, err := parseStage(append([]string{"-url", url, "-keyspace", "10000"}, args...))
	if err != nil {
		tb.Fatal(err)
	}
	return st
}

// TestCalibration is the calibration the study asks of its generator: against
// a server that sleeps exactly 5 ms, p50 must read 5 ms + RTT. The RTT here
// is not a wire's: it is what the loopback, TLS and two net/http stacks in
// one process cost, so it is measured first against the same server
// sleeping 0 ms, and the 5 ms server must sit exactly 5 ms above it. The
// absolute p50 is held under 5.5 ms as well, with half a millisecond more
// under the race detector, which slows both ends (measured on a developer's
// machine: floor 0.4 ms, 0.65 ms under -race).
//
// p99 and p99.9 are read too and printed, not asserted: they depend on the
// machine. Here they sit 0.4 to 1 ms above p50 — the scheduler's and the
// kernel timers' slack on both sides, and the GC's pauses, which land in
// the tail and not in the median. That inflation (p99.9 − p50) is the
// generator's noise floor; a campaign publishes it (§7.4).
func TestCalibration(t *testing.T) {
	run := func(d time.Duration) report {
		srv := sleepServer(t, d, nil)
		st := testStage(t, srv.URL, "-scenario", "unit", "-rate", "200", "-duration", "3s", "-conns", "16")
		rep, err := st.run(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("server sleeping %v: sent %d ok %d queue_max %d queued %d", d, rep.Sent, rep.OK, rep.QueueMax, rep.Queued)
		t.Logf("  late p50 %.3f p99 %.3f max %.3f ms", rep.Late.P50, rep.Late.P99, rep.Late.Max)
		t.Logf("  ttfb p50 %.3f p99 %.3f p999 %.3f max %.3f ms", rep.TTFB.P50, rep.TTFB.P99, rep.TTFB.P999, rep.TTFB.Max)
		t.Logf("  ttlb p50 %.3f p99 %.3f p999 %.3f max %.3f ms", rep.TTLB.P50, rep.TTLB.P99, rep.TTLB.P999, rep.TTLB.Max)
		if rep.Failed() || rep.OK != rep.Sent {
			t.Fatalf("errors %d bad_key %d connect %d", rep.Errors, rep.BadKey, rep.Connect)
		}
		if want := st.requests(); rep.Sent != want {
			t.Fatalf("sent %d, want %d", rep.Sent, want)
		}
		// 16 connections at 5 ms carry 3 200 req/s: at 200 req/s the queue
		// stays empty, and a non-empty one here would be the generator's
		// own fault (§3.5).
		if rep.QueueMax > 1 || rep.Queued > 0 {
			t.Fatalf("queue_max %d queued %d at a rate the budget carries", rep.QueueMax, rep.Queued)
		}
		return rep
	}
	floor := run(0)
	rep := run(5 * time.Millisecond)
	if floor.TTLB.P50 > 1 {
		t.Fatalf("floor p50 %.3f ms: the apparatus alone costs more than a millisecond", floor.TTLB.P50)
	}
	if above := rep.TTLB.P50 - floor.TTLB.P50; above < 4.95 || above > 5.3 {
		t.Fatalf("TTLB p50 %.3f ms sits %.3f ms above the floor %.3f, want 5 ms", rep.TTLB.P50, above, floor.TTLB.P50)
	}
	if rep.TTLB.P50 < 5.0 || rep.TTLB.P50 > 5.5+raceSlack {
		t.Fatalf("TTLB p50 %.3f ms, want 5 ms + loopback RTT, within [5.0, %.1f]", rep.TTLB.P50, 5.5+raceSlack)
	}
	if rep.TTFB.P50 < 5.0 || rep.TTFB.P50 > rep.TTLB.P50 {
		t.Fatalf("TTFB p50 %.3f ms, TTLB p50 %.3f ms", rep.TTFB.P50, rep.TTLB.P50)
	}
}

// TestOverloadedStageQueues is the correction under load: 4 connections at
// 5 ms carry 800 req/s, and the stage asks for 2 000. Measured from the due
// instant, the delays must climb with the queue (coordinated omission
// corrected); measured from the send instant, as a closed loop would, they
// would read 5 ms throughout and hide the stall.
func TestOverloadedStageQueues(t *testing.T) {
	srv := sleepServer(t, 5*time.Millisecond, nil)
	st := testStage(t, srv.URL, "-scenario", "unit", "-rate", "2000", "-duration", "2s", "-conns", "4")
	rep, err := st.run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("sent %d ok %d queue_max %d queued %d achieved %.0f req/s", rep.Sent, rep.OK, rep.QueueMax, rep.Queued, rep.Achieved)
	t.Logf("wait p50 %.1f ttlb p50 %.1f p99 %.1f max %.1f ms", rep.Wait.P50, rep.TTLB.P50, rep.TTLB.P99, rep.TTLB.Max)
	if rep.Failed() {
		t.Fatalf("errors %d bad_key %d connect %d", rep.Errors, rep.BadKey, rep.Connect)
	}
	// 2 s at 2 000 req/s with 800 served: ~2 400 due and waiting at the end.
	if rep.QueueMax < 1000 || rep.Queued < rep.Sent/2 {
		t.Fatalf("queue_max %d queued %d: the queue did not climb", rep.QueueMax, rep.Queued)
	}
	// The last request was due at 2 s and served ~3 s later.
	if rep.TTLB.P50 < 500 || rep.TTLB.Max < 2000 {
		t.Fatalf("ttlb p50 %.1f max %.1f ms: the wait in the queue is not counted", rep.TTLB.P50, rep.TTLB.Max)
	}
	if rep.Achieved > 900 {
		t.Fatalf("achieved %.0f req/s with 4 connections at 5 ms", rep.Achieved)
	}
}

// TestWrongKeyFailsTheStage: one answer carrying another key than the one
// asked for is counted, and the stage is failed, with no extrapolation (§4).
func TestWrongKeyFailsTheStage(t *testing.T) {
	st := testStage(t, "https://unused", "-scenario", "unit", "-rate", "100", "-duration", "1s", "-conns", "4")
	draw := st.scenario.draw(st.seed, st.keyspace)
	wrong := map[int64]bool{draw(7).key: true, draw(42).key: true}
	srv := sleepServer(t, 0, wrong)
	st.url = srv.URL
	rep, err := st.run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !rep.Failed() || rep.BadKey != 2 || rep.Errors != 2 || rep.OK != rep.Sent-2 {
		t.Fatalf("bad_key %d errors %d ok %d sent %d, want 2 bad keys failing the stage", rep.BadKey, rep.Errors, rep.OK, rep.Sent)
	}
}

func TestDrawNeverRepeatsAndMixes(t *testing.T) {
	st := testStage(t, "https://unused", "-scenario", "mixed", "-rate", "1000", "-duration", "10s")
	draw := st.scenario.draw(st.seed, st.keyspace)
	seen := make(map[int64]bool)
	kinds := make(map[kind]int)
	full := 0
	n := st.requests()
	for i := range n {
		rq := draw(i)
		if seen[rq.key] {
			t.Fatalf("key %d drawn twice", rq.key)
		}
		seen[rq.key] = true
		kinds[rq.kind]++
		if rq.full {
			full++
		}
	}
	if kinds[unit] != n*80/100 || kinds[list] != n*15/100 || kinds[write] != n*5/100 {
		t.Fatalf("mix %v, want 80/15/5 of %d", kinds, n)
	}
	if full < n/200 || full > n/50 {
		t.Fatalf("%d of %d fully validated, want about 1 %%", full, n)
	}
	again := st.scenario.draw(st.seed, st.keyspace)
	for i := range n {
		if a, b := draw(i), again(i); a.key != b.key || a.full != b.full {
			t.Fatalf("request %d differs between two draws of the same seed", i)
		}
	}
	if _, err := parseStage([]string{"-rate", "1000", "-duration", "200s", "-keyspace", "100000"}); err == nil {
		t.Fatal("a stage longer than its keyspace was accepted")
	}
}

func TestCheck(t *testing.T) {
	cases := []struct {
		name string
		rq   request
		body string
		ok   bool
	}{
		{"unit ok", unitRequest(0, 42, false), `{"id":42,"total":1,"label":"x"}`, true},
		{"unit other key", unitRequest(0, 42, false), `{"id":43,"total":1,"label":"x"}`, false},
		{"unit truncated", unitRequest(0, 42, false), `{"id":42,"tot`, false},
		{"unit full ok", unitRequest(0, 42, true), `{"label":"order-42","total":294,"id":42}`, true},
		{"unit full wrong total", unitRequest(0, 42, true), `{"id":42,"total":293,"label":"order-42"}`, false},
		{"list ok", listRequest(0, 10, false), `{"from":10,"limit":1000,"items":[{"id":11},{"id":12}]}`, true},
		{"list not increasing", listRequest(0, 10, false), `{"from":10,"items":[{"id":11},{"id":11}]}`, false},
		{"list at bound", listRequest(0, 10, false), `{"from":10,"items":[{"id":10}]}`, false},
		{"list empty", listRequest(0, 10, false), `{"from":10,"items":[]}`, false},
		{"list full ok", listRequest(0, 10, true), `{"from":10,"items":[{"id":11,"total":77,"label":"order-11"}]}`, true},
		{"list full wrong row", listRequest(0, 10, true), `{"from":10,"items":[{"id":11,"total":77,"label":"order-12"}]}`, false},
		{"write ok", writeRequest(0, 5, true), `{"key":5}`, true},
		{"write other key", writeRequest(0, 5, false), `{"key":6}`, false},
		{"static ok", scenarios["static"](0, 0, false), `{"status":"ok"}`, true},
		{"static not json", scenarios["static"](0, 0, false), `<html>`, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := check(c.rq, []byte(c.body))
			if (err == nil) != c.ok {
				t.Fatalf("check: %v, want ok=%v", err, c.ok)
			}
		})
	}
}
