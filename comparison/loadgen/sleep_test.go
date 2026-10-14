package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
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

// testCampaign parses the flags of a test: no warm-up and no drain unless
// the test asks for them, since a test runs its stages back to back.
func testCampaign(tb testing.TB, url string, args ...string) campaign {
	tb.Helper()
	c, err := parse(append([]string{"-url", url, "-keyspace", "10000", "-warmup", "0", "-drain", "0"}, args...))
	if err != nil {
		tb.Fatal(err)
	}
	return c
}

// runStage runs a single stage with a client of its own.
func runStage(tb testing.TB, c campaign) report {
	tb.Helper()
	cl, err := c.newClient()
	if err != nil {
		tb.Fatal(err)
	}
	defer cl.close()
	rep, err := c.stage.run(context.Background(), cl)
	if err != nil {
		tb.Fatal(err)
	}
	return rep
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
		c := testCampaign(t, srv.URL, "-scenario", "unit", "-rate", "200", "-duration", "3s", "-conns", "16")
		st := c.stage
		rep := runStage(t, c)
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
		if rep.QueueMax > 0 || rep.Queued > 0 {
			t.Fatalf("queue_max %d queued %d at a rate the budget carries", rep.QueueMax, rep.Queued)
		}
		if !rep.Held || rep.Cause != "" || rep.Handshakes != 0 {
			t.Fatalf("held %v cause %q handshakes %d under X = 25 ms with no fresh connection", rep.Held, rep.Cause, rep.Handshakes)
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
	c := testCampaign(t, srv.URL, "-scenario", "unit", "-rate", "2000", "-duration", "2s", "-conns", "4")
	rep := runStage(t, c)
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
	// p99 is far over X = 25 ms: the stage is broken by the SLO, and the
	// queue, however deep, is not the cause published (over X the wait in
	// the queue is latency, §3.1).
	if rep.Held || rep.Cause != causeSLO {
		t.Fatalf("held %v cause %q, want the SLO broken", rep.Held, rep.Cause)
	}
}

// TestWrongKeyFailsTheStage: one answer carrying another key than the one
// asked for is counted, and the stage is failed, with no extrapolation (§4).
func TestWrongKeyFailsTheStage(t *testing.T) {
	c := testCampaign(t, "https://unused", "-scenario", "unit", "-rate", "100", "-duration", "1s", "-conns", "4")
	draw := c.scenario.draw(c.seed, c.keyspace)
	wrong := map[int64]bool{draw(7).key: true, draw(42).key: true}
	srv := sleepServer(t, 0, wrong)
	c.url = srv.URL
	rep := runStage(t, c)
	if !rep.Failed() || rep.BadKey != 2 || rep.Errors != 2 || rep.OK != rep.Sent-2 {
		t.Fatalf("bad_key %d errors %d ok %d sent %d, want 2 bad keys failing the stage", rep.BadKey, rep.Errors, rep.OK, rep.Sent)
	}
	if rep.Held || rep.Cause != causeKey {
		t.Fatalf("held %v cause %q, want the key", rep.Held, rep.Cause)
	}
}

// TestRampStopsWhereTheBudgetEnds is the ramp against the 5 ms server with
// C recomputed per stage: C = 2 R (RTT + X) carries any rate the server
// answers under 2X, so what breaks is the tester's ceiling on C, here 4
// connections, which carry 800 req/s. Under a long X the wait in the
// queue stays under the SLO, and the cause published is the queue: the
// generator limits, the stage is invalid, not a verdict on the server.
// The stages before it are held, each with its own budget.
func TestRampStopsWhereTheBudgetEnds(t *testing.T) {
	srv := sleepServer(t, 5*time.Millisecond, nil)
	c := testCampaign(t, srv.URL, "-scenario", "unit", "-rates", "100,300,1200", "-duration", "1s",
		"-slo", "5s", "-max-conns", "4", "-warmup", "500ms", "-drain", "100ms")
	if c.budget(100) != 4 || c.budget(300) != 4 {
		t.Fatalf("budgets %d %d, want the ceiling", c.budget(100), c.budget(300))
	}
	var out strings.Builder
	ok, err := c.run(context.Background(), &out)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	t.Log(out.String())
	if ok || len(lines) != 3 {
		t.Fatalf("ok %v, %d stages, want 3 with the last invalid", ok, len(lines))
	}
	for i, want := range []struct {
		rate  int
		held  bool
		cause string
	}{{100, true, ""}, {300, true, ""}, {1200, false, causeQueue}} {
		var rep report
		if err := json.Unmarshal([]byte(lines[i]), &rep); err != nil {
			t.Fatal(err)
		}
		if rep.Rate != want.rate || rep.Held != want.held || rep.Cause != want.cause || rep.Conns != 4 {
			t.Fatalf("stage %d: rate %d held %v cause %q conns %d, want %+v", i, rep.Rate, rep.Held, rep.Cause, rep.Conns, want)
		}
		if rep.Sent != want.rate || rep.Errors != 0 {
			t.Fatalf("stage %d: sent %d errors %d, want the second (measured) at %d req/s", i, rep.Sent, rep.Errors, want.rate)
		}
		if want.held && rep.Queued > 0 {
			t.Fatalf("stage %d: queued %d at a rate 4 connections carry", i, rep.Queued)
		}
	}
}

// TestBudget is C = 2 R (RTT + X) on the study's example (§3.5): the unit
// read at 8 000 req/s under 20 ms of RTT and X = 25 ms wants 720.
func TestBudget(t *testing.T) {
	c := testCampaign(t, "https://unused", "-scenario", "unit", "-rtt", "20ms", "-ramp")
	if got := c.budget(8000); got != 720 {
		t.Fatalf("C(8000) = %d, want 720", got)
	}
	if got := c.at(8000).conns; got != 720 {
		t.Fatalf("the stage at 8000 carries C = %d", got)
	}
	c.maxConns = 500
	if got := c.budget(8000); got != 500 {
		t.Fatalf("C(8000) = %d under a ceiling of 500", got)
	}
	var grid []int
	for r := range gridFrom(95) {
		grid = append(grid, r)
		if len(grid) == 6 {
			break
		}
	}
	if want := []int{100, 105, 110, 116, 122, 128}; !slices.Equal(grid, want) {
		t.Fatalf("grid %v, want %v", grid, want)
	}
	if _, err := parse([]string{"-ramp", "-rates", "100"}); err == nil {
		t.Fatal("-ramp with -rates was accepted")
	}
}

// TestFreshConnections: with -new-conns 10, about a tenth of the requests
// open their own connection, whose handshake falls inside their delays
// and is counted apart; the budget's connections are untouched, and no
// connect fails on a loopback with a few hundred of them.
func TestFreshConnections(t *testing.T) {
	srv := sleepServer(t, 0, nil)
	c := testCampaign(t, srv.URL, "-scenario", "unit", "-rate", "500", "-duration", "2s", "-conns", "8", "-new-conns", "10")
	rep := runStage(t, c)
	t.Logf("sent %d handshakes %d connect %d ttlb p50 %.3f p99 %.3f ms", rep.Sent, rep.Handshakes, rep.Connect, rep.TTLB.P50, rep.TTLB.P99)
	if rep.Failed() || !rep.Held {
		t.Fatalf("errors %d connect %d held %v cause %q", rep.Errors, rep.Connect, rep.Held, rep.Cause)
	}
	if rep.Handshakes < rep.Sent*7/100 || rep.Handshakes > rep.Sent*13/100 {
		t.Fatalf("%d handshakes in %d requests, want about 10 %%", rep.Handshakes, rep.Sent)
	}
	fresh := c.freshDraw()
	n := 0
	for i := range rep.Sent {
		if fresh(i) {
			n++
		}
	}
	if n != rep.Handshakes {
		t.Fatalf("%d requests drawn fresh, %d handshakes", n, rep.Handshakes)
	}
}

func TestDrawNeverRepeatsAndMixes(t *testing.T) {
	st := testCampaign(t, "https://unused", "-scenario", "mixed", "-rate", "1000", "-duration", "10s").stage
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
	if _, err := parse([]string{"-rate", "1000", "-duration", "200s", "-keyspace", "100000"}); err == nil {
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
