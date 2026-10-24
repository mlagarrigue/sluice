package main

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// testCampaign parses the flags of a test: no warm-up and no drain unless
// the test asks for them, since a test runs its stages back to back.
func testCampaign(tb testing.TB, proto, url string, args ...string) campaign {
	tb.Helper()
	c, err := parse(append([]string{"-url", url, "-proto", proto, "-keyspace", "10000", "-warmup", "0", "-drain", "0"}, args...))
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
	protos(t, testCalibration)
}

// testCalibration is the calibration under one protocol; a sub-test of
// protos, not a helper.
func testCalibration(t *testing.T, proto string) { //nolint:thelper // a sub-test body, its failures point here
	run := func(d time.Duration) report {
		srv := sleepServer(t, proto, d, nil)
		c := testCampaign(t, proto, srv.URL, "-scenario", "unit", "-rate", "200", "-duration", "3s", "-conns", "16")
		st := c.stage
		rep := runStage(t, c)
		t.Logf("server sleeping %v: sent %d ok %d queue_max %d queued %d", d, rep.Sent, rep.OK, rep.QueueMax, rep.Queued)
		t.Logf("  late p50 %.3f p99 %.3f max %.3f ms", rep.Late.P50, rep.Late.P99, rep.Late.Max)
		t.Logf("  ttfb p50 %.3f p99 %.3f p999 %.3f max %.3f ms", rep.TTFB.P50, rep.TTFB.P99, rep.TTFB.P999, rep.TTFB.Max)
		t.Logf("  ttlb p50 %.3f p99 %.3f p999 %.3f max %.3f ms", rep.TTLB.P50, rep.TTLB.P99, rep.TTLB.P999, rep.TTLB.Max)
		if rep.Failed() || rep.OK != rep.Sent {
			t.Fatalf("errors %d bad_key %d tester %d", rep.Errors, rep.BadKey, rep.Tester)
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
	// The floor is what the apparatus alone costs: under a millisecond with
	// net/http at both ends; quic-go's stack, in user space at both ends of
	// the same process, costs about one more (measured: 1.1 ms under -race).
	maxFloor := 1.0
	if proto == protoH3 {
		maxFloor = 2.0
	}
	if floor.TTLB.P50 > maxFloor {
		t.Fatalf("floor p50 %.3f ms: the apparatus alone costs more than %.0f ms", floor.TTLB.P50, maxFloor)
	}
	// The server sleeps 5 ms at least, so a gap under 5 ms is noise on the
	// floor's own p50: 0.1 ms of it is allowed (a gap of 4.943 ms seen once
	// in eight runs of the suite under -race, 2026-10-24).
	if above := rep.TTLB.P50 - floor.TTLB.P50; above < 4.9 || above > 5.3 {
		t.Fatalf("TTLB p50 %.3f ms sits %.3f ms above the floor %.3f, want 5 ms", rep.TTLB.P50, above, floor.TTLB.P50)
	}
	if ceiling := 4.5 + maxFloor + raceSlack; rep.TTLB.P50 < 5.0 || rep.TTLB.P50 > ceiling {
		t.Fatalf("TTLB p50 %.3f ms, want 5 ms + loopback RTT, within [5.0, %.1f]", rep.TTLB.P50, ceiling)
	}
	if rep.TTFB.P50 < 5.0 || rep.TTFB.P50 > rep.TTLB.P50 {
		t.Fatalf("TTFB p50 %.3f ms, TTLB p50 %.3f ms", rep.TTFB.P50, rep.TTLB.P50)
	}
}

// TestOverloadedStageQueues is the correction under load: 4 connections at
// 5 ms carry 800 req/s, and the stage asks for 2 000. Measured from the due
// instant, the delays must climb with the queue (coordinated omission
// corrected); measured from the send instant, as a closed loop would, they
// would read 5 ms throughout and hide the stall. X is 500 ms, so that the
// deadline 10 X outlasts the 3 s the last request waits: nothing times out
// and every delay is read.
func TestOverloadedStageQueues(t *testing.T) {
	srv := sleepServer(t, protoH1, 5*time.Millisecond, nil)
	c := testCampaign(t, protoH1, srv.URL, "-scenario", "unit", "-rate", "2000", "-duration", "2s", "-conns", "4", "-slo", "500ms")
	rep := runStage(t, c)
	t.Logf("sent %d ok %d queue_max %d queued %d achieved %.0f req/s", rep.Sent, rep.OK, rep.QueueMax, rep.Queued, rep.Achieved)
	t.Logf("wait p50 %.1f ttlb p50 %.1f p99 %.1f max %.1f ms", rep.Wait.P50, rep.TTLB.P50, rep.TTLB.P99, rep.TTLB.Max)
	if rep.Failed() {
		t.Fatalf("errors %d bad_key %d tester %d", rep.Errors, rep.BadKey, rep.Tester)
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
	// p99 is far over X: the stage is broken by the SLO, and the
	// queue, however deep, is not the cause published (over X the wait in
	// the queue is latency, §3.1).
	if rep.Held || rep.Cause != causeSLO {
		t.Fatalf("held %v cause %q, want the SLO broken", rep.Held, rep.Cause)
	}
}

// TestWrongKeyFailsTheStage: one answer carrying another key than the one
// asked for is counted, and the stage is failed, with no extrapolation (§4).
func TestWrongKeyFailsTheStage(t *testing.T) {
	c := testCampaign(t, protoH1, "https://unused", "-scenario", "unit", "-rate", "100", "-duration", "1s", "-conns", "4")
	draw := c.scenario.draw(c.seed, c.keyspace)
	wrong := map[int64]fault{draw(7).key: wrongKey, draw(42).key: wrongKey}
	srv := sleepServer(t, protoH1, 0, wrong)
	c.url = srv.URL
	rep := runStage(t, c)
	if !rep.Failed() || rep.BadKey != 2 || rep.Errors != 0 || rep.OK != rep.Sent-2 {
		t.Fatalf("bad_key %d errors %d ok %d sent %d, want 2 bad keys failing the stage", rep.BadKey, rep.Errors, rep.OK, rep.Sent)
	}
	if rep.Held || rep.Cause != causeKey {
		t.Fatalf("held %v cause %q, want the key", rep.Held, rep.Cause)
	}
}

// TestRampStopsWhereTheBudgetEnds is the ramp against the 5 ms server with
// C recomputed per stage: C = 2 R (RTT + X) carries any rate the server
// answers under 2X, so what breaks is the tester's ceiling on C, here 4
// connections, which carry 800 req/s (one connection and its 8 streams,
// 1 600 req/s, under h2 and h3). Under a long X the wait in the queue
// stays under the SLO, and the cause published is the queue: the
// generator limits, the stage is invalid, not a verdict on the server.
// The stages before it are held, each with its own budget.
func TestRampStopsWhereTheBudgetEnds(t *testing.T) {
	protos(t, testRampStopsWhereTheBudgetEnds)
}

func testRampStopsWhereTheBudgetEnds(t *testing.T, proto string) { //nolint:thelper // a sub-test body
	ceiling, last := 4, 1200
	if proto != protoH1 {
		ceiling, last = 1, 3200
	}
	srv := sleepServer(t, proto, 5*time.Millisecond, nil)
	c := testCampaign(t, proto, srv.URL, "-scenario", "unit", "-rates", "100,300,"+strconv.Itoa(last), "-duration", "1s",
		"-slo", "5s", "-max-conns", strconv.Itoa(ceiling), "-warmup", "500ms", "-drain", "100ms")
	if c.budget(100) != ceiling || c.budget(300) != ceiling {
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
	}{{100, true, ""}, {300, true, ""}, {last, false, causeQueue}} {
		var rep report
		if err := json.Unmarshal([]byte(lines[i]), &rep); err != nil {
			t.Fatal(err)
		}
		if rep.Rate != want.rate || rep.Held != want.held || rep.Cause != want.cause || rep.Conns != ceiling || rep.Proto != proto {
			t.Fatalf("stage %d: rate %d held %v cause %q conns %d, want %+v", i, rep.Rate, rep.Held, rep.Cause, rep.Conns, want)
		}
		if rep.Sent != want.rate || rep.Errors != 0 {
			t.Fatalf("stage %d: sent %d errors %d, want the second (measured) at %d req/s", i, rep.Sent, rep.Errors, want.rate)
		}
		if want.held && rep.Queued > 0 {
			t.Fatalf("stage %d: queued %d at a rate the ceiling carries", i, rep.Queued)
		}
	}
	if n := srv.conns.Load(); n != int64(ceiling) {
		t.Fatalf("the server accepted %d connections, the ceiling is %d", n, ceiling)
	}
}

// TestStreamsPerConn is the ceiling of §3.5 seen from the server: under h2
// and h3, C connections and never more (the transport coalesces nothing),
// at most 8 requests in flight on each of them, and exactly 8 once the
// stage overloads them — the excess waits in the generator's queue.
func TestStreamsPerConn(t *testing.T) {
	protos(t, func(t *testing.T, proto string) { //nolint:thelper // a sub-test body
		if proto == protoH1 {
			t.Skip("one request in flight per connection, by construction")
		}
		srv := sleepServer(t, proto, 5*time.Millisecond, nil)
		// 4 connections × 8 streams carry 6 400 req/s: at 1 600, about 2
		// requests are in flight per connection. The queue is not asserted
		// here: under h3 and -race, one run in three holds 32 requests at
		// once for a moment and queues up to 60 of them (measured on
		// 2026-10-24, the same before and after the deadline of §1.2),
		// which is the race detector's, not the ceiling under test.
		c := testCampaign(t, proto, srv.URL, "-scenario", "unit", "-rate", "1600", "-duration", "1s", "-conns", "4")
		rep := runStage(t, c)
		t.Logf("at 1600 req/s: conns %d max in flight %d queued %d", srv.conns.Load(), srv.maxInFlight.Load(), rep.Queued)
		if rep.Failed() {
			t.Fatalf("errors %d tester %d", rep.Errors, rep.Tester)
		}
		if n, m := srv.conns.Load(), srv.maxInFlight.Load(); n != 4 || m > streamsPerConn || m < 2 {
			t.Fatalf("%d connections, %d in flight at most on one, want 4 and 2..8", n, m)
		}

		srv = sleepServer(t, proto, 5*time.Millisecond, nil)
		// 2 connections carry 3 200 req/s and the stage asks for 6 000.
		// Under a deadline of 2 s, no request is given up on: one would
		// leave its handler running on the server past the 8 streams.
		c = testCampaign(t, proto, srv.URL, "-scenario", "unit", "-rate", "6000", "-duration", "500ms", "-conns", "2", "-slo", "200ms")
		rep = runStage(t, c)
		t.Logf("at 6000 req/s: conns %d max in flight %d queue_max %d achieved %.0f", srv.conns.Load(), srv.maxInFlight.Load(), rep.QueueMax, rep.Achieved)
		if rep.Failed() {
			t.Fatalf("errors %d tester %d", rep.Errors, rep.Tester)
		}
		if n, m := srv.conns.Load(), srv.maxInFlight.Load(); n != 2 || m != streamsPerConn {
			t.Fatalf("%d connections, %d in flight at most on one, want 2 and exactly %d", n, m, streamsPerConn)
		}
		if rep.QueueMax < 1000 || rep.Achieved > 3600 {
			t.Fatalf("queue_max %d achieved %.0f req/s: 16 streams at 5 ms carry 3 200", rep.QueueMax, rep.Achieved)
		}
	})
}

// TestBudget is C = 2 R (RTT + X) on the study's example (§3.5): the unit
// read at 8 000 req/s under 20 ms of RTT and X = 25 ms wants 720.
func TestBudget(t *testing.T) {
	c := testCampaign(t, protoH1, "https://unused", "-scenario", "unit", "-rtt", "20ms", "-ramp")
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
	if _, err := parse([]string{"-proto", "h4"}); err == nil {
		t.Fatal("-proto h4 was accepted")
	}
}

// TestFreshConnections: with -new-conns 10, about a tenth of the requests
// open their own connection, whose handshake falls inside their delays
// and is counted apart; the budget's connections are untouched, and no
// connect fails on a loopback with a few hundred of them.
func TestFreshConnections(t *testing.T) {
	protos(t, testFreshConnections)
}

func testFreshConnections(t *testing.T, proto string) { //nolint:thelper // a sub-test body
	srv := sleepServer(t, proto, 0, nil)
	c := testCampaign(t, proto, srv.URL, "-scenario", "unit", "-rate", "500", "-duration", "2s", "-conns", "8", "-new-conns", "10")
	rep := runStage(t, c)
	t.Logf("sent %d handshakes %d tester %d ttlb p50 %.3f p99 %.3f ms", rep.Sent, rep.Handshakes, rep.Tester, rep.TTLB.P50, rep.TTLB.P99)
	if rep.Failed() || !rep.Held {
		t.Fatalf("errors %d tester %d held %v cause %q", rep.Errors, rep.Tester, rep.Held, rep.Cause)
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
	if got := srv.conns.Load(); got != int64(8+n) {
		t.Fatalf("the server accepted %d connections, want the budget's 8 and %d fresh", got, n)
	}
}

func TestDrawNeverRepeatsAndMixes(t *testing.T) {
	st := testCampaign(t, protoH1, "https://unused", "-scenario", "mixed", "-rate", "1000", "-duration", "10s").stage
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

// CMP-01: a list drawn on the table's last key asks for ids above the
// highest one, and its page is rightly empty — it used to be counted a
// wrong key. Empty anywhere else is still a wrong answer.
func TestAListFromTheLastKeyIsEmpty(t *testing.T) {
	const keyspace = 50
	draw := scenarios["list"].draw(10, keyspace)
	var last, other request
	for i := range keyspace {
		if rq := draw(i); rq.key == keyspace {
			last = rq
		} else if other.key == 0 {
			other = rq
		}
	}
	if last.key != keyspace || other.key == 0 {
		t.Fatalf("the draw gave no list from the last key (%d) or from another (%d)", last.key, other.key)
	}
	empty := func(from int64) []byte { return fmt.Appendf(nil, `{"from":%d,"items":[]}`, from) }
	if err := check(last, empty(keyspace)); err != nil {
		t.Errorf("the empty page from the last key was refused: %v", err)
	}
	if err := check(other, empty(other.key)); err == nil {
		t.Errorf("an empty page from key %d was accepted", other.key)
	}
}

// listPage is a page as the study's server writes it: rows from+1 ..
// from+n, id, total 7·id, label "order-id".
func listPage(from int64, n int) []byte {
	b := fmt.Appendf(nil, `{"from":%d,"limit":%d,"items":[`, from, listLimit)
	for i := range n {
		id := from + int64(i) + 1
		if i > 0 {
			b = append(b, ',')
		}
		b = fmt.Appendf(b, `{"id":%d,"total":%d,"label":"order-%d"}`, id, 7*id, id)
	}
	return append(b, "]}"...)
}

// CMP-02: the list's cheap control is one validation pass and one scan; it
// must refuse what the encoding/json control refused.
func TestCheckListRefusesWhatItMust(t *testing.T) {
	rq := request{kind: list, keyField: "from", key: 10}
	for _, tc := range []struct {
		name string
		body string
		ok   bool
	}{
		{"a full page", string(listPage(10, 1000)), true},
		{"spaces after the colons", `{"from": 10, "items": [{"id": 11}, {"id": 12}]}`, true},
		{"another bound", string(listPage(11, 3)), false},
		{"ids that go back", `{"from":10,"items":[{"id":12},{"id":11}]}`, false},
		{"an id at the bound", `{"from":10,"items":[{"id":10}]}`, false},
		{"an empty page", `{"from":10,"items":[]}`, false},
		{"no items", `{"from":10}`, false},
		{"no bound", `{"items":[{"id":11}]}`, false},
		{"cut short", string(listPage(10, 3)[:40]), false},
		{"an id inside a label is not an id", `{"from":10,"items":[{"id":12,"label":"x\"id\":5"}]}`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := check(rq, []byte(tc.body)); (err == nil) != tc.ok {
				t.Errorf("check = %v, want ok %v", err, tc.ok)
			}
		})
	}
}

func BenchmarkCheckList(b *testing.B) {
	body := listPage(10, 1000)
	rq := request{kind: list, keyField: "from", key: 10}
	b.SetBytes(int64(len(body)))
	for b.Loop() {
		if err := check(rq, body); err != nil {
			b.Fatal(err)
		}
	}
}
