package main

import (
	"slices"
	"testing"
	"time"
)

// TestPlateauWindows: a plateau longer than its keyspace walks it twice,
// each pass a permutation of its own, and is reported window by window —
// as many lines as windows, each with its share of the requests and its
// start half a second after the one before — then as a whole.
func TestPlateauWindows(t *testing.T) {
	srv := sleepServer(t, protoH1, 0, nil)
	c := testCampaign(t, protoH1, srv.URL, "-scenario", "unit", "-profile", "plateau", "-rate", "1000", "-duration", "2s", "-window", "500ms", "-keyspace", "1500", "-conns", "8")
	cl, err := c.newClient()
	if err != nil {
		t.Fatal(err)
	}
	defer cl.close()
	rep, windows := runWindows(t, cl, c)
	if rep.Failed() || rep.Sent != 2000 || rep.Passes != 2 || rep.Window != nil || rep.Profile != profilePlateau {
		t.Fatalf("sent %d passes %d window %v profile %q failed %v", rep.Sent, rep.Passes, rep.Window, rep.Profile, rep.Failed())
	}
	if len(windows) != 4 {
		t.Fatalf("%d windows, want 4", len(windows))
	}
	var prev time.Time
	for w, line := range windows {
		start, err := time.Parse(time.RFC3339Nano, line.Start)
		if err != nil {
			t.Fatal(err)
		}
		if *line.Window != w || line.Sent != 500 || line.OK != 500 || line.Rate != 1000 || line.Duration != 0.5 {
			t.Fatalf("window %d: index %d sent %d ok %d rate %d duration %v", w, *line.Window, line.Sent, line.OK, line.Rate, line.Duration)
		}
		if w > 0 && start.Sub(prev) != 500*time.Millisecond {
			t.Fatalf("window %d starts %v after the one before", w, start.Sub(prev))
		}
		prev = start
	}

	if _, err := parse([]string{"-scenario", "write", "-profile", "plateau", "-rate", "1000", "-duration", "2s", "-keyspace", "1500"}); err == nil {
		t.Error("a plateau of writes past its keyspace accepted: a write would repeat its key")
	}
	if _, err := parse([]string{"-profile", "plateau", "-duration", "2s", "-window", "300ms"}); err == nil {
		t.Error("a window that does not divide the duration accepted")
	}
}

// TestDrawPasses: past the keyspace, every pass is a permutation of it,
// in an order of its own.
func TestDrawPasses(t *testing.T) {
	const keyspace = 1200
	draw := scenarios["unit"].draw(3, keyspace)
	var passes [3][]int64
	for i := range 3 * keyspace {
		passes[i/keyspace] = append(passes[i/keyspace], draw(i).key)
	}
	for p, keys := range passes {
		sorted := slices.Sorted(slices.Values(keys))
		for k, key := range sorted {
			if key != int64(k+1) {
				t.Fatalf("pass %d is not a permutation of 1..%d", p, keyspace)
			}
		}
	}
	if slices.Equal(passes[0], passes[1]) || slices.Equal(passes[1], passes[2]) {
		t.Fatal("two passes in the same order")
	}
}

// TestPeak: the base rate, five times it, the base rate again, each phase
// in its own windows at its own rate; C is the peak's budget, opened before
// the clock, and no other connection opens.
func TestPeak(t *testing.T) {
	srv := sleepServer(t, protoH1, 0, nil)
	c := testCampaign(t, protoH1, srv.URL, "-scenario", "unit", "-profile", "peak", "-rate", "100", "-duration", "1s", "-peak-for", "1s", "-window", "500ms")
	if want := c.budget(500); c.conns != want {
		t.Fatalf("C %d, want the peak's budget %d", c.conns, want)
	}
	cl, err := c.newClient()
	if err != nil {
		t.Fatal(err)
	}
	defer cl.close()
	rep, windows := runWindows(t, cl, c)
	t.Logf("sent %d peak %d ttlb p99 %.3f ms", rep.Sent, rep.PeakRate, rep.TTLB.P99)
	if rep.Failed() || rep.Sent != 700 || rep.PeakRate != 500 {
		t.Fatalf("sent %d peak_rate %d failed %v", rep.Sent, rep.PeakRate, rep.Failed())
	}
	want := []struct {
		phase      string
		rate, sent int
	}{{"base", 100, 50}, {"base", 100, 50}, {"peak", 500, 250}, {"peak", 500, 250}, {"back", 100, 50}, {"back", 100, 50}}
	if len(windows) != len(want) {
		t.Fatalf("%d windows, want %d", len(windows), len(want))
	}
	for w, line := range windows {
		if line.Phase != want[w].phase || line.Rate != want[w].rate || line.Sent != want[w].sent {
			t.Fatalf("window %d: phase %q rate %d sent %d, want %+v", w, line.Phase, line.Rate, line.Sent, want[w])
		}
	}
	if got := srv.conns.Load(); got != int64(c.conns) {
		t.Fatalf("the server accepted %d connections, want C = %d", got, c.conns)
	}
}

// TestBrowserShape: the browser groups the same requests in flight as the
// API shape otherwise — six connections to a client under HTTP/1.1; under
// HTTP/2 and HTTP/3, 8 C streams on connections of 50, which an
// overloaded stage fills, seen from the server.
func TestBrowserShape(t *testing.T) {
	protos(t, func(t *testing.T, proto string) { //nolint:thelper // a sub-test body
		srv := sleepServer(t, proto, 20*time.Millisecond, nil)
		rate := "200"
		if proto != protoH1 {
			rate = "8000" // 100 streams at 20 ms carry 5 000 req/s
		}
		c := testCampaign(t, proto, srv.URL, "-scenario", "unit", "-shape", "browser", "-conns", "8", "-rate", rate, "-duration", "500ms", "-slo", "500ms")
		rep := runStage(t, c)
		t.Logf("conns %d clients %d, server: %d connections, %d in flight at most on one", rep.Conns, rep.Clients, srv.conns.Load(), srv.maxInFlight.Load())
		if rep.Failed() {
			t.Fatalf("errors %d timed out %d", rep.Errors, rep.TimedOut)
		}
		wantConns, wantClients, wantInFlight := 12, 2, int64(1)
		if proto != protoH1 {
			wantConns, wantClients, wantInFlight = 2, 2, browserStreamsH2
		}
		if rep.Conns != wantConns || rep.Clients != wantClients || rep.Shape != shapeBrowser {
			t.Fatalf("conns %d clients %d shape %q, want %d and %d", rep.Conns, rep.Clients, rep.Shape, wantConns, wantClients)
		}
		if n, m := srv.conns.Load(), srv.maxInFlight.Load(); n != int64(wantConns) || m != wantInFlight {
			t.Fatalf("the server saw %d connections, %d in flight at most on one, want %d and %d", n, m, wantConns, wantInFlight)
		}
	})
}

// -conns 0 is the budget a ramp computes at the stage's rate (§3.5): a
// campaign that plays one stage per process gets the same C.
func TestConnsZeroIsTheBudget(t *testing.T) {
	c, err := parse([]string{"-scenario", "unit", "-rate", "1000", "-rtt", "20ms", "-conns", "0"})
	if err != nil {
		t.Fatal(err)
	}
	if want := c.at(1000).conns; c.conns != want || want != 90 {
		t.Errorf("-conns 0 at 1000 req/s, RTT 20ms, X 25ms: C = %d, want the ramp's %d = 2 R (RTT + X) = 90", c.conns, want)
	}
	if _, err := parse([]string{"-conns", "-1"}); err == nil {
		t.Error("-conns -1 accepted")
	}
}
