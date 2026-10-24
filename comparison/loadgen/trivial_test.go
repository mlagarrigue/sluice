package main

import (
	"cmp"
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

// startTrivial runs the trivial server in the test's process, on a free
// port; the self-test script runs it in a process of its own.
func startTrivial(tb testing.TB, keyspace int) *trivialServer {
	tb.Helper()
	s, err := listenTrivial("127.0.0.1:0", keyspace)
	if err != nil {
		tb.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.serve(ctx) }()
	tb.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			tb.Error(err)
		}
	})
	return s
}

// TestTrivialServesEveryScenario is the trivial server under the
// generator's own checks: the five scenarios, the three protocols, the
// three codings, every answer passing the cheap control and the sampled
// ones the full validation, so that a ramp against it fails on the
// generator's capacity and nothing else. Under gzip and deflate, every
// answer comes compressed.
func TestTrivialServesEveryScenario(t *testing.T) {
	s := startTrivial(t, 10000)
	protos(t, func(t *testing.T, proto string) { //nolint:thelper // a sub-test body
		for _, encoding := range []string{"identity", codingGzip, codingDeflate} {
			for _, name := range []string{"static", "unit", "list", "write", "mixed"} {
				t.Run(encoding+"/"+name, func(t *testing.T) {
					testTrivialServes(t, s, proto, encoding, name)
				})
			}
		}
	})
}

func testTrivialServes(t *testing.T, s *trivialServer, proto, encoding, name string) { //nolint:thelper // a sub-test body
	// Under -race, compress/gzip and compress/zlib run far slower on both
	// ends, and 400 lists a second saturate them: the same 400 requests go
	// out at a quarter of the rate.
	rate, duration := "400", "1s"
	if raceSlack > 0 && encoding != "identity" {
		rate, duration = "100", "4s"
	}
	c := testCampaign(t, proto, s.URL, "-scenario", name, "-rate", rate, "-duration", duration, "-conns", "32", "-encoding", encoding)
	draw := c.scenario.draw(c.seed, c.keyspace)
	full := 0
	for i := range c.requests() {
		if draw(i).full {
			full++
		}
	}
	if full == 0 {
		t.Fatal("no request of the stage is fully validated: pick another seed or a longer stage")
	}
	rep := runStage(t, c)
	t.Logf("%s/%s: sent %d ok %d (%d fully validated) ttlb p50 %.3f p99 %.3f ms", proto, name, rep.Sent, rep.OK, full, rep.TTLB.P50, rep.TTLB.P99)
	if rep.Failed() || rep.OK != rep.Sent || !rep.Held {
		t.Fatalf("errors %d bad_key %d tester %d held %v cause %q", rep.Errors, rep.BadKey, rep.Tester, rep.Held, rep.Cause)
	}
	if want := 100.0; encoding == "identity" && rep.Compressed != 0 || encoding != "identity" && rep.Compressed != want {
		t.Fatalf("compressed %.1f %% under %s", rep.Compressed, encoding)
	}
}

// TestTrivialEdges is the handler alone: the table's bounds and the
// malformed requests, which the generator never sends.
func TestTrivialEdges(t *testing.T) {
	h := newTrivial(2000)
	get := func(path string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		return rec
	}
	for _, tc := range []struct {
		path   string
		status int
	}{
		{"/static", 200},
		{"/orders/1", 200},
		{"/orders/3000", 200}, // the table holds keyspace + 1000 rows
		{"/orders/3001", 404},
		{"/orders/0", 404},
		{"/orders/x", 404},
		{"/orders?from=2000&limit=1000", 200}, // the last full page
		{"/orders?from=2001&limit=1000", 400},
		{"/orders?from=0&limit=1001", 400},
		{"/orders?from=0&limit=0", 400},
		{"/orders?from=5", 400},
		{"/nowhere", 404},
	} {
		rec := get(tc.path)
		if rec.Code != tc.status {
			t.Errorf("GET %s: status %d, want %d (%s)", tc.path, rec.Code, tc.status, rec.Body.String())
		}
		if got := rec.Header().Get("Content-Length"); got == "" || got != strconv.Itoa(rec.Body.Len()) {
			t.Errorf("GET %s: Content-Length %q, body %d bytes", tc.path, got, rec.Body.Len())
		}
	}
	if got := get("/static").Body.Len(); got < 900 || got > 1200 {
		t.Errorf("static document of %d bytes, want about 1 KB", got)
	}
	if got := get("/orders?from=0&limit=1000").Body.Len(); got < 30000 || got > 120000 {
		t.Errorf("list of %d bytes, want tens of KB", got)
	}
	if err := check(unitRequest(0, 7, true), get("/orders/7").Body.Bytes()); err != nil {
		t.Error(err)
	}
	if err := check(listRequest(0, 1999, true), get("/orders?from=1999&limit=1000").Body.Bytes()); err != nil {
		t.Error(err)
	}

	post := func(body string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		h.ServeHTTP(rec, req)
		return rec
	}
	if rec := post(`{"key":42,"total":126}`); rec.Code != 200 || rec.Body.String() != `{"key":42}` {
		t.Errorf("POST: %d %s", rec.Code, rec.Body.String())
	}
	if rec := post(`{"key": 42}`); rec.Code != 200 || rec.Body.String() != `{"key":42}` {
		t.Errorf("POST with a space: %d %s", rec.Code, rec.Body.String())
	}
	for _, body := range []string{`{}`, `{"key":"a"}`, `{"key":`, ``} {
		if rec := post(body); rec.Code != 400 {
			t.Errorf("POST %q: status %d, want 400", body, rec.Code)
		}
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/orders/1", nil))
	if rec.Code != 404 {
		t.Errorf("DELETE: status %d, want 404", rec.Code)
	}

	// Compressed in the coding asked for, an error in clear.
	for _, tc := range []struct{ accept, coding string }{
		{"", ""},
		{"gzip", "gzip"},
		{"deflate", "deflate"},
		{"deflate, gzip", "gzip"},
		{"gzip;q=0, deflate", "deflate"},
		{"gzip;q=0", ""},
		{"br", ""},
	} {
		for _, path := range []string{"/orders?from=1999&limit=1000", "/nowhere"} {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, path, nil)
			req.Header.Set("Accept-Encoding", tc.accept)
			h.ServeHTTP(rec, req)
			want := tc.coding
			if rec.Code != 200 {
				want = ""
			}
			got := rec.Header().Get("Content-Encoding")
			if got != want || rec.Header().Get("Content-Length") != strconv.Itoa(rec.Body.Len()) {
				t.Errorf("GET %s, Accept-Encoding %q: Content-Encoding %q, want %q", path, tc.accept, got, want)
				continue
			}
			if rec.Code != 200 {
				continue
			}
			d, err := decode(cmp.Or(want, tc.accept), got, rec.Body.Bytes())
			if err == nil {
				err = check(listRequest(0, 1999, true), d.doc)
			}
			if err != nil {
				t.Errorf("GET %s, Accept-Encoding %q: %v", path, tc.accept, err)
			}
		}
	}
}
