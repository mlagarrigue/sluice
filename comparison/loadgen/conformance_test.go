package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

// variant serves what h serves, with the body of every 200 passed
// through edit first: a contender that differs from the reference in
// a way the test chooses.
func variant(t *testing.T, h http.Handler, edit func(path string, h http.Header, body []byte) []byte) string {
	t.Helper()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, r)
		body := rec.Body.Bytes()
		if rec.Code == http.StatusOK {
			body = edit(r.URL.Path, rec.Header(), body)
		}
		for k, v := range rec.Header() {
			w.Header()[k] = v
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(rec.Code)
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func check2(t *testing.T, url, reference string, args ...string) conformanceReport {
	t.Helper()
	c := testCampaign(t, protoH1, url, args...)
	rep, err := conformance{stage: c.stage, reference: reference, n: 100}.run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return rep
}

// TestConformance: the trivial server against itself is conformant, under
// each coding; a variant that only reorders keys, writes its numbers
// otherwise and names JSON's charset is conformant too, since the check
// compares documents and not bytes; a variant that changes one label, or
// one status, fails at that request, and the difference names where.
func TestConformance(t *testing.T) {
	ref := startTrivial(t, 10000)
	other := startTrivial(t, 10000)
	for _, enc := range []string{"identity", codingGzip} {
		if rep := check2(t, other.URL, ref.URL, "-encoding", enc); !rep.Conformant || rep.Checked != 100 {
			t.Fatalf("trivial against trivial under %s: %+v", enc, rep)
		}
	}

	h := newTrivial(10000)
	reorder := variant(t, h, func(_ string, hd http.Header, body []byte) []byte {
		hd.Set("Content-Type", "application/json; charset=UTF-8")
		var v map[string]any
		if json.Unmarshal(body, &v) != nil {
			return body
		}
		out, _ := json.MarshalIndent(v, "", "  ") // keys sorted, spaces added, 7 written 7
		if id, ok := v["id"].(float64); ok {      // a unit answer: the total as 7.0e0-style
			out = bytes.Replace(out, fmt.Appendf(nil, `"total": %d`, int64(id)*7), fmt.Appendf(nil, `"total": %de0`, int64(id)*7), 1)
		}
		return out
	})
	if rep := check2(t, reorder, ref.URL); !rep.Conformant {
		t.Fatalf("a reordered, reformatted variant: %+v", rep.First)
	}

	c := testCampaign(t, protoH1, ref.URL)
	key := scenarios["mixed"].draw(c.seed, c.keyspace)(0).key // the first request of the check is a unit read
	wrongLabel := variant(t, h, func(path string, _ http.Header, body []byte) []byte {
		if path == fmt.Sprintf("/orders/%d", key) {
			return bytes.Replace(body, []byte(`"label":"order-`), []byte(`"label":"Order-`), 1)
		}
		return body
	})
	rep := check2(t, wrongLabel, ref.URL)
	if rep.Conformant || rep.First == nil || rep.First.Where != "$.label" || rep.First.Request != fmt.Sprintf("GET /orders/%d", key) || rep.Checked != 1 {
		t.Fatalf("a wrong label: %+v %+v", rep, rep.First)
	}
	t.Logf("first difference: %+v", *rep.First)

	notFound := variant(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/orders") && r.Method == http.MethodPost {
			http.Error(w, "no", http.StatusNotFound)
			return
		}
		h.ServeHTTP(w, r)
	}), func(_ string, _ http.Header, b []byte) []byte { return b })
	rep = check2(t, notFound, ref.URL)
	if rep.Conformant || rep.First.Where != "status" || !strings.HasPrefix(rep.First.Request, "POST /orders") {
		t.Fatalf("writes refused: %+v", rep.First)
	}
}

// TestCompareJSON: numbers by value, objects by key, arrays by place.
func TestCompareJSON(t *testing.T) {
	parse := func(s string) any {
		dec := json.NewDecoder(strings.NewReader(s))
		dec.UseNumber()
		var v any
		if err := dec.Decode(&v); err != nil {
			t.Fatal(err)
		}
		return v
	}
	for _, tc := range []struct {
		c, r, where string
	}{
		{`{"a":1,"b":[1,2]}`, `{"b":[1,2.0],"a":1e0}`, ""},
		{`{"a":1}`, `{"a":2}`, "$.a"},
		{`{"a":1}`, `{"a":1,"b":null}`, "$.b"},
		{`{"a":1,"c":3}`, `{"a":1}`, "$.c"},
		{`{"items":[1,2,3]}`, `{"items":[1,2]}`, "$.items.length"},
		{`{"items":[{"id":1},{"id":3}]}`, `{"items":[{"id":1},{"id":2}]}`, "$.items[1].id"},
		{`{"a":"1"}`, `{"a":1}`, "$.a"},
		{`0.1`, `0.10000000000000001`, "$"},
	} {
		d := compareJSON("$", parse(tc.c), parse(tc.r))
		switch {
		case tc.where == "" && d != nil:
			t.Errorf("%s against %s: %+v, want equal", tc.c, tc.r, d)
		case tc.where != "" && (d == nil || d.Where != tc.where):
			t.Errorf("%s against %s: %+v, want a difference at %s", tc.c, tc.r, d, tc.where)
		}
	}
}

// TestFetchMetrics: -metrics writes the server's metrics point as it came,
// and refuses a status other than 200.
func TestFetchMetrics(t *testing.T) {
	const doc = `{"runtime":"go1.26.0","cpu_seconds":1.5,"gc":null,"batches":null}`
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/study/metrics" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(doc))
	}))
	defer srv.Close()
	c := testCampaign(t, protoH1, srv.URL)
	var out bytes.Buffer
	if err := fetchMetrics(context.Background(), c.stage, &out); err != nil || out.String() != doc {
		t.Fatalf("%q %v", out.String(), err)
	}
	c.url = srv.URL + "/nowhere"
	if err := fetchMetrics(context.Background(), c.stage, &out); err == nil {
		t.Fatal("a 404 accepted")
	}
}

// -ready waits through a server that is not listening yet, and stops at
// its first answer ok.
func TestWaitReady(t *testing.T) {
	s := startTrivial(t, 10000)
	c := testCampaign(t, protoH1, s.URL, "-scenario", "unit")
	var out bytes.Buffer
	before := time.Now().UnixNano()
	if err := waitReady(t.Context(), c.stage, 5*time.Second, &out); err != nil {
		t.Fatal(err)
	}
	var got struct {
		Ready    int64 `json:"ready_unix_ns"`
		Attempts int   `json:"attempts"`
	}
	if err := json.Unmarshal(out.Bytes(), &got); err != nil || got.Ready < before || got.Attempts != 1 {
		t.Fatalf("%s %v: want one attempt, after the call", out.String(), err)
	}
	c.url = "https://127.0.0.1:1" // nothing listens there
	start := time.Now()
	if err := waitReady(t.Context(), c.stage, 300*time.Millisecond, &out); err == nil || time.Since(start) < 300*time.Millisecond {
		t.Fatalf("%v after %s: want an error once the 300ms are spent", err, time.Since(start))
	}
}
