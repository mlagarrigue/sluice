package main

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The report is checked on readings written here rather than on a
// campaign's: a campaign takes hours and a machine of its own, and what
// has to be true of the report — every point of §5 present, zero
// published as a result, a verdict on the primary metrics and nowhere
// else, charts a parser accepts — is true of three stages as much as of
// thirty. Writing them by hand is also what discovers a field §5 asks for
// and no file of the campaign carries.

// synthetic writes a campaign directory: one family, two contenders,
// three rates, two rounds each. sluice-std groups (its counters say so),
// nethttp does not; nethttp crashes once at the top rate, sluice-std never.
func synthetic(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()

	write := func(name, body string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	write("matrix.json", `{
	  "name": "trial-unit", "trial": false, "rows": 100000,
	  "contenders": {"sluice-std": {"image": "sluice-study:doc", "args": []},
	                 "nethttp": {"image": "go-nethttp:doc", "args": []}},
	  "families": [{"scenario": "unit", "proto": "h2",
	                "contenders": ["sluice-std", "nethttp"], "points": [1000, 2000]}]
	}`)

	write("summary.json", `{
	  "name": "trial-unit", "trial": false, "publishable": true,
	  "families": [{"family": "unit/h2",
	    "capacity": {"rounds": 2, "contenders": {
	      "sluice-std": {"median": 8000, "lo": 7600, "hi": 8400, "unstable": false, "values": [8000, 8000]},
	      "nethttp":    {"median": 4000, "lo": 3800, "hi": 4200, "unstable": true,  "values": [4000, 4200]}}},
	    "points": {"designated": 2000, "full_rounds": [1, 2],
	      "p99": {"rounds": 2, "contenders": {
	        "sluice-std": {"median": 11.2, "lo": 10.8, "hi": 11.6, "unstable": false, "values": [11.2, 11.3]},
	        "nethttp":    {"median": 24.5, "lo": 23.0, "hi": 26.0, "unstable": false, "values": [24.5, 24.9]}}}}}]
	}`)

	// One merged stage line, as chain.sh writes it: the generator's report
	// plus the collector's roles, crash, valid.
	line := func(rate int, p50, p99, rssKB float64, closed, overflows, bad int, crash string, batches, keys uint64) string {
		metrics := "null, \"end\": null"
		if batches > 0 {
			metrics = fmt.Sprintf(`{"batches": {"count": 0, "keys": 0}}, "end": {"batches": {"count": %d, "keys": %d}}`, batches, keys)
		}
		return fmt.Sprintf(`{"rate": %d, "conns": 50, "slo_ms": 25, "rtt_ms": 0, "held": true, "cause": "",
		  "sent": 20000, "ok": 20000, "bad_key": %d, "timed_out": 0, "errors": 0,
		  "status_4xx": 0, "status_5xx": 0, "refused": 0, "reset": 0, "connect_timeout": 0, "other": 0,
		  "closed": %d, "reconnects": 0,
		  "ttlb_ms": {"p50": %g, "p99": %g, "p999": %g},
		  "roles": {"web": {"cpu_s": 8.1, "rss_kb_max": %g, "established_max": 50,
		                    "listen_overflows": %d, "listen_drops": 0},
		            "db": {"connections": 16}},
		  "crash": "%s", "valid": true, "metrics": {"start": %s}}`,
			rate, bad, closed, p50, p99, p99*1.4, rssKB, overflows, crash, metrics)
	}

	var raw strings.Builder
	rec := func(contender, kind string, round, rate int, body string) {
		t.Helper()
		record := fmt.Sprintf(`{"family": "unit/h2", "measure": "capacity", "phase": "round", "round": %d,`+
			`"contender": %q, "attempt": 1, "kind": %q, "rate": %d, "line": %s}`,
			round, contender, kind, rate, body)
		// raw.jsonl is one record per line, as the campaign writes it: the
		// fixture's readable spacing is compacted away rather than fed as is.
		var one bytes.Buffer
		if err := json.Compact(&one, []byte(record)); err != nil {
			t.Fatalf("the fixture is not JSON: %v", err)
		}
		raw.Write(one.Bytes())
		raw.WriteString("\n")
	}
	for round := 1; round <= 2; round++ {
		// A warm-up measures nothing and must not reach a curve.
		rec("sluice-std", "warmup", round, 500, line(500, 99, 999, 1<<20, 0, 0, 0, "no", 0, 0))
		for i, rate := range []int{1000, 2000, 4000} {
			rec("sluice-std", "ramp", round, rate, line(rate, 2+float64(i), 6+2*float64(i), 18000+float64(i)*500, 0, 0, 0, "no", 1000, 8000))
			crash := "no"
			if rate == 4000 {
				crash = "yes: pid/restarts 11 -> 12, oom/running true true"
			}
			rec("nethttp", "ramp", round, rate, line(rate, 3+float64(i), 9+4*float64(i), 42000+float64(i)*900, 7, 3, 0, crash, 0, 0))
		}
	}
	write("raw.jsonl", raw.String())

	write("verdicts.jsonl", `{"family":"unit/h2","name":"sluice-std vs nethttp","metric":"capacity","rounds":2,"wins":2,"losses":0,"p":0.5,"holm_threshold":0.025,"gap":1,"lo":1,"hi":1,"percent":"+5.0%","margin":1,"verdict":"faster"}
{"family":"unit/h2","name":"sluice-std vs nethttp","metric":"p99","rounds":2,"wins":2,"losses":0,"p":0.5,"holm_threshold":0.05,"gap":0.46,"lo":0.44,"hi":0.48,"percent":"-54.0%","margin":0.05,"verdict":"faster"}
`)
	return dir
}

func TestReportCoversSection5(t *testing.T) {
	dir := synthetic(t)
	out := filepath.Join(t.TempDir(), "report")
	if err := run(dir, "", out); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(out, "COMPARISON.md")) //nolint:gosec // G304: the directory is the test's own
	if err != nil {
		t.Fatal(err)
	}
	doc := string(b)

	// §5's nine points, in the order the study lists them.
	points := []string{
		"### What is measured",
		"### Latency and memory against the rate",
		"### The short table",
		"### Why",
		"### Verdicts",
		"### Batch size observed",
		"### Wrong answers",
		"### Per stage: the server's closures, the accept queue, the errors",
		"### Crashes",
	}
	at := -1
	for _, p := range points {
		i := strings.Index(doc, p)
		if i < 0 {
			t.Errorf("§5: %q is missing from the report", p)
			continue
		}
		if i < at {
			t.Errorf("§5: %q comes out of order", p)
		}
		at = i
	}

	// The scope of Holm is written beside the verdicts (§5.5, §7.4).
	for _, want := range []string{
		"Holm's correction holds over a family",
		"not over the publication",
		"primary metrics only",
	} {
		if !strings.Contains(doc, want) {
			t.Errorf("the verdicts' scope does not say %q", want)
		}
	}

	// A verdict on each primary metric, and none anywhere else: the short
	// table publishes medians and intervals without one.
	for _, want := range []string{
		"| sluice-std vs nethttp | capacity | **faster** | +5.0% | 1.00 – 1.00 steps |",
		"| sluice-std vs nethttp | p99 | **faster** | -54.0% | ×0.44 – ×0.48 |",
	} {
		if !strings.Contains(doc, want) {
			t.Errorf("the verdict table is missing %q", want)
		}
	}
	if n := strings.Count(doc, "**faster**"); n != 2 {
		t.Errorf("%d verdicts in the report, want the two primary metrics' only", n)
	}

	// Zero is a result: the counters are published even when nothing
	// happened (§5.7 to §5.9).
	for _, want := range []string{
		"| sluice-std | 0 | 120,000 |",              // wrong answers, none over the six stages
		"| sluice-std | 1000 | 2 | 0 | 0 | 0 | 0 |", // closures and the accept queue, none in two stages
		"| sluice-std | 0 | — |",                    // crashes, none
		"| nethttp | 2 | yes: pid/restarts",         // and the two that happened, with their cause
	} {
		if !strings.Contains(doc, want) {
			t.Errorf("the report does not publish %q", want)
		}
	}
	if !strings.Contains(doc, "| nethttp | 1000 | 2 | 14 | 0 | 6 | 0 |") {
		t.Error("the stage table loses the server's closures or the accept queue's overflows")
	}

	// The capacity with its interval, and the unstable one marked.
	if !strings.Contains(doc, "8000 req/s (7600 – 8400)") {
		t.Error("the short table does not publish the capacity and its interval")
	}
	if !strings.Contains(doc, "4000 req/s (3800 – 4200), **unstable**") {
		t.Error("an unstable capacity is published as if it were steady")
	}

	// The batch size of the variant that groups, and the silence of the one
	// that does not (§5.6).
	if !strings.Contains(doc, "| sluice-std | 6,000 | 48,000 | 8.00 |") {
		t.Error("the batch size observed is not what the counters say")
	}
	if !strings.Contains(doc, "| nethttp | — | — | no grouping counter |") {
		t.Error("a contender without a grouping counter must be named all the same")
	}

	// The two columns CMP-32 counts are left empty, and said to be.
	if !strings.Contains(doc, "Lines of code and dependencies are counted by CMP-32") {
		t.Error("the report does not say why two columns of §5.3 are empty")
	}
	// The three sentences are the author's, and the hole is visible.
	if !strings.Contains(doc, "**To write, three sentences:**") {
		t.Error("§5.4 is neither written nor marked as to write")
	}
}

func TestReportChartsParse(t *testing.T) {
	dir := synthetic(t)
	out := filepath.Join(t.TempDir(), "report")
	if err := run(dir, "", out); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"unit-h2-apparatus.svg", "unit-h2-latency.svg", "unit-h2-memory.svg"} {
		b, err := os.ReadFile(filepath.Join(out, name)) //nolint:gosec // G304: idem
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		d := xml.NewDecoder(strings.NewReader(string(b)))
		for {
			if _, err := d.Token(); errors.Is(err, io.EOF) {
				break
			} else if err != nil {
				t.Fatalf("%s is not well-formed XML: %v", name, err)
			}
		}
		if !strings.Contains(string(b), `xmlns="http://www.w3.org/2000/svg"`) {
			t.Errorf("%s carries no SVG namespace", name)
		}
	}
	// A warm-up stage must not reach a curve: its p99 of 999 ms would
	// flatten every real point against the frame.
	b, err := os.ReadFile(filepath.Join(out, "unit-h2-latency.svg")) //nolint:gosec // G304: idem
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "999") {
		t.Error("a warm-up stage reached the latency chart")
	}
	// Both contenders, p50 and p99, are in the legend.
	for _, want := range []string{"sluice-std p50", "sluice-std p99", "nethttp p50", "nethttp p99"} {
		if !strings.Contains(string(b), want) {
			t.Errorf("the latency chart has no series %q", want)
		}
	}
}

func TestReportRefusesAnIncompleteCampaign(t *testing.T) {
	dir := synthetic(t)
	if err := os.Remove(filepath.Join(dir, "raw.jsonl")); err != nil {
		t.Fatal(err)
	}
	err := run(dir, "", filepath.Join(t.TempDir(), "report"))
	if err == nil || !strings.Contains(err.Error(), "raw.jsonl") {
		t.Errorf("a campaign without its stages: %v, want an error naming raw.jsonl", err)
	}
	if _, err := read(t.TempDir(), "nowhere.jsonl"); err == nil {
		t.Error("an empty directory passed for a campaign")
	}
}

func TestReportSaysATrialIsNotPublishable(t *testing.T) {
	dir := synthetic(t)
	b, err := os.ReadFile(filepath.Join(dir, "summary.json")) //nolint:gosec // G304: idem
	if err != nil {
		t.Fatal(err)
	}
	trial := strings.Replace(string(b), `"publishable": true`, `"publishable": false`, 1)
	if trial == string(b) {
		t.Fatal("the fixture no longer says publishable")
	}
	if err := os.WriteFile(filepath.Join(dir, "summary.json"), []byte(trial), 0o600); err != nil { //nolint:gosec // G703: dir is this test's own TempDir
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "report")
	if err := run(dir, "", out); err != nil {
		t.Fatal(err)
	}
	doc, err := os.ReadFile(filepath.Join(out, "COMPARISON.md")) //nolint:gosec // G703,G304: the directory is the test's own
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(doc), "**Not publishable.**") {
		t.Error("a campaign that is not publishable produces a report that does not say so")
	}
}
