package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/mlagarrigue/sluice/comparison/loadgen/stats"
)

// TestVerdicts: a synthetic campaign of 21 rounds, one family, three
// contenders — fast is three steps and 20 % of p99 ahead of base, twin is
// base again. The declared comparisons come out one line each per metric,
// fast ahead of base, twin equivalent to it; an undeclared pair is not
// judged, and a family short of rounds is refused.
func TestVerdicts(t *testing.T) {
	var in strings.Builder
	step := func(k int) float64 { return math.Round(100 * math.Pow(1.05, float64(k))) }
	for r := 1; r <= 21; r++ {
		jitter := float64(r%3) * 0.01
		for _, c := range []struct {
			name string
			k    int
			p99  float64
		}{{"base", 40, 10}, {"fast", 43, 8}, {"twin", 40, 10}} {
			fmt.Fprintf(&in, `{"scenario":"unit","proto":"h1","contender":%q,"round":%d,"capacity_rps":%v,"p99_ms":%v}`+"\n",
				c.name, r, step(c.k), c.p99*(1+jitter+float64(len(c.name))*0.001))
		}
	}
	var out bytes.Buffer
	if err := runVerdicts(strings.NewReader(in.String()), "fast:base,twin:base", stats.DefaultFloors, &out); err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	sc := bufio.NewScanner(&out)
	for sc.Scan() {
		var l verdictLine
		if err := json.Unmarshal(sc.Bytes(), &l); err != nil {
			t.Fatal(err)
		}
		if l.Family != "unit/h1" || l.Rounds != 20 || l.Holm == 0 {
			t.Fatalf("%+v", l)
		}
		got[l.Name+" "+l.Metric] = l.Verdict
	}
	want := map[string]string{
		"fast vs base capacity": "faster", "fast vs base p99": "faster",
		"twin vs base capacity": "equivalent", "twin vs base p99": "equivalent",
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("verdicts %v, want %v", got, want)
	}

	short := strings.Join(strings.Split(in.String(), "\n")[:30], "\n")
	if err := runVerdicts(strings.NewReader(short), "fast:base", stats.DefaultFloors, &out); err == nil {
		t.Fatal("ten rounds judged")
	}
	if err := runVerdicts(strings.NewReader(in.String()), "fast", stats.DefaultFloors, &out); err == nil {
		t.Fatal("a comparison with one side accepted")
	}
}
