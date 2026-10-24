package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/comparison/loadgen/stats"
)

// fake is a chain with no containers: each contender has a true capacity,
// drawn again per repetition with its noise, and a stage holds when its
// rate is at most the repetition's draw. Its p99 grows with the load.
type fake struct {
	caps      map[string]float64
	noise     map[string]float64
	later     map[string]float64 // the capacity after the contender's first repetition, when it moves
	invalidOn func(n int, s stageSpec) bool

	rng      *rand.Rand
	cur      string
	draw     float64
	webs     map[string]int
	stages   int
	invalids int
	ups      []string
	downs    int
}

func newFake(caps, noise map[string]float64) *fake {
	return &fake{caps: caps, noise: noise, rng: rand.New(rand.NewPCG(3, 4)), webs: map[string]int{}} //nolint:gosec // G404: a synthetic chain's noise
}

func (f *fake) up(_ context.Context, _ family, rtt string) error {
	f.ups = append(f.ups, rtt)
	return nil
}

func (f *fake) web(_ context.Context, c contender) error {
	f.cur = c.Image
	f.webs[f.cur]++
	capacity := f.caps[f.cur]
	if v, ok := f.later[f.cur]; ok && f.webs[f.cur] > 1 {
		capacity = v
	}
	f.draw = capacity * (1 + f.noise[f.cur]*f.rng.NormFloat64())
	return nil
}

func (f *fake) stage(_ context.Context, s stageSpec) (stageResult, error) {
	f.stages++
	invalid := f.invalidOn != nil && f.invalidOn(f.stages, s)
	if invalid {
		f.invalids++
	}
	held := float64(s.rate) <= f.draw
	p99 := 10 * (1 + float64(s.rate)/f.draw) * (1 + f.noise[f.cur]*f.rng.NormFloat64())
	sent := int(float64(s.rate) * s.duration.Seconds())
	line := fmt.Sprintf(`{"rate":%d,"held":%t,"sent":%d,"ok":%d,"ttlb_ms":{"p99":%g,"p999":%g},"valid":%t}`,
		s.rate, held, sent, sent, p99, 2*p99, !invalid)
	return parseStage([]byte(line))
}

func (f *fake) down(context.Context) error {
	f.downs++
	return nil
}

func testMatrix(t *testing.T, trial bool, rounds, maxRounds, full int) matrix {
	t.Helper()
	m, err := parseMatrix(fmt.Appendf(nil, `{
	  "name": "test", "seed": 11, "trial": %t,
	  "rounds": %d, "max_rounds": %d, "full_rounds": %d,
	  "warmup": "3s", "stage": "2s", "stage_warmup": "1s", "survey_stage": "2s", "point_stage": "2s",
	  "rows": 10000000,
	  "contenders": {"A": {"image": "A"}, "B": {"image": "B"}},
	  "families": [{"scenario": "list", "proto": "h1", "contenders": ["A", "B"], "points": [200, 400, 900]}]}`,
		trial, rounds, maxRounds, full))
	if err != nil {
		t.Fatal(err)
	}
	return m
}

type outputs struct {
	raw, plan, rounds bytes.Buffer
	summary           summary
}

func playMatrix(t *testing.T, m matrix, ch chain) (*outputs, error) {
	t.Helper()
	var o outputs
	c := newCampaign(m, ch, &o.raw, &o.plan, io.Discard)
	c.wait = func(context.Context, time.Duration) error { return nil }
	var err error
	o.summary, err = c.run(t.Context(), &o.rounds)
	return &o, err
}

func lines[T any](t *testing.T, b *bytes.Buffer) []T {
	t.Helper()
	var out []T
	sc := bufio.NewScanner(bytes.NewReader(b.Bytes()))
	sc.Buffer(nil, 1<<20)
	for sc.Scan() {
		var v T
		if err := json.Unmarshal(sc.Bytes(), &v); err != nil {
			t.Fatal(err)
		}
		out = append(out, v)
	}
	return out
}

// largest is the grid's largest point at or below rate.
func largest(rate float64) float64 {
	k := stats.MinStep
	for stats.Grid(k+1) <= rate {
		k++
	}
	return stats.Grid(k)
}

// The reduced matrix of the brief, three rounds, played on a chain with no
// noise: the capacities are the grid's points under the true ones, the
// points eligible by the 0.8 rule, one full round drawn after the first,
// and the verdicts refuse three rounds.
func TestTrialCampaign(t *testing.T) {
	m := testMatrix(t, true, 3, 3, 1)
	ch := newFake(map[string]float64{"A": 1000, "B": 1200}, nil)
	o, err := playMatrix(t, m, ch)
	if err != nil {
		t.Fatal(err)
	}
	if o.summary.Publishable || !o.summary.Trial {
		t.Error("a trial marked publishable")
	}
	if !slices.Equal(ch.ups, []string{"0ms", "20ms"}) || ch.downs != 2 {
		t.Errorf("chains built %v, taken down %d times: want capacity at 0ms then points at 20ms", ch.ups, ch.downs)
	}
	rounds := lines[roundLine](t, &o.rounds)
	if len(rounds) != 6 {
		t.Fatalf("%d round lines, want 3 per contender", len(rounds))
	}
	var a, b []float64
	for _, r := range rounds {
		if !r.Trial || r.Capacity == nil || r.P99 == nil {
			t.Fatalf("round line %+v: want both metrics, marked trial", r)
		}
		want := largest(map[string]float64{"A": 1000, "B": 1200}[r.Contender])
		if *r.Capacity != want {
			t.Errorf("%s round %d: capacity %v, want %v", r.Contender, r.Round, *r.Capacity, want)
		}
		if r.Contender == "A" {
			a = append(a, *r.Capacity)
		} else {
			b = append(b, *r.Capacity)
		}
	}
	pts := o.summary.Families[0].Points
	if pts == nil {
		t.Fatal("no fixed-point campaign")
	}
	if pts.Designated != 400 || !slices.Equal(pts.Eligible["A"], []int{200, 400}) || !slices.Equal(pts.Eligible["B"], []int{200, 400, 900}) {
		t.Errorf("designated %d, eligible %v: want 400, A 200 400, B 200 400 900", pts.Designated, pts.Eligible)
	}
	if len(pts.FullRounds) != 1 || pts.FullRounds[0] < 2 {
		t.Errorf("full rounds %v: want one, after the first", pts.FullRounds)
	}
	if d := pts.Descriptive["B"]["900"]; len(d.Values) != 1 || d.Held != 1 || d.P999 == nil {
		t.Errorf("B at 900 req/s: %+v, want its one full round, held, with its p99.9 (counted %d)", d, pointCount)
	}
	_, err = stats.Judge([]stats.Comparison{{Name: "A vs B", Metric: stats.Capacity, A: a, B: b}}, stats.DefaultFloors)
	if err == nil || !strings.Contains(err.Error(), "a verdict needs 21") {
		t.Errorf("verdicts on three rounds: %v, want the refusal", err)
	}
	// Every draw is written down: the survey's order, each round's.
	plan := lines[planLine](t, &o.plan)
	if len(plan) != 1+3+1+3 {
		t.Errorf("%d plan lines, want the survey, 3 capacity rounds, the draw of the full rounds, 3 point rounds", len(plan))
	}
}

// The adaptive stop (§7.2): while one contender's interval is wide,
// everyone plays five rounds more, up to the maximum, and the one still
// wide there is unstable; with both narrow, the minimum is enough.
func TestAdaptiveStop(t *testing.T) {
	for _, tc := range []struct {
		name     string
		noiseB   float64
		rounds   int
		unstable bool
	}{
		{"narrow", 0, 21, false},
		{"wide", 0.25, 31, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := testMatrix(t, false, 21, 31, 7)
			ch := newFake(map[string]float64{"A": 1000, "B": 3000}, map[string]float64{"B": tc.noiseB})
			o, err := playMatrix(t, m, ch)
			if err != nil {
				t.Fatal(err)
			}
			c := o.summary.Families[0].Capacity
			if c.Rounds != tc.rounds || c.Contenders["B"].Unstable != tc.unstable || c.Contenders["A"].Unstable {
				t.Errorf("capacity: %d rounds, B unstable %v, A unstable %v: want %d, %v, false",
					c.Rounds, c.Contenders["B"].Unstable, c.Contenders["A"].Unstable, tc.rounds, tc.unstable)
			}
			if len(c.Contenders["A"].Values) != c.Rounds {
				t.Errorf("A has %d rounds of %d: every contender plays every round", len(c.Contenders["A"].Values), c.Rounds)
			}
			p := o.summary.Families[0].Points
			if p == nil || p.P99.Rounds != tc.rounds || p.P99.Contenders["B"].Unstable != tc.unstable {
				t.Errorf("points: %+v, want %d rounds", p, tc.rounds)
			}
			if p != nil && len(p.FullRounds) != 7 {
				t.Errorf("%d full rounds, want 7", len(p.FullRounds))
			}
		})
	}
}

// A stage the apparatus invalidates replays its whole repetition, a fresh
// server again (§4): one more server, the same rounds, and the replay
// recorded as the next attempt.
func TestInvalidationReplaysRepetition(t *testing.T) {
	m := testMatrix(t, true, 3, 3, 1)
	clean := newFake(map[string]float64{"A": 1000, "B": 1200}, nil)
	if _, err := playMatrix(t, m, clean); err != nil {
		t.Fatal(err)
	}
	ch := newFake(map[string]float64{"A": 1000, "B": 1200}, nil)
	ch.invalidOn = func(n int, s stageSpec) bool { return n >= 30 && s.warmup > 0 && ch.invalids == 0 }
	o, err := playMatrix(t, m, ch)
	if err != nil {
		t.Fatal(err)
	}
	if ch.invalids != 1 {
		t.Fatalf("%d stages invalidated, want 1", ch.invalids)
	}
	if ch.webs["A"]+ch.webs["B"] != clean.webs["A"]+clean.webs["B"]+1 {
		t.Errorf("servers started %v, without the invalidation %v: want one more", ch.webs, clean.webs)
	}
	if n := len(lines[roundLine](t, &o.rounds)); n != 6 {
		t.Errorf("%d round lines, want 6: the invalidated repetition counts once", n)
	}
	var bad record
	retried := false
	for _, r := range lines[record](t, &o.raw) {
		var l struct{ Valid bool }
		_ = json.Unmarshal(r.Line, &l)
		if !l.Valid {
			bad = r
		}
	}
	for _, r := range lines[record](t, &o.raw) {
		if r.Attempt == bad.Attempt+1 && r.Contender == bad.Contender && r.Round == bad.Round && r.Measure == bad.Measure && r.Phase == bad.Phase {
			retried = true
		}
	}
	if bad.Attempt != 1 || !retried {
		t.Errorf("invalid stage %+v: want attempt 1, then attempt 2 of the same repetition", bad)
	}
}

// A ramp that holds to its last stage is censored (§7.6): no capacity, a
// new survey, a wider window, and the repetition played again.
func TestCensoredRampWidens(t *testing.T) {
	m := testMatrix(t, true, 3, 3, 1)
	ch := newFake(map[string]float64{"A": 1000, "B": 1200}, nil)
	ch.later = map[string]float64{"A": 2000}
	o, err := playMatrix(t, m, ch)
	if err != nil {
		t.Fatal(err)
	}
	surveys := 0
	for _, r := range lines[record](t, &o.raw) {
		if r.Contender == "A" && r.Phase == "survey" && r.Kind == "warmup" {
			surveys++
		}
	}
	if surveys != 2 {
		t.Errorf("A surveyed %d times, want 2: the censored ramp asks for a new one", surveys)
	}
	st := o.summary.Families[0].Capacity.Contenders["A"]
	if float64(st.Median) != largest(2000) || st.Window[1] < 2000 {
		t.Errorf("A: median %v, window %v: want %v in a window widened past 2000", st.Median, *st.Window, largest(2000))
	}
}

func TestGridRange(t *testing.T) {
	if got := gridRange(90, 110); !slices.Equal(got, []int{86, 91, 95, 100, 105, 110}) {
		t.Errorf("gridRange(90, 110) = %v", got)
	}
	for _, r := range gridRange(10, 30) {
		if _, err := stats.Step(float64(r)); err != nil {
			t.Errorf("%d off the grid: %v", r, err)
		}
	}
}

func TestMatrixChecks(t *testing.T) {
	base := `"contenders": {"A": {"image": "a"}, "B": {"image": "b"}}, "families": [{"scenario": "unit", "proto": "h1", "contenders": ["A", "B"], "points": [500, 2000]}]`
	for _, tc := range []struct{ body, want string }{
		{`"name": "x", "rounds": 3, ` + base, "trial"},
		{`"name": "x", "rounds": 21, "full_rounds": 21, ` + base, "full_rounds"},
		{`"name": "x", "rounds": 21, "max_rounds": 20, ` + base, "max_rounds"},
		{`"name": "x", "contenders": {"A": {"image": "a"}}, "families": [{"scenario": "unit", "proto": "h1", "contenders": ["A", "C"], "points": [500]}]`, "no contender"},
		{`"name": "x", "contenders": {"A": {"image": "a"}, "B": {"image": "b"}}, "families": [{"scenario": "unit", "proto": "h1", "contenders": ["A", "B"], "points": [2000, 500]}]`, "ascending"},
		{`"name": "x", "stage": "1500ms", ` + base, "whole seconds"},
	} {
		if _, err := parseMatrix([]byte("{" + tc.body + "}")); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: %v, want an error naming %q", tc.body, err, tc.want)
		}
	}
	m, err := parseMatrix([]byte(`{"name": "x", ` + base + `}`))
	if err != nil || m.Rounds != 21 || m.MaxRounds != 40 || m.Extend != 5 || m.FullRounds != 7 || m.Warmup.Duration != time.Minute {
		t.Errorf("defaults: %+v %v", m, err)
	}
}

func TestParseStage(t *testing.T) {
	r, err := parseStage([]byte(`{"held":false,"cause":"slo","sent":10,"ok":0,"ttlb_ms":{"p99":3},"valid":false}`))
	if err != nil || !r.invalid || r.held || r.cause != "slo" || !math.IsNaN(r.p99) {
		t.Errorf("%+v %v", r, err)
	}
	if _, err := parseStage([]byte(`{"held":true}`)); err == nil {
		t.Error("a generator's line, not merged, accepted")
	}
	if seconds(1500*time.Millisecond) != "2s" {
		t.Error("seconds rounds down")
	}
}
