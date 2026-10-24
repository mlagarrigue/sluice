package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"slices"
	"strconv"
	"time"

	"github.com/mlagarrigue/sluice/comparison/loadgen/stats"
)

// errInvalid is a stage the apparatus invalidated (§4): its repetition is
// replayed whole, never the stage alone.
var errInvalid = errors.New("a stage invalidated by the apparatus")

const (
	// maxAttempts is how many times in a row a repetition may be
	// invalidated before the campaign stops: past it, the apparatus is
	// what needs fixing.
	maxAttempts = 5
	// maxWiden is how many times a repetition's window may be widened
	// after a censored capacity (§7.6).
	maxWiden = 4
	// surveyFactor is the survey's step (§3.3).
	surveyFactor = 1.25
	// Requests a stage must count for its percentile (§7.3): a capacity
	// stage decides on the p99, a fixed point publishes the p99.9.
	capacityCount = 1000
	pointCount    = 10000
)

// campaign plays a matrix on a chain. Its draws come from one seed and are
// written down; every stage's line goes to the raw records.
type campaign struct {
	m    matrix
	ch   chain
	rng  *rand.Rand
	raw  *json.Encoder
	plan *json.Encoder
	log  io.Writer
	wait func(context.Context, time.Duration) error
}

func newCampaign(m matrix, ch chain, raw, plan, log io.Writer) *campaign {
	return &campaign{
		m: m, ch: ch, rng: rand.New(rand.NewPCG(m.Seed, 0x5eed)), //nolint:gosec // G404: a published draw, reproducible from its seed
		raw: json.NewEncoder(raw), plan: json.NewEncoder(plan), log: log,
		wait: func(ctx context.Context, d time.Duration) error {
			t := time.NewTimer(d)
			defer t.Stop()
			select {
			case <-t.C:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	}
}

// record is one raw line: a stage's merged line and where it sits in the
// campaign.
type record struct {
	Family    string          `json:"family"`
	Measure   string          `json:"measure"` // capacity or points
	Phase     string          `json:"phase"`   // survey or round
	Round     int             `json:"round,omitempty"`
	Contender string          `json:"contender"`
	Attempt   int             `json:"attempt"` // the repetition's, from 1: a replay after an invalidation is the next
	Kind      string          `json:"kind"`    // warmup, survey, ramp or point
	Rate      int             `json:"rate"`
	Line      json.RawMessage `json:"line"`
}

// planLine is one draw of the campaign, published with it (§7.1, §7.6).
type planLine struct {
	Family     string   `json:"family"`
	Measure    string   `json:"measure"`
	Phase      string   `json:"phase"`
	Round      int      `json:"round,omitempty"`
	Order      []string `json:"order,omitempty"`
	FullRounds []int    `json:"full_rounds,omitempty"`
	Designated int      `json:"designated,omitempty"`
}

// roundLine is what loadgen -verdicts reads: one contender's round, its
// two primary metrics. A capacity round and a fixed-point round of the
// same index share the line; they are paired separately all the same.
type roundLine struct {
	Scenario  string   `json:"scenario"`
	Proto     string   `json:"proto"`
	Contender string   `json:"contender"`
	Round     int      `json:"round"`
	Capacity  *float64 `json:"capacity_rps,omitempty"`
	P99       *float64 `json:"p99_ms,omitempty"`
	Trial     bool     `json:"trial,omitempty"`
}

// summary is the campaign's account of itself: windows, medians and
// intervals, who was unstable, which points were measured.
type summary struct {
	Name        string          `json:"name"`
	Seed        uint64          `json:"seed"`
	Trial       bool            `json:"trial"`
	Publishable bool            `json:"publishable"`
	Families    []familySummary `json:"families"`
}

type familySummary struct {
	Family   string         `json:"family"`
	Capacity metricSummary  `json:"capacity"`
	Points   *pointsSummary `json:"points"`
	Note     string         `json:"note,omitempty"`
}

type metricSummary struct {
	Rounds     int                      `json:"rounds"`
	Contenders map[string]contenderStat `json:"contenders"`
}

// contenderStat is one contender's primary metric over the useful rounds
// (the first dropped): the median, its 95 % rank interval, and whether the
// interval was still wider than the adaptive stop allows at the last round
// (§7.2: published as unstable, not as a figure).
type contenderStat struct {
	Window   *[2]int `json:"window,omitempty"` // the survey's last held and first broken stages, capacity only
	Median   num     `json:"median"`
	Lo       num     `json:"lo"`
	Hi       num     `json:"hi"`
	Unstable bool    `json:"unstable"`
	Values   []num   `json:"values"` // every round, the first included
}

type pointsSummary struct {
	Designated  int                             `json:"designated"`
	Eligible    map[string][]int                `json:"eligible"`
	FullRounds  []int                           `json:"full_rounds"`
	P99         metricSummary                   `json:"p99"`
	Descriptive map[string]map[string]described `json:"descriptive"` // contender → point → its p99
}

// described is a descriptive point (§7.2): the median of its full rounds
// and the interval from their lowest to their highest value, whose
// confidence is 1 − 2 × 2^−n (98 % at 7 rounds). The p99.9 is published
// only from stages that counted their 10 000 requests (§7.3).
type described struct {
	Median     num      `json:"median"`
	Lo         num      `json:"lo"`
	Hi         num      `json:"hi"`
	Confidence float64  `json:"confidence"`
	Values     []num    `json:"values"`
	Held       int      `json:"held"` // stages under the SLO; the others are published "past SLO"
	P999       *float64 `json:"p999_ms,omitempty"`
}

// run plays every family of the matrix, one after another: a scenario
// stays whole on one chain (§7.6), and its capacity is done before its
// points, which need it (§7.1).
func (c *campaign) run(ctx context.Context, rounds io.Writer) (summary, error) {
	s := summary{Name: c.m.Name, Seed: c.m.Seed, Trial: c.m.Trial, Publishable: !c.m.Trial}
	enc := json.NewEncoder(rounds)
	for _, f := range c.m.Families {
		fs, lines, err := c.family(ctx, f)
		s.Families = append(s.Families, fs)
		if err != nil {
			return s, fmt.Errorf("%s: %w", f, err)
		}
		for i := range lines {
			if err := enc.Encode(&lines[i]); err != nil {
				return s, err
			}
		}
	}
	return s, nil
}

func (c *campaign) family(ctx context.Context, f family) (familySummary, []roundLine, error) {
	fs := familySummary{Family: f.String()}
	var caps map[string][]float64
	err := c.on(ctx, f, c.m.Network.CapacityRTT, func() error {
		var err error
		caps, fs.Capacity, err = c.capacity(ctx, f)
		return err
	})
	if err != nil {
		return fs, nil, err
	}
	var p99 map[string][]float64
	eligible, designated := c.eligible(f, fs.Capacity)
	if designated == 0 {
		fs.Note = "no fixed point is eligible for every contender (P ≤ 0.8 × the low bound of each capacity, §3.2): no fixed-point campaign"
	} else {
		err = c.on(ctx, f, c.m.Network.PointsRTT, func() error {
			var err error
			p99, fs.Points, err = c.points(ctx, f, eligible, designated)
			return err
		})
		if err != nil {
			return fs, nil, err
		}
	}
	var lines []roundLine
	for _, name := range f.Contenders {
		n := max(len(caps[name]), len(p99[name]))
		for r := range n {
			l := roundLine{Scenario: f.Scenario, Proto: f.Proto, Contender: name, Round: r + 1, Trial: c.m.Trial}
			if r < len(caps[name]) {
				l.Capacity = &caps[name][r]
			}
			if r < len(p99[name]) && !math.IsNaN(p99[name][r]) {
				l.P99 = &p99[name][r]
			}
			lines = append(lines, l)
		}
	}
	return fs, lines, nil
}

// on runs body on a chain built for the family under one client round
// trip, and takes the chain down after, whatever body returned.
func (c *campaign) on(ctx context.Context, f family, rtt string, body func() error) error {
	if err := c.ch.up(ctx, f, rtt); err != nil {
		_ = c.ch.down(context.WithoutCancel(ctx))
		return fmt.Errorf("chain up: %w", err)
	}
	err := body()
	if derr := c.ch.down(context.WithoutCancel(ctx)); err == nil && derr != nil {
		err = fmt.Errorf("chain down: %w", derr)
	}
	return err
}

// play runs one stage of a repetition and records it.
type play func(kind string, s stageSpec) (stageResult, error)

// repeat plays one repetition (§7.1): a fresh server, its warm-up at the
// first stage's rate, then body's stages. A stage the apparatus
// invalidates replays the whole repetition, a fresh server again; the
// warm-up's own validity decides nothing, it is never measured.
func (c *campaign) repeat(ctx context.Context, tag record, warmRate int, seed uint64, body func(play) error) error {
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		tag.Attempt = attempt
		if err := c.ch.web(ctx, c.m.Contenders[tag.Contender]); err != nil {
			return fmt.Errorf("chain web %s: %w", tag.Contender, err)
		}
		staged := false
		stage := func(kind string, s stageSpec) (stageResult, error) {
			if staged {
				if err := c.wait(ctx, c.m.Drain.Duration); err != nil {
					return stageResult{}, err
				}
			}
			staged = true
			r, err := c.ch.stage(ctx, s)
			if err != nil {
				return r, err
			}
			rec := tag
			rec.Kind, rec.Rate, rec.Line = kind, s.rate, r.line
			if err := c.raw.Encode(&rec); err != nil {
				return r, err
			}
			if r.invalid && kind != "warmup" {
				return r, errInvalid
			}
			return r, nil
		}
		// The warm-up is cut into stages the keyspace holds: a stage never
		// repeats a key (§2.1), and a minute at a high rate would.
		left := c.m.Warmup.Duration
		chunk := time.Duration(c.m.Rows/max(warmRate, 1)) * time.Second
		if chunk < time.Second {
			return fmt.Errorf("%d req/s for a second is more than the %d rows of the keyspace: raise rows", warmRate, c.m.Rows)
		}
		var err error
		for left > 0 && err == nil {
			d := min(left, chunk)
			_, err = stage("warmup", stageSpec{rate: warmRate, duration: d, seed: seed})
			left -= d
		}
		if err == nil {
			err = body(stage)
		}
		if !errors.Is(err, errInvalid) {
			return err
		}
		c.logf("campaign: %s %s %s %s %d: invalidated, the repetition is replayed (attempt %d)\n",
			tag.Family, tag.Measure, tag.Contender, tag.Phase, tag.Round, attempt+1)
	}
	return fmt.Errorf("%s: %d repetitions in a row invalidated by the apparatus: fix the chain, not the contender", tag.Contender, maxAttempts)
}

// seed is the generator's draw for a round, the same for every contender
// of it (§2.1).
func (c *campaign) seed(round int) uint64 { return c.m.Seed<<20 | uint64(round) } //nolint:gosec // G115: a round is small and positive

// shuffled is the order of a round, drawn (§7.1).
func (c *campaign) shuffled(names []string) []string {
	out := make([]string, len(names))
	for i, j := range c.rng.Perm(len(names)) {
		out[i] = names[j]
	}
	return out
}

// spec is a stage at rate, at least long enough to count the requests its
// percentile needs (§7.3), and refused if the keyspace cannot hold it.
func (c *campaign) spec(rate int, d time.Duration, count int, seed uint64) (stageSpec, error) {
	need := time.Duration(math.Ceil(float64(count)/float64(rate))) * time.Second
	s := stageSpec{rate: rate, duration: max(d, need), warmup: c.m.StageWarmup.Duration, seed: seed}
	if n := float64(rate) * (s.duration + s.warmup).Seconds(); n > float64(c.m.Rows) {
		return s, fmt.Errorf("a stage of %s at %d req/s sends %.0f requests, more than the %d rows of the keyspace: raise rows (§2.1, a stage never repeats a key)", s.duration+s.warmup, rate, n, c.m.Rows)
	}
	return s, nil
}

// capacity is the capacity campaign of a family (§7.6): a survey per
// contender, then rounds of ramps on the common grid, extended by the
// adaptive stop. It returns every contender's capacity by round.
func (c *campaign) capacity(ctx context.Context, f family) (map[string][]float64, metricSummary, error) {
	ms := metricSummary{Contenders: map[string]contenderStat{}}
	win := map[string][2]int{}
	order := c.shuffled(f.Contenders)
	if err := c.plan.Encode(planLine{Family: f.String(), Measure: "capacity", Phase: "survey", Order: order}); err != nil {
		return nil, ms, err
	}
	for _, name := range order {
		lo, hi, err := c.survey(ctx, f, name, f.SurveyFrom)
		if err != nil {
			return nil, ms, err
		}
		win[name] = [2]int{lo, hi}
		c.logf("campaign: %s survey %s: held %d, broke at %d req/s\n", f, name, lo, hi)
	}
	values := map[string][]float64{}
	wide := func() []string {
		var out []string
		for _, name := range f.Contenders {
			if capacityWide(values[name][1:]) {
				out = append(out, name)
			}
		}
		return out
	}
	rounds, err := c.rounds(ctx, f, "capacity", wide, func(r int, order []string) error {
		for _, name := range order {
			v, err := c.capacityRep(ctx, f, name, r, win)
			if err != nil {
				return err
			}
			values[name] = append(values[name], float64(v))
			c.logf("campaign: %s capacity round %d %s: %d req/s\n", f, r, name, v)
		}
		return nil
	})
	if err != nil {
		return nil, ms, err
	}
	ms.Rounds = rounds
	unstable := wide()
	for _, name := range f.Contenders {
		w := win[name]
		st := statOf(values[name], slices.Contains(unstable, name))
		st.Window = &w
		ms.Contenders[name] = st
	}
	return values, ms, nil
}

// rounds plays rounds of one measure until the adaptive stop (§7.2): the
// minimum, then extend more for everyone while wide names a contender,
// up to the maximum. Each round's order is drawn and written down. It
// returns how many rounds were played.
func (c *campaign) rounds(ctx context.Context, f family, measure string, wide func() []string,
	round func(r int, order []string) error,
) (int, error) {
	target := c.m.Rounds
	for r := 1; ; r++ {
		if r > target {
			w := wide()
			if len(w) == 0 || target >= c.m.MaxRounds {
				return r - 1, nil
			}
			target = min(target+c.m.Extend, c.m.MaxRounds)
			c.logf("campaign: %s %s: %v still wide past ±5 %%, everyone plays to round %d (§7.2)\n", f, measure, w, target)
		}
		pl := planLine{Family: f.String(), Measure: measure, Phase: "round", Round: r, Order: c.shuffled(slices.Clone(f.Contenders))}
		if err := c.plan.Encode(pl); err != nil {
			return r, err
		}
		if err := round(r, pl.Order); err != nil {
			return r, err
		}
	}
}

// capacityWide says whether the 95 % interval of a median capacity covers
// more than one step on either side of it (§7.2, in whole steps).
func capacityWide(useful []float64) bool {
	steps := make([]float64, len(useful))
	for i, v := range useful {
		k, err := stats.Step(v)
		if err != nil {
			return true
		}
		steps[i] = float64(k)
	}
	if len(steps) == 0 {
		return true
	}
	med, lo, hi := stats.MedianCI(steps)
	return med-lo > 1 || hi-med > 1
}

// p99Wide says whether the 95 % interval of a median p99 reaches past ±5 %
// of it (§7.2). A round with no answer counts as wide.
func p99Wide(useful []float64) bool {
	if len(useful) == 0 || slices.ContainsFunc(useful, math.IsNaN) {
		return true
	}
	med, lo, hi := stats.MedianCI(useful)
	return lo < 0.95*med || hi > 1.05*med
}

// survey is the coarse ramp of §3.3, ×1.25 from `from` until a stage
// breaks, in a repetition of its own: the last stage held and the first
// broken. A survey broken at its first stage starts again four times
// lower.
func (c *campaign) survey(ctx context.Context, f family, name string, from int) (lo, hi int, err error) {
	for {
		tag := record{Family: f.String(), Measure: "capacity", Phase: "survey", Contender: name}
		err = c.repeat(ctx, tag, from, c.seed(0), func(stage play) error {
			lo, hi = 0, 0
			for i := 0; ; i++ {
				rate := int(math.Round(float64(from) * math.Pow(surveyFactor, float64(i))))
				s, err := c.spec(rate, c.m.SurveyStage.Duration, capacityCount, c.seed(0))
				if err != nil {
					return fmt.Errorf("the survey of %s holds %d req/s: %w", name, lo, err)
				}
				r, err := stage("survey", s)
				if err != nil {
					return err
				}
				if !r.held {
					hi = rate
					return nil
				}
				lo = rate
			}
		})
		if err != nil || lo > 0 {
			return lo, hi, err
		}
		if from == 1 {
			return 0, 0, fmt.Errorf("%s breaks at 1 req/s", name)
		}
		from = max(1, from/4)
	}
}

// gridRange is the stages of a ramp (§7.6): the common grid from its
// largest point at or below lo to its smallest at or above hi.
func gridRange(lo, hi float64) []int {
	k := stats.MinStep
	for stats.Grid(k+1) <= lo {
		k++
	}
	var out []int
	for ; ; k++ {
		out = append(out, int(stats.Grid(k)))
		if stats.Grid(k) >= hi {
			return out
		}
	}
}

// capacityRep is one capacity repetition (§7.6): the ramp on the common
// grid from 0.9 L to 1.1 H, stopped at its first broken stage, the
// capacity the stage before it. No broken stage is replayed. A censored
// ramp — broken at its first stage, or held to its last — is no capacity:
// the window is widened by a new survey and the repetition played again.
func (c *campaign) capacityRep(ctx context.Context, f family, name string, round int, win map[string][2]int) (int, error) {
	for widen := 0; ; widen++ {
		w := win[name]
		rates := gridRange(0.9*float64(w[0]), 1.1*float64(w[1]))
		specs := make([]stageSpec, len(rates))
		for i, rate := range rates {
			var err error
			if specs[i], err = c.spec(rate, c.m.Stage.Duration, capacityCount, c.seed(round)); err != nil {
				return 0, err
			}
		}
		capacity, censored := 0, ""
		tag := record{Family: f.String(), Measure: "capacity", Phase: "round", Round: round, Contender: name}
		err := c.repeat(ctx, tag, rates[0], c.seed(round), func(stage play) error {
			capacity, censored = 0, "high"
			for i, s := range specs {
				r, err := stage("ramp", s)
				if err != nil {
					return err
				}
				if !r.held {
					if i == 0 {
						censored = "low"
					} else {
						capacity, censored = rates[i-1], ""
					}
					return nil
				}
			}
			return nil
		})
		if err != nil || censored == "" {
			return capacity, err
		}
		if widen == maxWiden {
			return 0, fmt.Errorf("%s round %d: the ramp still censored (%s) after %d new surveys", name, round, censored, maxWiden)
		}
		c.logf("campaign: %s round %d %s: capacity out of the window %v (%s), a new survey widens it\n", f, round, name, w, censored)
		lo, hi, err := c.survey(ctx, f, name, f.SurveyFrom)
		if err != nil {
			return 0, err
		}
		nw := [2]int{min(w[0], lo), max(w[1], hi)}
		if censored == "low" {
			nw[0] = min(nw[0], int(float64(w[0])/surveyFactor))
		} else {
			nw[1] = max(nw[1], int(math.Ceil(float64(w[1])*surveyFactor)))
		}
		win[name] = nw
	}
}

// eligible is, per contender, the fixed points it is measured at — P at
// most 0.8 × the low bound of its capacity's interval (§3.2) — and the
// designated point, the highest one eligible for all, 0 for none (§7.4).
func (c *campaign) eligible(f family, capacity metricSummary) (eligible map[string][]int, designated int) {
	eligible = map[string][]int{}
	for _, p := range f.Points {
		all := true
		for _, name := range f.Contenders {
			if float64(p) <= 0.8*float64(capacity.Contenders[name].Lo) {
				eligible[name] = append(eligible[name], p)
			} else {
				all = false
			}
		}
		if all {
			designated = p
		}
	}
	return eligible, designated
}

// points is the fixed-point campaign of a family (§7.6): every round at
// the designated point alone but the full rounds, drawn among the rounds
// after the first and written down, at every eligible point; the points
// of a repetition in one process, ascending (§7.1). Rounds added by the
// adaptive stop are at the designated point alone. It returns every
// contender's p99 at the designated point by round.
func (c *campaign) points(ctx context.Context, f family, eligible map[string][]int, designated int) (map[string][]float64, *pointsSummary, error) {
	ps := &pointsSummary{
		Designated: designated, Eligible: eligible,
		Descriptive: map[string]map[string]described{}, P99: metricSummary{Contenders: map[string]contenderStat{}},
	}
	for _, i := range c.rng.Perm(c.m.Rounds - 1)[:c.m.FullRounds] {
		ps.FullRounds = append(ps.FullRounds, i+2)
	}
	slices.Sort(ps.FullRounds)
	if err := c.plan.Encode(planLine{Family: f.String(), Measure: "points", Phase: "draw", FullRounds: ps.FullRounds, Designated: designated}); err != nil {
		return nil, nil, err
	}
	p99 := map[string][]float64{}
	full := map[string]map[int][]stageResult{}
	wide := func() []string {
		var out []string
		for _, name := range f.Contenders {
			if p99Wide(p99[name][1:]) {
				out = append(out, name)
			}
		}
		return out
	}
	rounds, err := c.rounds(ctx, f, "points", wide, func(r int, order []string) error {
		isFull := slices.Contains(ps.FullRounds, r)
		for _, name := range order {
			pts := []int{designated}
			if isFull {
				pts = eligible[name]
			}
			specs := make([]stageSpec, len(pts))
			for i, p := range pts {
				var err error
				if specs[i], err = c.spec(p, c.m.PointStage.Duration, pointCount, c.seed(r)); err != nil {
					return err
				}
			}
			var got []stageResult
			tag := record{Family: f.String(), Measure: "points", Phase: "round", Round: r, Contender: name}
			err := c.repeat(ctx, tag, pts[0], c.seed(r), func(stage play) error {
				got = got[:0]
				for _, s := range specs {
					res, err := stage("point", s)
					if err != nil {
						return err
					}
					got = append(got, res)
				}
				return nil
			})
			if err != nil {
				return err
			}
			d := got[slices.Index(pts, designated)]
			p99[name] = append(p99[name], d.p99)
			c.logf("campaign: %s points round %d %s: p99 %.2f ms at %d req/s\n", f, r, name, d.p99, designated)
			if isFull {
				if full[name] == nil {
					full[name] = map[int][]stageResult{}
				}
				for i, p := range pts {
					full[name][p] = append(full[name][p], got[i])
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, ps, err
	}
	ps.P99.Rounds = rounds
	unstable := wide()
	for _, name := range f.Contenders {
		ps.P99.Contenders[name] = statOf(p99[name], slices.Contains(unstable, name))
		ps.Descriptive[name] = map[string]described{}
		for p, rs := range full[name] {
			ps.Descriptive[name][strconv.Itoa(p)] = describe(rs)
		}
	}
	return p99, ps, nil
}

// statOf is a primary metric's account: the median and its 95 % rank
// interval over the useful rounds, the first one dropped (§7.1), a round
// with no answer left out.
func statOf(values []float64, unstable bool) contenderStat {
	med, lo, hi := stats.MedianCI(slices.DeleteFunc(slices.Clone(values[1:]), math.IsNaN))
	return contenderStat{Median: num(med), Lo: num(lo), Hi: num(hi), Unstable: unstable, Values: nums(values)}
}

// describe is a descriptive point over its full rounds.
func describe(rs []stageResult) described {
	var d described
	var p99, p999 []float64
	for _, r := range rs {
		d.Values = append(d.Values, num(r.p99))
		if r.held {
			d.Held++
		}
		if !math.IsNaN(r.p99) {
			p99 = append(p99, r.p99)
		}
		if r.counted >= pointCount && !math.IsNaN(r.p999) {
			p999 = append(p999, r.p999)
		}
	}
	d.Median, d.Lo, d.Hi = num(math.NaN()), num(math.NaN()), num(math.NaN())
	if n := len(p99); n > 0 {
		slices.Sort(p99)
		d.Median, d.Lo, d.Hi = num(median(p99)), num(p99[0]), num(p99[n-1])
		d.Confidence = 1 - 2*math.Pow(0.5, float64(n))
	}
	if len(p999) > 0 {
		slices.Sort(p999)
		m := median(p999)
		d.P999 = &m
	}
	return d
}

// median of sorted values.
func median(s []float64) float64 {
	n := len(s)
	if n%2 == 1 {
		return s[n/2]
	}
	return (s[n/2-1] + s[n/2]) / 2
}

// num is a figure of the summary: NaN, a metric with no value, is null.
type num float64

func (n num) MarshalJSON() ([]byte, error) {
	if math.IsNaN(float64(n)) || math.IsInf(float64(n), 0) {
		return []byte("null"), nil
	}
	return json.Marshal(float64(n))
}

func nums(xs []float64) []num {
	out := make([]num, len(xs))
	for i, x := range xs {
		out[i] = num(x)
	}
	return out
}

// logf writes the campaign's progress; a log that fails stops nothing.
func (c *campaign) logf(format string, args ...any) { _, _ = fmt.Fprintf(c.log, format, args...) }
