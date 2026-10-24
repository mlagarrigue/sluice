// Package stats turns the rounds of a campaign into the study's verdicts
// (§7.1 to §7.5 of its protocol): per comparison, the paired sign test over
// the rounds, Holm's correction over the family, the size of the gap with
// its rank interval, and one of four verdicts — faster, slower, equivalent,
// inconclusive. A verdict comes out of this code, never out of a reading.
//
// The method is the repository's own paired.awk (internal/bench/scripts),
// carried on: the first round is dropped, ties are dropped from the sign
// test, the gap is the median of per-round ratios (never the ratio of
// medians), and a significant gap under the floor is no difference. What
// it adds is what a published comparison needs and a regression gate does
// not: the p-value, Holm over a declared family, the equivalence test, and
// capacity in whole steps of the grid, so that no float decides a verdict.
package stats

import (
	"errors"
	"fmt"
	"math"
	"slices"
)

// MinRounds is the rounds a verdict needs, the first one dropped (§7.2):
// with a family of 46 comparisons, Holm's first threshold is 0.05/46 and
// the sign test cannot reach it under 11 untied rounds; 20 useful rounds
// leave room for ties and lost rounds.
const MinRounds = 21

// Metric is what a comparison is about. The two primary metrics are the
// only ones that receive a verdict (§7.4).
type Metric int

const (
	// Capacity is the highest rate held under the SLO, read as a step of
	// the grid 100 × 1.05^k: higher is better, gaps are whole steps.
	Capacity Metric = iota
	// P99 is the p99 TTLB at the designated fixed point: lower is better,
	// gaps are per-round ratios.
	P99
)

func (m Metric) String() string {
	if m == Capacity {
		return "capacity"
	}
	return "p99"
}

// Verdicts, in the order §7.4 tries them.
const (
	Faster       = "faster"
	Slower       = "slower"
	Equivalent   = "equivalent"
	Inconclusive = "inconclusive"
)

// Comparison is A against B on one metric, round by round: A[i] and B[i]
// were measured in the same round. For Capacity the values are rates on
// the grid (req/s), for P99 milliseconds.
type Comparison struct {
	Name   string
	Metric Metric
	A, B   []float64
}

// Floors are what the apparatus cannot tell apart, one per primary metric
// (§7.4): for capacity in steps (at least one), for p99 as a fraction (at
// least MINEFFECT, 3 %), each raised by the largest A/A gap (§7.5) and,
// for p99, the generator's tail inflation over the smaller p99.
type Floors struct {
	CapacitySteps int
	P99           float64
}

// DefaultFloors are the floors before any A/A has been measured.
var DefaultFloors = Floors{CapacitySteps: 1, P99: 0.03}

// Result is one comparison judged.
type Result struct {
	Name   string  `json:"name"`
	Metric string  `json:"metric"`
	Rounds int     `json:"rounds"` // the useful rounds, the first dropped
	Wins   int     `json:"wins"`   // rounds A did better
	Losses int     `json:"losses"` // rounds B did better; the rest are ties
	P      float64 `json:"p"`      // the two-sided sign test
	Holm   float64 `json:"holm_threshold"`
	// Gap is A against B: for capacity the median step difference kA − kB,
	// for p99 the median ratio A/B; Lo and Hi its 95 % rank interval.
	Gap     float64 `json:"gap"`
	Lo      float64 `json:"lo"`
	Hi      float64 `json:"hi"`
	Percent string  `json:"percent"` // the gap as the reader reads it
	Margin  float64 `json:"margin"`  // the equivalence margin it was judged against
	Verdict string  `json:"verdict"`
}

// Judge gives every comparison of one family its verdict: the sign test
// per comparison, Holm over all of them, then §7.4's three rules in order.
// The family is what was declared before the campaign — a scenario × a
// protocol, both primary metrics; its size sets Holm's thresholds.
func Judge(family []Comparison, floors Floors) ([]Result, error) {
	results := make([]Result, len(family))
	for i, c := range family {
		r, err := paired(c)
		if err != nil {
			return nil, fmt.Errorf("%s (%s): %w", c.Name, c.Metric, err)
		}
		results[i] = r
	}
	ps := make([]float64, len(results))
	for i := range results {
		ps[i] = results[i].P
	}
	significant, thresholds := Holm(ps, 0.05)
	for i, c := range family {
		results[i].Holm = thresholds[i]
		decide(&results[i], significant[i], c.Metric, floors)
	}
	return results, nil
}

// paired computes the sign test and the gap of one comparison.
func paired(c Comparison) (Result, error) {
	r := Result{Name: c.Name, Metric: c.Metric.String()}
	if len(c.A) != len(c.B) {
		return r, errors.New("the two sides differ in rounds")
	}
	if len(c.A) < MinRounds {
		return r, fmt.Errorf("%d rounds, a verdict needs %d (§7.2)", len(c.A), MinRounds)
	}
	var gaps []float64
	for i := 1; i < len(c.A); i++ { // the first round absorbs the machine's ramp (§7.1)
		a, b := c.A[i], c.B[i]
		var better, worse bool
		switch c.Metric {
		case Capacity:
			ka, err := Step(a)
			if err != nil {
				return r, err
			}
			kb, err := Step(b)
			if err != nil {
				return r, err
			}
			gaps = append(gaps, float64(ka-kb))
			better, worse = ka > kb, ka < kb
		case P99:
			if a <= 0 || b <= 0 {
				return r, fmt.Errorf("round %d: a p99 of %v against %v", i, a, b)
			}
			gaps = append(gaps, a/b)
			better, worse = a < b, a > b
		}
		switch {
		case better:
			r.Wins++
		case worse:
			r.Losses++
		}
	}
	r.Rounds = len(gaps)
	r.P = SignTest(r.Wins, r.Losses)
	r.Gap, r.Lo, r.Hi = MedianCI(gaps)
	return r, nil
}

// decide applies §7.4: significant after Holm and at least the floor is a
// difference; otherwise an interval inside the margin is an equivalence;
// otherwise nothing can be said.
func decide(r *Result, significant bool, m Metric, f Floors) {
	switch m {
	case Capacity:
		floor := float64(max(f.CapacitySteps, 1))
		r.Margin = floor
		r.Percent = fmt.Sprintf("%+.1f%%", 100*(math.Pow(1.05, r.Gap)-1))
		switch {
		case significant && math.Abs(r.Gap) >= floor:
			r.Verdict = pick(r.Gap > 0)
		case r.Lo >= -floor && r.Hi <= floor:
			r.Verdict = Equivalent
		default:
			r.Verdict = Inconclusive
		}
	case P99:
		floor := max(f.P99, 0.03)
		r.Margin = max(floor, 0.05)
		r.Percent = fmt.Sprintf("%+.1f%%", 100*(r.Gap-1))
		switch {
		case significant && math.Abs(r.Gap-1) >= floor:
			r.Verdict = pick(r.Gap < 1)
		case r.Lo >= 1-r.Margin && r.Hi <= 1+r.Margin:
			r.Verdict = Equivalent
		default:
			r.Verdict = Inconclusive
		}
	}
}

func pick(aBetter bool) string {
	if aBetter {
		return Faster
	}
	return Slower
}

// SignTest is the exact two-sided sign test over the untied rounds: twice
// the binomial tail of the larger count, at most 1.
func SignTest(wins, losses int) float64 {
	n := wins + losses
	if n == 0 {
		return 1
	}
	k := max(wins, losses)
	// P(X >= k) for X ~ B(n, 1/2), summed in log space so that no
	// binomial coefficient overflows at any round count.
	tail := 0.0
	for j := k; j <= n; j++ {
		tail += math.Exp(logChoose(n, j) - float64(n)*math.Ln2)
	}
	return min(1, 2*tail)
}

func logChoose(n, k int) float64 {
	a, _ := math.Lgamma(float64(n + 1))
	b, _ := math.Lgamma(float64(k + 1))
	c, _ := math.Lgamma(float64(n - k + 1))
	return a - b - c
}

// Holm is Holm's step-down correction at level alpha: the smallest p must
// pass alpha/n, the next alpha/(n−1), and the first that fails stops every
// larger one. It returns which hypotheses are rejected, and the threshold
// each was held to (in their original order).
func Holm(ps []float64, alpha float64) (rejected []bool, thresholds []float64) {
	n := len(ps)
	order := make([]int, n)
	for i := range order {
		order[i] = i
	}
	slices.SortStableFunc(order, func(i, j int) int {
		switch {
		case ps[i] < ps[j]:
			return -1
		case ps[i] > ps[j]:
			return 1
		}
		return 0
	})
	rejected = make([]bool, n)
	thresholds = make([]float64, n)
	stopped := false
	for rank, i := range order {
		thresholds[i] = alpha / float64(n-rank)
		if !stopped && ps[i] <= thresholds[i] {
			rejected[i] = true
			continue
		}
		stopped = true
	}
	return rejected, thresholds
}

// MedianCI is the median of xs and its 95 % interval by ranks: the exact
// binomial interval, from the k-th to the (n+1−k)-th smallest value, k the
// largest rank with P(X < k) ≤ 2.5 % for X ~ B(n, 1/2) (§7.4; with 20
// values, the 6th to the 15th). Under 6 values no rank interval reaches
// 95 %, and the interval is the whole range.
func MedianCI(xs []float64) (med, lo, hi float64) {
	s := slices.Sorted(slices.Values(xs))
	n := len(s)
	if n == 0 {
		return math.NaN(), math.NaN(), math.NaN()
	}
	if n%2 == 1 {
		med = s[n/2]
	} else {
		med = (s[n/2-1] + s[n/2]) / 2
	}
	k := 0 // P(X <= k-1) <= 0.025, k as large as it goes
	cum := 0.0
	for j := range n {
		cum += math.Exp(logChoose(n, j) - float64(n)*math.Ln2)
		if cum > 0.025 {
			break
		}
		k = j + 1
	}
	if k == 0 {
		return med, s[0], s[n-1]
	}
	return med, s[k-1], s[n-k]
}

// Step is the index k of a rate on the study's grid, round(100 × 1.05^k)
// (loadgen's gridFrom): a capacity is always one of its points, so a rate
// off the grid is refused rather than rounded.
func Step(rate float64) (int, error) {
	k := int(math.Round(math.Log(rate/100) / math.Log(1.05)))
	for _, c := range []int{k - 1, k, k + 1} {
		if c >= 0 && math.Round(100*math.Pow(1.05, float64(c))) == rate {
			return c, nil
		}
	}
	return 0, fmt.Errorf("%v req/s is not a point of the grid 100 × 1.05^k", rate)
}
