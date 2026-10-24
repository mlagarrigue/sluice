package stats

import (
	"math"
	"math/rand/v2"
	"testing"
)

// grid is the rate of step k, as loadgen's gridFrom writes it.
func grid(k int) float64 { return math.Round(100 * math.Pow(1.05, float64(k))) }

// rounds draws n rounds of a metric: the base value times a per-round
// noise, shared by both sides (the machine's drift, which pairing
// cancels), and a noise of each side's own.
func p99Rounds(seed uint64, n int, a, b, shared, own float64) (as, bs []float64) {
	r := rand.New(rand.NewPCG(seed, 7)) //nolint:gosec // G404: a seeded draw is the point
	for range n {
		drift := 1 + shared*r.NormFloat64()
		as = append(as, a*drift*(1+own*r.NormFloat64()))
		bs = append(bs, b*drift*(1+own*r.NormFloat64()))
	}
	return as, bs
}

func capacityRounds(seed uint64, n, ka, kb int, spread float64) (as, bs []float64) {
	r := rand.New(rand.NewPCG(seed, 9)) //nolint:gosec // G404: a seeded draw is the point
	for range n {
		as = append(as, grid(ka+int(math.Round(spread*r.NormFloat64()))))
		bs = append(bs, grid(kb+int(math.Round(spread*r.NormFloat64()))))
	}
	return as, bs
}

func judgeOne(t *testing.T, c Comparison) Result {
	t.Helper()
	rs, err := Judge([]Comparison{c}, DefaultFloors)
	if err != nil {
		t.Fatal(err)
	}
	return rs[0]
}

// TestClearGaps: a p99 15 % lower and a capacity three steps higher, under
// the noise of a laptop, are "faster"; the same the other way, "slower".
func TestClearGaps(t *testing.T) {
	a, b := p99Rounds(1, 21, 8.5, 10, 0.05, 0.03)
	if r := judgeOne(t, Comparison{Name: "p99", Metric: P99, A: a, B: b}); r.Verdict != Faster || r.Gap > 0.9 {
		t.Fatalf("%+v", r)
	}
	if r := judgeOne(t, Comparison{Name: "p99", Metric: P99, A: b, B: a}); r.Verdict != Slower {
		t.Fatalf("%+v", r)
	}
	a, b = capacityRounds(2, 21, 40, 37, 0.3)
	r := judgeOne(t, Comparison{Name: "capacity", Metric: Capacity, A: a, B: b})
	if r.Verdict != Faster || r.Gap != 3 || r.Percent != "+15.8%" {
		t.Fatalf("%+v", r)
	}
}

// TestEquivalent: a p99 1 % apart under 1 % noise, and two capacities on
// the same step, are equivalent — the interval lies inside the margin.
func TestEquivalent(t *testing.T) {
	a, b := p99Rounds(3, 21, 10.1, 10, 0.05, 0.01)
	if r := judgeOne(t, Comparison{Name: "p99", Metric: P99, A: a, B: b}); r.Verdict != Equivalent {
		t.Fatalf("%+v", r)
	}
	a, b = capacityRounds(4, 21, 40, 40, 0.3)
	if r := judgeOne(t, Comparison{Name: "capacity", Metric: Capacity, A: a, B: b}); r.Verdict != Equivalent {
		t.Fatalf("%+v", r)
	}
}

// TestAAFalseVerdictsStayUnderFivePercent: the same contender measured
// twice, under a noise of the size a laptop gives, over four hundred
// seeds. Holm holds the risk of a false "faster" or "slower" to 5 % per
// family (§7.4) — not to zero: §7.5 expects an A/A to fail one time in
// twenty. What is checked is that rate, family by family, and that no A/A
// ever reads as a gap past its floor by more than a step.
func TestAAFalseVerdictsStayUnderFivePercent(t *testing.T) {
	const seeds = 400
	counts := map[string]int{}
	falseFamilies := 0
	for seed := range uint64(seeds) {
		a, b := p99Rounds(seed, 21, 10, 10, 0.05, 0.04)
		ca, cb := capacityRounds(seed, 21, 40, 40, 0.6)
		rs, err := Judge([]Comparison{
			{Name: "p99", Metric: P99, A: a, B: b},
			{Name: "capacity", Metric: Capacity, A: ca, B: cb},
		}, DefaultFloors)
		if err != nil {
			t.Fatal(err)
		}
		wrong := false
		for _, r := range rs {
			counts[r.Verdict]++
			if r.Verdict == Faster || r.Verdict == Slower {
				wrong = true
				if r.Metric == "capacity" && math.Abs(r.Gap) > 1 || r.Metric == "p99" && math.Abs(r.Gap-1) > 0.05 {
					t.Fatalf("seed %d: an A/A read a gap past its floor: %+v", seed, r)
				}
			}
		}
		if wrong {
			falseFamilies++
		}
	}
	t.Logf("A/A verdicts over %d families: %v, %d with a false difference", seeds, counts, falseFamilies)
	if falseFamilies > seeds*5/100 {
		t.Fatalf("%d of %d A/A families read a difference, over 5 %%", falseFamilies, seeds)
	}
}

// TestHolmWithdrawsWhatOneTestWouldGive: 16 wins in 20 rounds pass the
// sign test alone (p = 0.012) but not Holm's first threshold in a family
// of 46 (0.05/46): the same gap is inconclusive there.
func TestHolmWithdrawsWhatOneTestWouldGive(t *testing.T) {
	a := make([]float64, 21)
	b := make([]float64, 21)
	for i := range a {
		a[i], b[i] = 9, 10 // A 10 % better…
		if i%5 == 0 && i > 0 {
			a[i] = 11 // …but in 4 of 20 rounds worse
		}
	}
	alone := judgeOne(t, Comparison{Name: "alone", Metric: P99, A: a, B: b})
	if alone.Wins != 16 || alone.Losses != 4 || alone.Verdict != Faster {
		t.Fatalf("alone: %+v", alone)
	}
	family := []Comparison{{Name: "p99", Metric: P99, A: a, B: b}}
	for range 45 { // the rest of a family of 46, nothing to find in it
		x, y := p99Rounds(uint64(len(family)), 21, 10, 10, 0.05, 0.01)
		family = append(family, Comparison{Name: "other", Metric: P99, A: x, B: y})
	}
	rs, err := Judge(family, DefaultFloors)
	if err != nil {
		t.Fatal(err)
	}
	if rs[0].Verdict == Faster || rs[0].Holm != 0.05/46 {
		t.Fatalf("in a family of 46: %+v", rs[0])
	}
}

// TestFirstRoundDroppedAndMinimum: the first round never counts, and a
// comparison with fewer than 21 rounds gets no verdict.
func TestFirstRoundDroppedAndMinimum(t *testing.T) {
	a, b := p99Rounds(5, 21, 10, 10, 0.01, 0.01)
	a[0], b[0] = 1, 1000 // a first round that would be a huge win
	if r := judgeOne(t, Comparison{Name: "p99", Metric: P99, A: a, B: b}); r.Rounds != 20 || r.Lo < 0.9 {
		t.Fatalf("%+v", r)
	}
	if _, err := Judge([]Comparison{{Name: "short", Metric: P99, A: a[:20], B: b[:20]}}, DefaultFloors); err == nil {
		t.Fatal("20 rounds accepted")
	}
}

// TestPieces: the sign test, the rank interval, the grid and Holm against
// values worked by hand.
func TestPieces(t *testing.T) {
	if p := SignTest(18, 2); math.Abs(p-0.000402) > 1e-6 {
		t.Errorf("sign test 18/20: %v, want 0.000402", p)
	}
	if p := SignTest(10, 10); p != 1 {
		t.Errorf("sign test 10/10: %v", p)
	}
	xs := make([]float64, 20)
	for i := range xs {
		xs[i] = float64(20 - i) // 20 … 1
	}
	if med, lo, hi := MedianCI(xs); med != 10.5 || lo != 6 || hi != 15 {
		t.Errorf("rank interval of 1..20: %v [%v, %v], want 10.5 [6, 15]", med, lo, hi)
	}
	for k := MinStep; k < 120; k++ {
		if got, err := Step(grid(k)); err != nil || got != k {
			t.Fatalf("step of %v: %d %v, want %d", grid(k), got, err, k)
		}
	}
	if _, err := Step(4321); err == nil {
		t.Error("a rate off the grid accepted")
	}
	rej, th := Holm([]float64{0.01, 0.04, 0.03}, 0.05)
	if !rej[0] || rej[1] || rej[2] || th[0] != 0.05/3 || th[2] != 0.05/2 || th[1] != 0.05 {
		t.Errorf("Holm: %v %v", rej, th)
	}
}
