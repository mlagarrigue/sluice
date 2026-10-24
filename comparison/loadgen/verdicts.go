package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"slices"
	"strings"

	"github.com/mlagarrigue/sluice/comparison/loadgen/stats"
)

// The verdicts of a campaign (§7.4), from its rounds. The input is one
// JSON line per contender per round, the two primary metrics on it:
//
//	{"scenario":"unit","proto":"h1","contender":"sluice-std","round":1,"capacity_rps":8000,"p99_ms":11.2}
//
// The comparisons are declared before the campaign (-compare a:b,…), and
// a family is a scenario × a protocol over both metrics: every declared
// comparison of the family is judged together, Holm over all of them. A
// round where either side is missing is not a round of that comparison.

// observation is one contender's round.
type observation struct {
	Scenario  string   `json:"scenario"`
	Proto     string   `json:"proto"`
	Contender string   `json:"contender"`
	Round     int      `json:"round"`
	Capacity  *float64 `json:"capacity_rps"`
	P99       *float64 `json:"p99_ms"`
}

// verdictLine is what the command writes, one per comparison per metric.
type verdictLine struct {
	Family string `json:"family"` // scenario/proto
	stats.Result
}

// runVerdicts reads the rounds from in, judges the declared comparisons
// family by family, and writes one line per comparison and metric.
func runVerdicts(in io.Reader, compare string, floors stats.Floors, out io.Writer) error {
	pairs, err := parsePairs(compare)
	if err != nil {
		return err
	}
	type key struct{ family, contender string }
	byRound := map[key]map[int]observation{}
	var families []string
	sc := bufio.NewScanner(in)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for line := 1; sc.Scan(); line++ {
		if strings.TrimSpace(sc.Text()) == "" {
			continue
		}
		var o observation
		if err := json.Unmarshal(sc.Bytes(), &o); err != nil {
			return fmt.Errorf("line %d: %w", line, err)
		}
		if o.Scenario == "" || o.Proto == "" || o.Contender == "" || o.Round <= 0 {
			return fmt.Errorf("line %d: scenario, proto, contender and a positive round are required", line)
		}
		fam := o.Scenario + "/" + o.Proto
		if !slices.Contains(families, fam) {
			families = append(families, fam)
		}
		k := key{fam, o.Contender}
		if byRound[k] == nil {
			byRound[k] = map[int]observation{}
		}
		if _, dup := byRound[k][o.Round]; dup {
			return fmt.Errorf("line %d: %s round %d twice in %s", line, o.Contender, o.Round, fam)
		}
		byRound[k][o.Round] = o
	}
	if err := sc.Err(); err != nil {
		return err
	}
	enc := json.NewEncoder(out)
	for _, fam := range families {
		var family []stats.Comparison
		for _, p := range pairs {
			ra, rb := byRound[key{fam, p[0]}], byRound[key{fam, p[1]}]
			if ra == nil || rb == nil {
				continue // the pair is not in this family
			}
			for _, m := range []stats.Metric{stats.Capacity, stats.P99} {
				c := stats.Comparison{Name: p[0] + " vs " + p[1], Metric: m}
				for _, r := range slices.Sorted(maps.Keys(ra)) {
					a, okA := metricOf(ra[r], m)
					b, okB := metricOf(rb[r], m)
					if okA && okB {
						c.A, c.B = append(c.A, a), append(c.B, b)
					}
				}
				if len(c.A) > 0 {
					family = append(family, c)
				}
			}
		}
		if len(family) == 0 {
			continue
		}
		results, err := stats.Judge(family, floors)
		if err != nil {
			return fmt.Errorf("%s: %w", fam, err)
		}
		for i := range results {
			if err := enc.Encode(verdictLine{Family: fam, Result: results[i]}); err != nil {
				return err
			}
		}
	}
	return nil
}

func metricOf(o observation, m stats.Metric) (float64, bool) {
	p := o.Capacity
	if m == stats.P99 {
		p = o.P99
	}
	if p == nil {
		return 0, false
	}
	return *p, true
}

// parsePairs reads "a:b,a:c" into the declared comparisons.
func parsePairs(s string) ([][2]string, error) {
	var pairs [][2]string
	for part := range strings.SplitSeq(s, ",") {
		a, b, ok := strings.Cut(strings.TrimSpace(part), ":")
		if !ok || a == "" || b == "" || a == b {
			return nil, fmt.Errorf("-compare %q: want contender:contender, comma-separated", s)
		}
		pairs = append(pairs, [2]string{a, b})
	}
	if len(pairs) == 0 {
		return nil, errors.New("-compare names no comparison")
	}
	return pairs, nil
}

// verdictsFile opens the rounds -verdicts names, "-" for stdin.
func verdictsFile(name string) (io.ReadCloser, error) {
	if name == "-" {
		return io.NopCloser(os.Stdin), nil
	}
	return os.Open(name) //nolint:gosec // G304: the file is the flag's
}
