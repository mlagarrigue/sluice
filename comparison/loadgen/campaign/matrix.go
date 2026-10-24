package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"time"

	"github.com/mlagarrigue/sluice/comparison/loadgen/stats"
)

// matrix is the campaign's file (§6): the contenders, the families they
// meet in, and the protocol's numbers (§3.3, §7.1, §7.2, §7.6). Every
// number left out takes the protocol's value; a matrix with fewer rounds
// than a verdict needs must say it is a trial, and nothing it produces is
// published.
//
//	{"name": "level-1", "seed": 7,
//	 "contenders": {"R": {"image": "sluice-study:opt"},
//	                "httpstream-h1": {"image": "sluice-study:opt", "args": ["-transport", "httpstream-h1"]}},
//	 "families": [{"scenario": "list", "proto": "h1", "contenders": ["R", "httpstream-h1"],
//	               "survey_from": 100, "points": [500, 2000, 8000, 32000]}]}
type matrix struct {
	Name  string `json:"name"`
	Seed  uint64 `json:"seed"`  // the draws of the campaign: orders, full rounds; published
	Trial bool   `json:"trial"` // fewer rounds than a verdict needs: run, never published

	Rounds     int `json:"rounds"`      // per scenario × primary metric before the adaptive stop: 21
	MaxRounds  int `json:"max_rounds"`  // where the adaptive stop gives up: 40
	Extend     int `json:"extend"`      // rounds added for everyone while an interval is wide: 5
	FullRounds int `json:"full_rounds"` // rounds at every eligible point, the others at the designated one: 7

	Warmup      duration `json:"warmup"`       // a repetition's, at its first stage's rate: 1m
	Stage       duration `json:"stage"`        // a capacity stage, its first seconds not counted: 30s
	StageWarmup duration `json:"stage_warmup"` // those first seconds: 5s
	SurveyStage duration `json:"survey_stage"` // a survey stage: 30s
	PointStage  duration `json:"point_stage"`  // a fixed point: 90s
	Drain       duration `json:"drain"`        // between two stages, once the first's requests are back: 5s

	Network struct {
		CapacityRTT string `json:"capacity_rtt"` // client round trip of the capacity: 0ms
		PointsRTT   string `json:"points_rtt"`   // client round trip of the fixed points: 20ms
		DBRTT       string `json:"db_rtt"`       // web ↔ db, both: 1ms
	} `json:"network"`
	Rows int `json:"rows"` // the table's rows, the keyspace: 100000

	Contenders map[string]contender `json:"contenders"`
	Families   []family             `json:"families"`
}

// contender is one server under test: its image, and the flags that make
// a variant of it (the study's server, §8.3).
type contender struct {
	Image string   `json:"image"`
	Args  []string `json:"args"`
}

// family is a scenario × a protocol: the contenders compared in it, and
// the scenario's absolute grid of fixed points (§3.2).
type family struct {
	Scenario   string   `json:"scenario"`
	Proto      string   `json:"proto"`
	Contenders []string `json:"contenders"`
	SurveyFrom int      `json:"survey_from"` // the survey's first stage, req/s: 100
	Points     []int    `json:"points"`      // ascending req/s, the same for every contender and level
}

func (f family) String() string { return f.Scenario + "/" + f.Proto }

// duration reads "30s" in a matrix.
type duration struct{ time.Duration }

func (d *duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	d.Duration = v
	return nil
}

func (d duration) MarshalJSON() ([]byte, error) { return json.Marshal(d.String()) }

func readMatrix(name string) (matrix, error) {
	b, err := os.ReadFile(name) //nolint:gosec // G304: the file is the flag's
	if err != nil {
		return matrix{}, err
	}
	return parseMatrix(b)
}

func parseMatrix(b []byte) (matrix, error) {
	var m matrix
	if err := json.Unmarshal(b, &m); err != nil {
		return m, err
	}
	m.defaults()
	err := m.check()
	return m, err
}

func (m *matrix) defaults() {
	setInt := func(p *int, v int) {
		if *p == 0 {
			*p = v
		}
	}
	setDur := func(p *duration, v time.Duration) {
		if p.Duration == 0 {
			p.Duration = v
		}
	}
	setStr := func(p *string, v string) {
		if *p == "" {
			*p = v
		}
	}
	setInt(&m.Rounds, stats.MinRounds)
	setInt(&m.MaxRounds, 40)
	setInt(&m.Extend, 5)
	setInt(&m.FullRounds, 7)
	setInt(&m.Rows, 100000)
	setDur(&m.Warmup, time.Minute)
	setDur(&m.Stage, 30*time.Second)
	setDur(&m.StageWarmup, 5*time.Second)
	setDur(&m.SurveyStage, 30*time.Second)
	setDur(&m.PointStage, 90*time.Second)
	setDur(&m.Drain, 5*time.Second)
	setStr(&m.Network.CapacityRTT, "0ms")
	setStr(&m.Network.PointsRTT, "20ms")
	setStr(&m.Network.DBRTT, "1ms")
	for i := range m.Families {
		setInt(&m.Families[i].SurveyFrom, 100)
	}
}

func (m *matrix) check() error {
	switch {
	case m.Name == "":
		return errors.New("matrix: a name is required")
	case m.Rounds < stats.MinRounds && !m.Trial:
		return fmt.Errorf("matrix: %d rounds, a verdict needs %d (§7.2); a run with fewer says \"trial\": true and is never published", m.Rounds, stats.MinRounds)
	case m.Rounds < 2:
		return errors.New("matrix: two rounds at least, the first one is dropped (§7.1)")
	case m.MaxRounds < m.Rounds || m.Extend <= 0:
		return errors.New("matrix: max_rounds at least rounds, extend positive")
	case m.FullRounds < 1 || m.FullRounds >= m.Rounds:
		return fmt.Errorf("matrix: full_rounds %d, want 1 to rounds − 1 (the first round is at the designated point alone, §7.6)", m.FullRounds)
	case len(m.Families) == 0:
		return errors.New("matrix: no family")
	}
	for _, d := range []duration{m.Warmup, m.Stage, m.SurveyStage, m.PointStage} {
		if d.Duration < time.Second || d.Duration%time.Second != 0 {
			return fmt.Errorf("matrix: %s: stages and warm-ups are whole seconds", d)
		}
	}
	if m.StageWarmup.Duration%time.Second != 0 || m.Drain.Duration < 0 {
		return errors.New("matrix: stage_warmup is whole seconds and drain is not negative")
	}
	for name, c := range m.Contenders {
		if c.Image == "" {
			return fmt.Errorf("matrix: contender %s has no image", name)
		}
	}
	seen := map[string]bool{}
	for _, f := range m.Families {
		switch {
		case seen[f.String()]:
			return fmt.Errorf("matrix: family %s twice", f)
		case f.Scenario == "" || f.Proto == "":
			return errors.New("matrix: a family names its scenario and its protocol")
		case len(f.Contenders) < 2:
			return fmt.Errorf("matrix: family %s compares fewer than two contenders", f)
		case f.SurveyFrom <= 0:
			return fmt.Errorf("matrix: family %s: survey_from must be positive", f)
		case len(f.Points) == 0 || f.Points[0] <= 0 || !slices.IsSorted(f.Points) || len(slices.Compact(slices.Clone(f.Points))) != len(f.Points):
			return fmt.Errorf("matrix: family %s: points are positive req/s, ascending, each once", f)
		}
		seen[f.String()] = true
		for _, c := range f.Contenders {
			if _, ok := m.Contenders[c]; !ok {
				return fmt.Errorf("matrix: family %s names %s, which is no contender", f, c)
			}
		}
		if len(slices.Compact(slices.Sorted(slices.Values(f.Contenders)))) != len(f.Contenders) {
			return fmt.Errorf("matrix: family %s names a contender twice", f)
		}
	}
	return nil
}
