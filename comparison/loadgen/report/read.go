package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// What the campaign wrote, read back: only the fields §5 publishes. The
// campaign's own structures are not shared — this reads its files as any
// later reader would, so a field that disappears from them breaks here
// rather than in six months on a report nobody could reproduce.

// matrix is matrix.json: the campaign as played, every default filled in.
type matrix struct {
	Name       string              `json:"name"`
	Trial      bool                `json:"trial"`
	Rows       int                 `json:"rows"`
	Contenders map[string]struct { // the image each ran
		Image string   `json:"image"`
		Args  []string `json:"args"`
	} `json:"contenders"`
	Families []struct {
		Scenario   string   `json:"scenario"`
		Proto      string   `json:"proto"`
		Contenders []string `json:"contenders"`
		Points     []int    `json:"points"`
	} `json:"families"`
}

// stat is one contender's metric over the rounds: the median published and
// its interval by the ranks (§7.4).
type stat struct {
	Window   *[2]int   `json:"window,omitempty"`
	Median   float64   `json:"median"`
	Lo       float64   `json:"lo"`
	Hi       float64   `json:"hi"`
	Unstable bool      `json:"unstable"`
	Values   []float64 `json:"values"`
}

type metricSummary struct {
	Rounds     int             `json:"rounds"`
	Contenders map[string]stat `json:"contenders"`
}

// summary is summary.json: the medians and intervals of the primary
// metrics, family by family.
type summary struct {
	Name        string `json:"name"`
	Trial       bool   `json:"trial"`
	Publishable bool   `json:"publishable"`
	Families    []struct {
		Family   string        `json:"family"`
		Capacity metricSummary `json:"capacity"`
		Points   *struct {
			Designated int           `json:"designated"`
			FullRounds []int         `json:"full_rounds"`
			P99        metricSummary `json:"p99"`
		} `json:"points"`
		Note string `json:"note,omitempty"`
	} `json:"families"`
}

// stageLine is the merged line of one stage — the generator's report plus
// what the collector read in the server's namespaces. §5 asks for seven of
// its counters by name, each published even at zero.
type stageLine struct {
	Rate  int     `json:"rate"`
	Conns int     `json:"conns"`
	SLO   float64 `json:"slo_ms"`
	RTT   float64 `json:"rtt_ms"`
	Held  bool    `json:"held"`
	Cause string  `json:"cause"`

	Sent     int `json:"sent"`
	OK       int `json:"ok"`
	BadKey   int `json:"bad_key"`
	TimedOut int `json:"timed_out"`
	Errors   int `json:"errors"`

	Status4xx      int `json:"status_4xx"`
	Status5xx      int `json:"status_5xx"`
	Refused        int `json:"refused"`
	Reset          int `json:"reset"`
	ConnectTimeout int `json:"connect_timeout"`
	Other          int `json:"other"`

	Closed     int `json:"closed"`
	Reconnects int `json:"reconnects"`

	TTLB struct {
		P50  float64 `json:"p50"`
		P99  float64 `json:"p99"`
		P999 float64 `json:"p999"`
	} `json:"ttlb_ms"`

	Roles struct {
		Web struct {
			CPUSeconds      float64 `json:"cpu_s"`
			RSSMax          float64 `json:"rss_kb_max"`
			EstablishedMax  *int    `json:"established_max"`
			ListenOverflows int     `json:"listen_overflows"`
			ListenDrops     int     `json:"listen_drops"`
		} `json:"web"`
		DB struct {
			Connections *int `json:"connections"`
		} `json:"db"`
	} `json:"roles"`

	Crash   string `json:"crash"`
	Valid   bool   `json:"valid"`
	Metrics struct {
		Start *metricsDoc `json:"start"`
		End   *metricsDoc `json:"end"`
	} `json:"metrics"`
}

// metricsDoc is the contender's own counters, as the contract has it
// (§1.2, §1.3): the batch sizes of the variants that group are read here
// and nowhere else.
type metricsDoc struct {
	Batches *struct {
		Count uint64 `json:"count"`
		Keys  uint64 `json:"keys"`
	} `json:"batches"`
}

// record is one line of raw.jsonl: a stage, where it sits in the campaign.
type record struct {
	Family    string          `json:"family"`
	Measure   string          `json:"measure"`
	Phase     string          `json:"phase"`
	Round     int             `json:"round,omitempty"`
	Contender string          `json:"contender"`
	Attempt   int             `json:"attempt"`
	Kind      string          `json:"kind"`
	Rate      int             `json:"rate"`
	Line      json.RawMessage `json:"line"`
}

// verdict is one line of what `loadgen -verdicts` wrote.
type verdict struct {
	Family  string  `json:"family"`
	Name    string  `json:"name"`
	Metric  string  `json:"metric"`
	Rounds  int     `json:"rounds"`
	Wins    int     `json:"wins"`
	Losses  int     `json:"losses"`
	P       float64 `json:"p"`
	Holm    float64 `json:"holm_threshold"`
	Gap     float64 `json:"gap"`
	Lo      float64 `json:"lo"`
	Hi      float64 `json:"hi"`
	Percent string  `json:"percent"`
	Margin  float64 `json:"margin"`
	Verdict string  `json:"verdict"`
}

// stage is a stage of the campaign with its place: what the curves and the
// counters are built from.
type stage struct {
	family    string
	contender string
	measure   string
	kind      string
	round     int
	line      stageLine
}

// inputs is a campaign read whole.
type inputs struct {
	matrix   matrix
	summary  summary
	stages   []stage
	verdicts []verdict
}

// read loads a campaign directory and the verdicts file beside it. Every
// file is required: a report missing one of them would publish a §5 point
// as absent when it is only unread.
func read(dir, verdictsPath string) (*inputs, error) {
	in := &inputs{}
	if err := readJSON(filepath.Join(dir, "matrix.json"), &in.matrix); err != nil {
		return nil, err
	}
	if err := readJSON(filepath.Join(dir, "summary.json"), &in.summary); err != nil {
		return nil, err
	}
	raw := filepath.Join(dir, "raw.jsonl")
	if err := readLines(raw, func(n int, b []byte) error {
		var r record
		if err := json.Unmarshal(b, &r); err != nil {
			return fmt.Errorf("%s line %d: %w", raw, n, err)
		}
		if r.Kind == "warmup" {
			return nil // a warm-up measures nothing (§7.1)
		}
		var l stageLine
		if err := json.Unmarshal(r.Line, &l); err != nil {
			return fmt.Errorf("%s line %d: its stage line: %w", raw, n, err)
		}
		in.stages = append(in.stages, stage{
			family: r.Family, contender: r.Contender, measure: r.Measure,
			kind: r.Kind, round: r.Round, line: l,
		})
		return nil
	}); err != nil {
		return nil, err
	}
	if err := readLines(verdictsPath, func(n int, b []byte) error {
		var v verdict
		if err := json.Unmarshal(b, &v); err != nil {
			return fmt.Errorf("%s line %d: %w", verdictsPath, n, err)
		}
		in.verdicts = append(in.verdicts, v)
		return nil
	}); err != nil {
		return nil, err
	}
	if len(in.stages) == 0 {
		return nil, fmt.Errorf("%s holds no measured stage", raw)
	}
	return in, nil
}

func readJSON(path string, dst any) error {
	b, err := os.ReadFile(path) //nolint:gosec // G304: the directory is the flag's
	if err != nil {
		return err
	}
	if err := json.Unmarshal(b, dst); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	return nil
}

func readLines(path string, fn func(n int, b []byte) error) error {
	f, err := os.Open(path) //nolint:gosec // G304: the directory is the flag's
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64<<10), 4<<20)
	for n := 1; sc.Scan(); n++ {
		if strings.TrimSpace(sc.Text()) == "" {
			continue
		}
		if err := fn(n, sc.Bytes()); err != nil {
			return err
		}
	}
	return sc.Err()
}

// families returns the families in the matrix's order: the report follows
// the campaign, not a map's iteration.
func (in *inputs) families() []string {
	var out []string
	for _, f := range in.matrix.Families {
		name := f.Scenario + "/" + f.Proto
		if !slices.Contains(out, name) {
			out = append(out, name)
		}
	}
	for _, f := range in.summary.Families {
		if !slices.Contains(out, f.Family) {
			out = append(out, f.Family) // a family the matrix no longer names
		}
	}
	return out
}

// contenders of a family, in the matrix's order.
func (in *inputs) contenders(family string) []string {
	for _, f := range in.matrix.Families {
		if f.Scenario+"/"+f.Proto == family {
			return f.Contenders
		}
	}
	var out []string
	for i := range in.stages {
		if in.stages[i].family == family && !slices.Contains(out, in.stages[i].contender) {
			out = append(out, in.stages[i].contender)
		}
	}
	slices.Sort(out)
	return out
}
