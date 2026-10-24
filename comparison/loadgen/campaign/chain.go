package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// chain is what a campaign drives: the three roles of §0. up builds the
// database and the injected network, web starts a fresh server (a
// repetition, §7.1), stage plays one stage on it, down removes everything.
type chain interface {
	up(ctx context.Context, f family, rtt string) error
	web(ctx context.Context, c contender) error
	stage(ctx context.Context, s stageSpec) (stageResult, error)
	down(ctx context.Context) error
}

// stageSpec is one stage as the campaign asks for it.
type stageSpec struct {
	rate     int
	duration time.Duration // measured
	warmup   time.Duration // sent before the clock, never counted
	seed     uint64        // the draw, the same for every contender of a round (§2.1)
}

// stageResult is what the campaign reads off a stage's merged line; the
// line itself goes to the raw records whole.
type stageResult struct {
	line    json.RawMessage
	invalid bool    // the apparatus invalidated it (§4): the repetition is replayed
	held    bool    // the SLO held (§3.2)
	cause   string  // why not
	p99     float64 // TTLB, ms; NaN with no answer ok
	p999    float64
	counted int // requests sent inside the measurement (§7.3)
}

// parseStage reads a merged line of chain.sh.
func parseStage(line []byte) (stageResult, error) {
	var l struct {
		Held  bool   `json:"held"`
		Cause string `json:"cause"`
		Sent  int    `json:"sent"`
		OK    int    `json:"ok"`
		TTLB  struct {
			P99  float64 `json:"p99"`
			P999 float64 `json:"p999"`
		} `json:"ttlb_ms"`
		Valid *bool `json:"valid"`
	}
	if err := json.Unmarshal(line, &l); err != nil {
		return stageResult{}, err
	}
	if l.Valid == nil {
		return stageResult{}, errors.New("the stage's line carries no validity: not a merged line")
	}
	r := stageResult{
		line: json.RawMessage(bytes.Clone(line)), invalid: !*l.Valid,
		held: l.Held, cause: l.Cause, p99: l.TTLB.P99, p999: l.TTLB.P999, counted: l.Sent,
	}
	if l.OK == 0 {
		r.p99, r.p999 = math.NaN(), math.NaN()
	}
	return r, nil
}

// script is the chain of comparison/chain/chain.sh, one process per verb:
// the environment of every call carries what the previous ones set, so
// that stage reads back the network up shaped.
type script struct {
	path  string
	dir   string // where each stage's files go
	env   map[string]string
	count int
}

func newScript(path, dir string, m matrix) *script {
	return &script{path: path, dir: dir, env: map[string]string{
		"ROWS":   strconv.Itoa(m.Rows),
		"DB_RTT": m.Network.DBRTT,
	}}
}

func (s *script) run(ctx context.Context, verb string, extra map[string]string) error {
	cmd := exec.CommandContext(ctx, s.path, verb) //nolint:gosec // G204: the chain is the flag's script
	cmd.Env = os.Environ()
	for k, v := range s.env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	for k, v := range extra {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	// The chain's own report (the stage's summary, the server's banner)
	// goes to the campaign's log; its result is the files it writes.
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	return cmd.Run()
}

func (s *script) up(ctx context.Context, f family, rtt string) error {
	s.env["SCENARIO"], s.env["PROTO"], s.env["RTT"] = f.Scenario, f.Proto, rtt
	return s.run(ctx, "up", nil)
}

func (s *script) web(ctx context.Context, c contender) error {
	s.env["WEB_IMAGE"], s.env["WEB_ARGS"] = c.Image, strings.Join(c.Args, " ")
	return s.run(ctx, "web", nil)
}

func (s *script) stage(ctx context.Context, st stageSpec) (stageResult, error) {
	s.count++
	out := filepath.Join(s.dir, fmt.Sprintf("stage-%05d", s.count))
	if err := os.MkdirAll(out, 0o750); err != nil {
		return stageResult{}, err
	}
	err := s.run(ctx, "stage", map[string]string{
		"OUT":      out,
		"RATE":     strconv.Itoa(st.rate),
		"DURATION": seconds(st.duration),
		"WARMUP":   seconds(st.warmup),
		"SEED":     strconv.FormatUint(st.seed, 10),
	})
	var exit *exec.ExitError
	if err != nil && (!errors.As(err, &exit) || exit.ExitCode() != 2) {
		return stageResult{}, fmt.Errorf("chain stage at %d req/s: %w (a wrong answer or the chain itself; %s)", st.rate, err, out)
	}
	line, rerr := os.ReadFile(filepath.Join(out, "stage-merged.jsonl")) //nolint:gosec // G304: the chain's own file
	if rerr != nil {
		return stageResult{}, rerr
	}
	r, perr := parseStage(bytes.TrimSpace(line))
	if perr != nil {
		return r, fmt.Errorf("%s: %w", out, perr)
	}
	if err != nil != r.invalid {
		return r, fmt.Errorf("%s: the chain's exit status and its line disagree on the stage's validity", out)
	}
	return r, nil
}

func (s *script) down(ctx context.Context) error { return s.run(ctx, "down", nil) }

// seconds writes a duration as chain.sh reads it, whole seconds.
func seconds(d time.Duration) string {
	return strconv.Itoa(int(math.Ceil(d.Seconds()))) + "s"
}
