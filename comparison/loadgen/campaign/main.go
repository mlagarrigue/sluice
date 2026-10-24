// Command campaign plays a matrix of the study on the local chain (§3.3,
// §6, §7.1, §7.2, §7.6): per family, a survey per contender, then rounds
// of capacity ramps on the common grid, then rounds at the fixed points,
// each round in an order drawn at random, each repetition a fresh server
// and its warm-up, a repetition the apparatus invalidated played again
// whole, and the adaptive stop on the width of the intervals. It writes,
// in the directory -out names:
//
//	matrix.json   the matrix as played, every default filled in
//	plan.jsonl    the draws: survey order, each round's order, the full rounds
//	raw.jsonl     every stage's merged line, where it sits in the campaign
//	rounds.jsonl  one line per contender per round, what loadgen -verdicts reads
//	summary.json  windows, medians and intervals, the unstable, the points
//
// Run from the repository root, the chain's images built
// (comparison/README.md):
//
//	CORES="2 1 4" go run -C comparison/loadgen ./campaign -matrix ../chain/trial.json -out /tmp/trial
//
// A matrix marked trial plays fewer rounds than a verdict needs; its
// rounds carry the mark, and loadgen -verdicts refuses them for want of
// rounds. Nothing it prints is a result of the study: it is the apparatus.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "campaign:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("campaign", flag.ContinueOnError)
	matrixFile := fs.String("matrix", "", "the campaign's matrix, JSON")
	out := fs.String("out", "", "a directory for the records, which must not exist yet")
	script := fs.String("chain", "../chain/chain.sh", "the chain's script, from the working directory (comparison/loadgen under go run -C)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *matrixFile == "" || *out == "" {
		return errors.New("-matrix and -out are required")
	}
	m, err := readMatrix(*matrixFile)
	if err != nil {
		return err
	}
	chainPath, err := filepath.Abs(*script)
	if err != nil {
		return err
	}
	if err := os.Mkdir(*out, 0o750); err != nil {
		return fmt.Errorf("-out: %w (a campaign never writes over another)", err)
	}
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(*out, "matrix.json"), append(b, '\n'), 0o600); err != nil {
		return err
	}
	files := map[string]*os.File{}
	defer func() {
		for _, f := range files {
			_ = f.Close() // closed below already, unless the run failed first
		}
	}()
	for _, name := range []string{"plan.jsonl", "raw.jsonl", "rounds.jsonl"} {
		f, err := os.Create(filepath.Join(*out, name)) //nolint:gosec // G304: under the flag's directory
		if err != nil {
			return err
		}
		files[name] = f
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ch := newScript(chainPath, filepath.Join(*out, "stages"), m)
	c := newCampaign(m, ch, files["raw.jsonl"], files["plan.jsonl"], os.Stderr)
	s, runErr := c.run(ctx, files["rounds.jsonl"])
	b, err = json.MarshalIndent(s, "", "  ")
	if err == nil {
		err = os.WriteFile(filepath.Join(*out, "summary.json"), append(b, '\n'), 0o600)
	}
	for _, f := range files {
		if cerr := f.Close(); err == nil {
			err = cerr
		}
	}
	if runErr != nil {
		return runErr
	}
	return err
}
