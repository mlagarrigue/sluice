// Command report turns a campaign into the publication of §5: one
// document and its charts, every point of §5 present or saying why it is
// not. It computes nothing a reader could not recompute from the
// campaign's own files — that is the point of a generated report: the
// figures and the text move together, and a run of the campaign is a run
// of the report.
//
// Run from the repository root, on a campaign's output directory:
//
//	go run -C comparison/loadgen ./report -campaign /tmp/trial -out /tmp/trial/report
//
// It reads matrix.json, summary.json and raw.jsonl from -campaign, and the
// verdicts from -verdicts (default: verdicts.jsonl of that directory, as
// `loadgen -verdicts` writes it). It writes COMPARISON.md and one SVG per
// family and per chart into -out.
//
// Nothing of a laptop's belongs in the public repository (comparison/
// README.md): the output directory is a working one, and what reaches
// docs/COMPARISON.md is a campaign's, from the machines §0 names.
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
)

func main() {
	campaign := flag.String("campaign", "", "the campaign's output directory: matrix.json, summary.json, raw.jsonl")
	verdicts := flag.String("verdicts", "", "the verdicts of `loadgen -verdicts` (default: verdicts.jsonl of -campaign)")
	out := flag.String("out", "", "the directory to write COMPARISON.md and the charts into")
	flag.Parse()

	if err := run(*campaign, *verdicts, *out); err != nil {
		fmt.Fprintln(os.Stderr, "report:", err)
		os.Exit(1)
	}
}

func run(campaign, verdicts, out string) error {
	if campaign == "" || out == "" {
		return errors.New("-campaign and -out are required")
	}
	if verdicts == "" {
		verdicts = filepath.Join(campaign, "verdicts.jsonl")
	}
	in, err := read(campaign, verdicts)
	if err != nil {
		return err
	}
	files := render(in)
	if err := os.MkdirAll(out, 0o750); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(out, "COMPARISON.md"), []byte(files.markdown), 0o644); err != nil { //nolint:gosec // G306: a report to read, not a secret
		return err
	}
	for name, svg := range files.charts {
		if err := os.WriteFile(filepath.Join(out, name), []byte(svg), 0o644); err != nil { //nolint:gosec // G306: idem
			return err
		}
	}
	fmt.Printf("report: %s and %d charts in %s\n", "COMPARISON.md", len(files.charts), out)
	return nil
}
