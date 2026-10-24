// Command audit reads what something was set to and says what each
// setting costs. It changes nothing, ever: every finding is a value found,
// a value proposed, the goal it serves, the reason in plain words and a
// link to the documentation it comes from.
//
// This first form reads a Go binary's build record:
//
//	go run -C audit ./cmd/audit -binary ./sluice-interop
//	go run -C audit ./cmd/audit -binary ./x -goals security -output json
//
// Every parameter is an axis, and every combination of them is valid. An
// axis left out means all of its values, so the line above with no -goals
// reports performance, security and reliability together. The axes and
// their values are printed by -axes.
//
// Nothing is asked in this form: the questions a reading cannot settle —
// the role of the machine, how exposed it is — take their default, which
// is what a run in CI wants. -answer sets one.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/mlagarrigue/sluice/audit"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "audit:", err)
		os.Exit(1)
	}
}

// axisFlags binds one flag per axis, so a new axis needs no new flag.
type axisFlags map[audit.Axis]*string

func run(args []string, out *os.File) error {
	fs := flag.NewFlagSet("audit", flag.ContinueOnError)
	binary := fs.String("binary", "", "the Go binary to read")
	axesOnly := fs.Bool("axes", false, "print the axes and their values, and stop")
	var answers answerFlag
	fs.Var(&answers, "answer", "answer a question, key=value, repeatable")

	flags := axisFlags{}
	for _, a := range audit.Axes() {
		flags[a] = fs.String(string(a), "", "comma-separated "+string(a)+": "+strings.Join(audit.Values(a), ", ")+" (default: all)")
	}
	if err := fs.Parse(args); err != nil {
		return err
	}

	if *axesOnly {
		for _, a := range audit.Axes() {
			_, _ = fmt.Fprintf(out, "%-12s %s\n", a, strings.Join(audit.Values(a), ", "))
		}
		for _, q := range audit.Questions {
			_, _ = fmt.Fprintf(out, "question     %-9s %s (default %s)\n", q.Key, q.Ask, q.Default)
		}
		return nil
	}
	if *binary == "" {
		return errors.New("-binary names the Go binary to read")
	}

	sel := audit.Selection{}
	for a, v := range flags {
		if *v != "" {
			sel[a] = strings.Split(*v, ",")
		}
	}
	target, err := audit.OpenBinary(*binary)
	if err != nil {
		return err
	}
	findings, err := audit.Run(sel, audit.Answers(answers), target)
	if err != nil {
		return err
	}
	if sel.Has(audit.Output, "json") && !sel.Has(audit.Output, "text") {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(findings)
	}
	if len(findings) == 0 {
		_, _ = fmt.Fprintf(out, "%s: nothing to propose under this selection\n", *binary)
		return nil
	}
	for _, f := range findings {
		_, _ = fmt.Fprintf(out, "%s (%s)\n  found:    %s\n  proposed: %s\n  why:      %s\n  doc:      %s\n\n",
			f.Rule, f.Goal, f.Current, f.Proposed, wrap(f.Why, 72, "            "), f.Doc)
	}
	return nil
}

// answerFlag collects -answer key=value.
type answerFlag map[string]string

func (a answerFlag) String() string { return "" }

func (a *answerFlag) Set(v string) error {
	key, value, ok := strings.Cut(v, "=")
	if !ok || key == "" {
		return fmt.Errorf("-answer %q: want key=value", v)
	}
	if *a == nil {
		*a = answerFlag{}
	}
	known := false
	for _, q := range audit.Questions {
		known = known || q.Key == key
	}
	if !known {
		return fmt.Errorf("-answer %q: no question named %q", v, key)
	}
	(*a)[key] = value
	return nil
}

// wrap folds a reason to a width, so a terminal reads it as a paragraph.
func wrap(s string, width int, indent string) string {
	var b strings.Builder
	line := 0
	for i, word := range strings.Fields(s) {
		switch {
		case i == 0:
			b.WriteString(word)
			line = len(word)
		case line+1+len(word) > width:
			b.WriteString("\n" + indent + word)
			line = len(word)
		default:
			b.WriteString(" " + word)
			line += 1 + len(word)
		}
	}
	return b.String()
}
