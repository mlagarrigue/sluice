// Command audit reads what something was set to and says what each
// setting costs. It changes nothing, ever: every finding is a value found,
// a value proposed, the goal it serves, the reason in plain words and a
// link to the documentation it comes from.
//
// This first form reads a Go binary's build record:
//
//	go run -C audit ./cmd/audit -binary ./sluice-interop
//	go run -C audit ./cmd/audit -root / -pid 1234 -goals performance
//	go run -C audit ./cmd/audit -binary ./x -goals security -output json
//	go run -C audit ./cmd/audit -container web -kernel-log dmesg.txt -root / -profile comparative
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
	"path/filepath"
	"strings"

	"github.com/mlagarrigue/sluice/audit"
)

func main() {
	err := run(os.Args[1:], os.Stdout)
	var gate gateError
	switch {
	case errors.As(err, &gate):
		for _, line := range gate {
			fmt.Fprintln(os.Stderr, "audit: fails on", line)
		}
		os.Exit(2)
	case err != nil:
		fmt.Fprintln(os.Stderr, "audit:", err)
		os.Exit(1)
	}
}

// gateError is a run that -fail-on stops: one line per finding that
// tripped it. Its exit code is 2, so that a CI step tells it from a run
// that could not read what it was given (1).
type gateError []string

func (g gateError) Error() string { return strings.Join(g, "; ") }

// axisFlags binds one flag per axis, so a new axis needs no new flag.
type axisFlags map[audit.Axis]*string

// run is one invocation, in this order: the flags, one per input and one
// per axis; -axes, which prints and stops; the selection checked; the
// target opened; -sbom, which prints the bill of materials and stops; the
// rules run; then -compare, -propose and -fail-on on their findings, the
// report printed before the gate decides the exit.
func run(args []string, out *os.File) error {
	fs := flag.NewFlagSet("audit", flag.ContinueOnError)
	binary := fs.String("binary", "", "the Go binary to read")
	module := fs.String("module", "", "a directory holding a go.mod, whose requirements and advisories to read")
	dockerfile := fs.String("dockerfile", "", "a Dockerfile to read as text")
	root := fs.String("root", "", `the host's filesystem root to read, "/" for this machine`)
	pid := fs.Int("pid", 0, "a process whose own limits to read, under -root")
	container := fs.String("container", "", "a running container to read, by calling docker inspect (needs the tools source)")
	containerJSON := fs.String("container-json", "", "a file holding docker inspect's output for one container")
	kernelLog := fs.String("kernel-log", "", "a file holding the kernel's log (dmesg's output, or kern.log)")
	appLog := fs.String("app-log", "", "a file holding the application's log during a load: what the server wrote to its standard error or its log file")
	pgSettings := fs.String("pg-settings", "", "a file holding pg_settings as CSV: psql -X --csv -c 'SELECT name, setting, unit FROM pg_settings'")
	pgHBA := fs.String("pg-hba", "", "a PostgreSQL server's pg_hba.conf")
	pgLog := fs.String("pg-log", "", "a PostgreSQL server's log")
	pgRoles := fs.String("pg-roles", "", "a file holding the roles as CSV: psql -X --csv -c 'SELECT rolname, rolsuper, rolcanlogin FROM pg_roles'")
	firewall := fs.String("firewall", "", "a file holding the packet filter's rules: nft list ruleset, or iptables-save")
	proxyConfig := fs.String("proxy-config", "", "a reverse proxy's configuration: nginx.conf, or haproxy.cfg")
	proxyVersion := fs.String("proxy-version", "", "what nginx -v or haproxy -v printed, for the advisories")
	netstatBefore := fs.String("netstat-before", "", "the kernel's network counters before a load: a copy of /proc/net/netstat, or nstat -az")
	netstatAfter := fs.String("netstat-after", "", "the same counters after the load")
	pgActivity := fs.String("pg-activity", "", "pg_stat_activity read during a load, as CSV: psql -X --csv -c 'SELECT state, backend_type FROM pg_stat_activity', several readings appended")
	vulndb := fs.String("vulndb", "", "a local copy of the Go vulnerability database (vuln.go.dev/vulndb.zip, unzipped): -cve yes then needs no network")
	metricsBefore := fs.String("metrics-before", "", "the comparison server's GET /study/metrics at the start of a load, saved")
	metricsAfter := fs.String("metrics-after", "", "the same at its end")
	ss := fs.String("ss", "", "what ss -tan printed during a load")
	statBefore := fs.String("stat-before", "", "a copy of /proc/stat taken before a load")
	statAfter := fs.String("stat-after", "", "a copy of /proc/stat taken after it")
	imds := fs.String("imds", "", "a file holding Azure's metadata: curl -H Metadata:true 'http://169.254.169.254/metadata/instance?api-version=2025-04-07'")
	appArgs := fs.String("app-args", "", "a file holding how the comparison's server was started: NAME=value lines (env), then its command line")
	compare := fs.String("compare", "", "an earlier JSON report (-output json) of the same target: report what appeared, went and changed since")
	failOn := fs.String("fail-on", "", "comma-separated: new (any new finding), cve (a new advisory that applies), security (a new security finding); exit 2 when one trips")
	propose := fs.String("propose", "", "a directory to write the fixes in: a sysctl.d file, ALTER SYSTEM statements; written, never applied")
	sbom := fs.Bool("sbom", false, "print the binary's bill of materials instead of the findings")
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
	if *binary == "" && *module == "" && *dockerfile == "" && *root == "" &&
		*container == "" && *containerJSON == "" && *kernelLog == "" && *appLog == "" &&
		*pgSettings == "" && *pgHBA == "" && *pgLog == "" && *pgRoles == "" && *firewall == "" && *proxyConfig == "" && *appArgs == "" &&
		*netstatBefore == "" && *netstatAfter == "" && *pgActivity == "" &&
		*statBefore == "" && *statAfter == "" && *imds == "" &&
		*metricsBefore == "" && *metricsAfter == "" && *ss == "" {
		return errors.New("name what to read: -binary, -module, -dockerfile, -root, -container, -container-json, -kernel-log, -app-log, -pg-settings, -pg-hba, -pg-log, -pg-roles, -pg-activity, -firewall, -proxy-config, -app-args, -imds, -ss, -metrics-before and -metrics-after, or -netstat-before and -netstat-after, or -stat-before and -stat-after")
	}

	sel := audit.Selection{}
	for a, v := range flags {
		if *v != "" {
			sel[a] = strings.Split(*v, ",")
		}
	}
	if err := sel.Validate(); err != nil {
		return err
	}
	if *container != "" && !sel.Has(audit.Sources, "tools") {
		return errors.New("-container calls docker inspect, and the selected sources do not allow tools: " +
			"add tools to -sources, or save docker inspect's output and name it with -container-json")
	}
	target, err := audit.Open(audit.Spec{
		Binary: *binary, Module: *module, Dockerfile: *dockerfile, Root: *root, PID: *pid,
		Container: *container, ContainerJSON: *containerJSON, KernelLog: *kernelLog, AppLog: *appLog,
		PGSettings: *pgSettings, PGHBA: *pgHBA, PGLog: *pgLog, PGRoles: *pgRoles, PGActivity: *pgActivity,
		Firewall: *firewall, ProxyConfig: *proxyConfig, ProxyVersion: *proxyVersion,
		AppArgs: *appArgs, NetstatBefore: *netstatBefore, NetstatAfter: *netstatAfter,
		StatBefore: *statBefore, StatAfter: *statAfter, IMDS: *imds, VulnDB: *vulndb,
		MetricsBefore: *metricsBefore, MetricsAfter: *metricsAfter, SS: *ss,
	})
	if err != nil {
		return err
	}
	asJSON := sel.Has(audit.Output, "json") && !sel.Has(audit.Output, "text")
	if *sbom {
		if *binary == "" {
			return errors.New("-sbom reads a binary: name one with -binary")
		}
		modules := audit.SBOM(target)
		if asJSON {
			enc := json.NewEncoder(out)
			enc.SetIndent("", "  ")
			return enc.Encode(modules)
		}
		for _, m := range modules {
			main := ""
			if m.Main {
				main = "  (main)"
			}
			_, _ = fmt.Fprintf(out, "%-60s %s%s\n", m.Path, m.Version, main)
		}
		return nil
	}
	findings, err := audit.Run(sel, audit.Answers(answers), target)
	if err != nil {
		return err
	}
	// Under the comparative profile the report is also the table the
	// study publishes for each machine: values read, which every machine
	// of the comparison must share.
	var system []audit.Setting
	if sel.Has(audit.Profile, "comparative") {
		system = audit.System(target)
	}
	if findings == nil {
		findings = []audit.Finding{} // "findings": [] says nothing was found; null would say nothing was asked
	}
	current := audit.Report{Findings: findings, System: system}
	var diff *audit.Diff
	if *compare != "" {
		b, err := os.ReadFile(*compare) //nolint:gosec // G304: the path is the caller's
		if err != nil {
			return err
		}
		var earlier audit.Report
		if err := json.Unmarshal(b, &earlier); err != nil {
			return fmt.Errorf("%s: not a JSON report of this tool (-output json): %w", *compare, err)
		}
		if earlier.Findings == nil {
			return fmt.Errorf("%s: not a JSON report of this tool (-output json): no findings array", *compare)
		}
		d := audit.Compare(earlier, current)
		diff = &d
	}
	if *propose != "" {
		if err := writeFixes(*propose, findings); err != nil {
			return err
		}
	}
	var tripped []string
	if *failOn != "" {
		if tripped, err = audit.Gate(strings.Split(*failOn, ","), findings, diff); err != nil {
			return err
		}
	}
	if err := printReport(out, asJSON, current, diff); err != nil {
		return err
	}
	if len(tripped) > 0 {
		return gateError(tripped)
	}
	return nil
}

// printReport writes the report, or the comparison when there is one.
func printReport(out *os.File, asJSON bool, current audit.Report, diff *audit.Diff) error {
	findings, system := current.Findings, current.System
	if asJSON {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		if diff != nil {
			return enc.Encode(diff)
		}
		return enc.Encode(current)
	}
	if diff != nil {
		printDiff(out, diff)
		return nil
	}
	if len(system) > 0 {
		_, _ = fmt.Fprintf(out, "system, as a comparison publishes it\n")
		width := 0
		for _, s := range system {
			width = max(width, len(s.Name))
		}
		for _, s := range system {
			_, _ = fmt.Fprintf(out, "  %-*s  %s\n", width, s.Name, s.Value)
		}
		_, _ = fmt.Fprintln(out)
	}
	if len(findings) == 0 {
		_, _ = fmt.Fprintf(out, "nothing to propose under this selection\n")
		return nil
	}
	for _, f := range findings {
		head := f.Rule + " (" + f.Goal + ")"
		if f.Verdict != "" {
			head += " — " + f.Verdict
		}
		_, _ = fmt.Fprintf(out, "%s\n  found:    %s\n  proposed: %s\n  why:      %s\n  doc:      %s\n\n",
			head, f.Current, f.Proposed, wrap(f.Why, 72, "            "), f.Doc)
	}
	return nil
}

// writeFixes writes the proposed files in dir, refusing to replace one
// that is there: a file a person edited is theirs.
func writeFixes(dir string, findings []audit.Finding) error {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	for _, f := range audit.Propose(findings) {
		path := filepath.Join(dir, f.Name)
		file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600) //nolint:gosec // G304: under the directory the caller named
		if err != nil {
			return fmt.Errorf("-propose: %w", err)
		}
		_, werr := file.WriteString(f.Text)
		if err := errors.Join(werr, file.Close()); err != nil {
			return err
		}
	}
	return nil
}

// printDiff writes a comparison: what appeared, what changed, what went,
// and the lines of the system table that moved.
func printDiff(out *os.File, d *audit.Diff) {
	for _, f := range d.Appeared {
		_, _ = fmt.Fprintf(out, "appeared  %s: %s\n", f.Rule, f.Current)
	}
	for i := range d.Modified {
		m := &d.Modified[i]
		_, _ = fmt.Fprintf(out, "changed   %s: %s\n          was: %s\n", m.Is.Rule, m.Is.Current, m.Was.Current)
	}
	for _, f := range d.Resolved {
		_, _ = fmt.Fprintf(out, "resolved  %s: %s\n", f.Rule, f.Current)
	}
	for _, s := range d.System {
		_, _ = fmt.Fprintf(out, "system    %s: %s -> %s\n", s.Name, s.Was, s.Is)
	}
	_, _ = fmt.Fprintf(out, "%d appeared, %d changed, %d resolved, %d unchanged\n",
		len(d.Appeared), len(d.Modified), len(d.Resolved), d.Unchanged)
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
