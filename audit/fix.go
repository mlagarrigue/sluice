package audit

import (
	"fmt"
	"slices"
	"strings"
)

// The fixes the audit can write, never apply. A finding whose correct
// value is sure without asking — a kernel bound, a server setting with one
// right answer — carries it as a [Fix]; [Propose] gathers them into files
// a person reads, then applies with the command written at their head. A
// fix that needs a judgment (a certificate for ssl, a memory size for
// shared_buffers) is not written: the finding's Proposed says what to
// decide.

// The kinds of fix, one file each.
const (
	// FixSysctl is a kernel value, written as a sysctl.d file.
	FixSysctl = "sysctl"
	// FixPostgres is a PostgreSQL setting, written as ALTER SYSTEM
	// statements: the form every installation accepts, whichever file its
	// configuration lives in, and that pg_settings reads back.
	FixPostgres = "postgres"
)

// A Fix is one setting to give one value.
type Fix struct {
	Kind  string `json:"kind"`
	Name  string `json:"name"`
	Value string `json:"value"`
}

// sysctlName turns a path under /proc/sys into the name sysctl takes:
// proc/sys/net/core/somaxconn is net.core.somaxconn.
func sysctlName(path string) string {
	return strings.ReplaceAll(strings.TrimPrefix(path, "proc/sys/"), "/", ".")
}

// A ProposedFile is one file of fixes: its name, and its text.
type ProposedFile struct {
	Name string
	Text string
}

// Propose writes the fixes the findings carry, one file per kind, each
// line preceded by the rule that asked for it and its documentation. A
// name set twice keeps the last value, as the kernel and the server would.
func Propose(findings []Finding) []ProposedFile {
	type line struct{ rule, doc, name, value string }
	byKind := map[string][]line{}
	for i := range findings {
		f := &findings[i]
		if x := f.Fix; x != nil {
			byKind[x.Kind] = append(byKind[x.Kind], line{f.Rule, f.Doc, x.Name, x.Value})
		}
	}
	var out []ProposedFile
	if lines := byKind[FixSysctl]; len(lines) > 0 {
		var b strings.Builder
		b.WriteString("# Written by the audit, not applied by it. Read it, then:\n" +
			"#   sudo cp 99-sluice-audit.conf /etc/sysctl.d/ && sudo sysctl --system\n")
		for _, l := range lines {
			fmt.Fprintf(&b, "\n# %s — %s\n%s = %s\n", l.rule, l.doc, l.name, l.value)
		}
		out = append(out, ProposedFile{"99-sluice-audit.conf", b.String()})
	}
	if lines := byKind[FixPostgres]; len(lines) > 0 {
		var b strings.Builder
		b.WriteString("-- Written by the audit, not applied by it. Read it, then, as a superuser:\n" +
			"--   psql -X -v ON_ERROR_STOP=1 -f sluice-audit.sql\n" +
			"-- ALTER SYSTEM writes postgresql.auto.conf; pg_reload_conf() applies what a reload can.\n")
		for _, l := range lines {
			fmt.Fprintf(&b, "\n-- %s — %s\nALTER SYSTEM SET %s = '%s';\n", l.rule, l.doc, l.name, strings.ReplaceAll(l.value, "'", "''"))
		}
		b.WriteString("\nSELECT pg_reload_conf();\n")
		out = append(out, ProposedFile{"sluice-audit.sql", b.String()})
	}
	slices.SortFunc(out, func(a, b ProposedFile) int { return strings.Compare(a.Name, b.Name) })
	return out
}
