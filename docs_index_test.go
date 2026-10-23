package sluice_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The index of decisions (docs/design/adr/README.md) is how a reader finds the
// record that answers their question: one line per record, under the section
// of its theme, linked by its exact title. A record added without its line,
// retitled without it, or filed under another theme is a decision the index
// hides.
func TestEveryDecisionHasItsLineInTheIndex(t *testing.T) {
	records, err := filepath.Glob("docs/design/adr/[0-9][0-9][0-9][0-9]-*.md")
	if err != nil {
		t.Fatal(err)
	}
	index, err := os.ReadFile("docs/design/adr/README.md")
	if err != nil {
		t.Fatal(err)
	}
	type line struct {
		title, section string
	}
	entry := regexp.MustCompile(`^- \[(.+)\]\((\d{4}-[\w-]+\.md)\)`)
	listed := map[string][]line{}
	section := ""
	for l := range strings.SplitSeq(string(index), "\n") {
		if strings.HasPrefix(l, "## ") {
			section = l
			continue
		}
		if m := entry.FindStringSubmatch(l); m != nil {
			listed[m[2]] = append(listed[m[2]], line{m[1], section})
		}
	}
	for _, f := range records {
		b, err := os.ReadFile(f) //nolint:gosec // G304: a repository path, not input
		if err != nil {
			t.Fatal(err)
		}
		title, _, _ := strings.Cut(string(b), "\n")
		title = strings.TrimPrefix(title, "# ")
		theme := ""
		for l := range strings.SplitSeq(string(b), "\n") {
			if t, ok := strings.CutPrefix(l, "- **Theme:** "); ok {
				theme = strings.TrimSpace(t)
				break
			}
		}
		name := filepath.Base(f)
		lines := listed[name]
		delete(listed, name)
		switch {
		case len(lines) != 1:
			t.Errorf("%s has %d lines in the index, not one", name, len(lines))
		case lines[0].title != title:
			t.Errorf("%s is listed as %q; its title is %q", name, lines[0].title, title)
		case !strings.Contains(lines[0].section, "(`"+theme+"`)"):
			t.Errorf("%s is listed under %q; its theme is %q", name, lines[0].section, theme)
		}
	}
	for name := range listed {
		t.Errorf("the index lists %s, which is not a decision record", name)
	}
}

// Every breaking commit since the last release — a subject with "!" before
// its colon — has its entry in docs/migration.md, found by its short hash: a
// user upgrading reads one page, and a break that lands without its entry is
// one they discover by compiling. An entry written in the breaking commit
// itself cannot know that commit's hash, so it may quote its subject instead.
func TestEveryBreakingCommitHasAMigrationEntry(t *testing.T) {
	if shallow, err := exec.Command("git", "rev-parse", "--is-shallow-repository").Output(); err != nil || strings.TrimSpace(string(shallow)) != "false" {
		t.Skip("a shallow clone does not hold the history since the last tag")
	}
	tag, err := exec.Command("git", "describe", "--tags", "--abbrev=0").Output()
	if err != nil {
		t.Skipf("no tag reachable: %v", err)
	}
	out, err := exec.Command("git", "log", "--format=%h %s", strings.TrimSpace(string(tag))+"..HEAD").Output() //nolint:gosec // G204: a tag of this repository
	if err != nil {
		t.Fatal(err)
	}
	page, err := os.ReadFile("docs/migration.md")
	if err != nil {
		t.Fatal(err)
	}
	breaking := regexp.MustCompile(`^([0-9a-f]+) [a-z]+(\([a-z]+\))?!: (.+)$`)
	var n int
	for line := range strings.SplitSeq(string(out), "\n") {
		m := breaking.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		n++
		if !strings.Contains(string(page), "`"+m[1][:7]+"`") && !strings.Contains(string(page), m[3]) {
			t.Errorf("%s has no entry in docs/migration.md", line)
		}
	}
	t.Logf("%d breaking commits since %s", n, strings.TrimSpace(string(tag)))
}
