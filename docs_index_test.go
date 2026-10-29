package sluice_test

import (
	"bytes"
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

// The CHANGELOG's latest release lists every breaking commit of its own
// range. release-please writes it, and the tool reads only its last 500
// commits by default: 0.3.0 was 762 commits wide and lost 26 of its 47
// breaking changes, every one of them older than the cut, with no warning
// anywhere. commit-search-depth now covers a lot that size and this counts
// the result, since a silent omission is exactly what the setting failed
// to prevent once.
func TestEveryBreakingCommitIsInTheChangelog(t *testing.T) {
	if shallow, err := exec.Command("git", "rev-parse", "--is-shallow-repository").Output(); err != nil || strings.TrimSpace(string(shallow)) != "false" {
		t.Skip("a shallow clone does not hold the history of the release")
	}
	page, err := os.ReadFile("CHANGELOG.md")
	if err != nil {
		t.Fatal(err)
	}
	// The first section is the latest release, and its heading carries the
	// range it was written from: ## [0.3.0](…/compare/v0.2.2...v0.3.0) (date)
	head := regexp.MustCompile(`(?m)^## \[[^\]]+\]\([^)]*/compare/(.+?)\.{3}(.+?)\)`)
	m := head.FindSubmatch(page)
	if m == nil {
		// A failure, not a skip: the day the heading changes shape is the
		// day this check would go green forever without reading anything.
		t.Fatal("the latest CHANGELOG section names no compare range; update this test with the heading")
	}
	from, to := string(m[1]), string(m[2])
	section := string(page)[bytes.Index(page, m[0]):]
	if next := regexp.MustCompile(`(?m)^## \[`).FindStringIndex(section[1:]); next != nil {
		section = section[:next[0]+1]
	}
	out, err := exec.Command("git", "log", "--no-merges", "--format=%h %s", from+".."+to).Output() //nolint:gosec // G204: two tags of this repository
	if err != nil {
		t.Skipf("no history for %s..%s: %v", from, to, err)
	}
	breaking := regexp.MustCompile(`^([0-9a-f]+) [a-z]+(?:\([a-z]+\))?!: (.+)$`)
	var n, missing int
	for line := range strings.SplitSeq(string(out), "\n") {
		b := breaking.FindStringSubmatch(line)
		if b == nil {
			continue
		}
		n++
		if !strings.Contains(section, b[2]) {
			missing++
			t.Errorf("%s is not in the %s section of CHANGELOG.md: %q", b[1], to, b[2])
		}
	}
	t.Logf("%d breaking commits in %s..%s, %d missing", n, from, to, missing)
}

// The CHANGELOG lists breaking changes by their subject; what to write
// instead is in docs/migration.md, and the note that says so sits above the
// first release, where release-please inserts each new one and leaves the
// header as it is. A release that drops the note fails here, on its pull
// request.
func TestChangelogPointsToTheMigrationGuide(t *testing.T) {
	page, err := os.ReadFile("CHANGELOG.md")
	if err != nil {
		t.Fatal(err)
	}
	first := regexp.MustCompile(`(?m)^## \[`).FindIndex(page)
	if first == nil {
		t.Fatal("CHANGELOG.md has no release section")
	}
	if !bytes.Contains(page[:first[0]], []byte("(docs/migration.md)")) {
		t.Error("CHANGELOG.md does not link docs/migration.md above its first release")
	}
}

// A nested module's README that tables the versions it pins says what its
// go.mod requires: the table is what a reader checks a result against, and
// dependabot moves the go.mod alone. Every row "| `module` | vX |" is
// compared with the module's require line.
func TestNestedReadmesPinWhatTheirGoModRequires(t *testing.T) {
	mods, err := filepath.Glob("*/go.mod")
	if err != nil {
		t.Fatal(err)
	}
	more, err := filepath.Glob("*/*/go.mod")
	if err != nil {
		t.Fatal(err)
	}
	row := regexp.MustCompile("(?m)^\\| `([^`]+)` \\| (v[^ |]+) \\|")
	var rows int
	for _, mod := range append(mods, more...) {
		dir := filepath.Dir(mod)
		readme, err := os.ReadFile(filepath.Join(dir, "README.md")) //nolint:gosec // G304: a repository path, not input
		if err != nil {
			continue
		}
		gomod, err := os.ReadFile(mod) //nolint:gosec // G304: a repository path, not input
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range row.FindAllSubmatch(readme, -1) {
			rows++
			path, pinned := string(m[1]), string(m[2])
			req := regexp.MustCompile(`(?m)^(?:require\s+|\s+)` + regexp.QuoteMeta(path) + `\s+(v\S+)`).FindSubmatch(gomod)
			switch {
			case req == nil:
				t.Errorf("%s/README.md pins %s %s, which %s does not require", dir, path, pinned, mod)
			case string(req[1]) != pinned:
				t.Errorf("%s/README.md pins %s %s; %s requires %s", dir, path, pinned, mod, req[1])
			}
		}
	}
	if rows == 0 {
		t.Fatal("no pinned version found in a nested module's README; update this test with the table's shape")
	}
}
