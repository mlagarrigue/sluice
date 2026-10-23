package sluice_test

import (
	"os"
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
