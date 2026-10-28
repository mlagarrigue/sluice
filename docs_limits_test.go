package sluice_test

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// maxLimitLines is how long one entry of the limits page may run. Twelve lines
// hold what is missing or refused, why, and the condition that lifts it; past
// that, an entry is telling the story of how the limit was found, which
// belongs to the page that explains it, reached by a link.
const maxLimitLines = 12

// Every top-level bullet of docs/guide/limits.md says its limit in twelve
// lines at most. A bullet runs from its "- " to the blank line that is
// followed by a line not indented under it; its nested lines and its own
// indented paragraphs count, the prose after the list does not.
func TestLimitsAreShort(t *testing.T) {
	const page = "docs/guide/limits.md"
	b, err := os.ReadFile(page)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(string(b), "\n")
	start, n, blanks := 0, 0, 0
	flush := func() {
		if start > 0 && n > maxLimitLines {
			t.Errorf("%s:%d: the bullet runs %d lines; say the limit in %d and link the story",
				page, start, n, maxLimitLines)
		}
		start, n, blanks = 0, 0, 0
	}
	fenced := false
	for i, line := range lines {
		if strings.HasPrefix(strings.TrimSpace(line), "```") {
			fenced = !fenced
		}
		switch {
		case !fenced && strings.HasPrefix(line, "- "):
			flush()
			start, n = i+1, 1
		case start == 0:
		case line == "":
			blanks++
		case blanks > 0 && !fenced && !strings.HasPrefix(line, " "):
			flush()
		default:
			n += blanks + 1
			blanks = 0
		}
	}
	flush()
}

// A learning guide ends on what its reader can now do and the command that
// shows it: one "## You can now" section, the last but for "What next", with
// a sh block that runs `go`. A guide that ends on a figure or a link leaves the
// reader to work out what they learned and whether it worked.
func TestGuidesEndOnWhatYouCanDo(t *testing.T) {
	for _, page := range []string{
		"docs/guide/getting-started.md", "docs/guide/operators.md",
		"docs/guide/postgres.md", "docs/guide/web.md",
	} {
		b, err := os.ReadFile(page) //nolint:gosec // G304: a repository path, not input
		if err != nil {
			t.Fatal(err)
		}
		var sections []string
		var body []string
		fenced := false
		for _, line := range strings.Split(string(b), "\n") {
			if strings.HasPrefix(line, "```") {
				fenced = !fenced
			}
			if !fenced && strings.HasPrefix(line, "## ") {
				sections = append(sections, line)
				body = append(body, "")
				continue
			}
			if len(body) > 0 {
				body[len(body)-1] += line + "\n"
			}
		}
		i := slices.Index(sections, "## You can now")
		switch {
		case i < 0:
			t.Errorf("%s: no \"## You can now\" section", page)
			continue
		case slices.Index(sections[i+1:], "## You can now") >= 0:
			t.Errorf("%s: two \"## You can now\" sections", page)
		case i != len(sections)-1 && sections[len(sections)-1] != "## What next" || i < len(sections)-2:
			t.Errorf("%s: \"## You can now\" is followed by %v; it closes the guide", page, sections[i+1:])
		}
		if !strings.Contains(body[i], "```sh\ngo ") {
			t.Errorf("%s: \"## You can now\" has no sh block starting with a go command", page)
		}
	}
}

// Every guide explains at least one term for a beginner: in a box of its
// own, "> **In plain words…", or inline, "In plain words:", the form the
// limits page uses in each of its entries. One name for the box, in every
// document: a second name is a reader wondering whether it means something
// else.
func TestEveryGuideHasAPlainWordsBox(t *testing.T) {
	guides, err := filepath.Glob("docs/guide/*.md")
	if err != nil {
		t.Fatal(err)
	}
	for _, g := range guides {
		b, err := os.ReadFile(g) //nolint:gosec // G304: a repository path, not input
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(b), "\n> **In plain words") && !strings.Contains(string(b), "In plain words:") {
			t.Errorf("%s: no \"> **In plain words\" box and no \"In plain words:\"", g)
		}
	}
	for _, f := range markdownFiles(t) {
		b, err := os.ReadFile(f) //nolint:gosec // G304: a repository path, not input
		if err != nil {
			t.Fatal(err)
		}
		if n := strings.Count(string(b), "In plain terms"); n > 0 {
			t.Errorf("%s: %d boxes named \"In plain terms\"; the name is \"In plain words\"", f, n)
		}
	}
}

// maxRecordParagraph is the longest paragraph a decision record may hold.
const maxRecordParagraph = 14

// A decision record is read one idea at a time: no paragraph — the lines
// between two blank ones, a list without blank lines between its items
// counting as one — runs past fourteen lines outside code blocks.
func TestDecisionRecordParagraphsAreShort(t *testing.T) {
	records, err := filepath.Glob("docs/design/adr/[0-9][0-9][0-9][0-9]-*.md")
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range records {
		b, err := os.ReadFile(f) //nolint:gosec // G304: a repository path, not input
		if err != nil {
			t.Fatal(err)
		}
		start, n, fenced := 0, 0, false
		for i, line := range append(strings.Split(string(b), "\n"), "") {
			if strings.HasPrefix(strings.TrimSpace(line), "```") {
				fenced = !fenced
				n = 0
				continue
			}
			if fenced {
				continue
			}
			if strings.TrimSpace(line) != "" {
				if n == 0 {
					start = i + 1
				}
				n++
				continue
			}
			if n > maxRecordParagraph {
				t.Errorf("%s:%d: a paragraph of %d lines; one idea per paragraph", f, start, n)
			}
			n = 0
		}
	}
}

// maxQuoteLines is the longest run of quotation the architecture may hold
// without a heading: past it, several subjects run together in one aside and
// a reader cannot find the one they came for.
const maxQuoteLines = 40

// The architecture quotes what the implementation learned, under the decision
// it revised. No run of quoted lines — blank ">" lines included — goes past
// forty: a longer one is a journal, and its subjects get headings.
func TestArchitectureQuotesAreShort(t *testing.T) {
	const page = "docs/design/architecture.md"
	b, err := os.ReadFile(page)
	if err != nil {
		t.Fatal(err)
	}
	start, n := 0, 0
	for i, line := range append(strings.Split(string(b), "\n"), "") {
		if strings.HasPrefix(line, ">") {
			if n == 0 {
				start = i + 1
			}
			n++
			continue
		}
		if n > maxQuoteLines {
			t.Errorf("%s:%d: %d quoted lines in a row; give their subjects headings", page, start, n)
		}
		n = 0
	}
}

// maxParagraphLines is the longest paragraph of prose a guide or an entry
// document may hold. Past it a paragraph carries eight or more things at
// once, and a beginner the guides are written for loses the thread; it is
// cut where its subject changes, or becomes a list.
const maxParagraphLines = 18

var numbered = regexp.MustCompile(`^\d+\. `)

// Every paragraph of prose in the README, CONTRIBUTING, the guides and the
// architecture runs maxParagraphLines lines at most. Code blocks and tables
// do not count; a list item counts as a paragraph of its own, from its
// marker; a quoted box counts without its "> ".
func TestProseParagraphsAreShort(t *testing.T) {
	guides, err := filepath.Glob("docs/guide/*.md")
	if err != nil {
		t.Fatal(err)
	}
	for _, page := range append(guides, "README.md", "CONTRIBUTING.md", "docs/design/architecture.md") {
		b, err := os.ReadFile(page) //nolint:gosec // G304: a repository path, not input
		if err != nil {
			t.Fatal(err)
		}
		start, n, fenced := 0, 0, false
		flush := func() {
			if n > maxParagraphLines {
				t.Errorf("%s:%d: a paragraph of %d lines; cut it where its subject changes (at most %d)",
					page, start, n, maxParagraphLines)
			}
			n = 0
		}
		for i, line := range strings.Split(string(b), "\n") {
			if strings.HasPrefix(line, "```") {
				fenced = !fenced
				flush()
				continue
			}
			if fenced {
				continue
			}
			text := strings.TrimPrefix(strings.TrimPrefix(line, ">"), " ")
			if strings.TrimSpace(text) == "" || strings.HasPrefix(text, "- ") || numbered.MatchString(text) || strings.HasPrefix(text, "|") {
				flush()
				if strings.HasPrefix(text, "- ") || numbered.MatchString(text) {
					start, n = i+1, 1
				}
				continue
			}
			if n == 0 {
				start = i + 1
			}
			n++
		}
		flush()
	}
}
