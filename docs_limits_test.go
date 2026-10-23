package sluice_test

import (
	"os"
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
