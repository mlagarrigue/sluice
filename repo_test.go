package sluice_test

import (
	"bufio"
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"unicode"
)

// scopes is the closed list a commit's scope, an ADR's theme and an issue's
// component are drawn from. CONTRIBUTING.md and the issue template repeat it
// for readers; TestScopeListIsRepeatedVerbatim keeps the copies honest.
var scopes = []string{
	"core", "join", "window", "parallel", "distinct", "postgres", "quic",
	"httpstream", "web", "gateway", "diagnostics", "bench", "docs", "ci",
	"devcontainer",
}

func TestScopeListIsRepeatedVerbatim(t *testing.T) {
	for _, f := range []string{"CONTRIBUTING.md", ".github/ISSUE_TEMPLATE/bug.yml"} {
		b, err := os.ReadFile(f) //nolint:gosec // G304: a repository path, not input
		if err != nil {
			t.Fatal(err)
		}
		for _, s := range scopes {
			if !bytes.Contains(b, []byte(s)) {
				t.Errorf("%s does not list the scope %q", f, s)
			}
		}
	}
}

// Markdown documents meant for readers. The links between them are checked
// as a reader would follow them: a relative path must exist, and a fragment
// must name a heading of the target, slugged the way GitHub renders it.
func markdownFiles(t *testing.T) []string {
	t.Helper()
	out := []string{"README.md", "CONTRIBUTING.md", "SECURITY.md"}
	err := filepath.WalkDir("docs", func(p string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.HasSuffix(p, ".md") {
			out = append(out, p)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// slug renders a heading as GitHub does: lowercased, punctuation dropped,
// spaces to hyphens, a numeric suffix for repeats.
func headingSlugs(content string) map[string]bool {
	seen := map[string]int{}
	out := map[string]bool{}
	inFence := false
	for line := range strings.SplitSeq(content, "\n") {
		if strings.HasPrefix(line, "```") {
			inFence = !inFence
			continue
		}
		if inFence || !strings.HasPrefix(line, "#") {
			continue
		}
		text := strings.TrimLeft(line, "#")
		if !strings.HasPrefix(text, " ") {
			continue
		}
		text = strings.TrimSpace(text)
		text = regexp.MustCompile(`\[([^\]]*)\]\([^)]*\)`).ReplaceAllString(text, "$1")
		var b strings.Builder
		for _, r := range strings.ToLower(text) {
			switch {
			case r == ' ':
				b.WriteByte('-')
			case r == '-' || r == '_' || unicode.IsLetter(r) || unicode.IsNumber(r):
				b.WriteRune(r)
			}
		}
		s := b.String()
		n := seen[s]
		seen[s] = n + 1
		if n > 0 {
			s = s + "-" + string(rune('0'+n))
		}
		out[s] = true
	}
	return out
}

var linkRE = regexp.MustCompile(`\]\(([^)\s]+)\)`)

func TestDocumentationLinksResolve(t *testing.T) {
	slugs := map[string]map[string]bool{}
	for _, f := range markdownFiles(t) {
		b, err := os.ReadFile(f) //nolint:gosec // G304: a repository path, not input
		if err != nil {
			t.Fatal(err)
		}
		slugs[filepath.Clean(f)] = headingSlugs(string(b))
	}
	for _, f := range markdownFiles(t) {
		b, _ := os.ReadFile(f) //nolint:gosec // G304: a repository path, not input
		inFence := false
		for i, line := range strings.Split(string(b), "\n") {
			if strings.HasPrefix(line, "```") {
				inFence = !inFence
			}
			if inFence {
				continue
			}
			for _, m := range linkRE.FindAllStringSubmatch(line, -1) {
				target := m[1]
				if strings.Contains(target, "://") || strings.HasPrefix(target, "mailto:") {
					continue
				}
				path, frag, _ := strings.Cut(target, "#")
				dest := filepath.Clean(f)
				if path != "" {
					dest = filepath.Clean(filepath.Join(filepath.Dir(f), path))
					if _, err := os.Stat(dest); err != nil {
						t.Errorf("%s:%d: link to %q: %v", f, i+1, target, err)
						continue
					}
				}
				if frag == "" {
					continue
				}
				heads, ok := slugs[dest]
				if !ok {
					continue // a fragment into a non-markdown file or a directory
				}
				if !heads[frag] {
					t.Errorf("%s:%d: no heading %q in %s", f, i+1, frag, dest)
				}
			}
		}
	}
}

// Every decision record has the same header, so the index of decisions can
// be built from the files and a reader always finds the date and the theme
// in the same place. The history of a decision is its git log, so a record
// is revised in place: no "superseded" status exists.
func TestDecisionRecordsShareOneHeader(t *testing.T) {
	files, err := filepath.Glob("docs/design/adr/[0-9][0-9][0-9][0-9]-*.md")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatal("no decision records found")
	}
	title := regexp.MustCompile(`^# ADR (\d{4}) — .+`)
	date := regexp.MustCompile(`^\d{4}-\d{2}-\d{2}`)
	for i, f := range files {
		b, err := os.ReadFile(f) //nolint:gosec // G304: a repository path, not input
		if err != nil {
			t.Fatal(err)
		}
		lines := strings.Split(string(b), "\n")
		m := title.FindStringSubmatch(lines[0])
		if m == nil {
			t.Errorf("%s: first line %q is not `# ADR NNNN — title`", f, lines[0])
			continue
		}
		if want := filepath.Base(f)[:4]; m[1] != want {
			t.Errorf("%s: title numbers the record %s", f, m[1])
		}
		if want := i + 1; int(m[1][0]-'0')*1000+int(m[1][1]-'0')*100+int(m[1][2]-'0')*10+int(m[1][3]-'0') != want {
			t.Errorf("%s: records are numbered contiguously from 0001; expected %04d", f, want)
		}
		sc := bufio.NewScanner(strings.NewReader(string(b)))
		var created, theme bool
		var sections []string
		for sc.Scan() {
			l := sc.Text()
			switch {
			case strings.HasPrefix(l, "- **Created:** "):
				created = date.MatchString(strings.TrimPrefix(l, "- **Created:** "))
			case strings.HasPrefix(l, "- **Revised:** "):
				// "—" says the record was never revised.
				if r := strings.TrimPrefix(l, "- **Revised:** "); r != "—" && !date.MatchString(r) {
					t.Errorf("%s: a revision is dated YYYY-MM-DD: %q", f, l)
				}
			case strings.HasPrefix(l, "- **Theme:** "):
				th := strings.TrimPrefix(l, "- **Theme:** ")
				theme = slices.Contains(scopes, th)
				if !theme {
					t.Errorf("%s: theme %q is not in the closed list", f, th)
				}
			case strings.HasPrefix(l, "## "):
				sections = append(sections, strings.TrimPrefix(l, "## "))
			}
		}
		if !created {
			t.Errorf("%s: no `- **Created:** YYYY-MM-DD` line", f)
		}
		if !theme {
			t.Errorf("%s: no `- **Theme:**` line", f)
		}
		if want := []string{"Problem", "Decision", "Rationale"}; !slices.Equal(sections, want) {
			t.Errorf("%s: sections are %v, want %v", f, sections, want)
		}
		if bytes.Contains(bytes.ToLower(b), []byte("superseded")) {
			t.Errorf("%s: a record is revised in place, never superseded", f)
		}
	}
}

// Commit messages are one line in Conventional Commits form with a scope
// from the closed list. Checked over the whole history, which is what a
// reader of `git log --oneline` gets. Merge commits are skipped: a pull
// request's CI runs on a synthetic merge whose subject GitHub writes. In a
// shallow clone that merge is a parentless boundary commit, which
// --no-merges cannot tell from an ordinary one, so the test asks for the
// whole history and skips without it; the hygiene job fetches it.
func TestCommitMessagesFollowTheConvention(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
	if shallow, err := exec.Command("git", "rev-parse", "--is-shallow-repository").Output(); err != nil || strings.TrimSpace(string(shallow)) != "false" {
		t.Skip("needs the full history: a shallow clone hides which commits are merges")
	}
	out, err := exec.Command("git", "log", "--no-merges", "--format=%H%x00%s%x00%b%x1e").Output()
	if err != nil {
		t.Skipf("not a git checkout: %v", err)
	}
	subject := regexp.MustCompile(`^(feat|fix|perf|refactor|docs|test|build|ci|chore)(\(([a-z]+)\))?!?: \S.*[^.]$`)
	for rec := range strings.SplitSeq(strings.TrimSuffix(string(out), "\x1e\n"), "\x1e\n") {
		parts := strings.SplitN(rec, "\x00", 3)
		if len(parts) < 2 {
			continue
		}
		sha, subj := parts[0][:7], parts[1]
		m := subject.FindStringSubmatch(subj)
		if m == nil {
			t.Errorf("%s: subject %q is not `<type>(<scope>): <subject>` without a trailing period", sha, subj)
			continue
		}
		// release-please's own commits are "chore(main): release X.Y.Z";
		// that scope is its, not ours, and only that shape is let through.
		releaseCommit := m[1] == "chore" && m[3] == "main" && strings.HasPrefix(subj, "chore(main): release ")
		if m[3] != "" && !releaseCommit && !slices.Contains(scopes, m[3]) {
			t.Errorf("%s: scope %q is not in the closed list", sha, m[3])
		}
		if len(subj) > 72 {
			t.Errorf("%s: subject is %d characters, 72 at most", sha, len(subj))
		}
		if len(parts) == 3 && regexp.MustCompile(`(?mi)^co-authored-by:`).MatchString(parts[2]) {
			t.Errorf("%s: a Co-Authored-By trailer", sha)
		}
	}
}
