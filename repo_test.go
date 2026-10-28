package sluice_test

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"go/ast"
	godoc "go/doc"
	"go/doc/comment"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
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
	"devcontainer", "audit",
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

// linkedPages is markdownFiles plus the pages a reader reaches outside docs/:
// the changelog, the code of conduct, the nested modules' READMEs and the
// pull request template. Their links are checked like the others; they are
// not documentation, so the tests that ask whether something is documented
// do not read them.
func linkedPages(t *testing.T) []string {
	t.Helper()
	out := append(markdownFiles(t), "CHANGELOG.md", "CODE_OF_CONDUCT.md")
	for _, dir := range []string{"benchmarks", "comparison", "interop", ".github"} {
		err := filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
			if err == nil && !d.IsDir() && strings.HasSuffix(p, ".md") {
				out = append(out, p)
			}
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
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
		s := slugOf(text)
		n := seen[s]
		seen[s] = n + 1
		if n > 0 {
			s = s + "-" + string(rune('0'+n))
		}
		out[s] = true
	}
	return out
}

// slugOf is the slug of one heading's text, before any suffix for repeats.
func slugOf(text string) string {
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
	return b.String()
}

var linkRE = regexp.MustCompile(`\]\(([^)\s]+)\)`)

func TestDocumentationLinksResolve(t *testing.T) {
	slugs := map[string]map[string]bool{}
	for _, f := range linkedPages(t) {
		b, err := os.ReadFile(f) //nolint:gosec // G304: a repository path, not input
		if err != nil {
			t.Fatal(err)
		}
		slugs[filepath.Clean(f)] = headingSlugs(string(b))
	}
	for _, f := range linkedPages(t) {
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

// titleRef is a page named and then one of its titles quoted, in one
// sentence: `docs/README.md, "Security — verifiable properties"` or
// `docs/README.md ("I want to connect another system")`.
var titleRef = regexp.MustCompile(`([\w./-]+\.md)\)?[^".]{0,40}?["“]([A-Z][^"”]{2,80})["”]`)

// A sentence that sends the reader to a page by one of its titles is a link
// no tool follows: the page is renamed or the section retitled, and the
// sentence still reads as if it pointed somewhere. The quoted title must be a
// heading of the page or the text of one of its links — an index such as
// docs/README.md reaches a section through a link named after it. The issue
// templates are read too, since they cite the index the same way.
func TestReferencesByTitleResolve(t *testing.T) {
	templates, err := filepath.Glob(".github/ISSUE_TEMPLATE/*.yml")
	if err != nil {
		t.Fatal(err)
	}
	linkText := regexp.MustCompile(`\[([^\]]+)\]\(`)
	titles := map[string]map[string]bool{}
	titlesOf := func(page string) (map[string]bool, bool) {
		if ts, ok := titles[page]; ok {
			return ts, true
		}
		b, err := os.ReadFile(page) //nolint:gosec // G304: a repository path, not input
		if err != nil {
			return nil, false
		}
		ts := map[string]bool{}
		for line := range strings.SplitSeq(string(b), "\n") {
			if h := strings.TrimLeft(line, "#"); h != line && strings.HasPrefix(h, " ") {
				ts[strings.TrimSpace(h)] = true
			}
			for _, m := range linkText.FindAllStringSubmatch(line, -1) {
				ts[m[1]] = true
			}
		}
		titles[page] = ts
		return ts, true
	}
	var refs int
	for _, f := range append(linkedPages(t), templates...) {
		b, err := os.ReadFile(f) //nolint:gosec // G304: a repository path, not input
		if err != nil {
			t.Fatal(err)
		}
		// A sentence may wrap: the page and the title are looked for in the
		// text with each line break and its indentation read as one space.
		lines := strings.Split(string(b), "\n")
		var text strings.Builder
		starts := make([]int, len(lines))
		for i, line := range lines {
			starts[i] = text.Len()
			text.WriteString(strings.TrimSpace(line))
			text.WriteByte(' ')
		}
		for _, m := range titleRef.FindAllStringSubmatchIndex(text.String(), -1) {
			page := text.String()[m[2]:m[3]]
			title := text.String()[m[4]:m[5]]
			line, _ := slices.BinarySearch(starts, m[0]+1)
			dest := filepath.Clean(filepath.Join(filepath.Dir(f), page))
			ts, ok := titlesOf(dest)
			if !ok {
				dest = filepath.Clean(page) // written from the repository root
				if ts, ok = titlesOf(dest); !ok {
					continue // a file name in passing, not a page of this tree
				}
			}
			refs++
			if !ts[title] {
				t.Errorf("%s:%d: %q is neither a heading nor a link of %s", f, line, title, dest)
			}
		}
	}
	t.Logf("%d references by title", refs)
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

// A record revised in place says when: its header's last date — Created or
// the last Revised — is no older than the last commit that changed its
// Decision or its Rationale. The header is what a reader dates the decision
// by, and ADR 0008's stopped a day before two commits changed what it
// decides. Problem, the header and the title may change without a date.
// Read from the history, so a shallow clone skips.
func TestDecisionRecordsDateTheirLastRevision(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
	if shallow, err := exec.Command("git", "rev-parse", "--is-shallow-repository").Output(); err != nil || strings.TrimSpace(string(shallow)) != "false" {
		t.Skip("needs the full history")
	}
	files, err := filepath.Glob("docs/design/adr/[0-9][0-9][0-9][0-9]-*.md")
	if err != nil {
		t.Fatal(err)
	}
	date := regexp.MustCompile(`\b\d{4}-\d{2}-\d{2}\b`)
	for _, f := range files {
		b, err := os.ReadFile(f) //nolint:gosec // G304: a repository path, not input
		if err != nil {
			t.Fatal(err)
		}
		var dated string
		for l := range strings.SplitSeq(string(b), "\n") {
			if strings.HasPrefix(l, "- **Created:** ") || strings.HasPrefix(l, "- **Revised:** ") {
				for _, d := range date.FindAllString(l, -1) {
					dated = max(dated, d)
				}
			}
		}
		// The whole file as context, so every changed line is seen under
		// its section's heading; one commit per record, newest first.
		out, err := exec.Command("git", "log", "--no-merges", "--format=\x1e%as", "-p", "-U100000", "--", f).Output() //nolint:gosec // G204: a repository path from a glob, not input
		if err != nil {
			t.Skipf("not a git checkout: %v", err)
		}
		for rec := range strings.SplitSeq(string(out), "\x1e") {
			when, diff, ok := strings.Cut(rec, "\n")
			if !ok {
				continue
			}
			if strings.Contains(diff, "\nnew file mode ") {
				// The commit that brings the record in — the history was
				// imported on 2026-10-09 — is not a revision: Created
				// dates the decision, which may be older than the file.
				break
			}
			section, inHunk, changed := "", false, false
			for l := range strings.SplitSeq(diff, "\n") {
				if strings.HasPrefix(l, "@@") {
					inHunk = true
					continue
				}
				if !inHunk || l == "" {
					continue
				}
				if text := l[1:]; strings.HasPrefix(text, "## ") && l[0] != '-' {
					section = strings.TrimPrefix(text, "## ")
				}
				if (l[0] == '+' || l[0] == '-') && (section == "Decision" || section == "Rationale") {
					changed = true
					break
				}
			}
			if !changed {
				continue
			}
			if when > dated {
				t.Errorf("%s: its Decision or Rationale changed on %s, and its header's last date is %s", f, when, dated)
			}
			break
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
	out, err := exec.Command("git", "log", "--no-merges", "--format=%H%x00%an%x00%s%x00%b%x1e").Output()
	if err != nil {
		t.Skipf("not a git checkout: %v", err)
	}
	for rec := range strings.SplitSeq(strings.TrimSuffix(string(out), "\x1e\n"), "\x1e\n") {
		parts := strings.SplitN(rec, "\x00", 4)
		if len(parts) < 3 {
			continue
		}
		body := ""
		if len(parts) == 4 {
			body = parts[3]
		}
		for _, fault := range commitMessageFaults(parts[1], parts[2], body) {
			t.Errorf("%s: %s", parts[0][:7], fault)
		}
	}
}

var (
	commitSubject = regexp.MustCompile(`^(feat|fix|perf|refactor|docs|test|build|ci|chore)(\(([a-z]+)\))?!?: \S.*[^.]$`)
	commitTrailer = regexp.MustCompile(`(?mi)^co-authored-by:`)
	// The same shape scripts/check-hygiene.sh exempts.
	releaseSubject = regexp.MustCompile(`^chore\([a-z][a-z-]*\): release ([a-z-]+ )?[0-9]+\.[0-9]+\.[0-9]+( \(#[0-9]+\))?$`)
)

// commitMessageFaults says what is wrong with one commit message — author,
// subject and body apart — or nothing. release-please's own commits are
// "chore(main): release X.Y.Z", or "chore(release-dry-run): release dryrun
// X.Y.Z" for a rehearsal under a component: that scope is its, not ours,
// and so is the trailer naming the bot that opened the pull request — both
// are let through on that one shape of subject, version included, and
// refused on every other. The
// trailer is also let through when the author is dependabot[bot]: GitHub
// writes the squash message of its pull requests, and adds a
// Co-authored-by line for every author the branch carried, as it does after
// an update of the branch from main. A person's commit with that trailer is
// still refused.
func commitMessageFaults(author, subj, body string) (faults []string) {
	releaseCommit := releaseSubject.MatchString(subj)
	m := commitSubject.FindStringSubmatch(subj)
	if m == nil && !releaseCommit {
		return []string{fmt.Sprintf("subject %q is not `<type>(<scope>): <subject>` without a trailing period", subj)}
	}
	if !releaseCommit && m[3] != "" && !slices.Contains(scopes, m[3]) {
		faults = append(faults, fmt.Sprintf("scope %q is not in the closed list", m[3]))
	}
	if len(subj) > 72 {
		faults = append(faults, fmt.Sprintf("subject is %d characters, 72 at most", len(subj)))
	}
	botCommit := releaseCommit || author == "dependabot[bot]"
	if !botCommit && commitTrailer.MatchString(body) {
		faults = append(faults, "a Co-Authored-By trailer")
	}
	return faults
}

// The trailer is let through for release-please's commits and dependabot's
// alone: the same trailer under any other subject or author is still
// refused.
func TestCommitMessageFaults(t *testing.T) {
	const trailer = "Release PR.\n\nCo-authored-by: github-actions[bot] <41898282+github-actions[bot]@users.noreply.github.com>\n"
	for _, tc := range []struct {
		author, subj, body string
		want               int
	}{
		{"mlagarrigue", "chore(main): release 0.1.1 (#11)", trailer, 0},
		{"mlagarrigue", "chore(main): release 0.1.1 (#11)", "", 0},
		{"mlagarrigue", "fix(web): bound the int conversion explicitly", "", 0},
		{"mlagarrigue", "fix(web): bound the int conversion explicitly", trailer, 1},
		{"mlagarrigue", "chore(main): tidy the release notes", trailer, 2},
		{"sluice-release[bot]", "chore(release-dry-run): release dryrun 0.4.0 (#25)", trailer, 0},
		{"mlagarrigue", "chore(release-dry-run): tidy the rehearsal", trailer, 1},
		{"mlagarrigue", "chore(web): release 1.2.3 early", trailer, 1},
		{"mlagarrigue", "docs: a trailing period.", "", 1},
		{"mlagarrigue", "feat(nowhere): an unknown scope", "", 1},
		{"dependabot[bot]", "ci: bump actions/upload-artifact from 7.0.1 to 7.0.2 (#13)", trailer, 0},
		{"dependabot[bot]", "ci: bump actions/upload-artifact from 7.0.1 to 7.0.2.", trailer, 1},
	} {
		if got := commitMessageFaults(tc.author, tc.subj, tc.body); len(got) != tc.want {
			t.Errorf("%q: %d faults %v, want %d", tc.subj, len(got), got, tc.want)
		}
	}
}

// The package list is written in three places a reader meets before the
// code — the README's table, the experimental guide's opening sentence and
// the exit conditions in the limits guide — and each is checked against the
// tree and the godoc, so a package added, removed or relabelled fails here
// rather than waiting for a reader to notice.
func modulePackages(t *testing.T) (module string, docs map[string]string) {
	t.Helper()
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go is not installed")
	}
	out, err := exec.Command("go", "list", "-f", "{{.ImportPath}}\t{{.Doc}}", "./...").Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	docs = map[string]string{}
	for line := range strings.SplitSeq(strings.TrimSpace(string(out)), "\n") {
		path, doc, _ := strings.Cut(line, "\t")
		if module == "" {
			module = path // the root is listed first
		}
		if strings.Contains(path, "/internal/") || strings.Contains(path, "/example/") {
			continue
		}
		docs[path] = doc
	}
	return module, docs
}

// readmePackageRows reads the README's package table: each row names one
// package (or two, joined by " · ") in the first cell and says in the last
// whether it is supported or experimental.
func readmePackageRows(t *testing.T, module string) (status map[string]string) {
	t.Helper()
	b, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	// The packages are listed in small tables under "### The packages", one
	// per group; a group's "####" title is the status of its rows.
	_, section, _ := strings.Cut(string(b), "\n### The packages\n")
	section, _, _ = strings.Cut(section, "\n### ")
	row := regexp.MustCompile("^\\| (`[^`]+`(?: · `[^`]+`)*) \\| [^|]+ \\|$")
	status = map[string]string{}
	group := ""
	for _, line := range strings.Split(section, "\n") {
		if g, ok := strings.CutPrefix(line, "#### "); ok {
			group = strings.ToLower(g)
			continue
		}
		m := row.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		for _, name := range regexp.MustCompile("`([^`]+)`").FindAllStringSubmatch(m[1], -1) {
			n := name[1]
			switch {
			case n == "sluice":
				n = module
			case strings.HasPrefix(n, "sluice/"):
				n = module + strings.TrimPrefix(n, "sluice")
			default:
				n = module + "/" + n
			}
			status[n] = group
		}
	}
	return status
}

func TestREADMEPackageTableFollowsTheTree(t *testing.T) {
	module, docs := modulePackages(t)
	rows := readmePackageRows(t, module)
	for path := range docs {
		if _, ok := rows[path]; !ok {
			t.Errorf("%s: not in the README's package table", path)
		}
	}
	for path, st := range rows {
		doc, ok := docs[path]
		if !ok {
			t.Errorf("README: row %q names a package that is not in the tree", path)
			continue
		}
		labelled := strings.Contains(strings.ToLower(doc), "experiment")
		if labelled != strings.Contains(st, "experimental") {
			t.Errorf("%s: README says %q, the godoc's first sentence %s the experimental label", path, st, map[bool]string{true: "carries", false: "lacks"}[labelled])
		}
	}
}

func TestExperimentalLabelMatchesTheGuides(t *testing.T) {
	module, docs := modulePackages(t)
	labelled := map[string]bool{}
	for path, doc := range docs {
		if strings.Contains(strings.ToLower(doc), "experiment") {
			labelled[path] = true
		}
	}
	// The guide's opening paragraph, up to the first blank line after the
	// title, names every labelled package in backticks.
	b, err := os.ReadFile("docs/guide/experimental.md")
	if err != nil {
		t.Fatal(err)
	}
	opening, _, _ := strings.Cut(strings.TrimPrefix(string(b), "# Experimental\n\n"), "\n\n")
	named := map[string]bool{}
	for _, m := range regexp.MustCompile("`([^`]+)`").FindAllStringSubmatch(opening, -1) {
		named[module+"/"+m[1]] = true
	}
	// The limits guide gives each one its exit conditions, one bullet each.
	b, err = os.ReadFile("docs/guide/limits.md")
	if err != nil {
		t.Fatal(err)
	}
	_, exits, ok := strings.Cut(string(b), "## Experimental: the exit conditions\n")
	if !ok {
		t.Fatal("docs/guide/limits.md: no section \"Experimental: the exit conditions\"")
	}
	exits, _, _ = strings.Cut(exits, "\n## ")
	conditioned := map[string]bool{}
	for _, m := range regexp.MustCompile("(?m)^- \\*\\*`([^`]+)`\\*\\*").FindAllStringSubmatch(exits, -1) {
		conditioned[module+"/"+m[1]] = true
	}
	for path := range labelled {
		if !named[path] {
			t.Errorf("%s: labelled experimental in its godoc, not named in docs/guide/experimental.md's opening", path)
		}
		if !conditioned[path] {
			t.Errorf("%s: labelled experimental in its godoc, no exit conditions in docs/guide/limits.md", path)
		}
	}
	for path := range named {
		if !labelled[path] {
			t.Errorf("%s: named in docs/guide/experimental.md, its godoc's first sentence does not say so", path)
		}
	}
	for path := range conditioned {
		if !labelled[path] {
			t.Errorf("%s: has exit conditions in docs/guide/limits.md, its godoc's first sentence does not say so", path)
		}
	}
}

// TestWorkflowCheckoutsDropTheirCredentials: a checkout leaves the job's
// token in the clone unless told not to, where every later step, a test
// binary or a tool built from a module included, can read it. No job here
// pushes through the clone (the badge job pushes by an explicit URL), so
// every checkout drops it.
func TestWorkflowCheckoutsDropTheirCredentials(t *testing.T) {
	files, err := filepath.Glob(".github/workflows/*.yml")
	if err != nil || len(files) == 0 {
		t.Fatalf("no workflow found: %v", err)
	}
	for _, f := range files {
		b, err := os.ReadFile(f) //nolint:gosec // G304: a repository path, not input
		if err != nil {
			t.Fatal(err)
		}
		lines := strings.Split(string(b), "\n")
		for i, l := range lines {
			if !strings.Contains(l, "- uses: actions/checkout@") {
				continue
			}
			// The step's body is every following line indented deeper than
			// its dash.
			indent := len(l) - len(strings.TrimLeft(l, " "))
			dropped := false
			for _, body := range lines[i+1:] {
				if len(body)-len(strings.TrimLeft(body, " ")) <= indent {
					break
				}
				dropped = dropped || strings.TrimSpace(body) == "persist-credentials: false"
			}
			if !dropped {
				t.Errorf("%s:%d: a checkout without persist-credentials: false", f, i+1)
			}
		}
	}
}

// TestCheckRunsTheNestedTestsCIRuns: scripts/check.sh is the acceptance
// criterion, so what it skips of a nested module's tests must be what CI
// skips — no more. The two lists are written twice, once in the script and
// once in the workflow, and a list that drifts turns the script green while
// CI is red: on 2026-10-24 main stayed red for four commits behind a check
// that ran neither module's tests at all.
func TestCheckRunsTheNestedTestsCIRuns(t *testing.T) {
	skip := regexp.MustCompile(`-skip\s+'([^']+)'`)
	find := func(path string) string {
		b, err := os.ReadFile(path) //nolint:gosec // G304: a repository path, not input
		if err != nil {
			t.Fatal(err)
		}
		m := skip.FindAllStringSubmatch(string(b), -1)
		if len(m) != 1 {
			t.Fatalf("%s: %d -skip expressions, want exactly one", path, len(m))
		}
		return m[0][1]
	}
	if a, b := find("scripts/check.sh"), find(".github/workflows/ci.yml"); a != b {
		t.Errorf("check.sh skips %s, ci.yml skips %s", a, b)
	}
	// And the script does run them: a `nested` that only vets is the state
	// this test exists to prevent.
	b, err := os.ReadFile("scripts/check.sh")
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range []string{"interop) ", "comparison/sluice) ", "comparison/loadgen) "} {
		if !strings.Contains(string(b), m) {
			t.Errorf("scripts/check.sh runs no tests for %s", strings.TrimSuffix(m, ") "))
		}
	}
}

// TestDockerImagesArePinned: a tag moves, a digest does not. The interop
// image is published, so its base is pinned for the same reason the
// workflows pin every action by commit; Dependabot moves the digests.
func TestDockerImagesArePinned(t *testing.T) {
	from := regexp.MustCompile(`(?m)^FROM\s+(\S+)`)
	for _, f := range []string{"interop/Dockerfile", ".devcontainer/Dockerfile"} {
		b, err := os.ReadFile(f) //nolint:gosec // G304: a repository path, not input
		if err != nil {
			t.Fatal(err)
		}
		images := from.FindAllSubmatch(b, -1)
		if len(images) == 0 {
			t.Errorf("%s: no FROM line; if the file changed shape, update this test with it", f)
		}
		for _, m := range images {
			if !regexp.MustCompile(`@sha256:[0-9a-f]{64}$`).Match(m[1]) {
				t.Errorf("%s: FROM %s is not pinned by digest", f, m[1])
			}
		}
	}
}

// TestInteropRunnerIsPinned: the QUIC interop runner is cloned at a commit,
// as h2spec is built from one, so that a result read today can be read
// again tomorrow; and the limits guide names the same revision as the one
// the script runs, since the guide's results are the script's.
func TestInteropRunnerIsPinned(t *testing.T) {
	script, err := os.ReadFile("scripts/interop-runner.sh")
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`(?m)^RUNNER_REF=\$\{RUNNER_REF:-([0-9a-f]{40})\}`).FindSubmatch(script)
	if m == nil {
		t.Fatal("scripts/interop-runner.sh: RUNNER_REF does not default to a 40-hex commit; a branch moves under the results")
	}
	guide, err := os.ReadFile("docs/guide/limits.md")
	if err != nil {
		t.Fatal(err)
	}
	if short := string(m[1][:7]); !bytes.Contains(guide, []byte(short)) {
		t.Errorf("docs/guide/limits.md does not name the runner revision %s the script runs", short)
	}
}

// h3spec is a release binary the HTTP/3 conformance run downloads: it is held
// to one release by its tag and to one build by its SHA-256, as the runner is
// held to one commit, so a moved release cannot change what the exclusions
// file is read against.
func TestH3SpecIsPinned(t *testing.T) {
	script, err := os.ReadFile("scripts/interop-http3.sh")
	if err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`(?m)^H3SPEC_URL=\S+/releases/download/v\d+\.\d+\.\d+/\S+$`).Match(script) {
		t.Error("scripts/interop-http3.sh: H3SPEC_URL does not name a release tag; a latest link moves under the results")
	}
	if !regexp.MustCompile(`(?m)^H3SPEC_SHA256=[0-9a-f]{64}$`).Match(script) {
		t.Error("scripts/interop-http3.sh: H3SPEC_SHA256 is not a 64-hex digest; the binary would run unchecked")
	}
}

// packageDirs lists the directory of every package of the root module, test
// files included or not: go list stops at nested modules, which have their
// own go.mod and their own reviews.
func packageDirs(t *testing.T) []string {
	t.Helper()
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go is not installed")
	}
	out, err := exec.Command("go", "list", "-f", "{{.Dir}}", "./...").Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	return strings.Fields(string(out))
}

// parseDir parses the Go files of one directory, its tests or the rest.
func parseDir(t *testing.T, fset *token.FileSet, dir string, tests bool) []*ast.File {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var files []*ast.File
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") != tests {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, parser.ParseComments)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, f)
	}
	return files
}

// unresolvedLink is a doc link go doc leaves as plain text: [Name],
// [Type.Method], [pkg.Name]. A lowercase single word in brackets is prose.
var unresolvedLink = regexp.MustCompile(`(?:^|[^A-Za-z0-9_\]])\[([A-Z][A-Za-z0-9_]*|[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_.]*)\]`)

// Doc links are resolved by go doc and pkg.go.dev against the package's own
// symbols, its imports and the standard library; one that resolves to
// nothing is shown as the bracketed text, a reader's dead end. The parser
// below is go doc's own, so what it leaves unresolved is exactly that.
func TestDocLinksResolve(t *testing.T) {
	for _, dir := range packageDirs(t) {
		fset := token.NewFileSet()
		files := parseDir(t, fset, dir, false)
		if len(files) == 0 {
			continue
		}
		p, err := godoc.NewFromFiles(fset, files, "example.com/p", godoc.PreserveAST)
		if err != nil {
			t.Fatal(err)
		}
		parse := p.Parser()
		for _, d := range exportedDocs(p) {
			for _, link := range plainLinks(parse.Parse(d.text).Content) {
				t.Errorf("%s, doc of %s: [%s] resolves to nothing go doc can link to", filepath.Base(dir), d.name, link)
			}
		}
	}
}

// plainLinks returns the bracketed identifiers left in the plain text of
// blocks, code blocks excluded.
func plainLinks(blocks []comment.Block) []string {
	var out []string
	var text func([]comment.Text)
	text = func(ts []comment.Text) {
		for _, x := range ts {
			switch x := x.(type) {
			case comment.Plain:
				for _, m := range unresolvedLink.FindAllStringSubmatch(string(x), -1) {
					out = append(out, m[1])
				}
			case *comment.Link:
				text(x.Text)
			}
		}
	}
	for _, b := range blocks {
		switch b := b.(type) {
		case *comment.Paragraph:
			text(b.Text)
		case *comment.Heading:
			text(b.Text)
		case *comment.List:
			for _, item := range b.Items {
				out = append(out, plainLinks(item.Content)...)
			}
		}
	}
	return out
}

// A code block in a doc comment is what a reader copies, and nothing
// compiles it the way an Example is compiled or resolves it the way a doc
// link is (plainLinks leaves code blocks out). So every pkg.Name in one,
// where pkg is the name of a package of this module, names an exported
// symbol of that package. Four names the doc of postgres.Param and
// Tx.ExecParams kept after 1608e0c moved them to pgtype were dead for a week
// under a green CI. A variable that shares a package's name would read as
// that package here; none does today.
func TestDocCodeBlocksNameRealSymbols(t *testing.T) {
	type pkgDoc struct {
		dir string
		p   *godoc.Package
	}
	var pkgs []pkgDoc
	exported := map[string]map[string]bool{}
	for _, dir := range packageDirs(t) {
		fset := token.NewFileSet()
		files := parseDir(t, fset, dir, false)
		if len(files) == 0 {
			continue
		}
		p, err := godoc.NewFromFiles(fset, files, "example.com/p", godoc.PreserveAST)
		if err != nil {
			t.Fatal(err)
		}
		pkgs = append(pkgs, pkgDoc{dir, p})
		names := exported[p.Name]
		if names == nil {
			names = map[string]bool{}
			exported[p.Name] = names
		}
		for _, v := range append(p.Consts, p.Vars...) {
			for _, n := range v.Names {
				names[n] = true
			}
		}
		for _, f := range p.Funcs {
			names[f.Name] = true
		}
		for _, ty := range p.Types {
			names[ty.Name] = true
			for _, v := range append(ty.Consts, ty.Vars...) {
				for _, n := range v.Names {
					names[n] = true
				}
			}
			for _, f := range ty.Funcs {
				names[f.Name] = true
			}
		}
	}
	checked := 0
	for _, pd := range pkgs {
		parse := pd.p.Parser()
		for _, d := range exportedDocs(pd.p) {
			for _, b := range parse.Parse(d.text).Content {
				code, ok := b.(*comment.Code)
				if !ok {
					continue
				}
				for _, m := range qualified.FindAllStringSubmatch(code.Text, -1) {
					names, ok := exported[m[1]]
					if !ok {
						continue // the standard library, or a variable
					}
					checked++
					if !names[m[2]] {
						t.Errorf("%s, doc of %s: the code block names %s.%s, which package %s does not export",
							filepath.Base(pd.dir), d.name, m[1], m[2], m[1])
					}
				}
			}
		}
	}
	if checked == 0 {
		t.Error("no code block of a doc comment names a symbol of this module: the parser or the regexp changed")
	}
}

// A section number means something only beside the document it numbers.
// The protocol packages cite their RFCs and protocol pages by section, and
// name them in their package documentation; elsewhere a section sign and a number
// pointed at a numbered specification of this repository that no longer
// exists, and a reader could only guess. Outside the protocol packages, a
// comment that cites a section names its source in the same comment.
func TestSectionReferencesNameTheirSource(t *testing.T) {
	protocol := regexp.MustCompile(`/(net/quic|net/httpstream|database/postgres|internal/pgwire|internal/dgram)(/|$)`)
	section := regexp.MustCompile(`§\s?\d+(\.\d+)*`)
	source := regexp.MustCompile(`\bRFC\s*[0-9]{3,5}|PostgreSQL|\bdraft-|\bADR\b|\bBCP\s*[0-9]+`)
	for _, dir := range packageDirs(t) {
		if protocol.MatchString(filepath.ToSlash(dir)) {
			continue
		}
		fset := token.NewFileSet()
		for _, tests := range []bool{false, true} {
			for _, f := range parseDir(t, fset, dir, tests) {
				for _, g := range f.Comments {
					text := g.Text()
					if s := section.FindString(text); s != "" && !source.MatchString(text) {
						t.Errorf("%s: %s with no RFC, PostgreSQL page or ADR named in the comment", fset.Position(g.Pos()), s)
					}
				}
			}
		}
	}
}

// namedDoc is one doc comment go doc shows, with what it documents.
type namedDoc struct{ name, text string }

// exportedDocs lists the doc comments of a package as go doc shows them: the
// package's, and its exported declarations'.
func exportedDocs(p *godoc.Package) []namedDoc {
	out := []namedDoc{{"package " + p.Name, p.Doc}}
	values := func(vs []*godoc.Value) {
		for _, v := range vs {
			out = append(out, namedDoc{strings.Join(v.Names, ", "), v.Doc})
		}
	}
	funcs := func(fs []*godoc.Func) {
		for _, f := range fs {
			name := f.Name
			if f.Recv != "" {
				name = strings.TrimLeft(f.Recv, "*") + "." + f.Name
			}
			out = append(out, namedDoc{name, f.Doc})
		}
	}
	values(p.Consts)
	values(p.Vars)
	funcs(p.Funcs)
	for _, ty := range p.Types {
		out = append(out, namedDoc{ty.Name, ty.Doc})
		values(ty.Consts)
		values(ty.Vars)
		funcs(ty.Funcs)
		funcs(ty.Methods)
	}
	return out
}

// httpstreamStatus is how net/httpstream's status is said, once, wherever a
// reader may ask: it had been "supported" in one page, "on no supported
// path" in its own documentation, "fuzzed in CI" in a guide and "not fuzzed"
// in the architecture.
const httpstreamStatus = "net/httpstream is supported — outside the experimental label " +
	"and under the compatibility promise — and fuzzed in CI without " +
	"a public corpus; it has neither run in production nor had an outside " +
	"security review."

func TestHTTPStreamStatusIsSaidOnce(t *testing.T) {
	flat := func(s string) string {
		s = strings.NewReplacer("//", " ", "**", "", "\n", " ").Replace(s)
		return strings.Join(strings.Fields(s), " ")
	}
	for _, f := range []string{"README.md", "docs/guide/httpstream.md", "docs/design/architecture.md", "net/httpstream/httpstream.go"} {
		b, err := os.ReadFile(f) //nolint:gosec // G304: a repository path, not input
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(flat(string(b)), httpstreamStatus) {
			t.Errorf("%s does not state net/httpstream's status as %q", f, httpstreamStatus)
		}
	}
}

// Every error the library exports is named in a document a reader reaches —
// the troubleshooting guide at least — so that someone who meets it in a log
// finds when it happens and what to do (docs/guide/troubleshooting.md). The
// examples and internal packages are not the library's surface; the nested
// modules are measured, not imported.
func TestEveryErrorIsDocumented(t *testing.T) {
	var docs strings.Builder
	for _, f := range markdownFiles(t) {
		b, err := os.ReadFile(f) //nolint:gosec // G304: a repository path, not input
		if err != nil {
			t.Fatal(err)
		}
		docs.Write(b)
	}
	text := docs.String()
	skip := []string{"example", "internal", "cmd", "benchmarks", "comparison", "interop"}
	err := filepath.WalkDir(".", func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			// Hidden directories are not packages of the module.
			if slices.Contains(skip, d.Name()) || (p != "." && strings.HasPrefix(d.Name(), ".")) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
			return nil
		}
		f, err := parser.ParseFile(token.NewFileSet(), p, nil, parser.SkipObjectResolution|parser.ParseComments)
		if err != nil {
			return err
		}
		for _, decl := range f.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.VAR {
				continue
			}
			for _, spec := range gen.Specs {
				vs := spec.(*ast.ValueSpec)
				for _, name := range vs.Names {
					n := name.Name
					if !strings.HasPrefix(n, "Err") || !name.IsExported() {
						continue
					}
					if !regexp.MustCompile(`\b` + n + `\b`).MatchString(text) {
						t.Errorf("%s: %s.%s is named in no document; add it to docs/guide/troubleshooting.md", p, f.Name.Name, n)
					}
					// go doc shows a grouped variable under the group's
					// comment: each error says its own action, on its own
					// declaration or its own spec.
					if vs.Doc == nil && (gen.Lparen.IsValid() || gen.Doc == nil) {
						t.Errorf("%s: %s.%s has no doc comment of its own", p, f.Name.Name, n)
					}
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestSettingsHaveATableRow keeps the settings tables of the guides in step
// with the code: every exported field of a configuration type a guide
// tabulates has a row whose first cell names it, so a field added without a
// row fails here rather than at the next review.
func TestSettingsHaveATableRow(t *testing.T) {
	guides, err := filepath.Glob("docs/guide/*.md")
	if err != nil {
		t.Fatal(err)
	}
	rows := map[string]bool{}
	firstCell := regexp.MustCompile("^\\| `([A-Za-z0-9]+)` \\|")
	for _, g := range guides {
		b, err := os.ReadFile(g) //nolint:gosec // G304: a repository path, not input
		if err != nil {
			t.Fatal(err)
		}
		for line := range strings.SplitSeq(string(b), "\n") {
			if m := firstCell.FindStringSubmatch(line); m != nil {
				rows[m[1]] = true
			}
		}
	}
	// Every configuration the library exports: a struct named Config, or
	// ending in Config or Parameters, outside the examples, the internal
	// packages and the nested modules.
	skip := []string{"example", "internal", "cmd", "benchmarks", "comparison", "interop", "audit"}
	configs := 0
	err = filepath.WalkDir(".", func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if slices.Contains(skip, d.Name()) || (p != "." && strings.HasPrefix(d.Name(), ".")) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
			return nil
		}
		f, err := parser.ParseFile(token.NewFileSet(), p, nil, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		ast.Inspect(f, func(n ast.Node) bool {
			ts, ok := n.(*ast.TypeSpec)
			if !ok || !ts.Name.IsExported() || !strings.HasSuffix(ts.Name.Name, "Config") && !strings.HasSuffix(ts.Name.Name, "Parameters") {
				return true
			}
			st, ok := ts.Type.(*ast.StructType)
			if !ok {
				return true
			}
			configs++
			for _, field := range st.Fields.List {
				for _, name := range field.Names {
					if name.IsExported() && !rows[name.Name] {
						t.Errorf("%s: %s.%s.%s has no row in a settings table of docs/guide", p, f.Name.Name, ts.Name.Name, name.Name)
					}
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if configs < 10 {
		t.Fatalf("found %d configuration types; the walk no longer sees them", configs)
	}
}

// TestSecurityPropertiesNameTheirTests holds SECURITY.md's promise that the
// security properties are testable: every row S1 to S14 of the architecture's
// table names, in its verification cell, a test or fuzz target declared in
// the module or a script of the repository, and every one it names exists —
// so a reporter knows which test a breach makes fail.
func TestSecurityPropertiesNameTheirTests(t *testing.T) {
	declared := map[string]bool{}
	decl := regexp.MustCompile(`(?m)^func ((?:Test|Fuzz)\w+)\(`)
	err := filepath.WalkDir(".", func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if p != "." && strings.HasPrefix(d.Name(), ".") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(p, "_test.go") {
			return nil
		}
		b, err := os.ReadFile(p) //nolint:gosec // G304: a repository path, not input
		if err != nil {
			return err
		}
		for _, m := range decl.FindAllSubmatch(b, -1) {
			declared[string(m[1])] = true
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile("docs/design/architecture.md")
	if err != nil {
		t.Fatal(err)
	}
	row := regexp.MustCompile(`^\| (?:\*\*)?(S\d+)(?:\*\*)? \|`)
	named := regexp.MustCompile("`((?:Test|Fuzz)[A-Z]\\w*)`")
	script := regexp.MustCompile("`(scripts/[\\w./-]+\\.sh)`")
	var seen []string
	for line := range strings.SplitSeq(string(b), "\n") {
		m := row.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		seen = append(seen, m[1])
		cells := strings.Split(strings.TrimSuffix(strings.TrimSpace(line), "|"), " | ")
		cell := cells[len(cells)-1]
		tests, scripts := named.FindAllStringSubmatch(cell, -1), script.FindAllStringSubmatch(cell, -1)
		if len(tests) == 0 && len(scripts) == 0 {
			t.Errorf("%s names no test, fuzz target or script in its verification cell", m[1])
		}
		for _, n := range tests {
			if !declared[n[1]] {
				t.Errorf("%s names %s, which no _test.go declares", m[1], n[1])
			}
		}
		for _, s := range scripts {
			if _, err := os.Stat(s[1]); err != nil {
				t.Errorf("%s names %s: %v", m[1], s[1], err)
			}
		}
	}
	if len(seen) != 14 {
		t.Errorf("found the rows %v, want S1 to S14", seen)
	}
}

// TestEveryDecisionRecordIsLinked keeps every decision record reachable from
// the page that applies it: a record's file name appears in a document
// outside docs/design/adr, so a reader learns why from where the decision
// shows, not from a directory listing.
func TestEveryDecisionRecordIsLinked(t *testing.T) {
	var docs strings.Builder
	for _, f := range markdownFiles(t) {
		if strings.HasPrefix(f, filepath.Join("docs", "design", "adr")+string(filepath.Separator)) {
			continue
		}
		b, err := os.ReadFile(f) //nolint:gosec // G304: a repository path, not input
		if err != nil {
			t.Fatal(err)
		}
		docs.Write(b)
	}
	text := docs.String()
	records, err := filepath.Glob("docs/design/adr/[0-9][0-9][0-9][0-9]-*.md")
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range records {
		if !strings.Contains(text, "adr/"+filepath.Base(r)) {
			t.Errorf("%s is linked by name from no document outside docs/design/adr", r)
		}
	}
}

// TestGuideCodeBlocksTypeCheck holds the Go blocks of the README and the
// guides to the code: each is type-checked as a reader would paste it, so a
// renamed field, a changed signature or a := that redeclares fails here
// rather than in the reader's editor. A block that declares at package level
// is checked as a file, any other as the body of a function. The names a
// block leaves to the reader — ctx, ln, handle, the reader's own types — are
// the one error tolerated: "undefined: x", never "undefined: pkg.X".
func TestGuideCodeBlocksTypeCheck(t *testing.T) {
	if testing.Short() {
		t.Skip("type-checks the module from source")
	}
	packages := importablePackages(t)
	fset := token.NewFileSet()
	conf := types.Config{Importer: importer.ForCompiler(fset, "source", nil)}
	files, err := filepath.Glob("docs/guide/*.md")
	if err != nil {
		t.Fatal(err)
	}
	files = append([]string{"README.md"}, files...)
	tolerated := regexp.MustCompile(`^(undefined: \w+|declared and not used: \w+|"[\w/.]+" imported and not used)$`)
	qualifier := regexp.MustCompile(`\b([a-z][a-z0-9]*)\.[A-Za-z]`)
	for _, f := range files {
		for _, b := range goBlocks(t, f) {
			var imports []string
			seen := map[string]bool{}
			for _, m := range qualifier.FindAllStringSubmatch(b.code, -1) {
				if path, ok := packages[m[1]]; ok && !seen[path] {
					seen[path] = true
					imports = append(imports, fmt.Sprintf("%q", path))
				}
			}
			head := "package guide\n"
			if len(imports) > 0 {
				head += "import (\n" + strings.Join(imports, "\n") + "\n)\n"
			}
			// The prefix is what goes before the block's first line. A body
			// that returns a value of its own — "return err" — was written
			// inside a function returning an error; any other, inside one
			// returning nothing, as a net/http handler does.
			var prefix, src string
			switch {
			case strings.HasPrefix(strings.TrimSpace(b.code), "package "):
				src = b.code
			case parses(head + b.code):
				prefix = head
				src = head + b.code
			case returnsAValue(head + "func _() {\n" + b.code + "\n}\n"):
				prefix = head + "func _() error {\n"
				src = prefix + b.code + "return nil\n}\n"
			default:
				prefix = head + "func _() {\n"
				src = prefix + b.code + "\n}\n"
			}
			at := func(p token.Pos) int { return b.line + fset.Position(p).Line - 1 - strings.Count(prefix, "\n") }
			file, err := parser.ParseFile(fset, f, src, parser.SkipObjectResolution)
			if err != nil {
				t.Errorf("%s:%d: the block does not parse: %v", f, b.line, err)
				continue
			}
			conf.Error = func(err error) {
				var te types.Error
				if !errors.As(err, &te) {
					t.Errorf("%s:%d: %v", f, b.line, err)
					return
				}
				if !tolerated.MatchString(te.Msg) {
					t.Errorf("%s:%d: %s", f, at(te.Pos), te.Msg)
				}
			}
			_, _ = conf.Check("guide", fset, []*ast.File{file}, nil)
		}
	}
}

// goBlock is a fenced Go block of a document and the line its code starts on.
type goBlock struct {
	line int
	code string
}

// goBlocks returns the blocks fenced as ```go in a document.
func goBlocks(t *testing.T, path string) []goBlock {
	t.Helper()
	b, err := os.ReadFile(path) //nolint:gosec // G304: a repository path, not input
	if err != nil {
		t.Fatal(err)
	}
	var out []goBlock
	var cur *goBlock
	var code strings.Builder
	for i, line := range strings.Split(string(b), "\n") {
		switch {
		case cur == nil && strings.TrimSpace(line) == "```go":
			cur = &goBlock{line: i + 2}
			code.Reset()
		case cur != nil && strings.TrimSpace(line) == "```":
			cur.code = code.String()
			out = append(out, *cur)
			cur = nil
		case cur != nil:
			code.WriteString(line)
			code.WriteByte('\n')
		}
	}
	return out
}

// parses reports whether src is a Go file as it stands.
func parses(src string) bool {
	_, err := parser.ParseFile(token.NewFileSet(), "", src, parser.SkipObjectResolution)
	return err == nil
}

// returnsAValue reports whether a return with results appears in src outside
// the function literals it holds: the block's own returns, not a handler's.
func returnsAValue(src string) bool {
	f, err := parser.ParseFile(token.NewFileSet(), "", src, parser.SkipObjectResolution)
	if err != nil {
		return false
	}
	found := false
	for _, d := range f.Decls {
		fn, ok := d.(*ast.FuncDecl)
		if !ok {
			continue
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.FuncLit:
				return false
			case *ast.ReturnStmt:
				found = found || len(n.Results) > 0
			}
			return !found
		})
	}
	return found
}

// importablePackages maps the name a block qualifies an identifier with to
// the package it means: the module's packages by their last element, and the
// standard library's where no module package takes the name — the shortest
// path when two share one, as crypto/rand and math/rand do.
func importablePackages(t *testing.T) map[string]string {
	t.Helper()
	out := map[string]string{}
	add := func(list string, prefer bool) {
		for path := range strings.FieldsSeq(list) {
			if strings.Contains(path, "internal") || strings.HasPrefix(path, "vendor/") {
				continue
			}
			name := path[strings.LastIndex(path, "/")+1:]
			if old, ok := out[name]; ok && (!prefer || strings.Count(old, "/") <= strings.Count(path, "/")) {
				continue
			}
			out[name] = path
		}
	}
	std, err := exec.Command("go", "list", "std").Output()
	if err != nil {
		t.Fatal(err)
	}
	module, err := exec.Command("go", "list", "./...").Output()
	if err != nil {
		t.Fatal(err)
	}
	add(string(std), true)
	for path := range strings.FieldsSeq(string(module)) {
		out[path[strings.LastIndex(path, "/")+1:]] = path
	}
	out["sluice"] = "github.com/mlagarrigue/sluice"
	return out
}

// zeroSays is how a field's doc comment says what its zero value does:
// "Zero means 64 MiB", "zero is no bound", "A zero ... is refused".
var zeroSays = regexp.MustCompile(`(?i)\bzero\b|\bunset\b|\bleft at 0\b|\brequired\b`)

// zeroIsTheProtocols names the fields whose zero is not a default but the
// protocol's own value, carried as is: a transport parameter announced as 0
// grants nothing (RFC 9000 §18.2), and DefaultParameters fills the defaults.
var zeroIsTheProtocols = map[string]bool{
	"TransportParameters.InitialMaxData": true, "TransportParameters.InitialMaxStreamDataUni": true,
	"TransportParameters.InitialMaxStreamDataBidiLocal": true, "TransportParameters.InitialMaxStreamDataBidiRemote": true,
	"TransportParameters.InitialMaxStreamsBidi": true, "TransportParameters.InitialMaxStreamsUni": true,
	"TransportParameters.AckDelayExponent": true, "TransportParameters.MaxAckDelay": true,
}

// ADR 0018: a numeric field of a configuration whose zero is not refused
// means a default the field's documentation names. Every integer or
// duration field of an exported configuration says what its zero does.
func TestConfigZeroValuesAreNamed(t *testing.T) {
	skip := []string{"example", "internal", "cmd", "benchmarks", "comparison", "interop", "audit"}
	numeric := map[string]bool{
		"int": true, "int32": true, "int64": true, "uint": true, "uint8": true,
		"uint16": true, "uint32": true, "uint64": true, "time.Duration": true, "float64": true,
	}
	err := filepath.WalkDir(".", func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if slices.Contains(skip, d.Name()) || (p != "." && strings.HasPrefix(d.Name(), ".")) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
			return nil
		}
		f, err := parser.ParseFile(token.NewFileSet(), p, nil, parser.SkipObjectResolution|parser.ParseComments)
		if err != nil {
			return err
		}
		ast.Inspect(f, func(n ast.Node) bool {
			ts, ok := n.(*ast.TypeSpec)
			if !ok || !ts.Name.IsExported() || !strings.HasSuffix(ts.Name.Name, "Config") && !strings.HasSuffix(ts.Name.Name, "Parameters") {
				return true
			}
			st, ok := ts.Type.(*ast.StructType)
			if !ok {
				return true
			}
			prev := ""
			for _, field := range st.Fields.List {
				doc := field.Doc.Text() + field.Comment.Text()
				// "IPv4PrefixBits and IPv6PrefixBits are …": one comment
				// above two fields says for both.
				if field.Doc == nil && len(field.Names) > 0 && strings.Contains(prev, field.Names[0].Name) {
					doc = prev
				}
				prev = doc
				typ := types.ExprString(field.Type)
				if !numeric[typ] || len(field.Names) == 0 || !field.Names[0].IsExported() ||
					zeroIsTheProtocols[ts.Name.Name+"."+field.Names[0].Name] {
					continue
				}
				if !zeroSays.MatchString(doc) {
					t.Errorf("%s: %s.%s.%s (%s) does not say what its zero does (ADR 0018)", p, f.Name.Name, ts.Name.Name, field.Names[0].Name, typ)
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
