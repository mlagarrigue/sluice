package audit

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
)

// TestEveryRuleIsClassified: every rule registered has its line in
// references.go — its benchmark items, or none said once — and every line
// there is a rule, so that a rule added, renamed or removed is looked at
// against the benchmarks rather than passing for one no item carries.
func TestEveryRuleIsClassified(t *testing.T) {
	names := map[string]bool{}
	for _, r := range Rules() {
		names[r.Name] = true
		refs, ok := references[r.Name]
		if !ok {
			t.Errorf("%s: not classified in references.go (its benchmark items, or nil for none)", r.Name)
			continue
		}
		if !slices.Equal(r.Refs, refs) {
			t.Errorf("%s: carries %v, references.go says %v", r.Name, r.Refs, refs)
		}
		if want := referentialsOf(refs); !slices.Equal(r.Needs[Referentials], want) {
			t.Errorf("%s: on the referentials axis as %v, want %v", r.Name, r.Needs[Referentials], want)
		}
		for _, ref := range refs {
			if !slices.Contains(axisValues[Referentials], ref.Referential) || ref.Benchmark == "" || ref.ID == "" {
				t.Errorf("%s: an incomplete reference %+v", r.Name, ref)
			}
		}
	}
	for name := range references {
		if !names[name] {
			t.Errorf("references.go classifies %s, which is no rule", name)
		}
	}
}

// TestTheReportFiltersByReferential: on a Dockerfile and a container that
// break CIS items and settings no benchmark covers, "cis" keeps the
// findings whose rule carries an item, each printed with it, "none" the
// others, and the two together are the whole report.
func TestTheReportFiltersByReferential(t *testing.T) {
	dir := t.TempDir()
	dockerfile := filepath.Join(dir, "Dockerfile")
	if err := os.WriteFile(dockerfile, []byte("FROM golang:latest\nENV API_TOKEN=s3cret\nRUN go build -o /app .\nENTRYPOINT [\"/app\"]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	target, err := Open(Spec{Dockerfile: dockerfile, ContainerJSON: seedFile(t, "web.json", seededContainer)})
	if err != nil {
		t.Fatal(err)
	}
	run := func(sel Selection) []Finding {
		t.Helper()
		findings, err := Run(sel, nil, target)
		if err != nil {
			t.Fatal(err)
		}
		return findings
	}
	all := run(Selection{Components: {"docker"}})
	withCIS := run(Selection{Components: {"docker"}, Referentials: {"cis"}})
	without := run(Selection{Components: {"docker"}, Referentials: {"none"}})
	if len(withCIS) == 0 || len(without) == 0 {
		t.Fatalf("the seeded files should break both kinds: %d under cis, %d under none", len(withCIS), len(without))
	}
	if len(withCIS)+len(without) != len(all) {
		t.Errorf("cis %d and none %d do not make the whole report's %d", len(withCIS), len(without), len(all))
	}
	for _, f := range withCIS {
		if len(f.Refs) == 0 {
			t.Errorf("%s under cis with no reference", f.Rule)
		}
	}
	for _, f := range without {
		if len(f.Refs) != 0 {
			t.Errorf("%s under none with references %v", f.Rule, f.Refs)
		}
	}
	for _, want := range []string{"docker/no-secret", "container/capabilities", "container/docker-socket"} {
		if !slices.ContainsFunc(withCIS, func(f Finding) bool { return f.Rule == want }) {
			t.Errorf("%s not reported under cis", want)
		}
	}
	if !slices.ContainsFunc(withCIS, func(f Finding) bool {
		return f.Rule == "container/capabilities" && slices.ContainsFunc(f.Refs, func(r Reference) bool { return r.String() == "CIS Docker Benchmark v1.9.0 5.5" })
	}) {
		t.Error("container/capabilities does not name CIS Docker 5.5, privileged containers")
	}
}
