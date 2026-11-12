package audit

import (
	"archive/zip"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// seed writes files under dir, creating their directories.
func seed(t *testing.T, dir string, files map[string]string) {
	t.Helper()
	for name, body := range files {
		path := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil { //nolint:gosec // G703: under the test's own directory
			t.Fatal(err)
		}
	}
}

// TestAdvisoriesAreJudgedFromALocalCopy runs govulncheck with no network:
// a vulnerability database of one advisory, seeded as the copy a caller
// unzips, and a module that calls the vulnerable function, its dependency
// served by a proxy on the file system. The verdict comes back, and the
// system table says which copy and of what date.
func TestAdvisoriesAreJudgedFromALocalCopy(t *testing.T) {
	if _, err := exec.LookPath("govulncheck"); err != nil {
		t.Skip("govulncheck is not installed")
	}
	root := t.TempDir()
	db := filepath.Join(root, "db")
	seed(t, db, map[string]string{
		"index/db.json":      `{"modified":"2026-11-01T00:00:00Z"}`,
		"index/modules.json": `[{"path":"example.com/vulnmod","vulns":[{"id":"GO-2099-0001","modified":"2026-11-01T00:00:00Z","fixed":"1.0.1"}]}]`,
		"index/vulns.json":   `[{"id":"GO-2099-0001","modified":"2026-11-01T00:00:00Z","aliases":["CVE-2099-0001"]}]`,
		"ID/GO-2099-0001.json": `{"schema_version":"1.3.1","id":"GO-2099-0001","modified":"2026-11-01T00:00:00Z",
"published":"2026-11-01T00:00:00Z","aliases":["CVE-2099-0001"],"summary":"Bad does bad things","details":"Seeded.",
"affected":[{"package":{"name":"example.com/vulnmod","ecosystem":"Go"},"ranges":[{"type":"SEMVER","events":[{"introduced":"0"},{"fixed":"1.0.1"}]}],
"ecosystem_specific":{"imports":[{"path":"example.com/vulnmod","symbols":["Bad"]}]}}]}`,
	})
	// The dependency, as a module proxy serves it.
	at := filepath.Join(root, "proxy", "example.com", "vulnmod", "@v")
	seed(t, at, map[string]string{
		"list":        "v1.0.0\n",
		"v1.0.0.info": `{"Version":"v1.0.0","Time":"2026-01-01T00:00:00Z"}`,
		"v1.0.0.mod":  "module example.com/vulnmod\n\ngo 1.22\n",
	})
	zf, err := os.Create(filepath.Join(at, "v1.0.0.zip")) //nolint:gosec // G304: under the test's own directory
	if err != nil {
		t.Fatal(err)
	}
	zw := zip.NewWriter(zf)
	for name, body := range map[string]string{
		"go.mod":     "module example.com/vulnmod\n\ngo 1.22\n",
		"vulnmod.go": "package vulnmod\n\nfunc Bad() int { return 1 }\n",
	} {
		w, err := zw.Create("example.com/vulnmod@v1.0.0/" + name)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = w.Write([]byte(body))
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	_ = zf.Close()
	mod := filepath.Join(root, "app")
	seed(t, mod, map[string]string{
		"go.mod":  "module example.com/app\n\ngo 1.22\n\nrequire example.com/vulnmod v1.0.0\n",
		"go.sum":  "",
		"main.go": "package main\n\nimport \"example.com/vulnmod\"\n\nfunc main() { println(vulnmod.Bad()) }\n",
	})
	t.Setenv("GOPROXY", "file://"+filepath.ToSlash(filepath.Join(root, "proxy")))
	t.Setenv("GOSUMDB", "off")
	t.Setenv("GOFLAGS", "-mod=mod -modcacherw")
	t.Setenv("GOMODCACHE", filepath.Join(root, "modcache"))

	target, err := Open(Spec{Module: mod, VulnDB: db})
	if err != nil {
		t.Fatal(err)
	}
	findings, err := Run(Selection{Components: {"go"}, Goals: {"security"}, Sources: {"tools"}, CVEs: {"yes"}}, nil, target)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for i := range findings {
		f := &findings[i]
		if f.Rule == "deps/cve" {
			got = append(got, f.Verdict+" "+f.Current)
		}
	}
	if len(got) != 1 || !strings.HasPrefix(got[0], VerdictApplies+" GO-2099-0001") {
		t.Fatalf("deps/cve said %q, want GO-2099-0001 applying", got)
	}
	system := map[string]string{}
	for _, s := range System(target) {
		system[s.Name] = s.Value
	}
	if system["go vulndb modified"] != "2026-11-01T00:00:00Z" || system["go vulndb"] != db {
		t.Errorf("system %v", system)
	}
}

func TestAVulnDBMustBeACopyOfTheDatabase(t *testing.T) {
	for name, files := range map[string]map[string]string{
		"no index":    {"README": "x"},
		"no modified": {"index/db.json": `{}`},
	} {
		dir := t.TempDir()
		seed(t, dir, files)
		if _, err := Open(Spec{VulnDB: dir}); err == nil {
			t.Errorf("%s: read as a database", name)
		}
	}
}
