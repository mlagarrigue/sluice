package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func write(t *testing.T, name, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// runTo runs the command with its output in a file, and returns the file's
// text and the run's error.
func runTo(t *testing.T, args ...string) (string, error) {
	t.Helper()
	out, err := os.Create(filepath.Join(t.TempDir(), "out"))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	runErr := run(args, out)
	b, err := os.ReadFile(out.Name())
	if err != nil {
		t.Fatal(err)
	}
	return string(b), runErr
}

// TestTheGateFailsOnANewApplicableCVE: a PostgreSQL 14.0 with cert
// authentication carries CVE-2021-23214. The gate trips when it is new,
// and stays green when the earlier report had it already, or when nothing
// in pg_hba.conf reaches it.
func TestTheGateFailsOnANewApplicableCVE(t *testing.T) {
	settings := write(t, "settings.csv", "name,setting,unit\nserver_version,14.0,\n")
	cert := write(t, "cert.conf", "hostssl all all 0.0.0.0/0 cert\n")
	scram := write(t, "scram.conf", "hostssl all all 10.0.0.0/8 scram-sha-256\n")
	args := func(hba string, extra ...string) []string {
		return append([]string{"-pg-settings", settings, "-pg-hba", hba, "-components", "postgres", "-cve", "yes", "-goals", "security", "-output", "json"}, extra...)
	}

	before, err := runTo(t, args(scram)...)
	if err != nil {
		t.Fatal(err)
	}
	earlier := write(t, "before.json", before)

	_, err = runTo(t, args(cert, "-compare", earlier, "-fail-on", "cve")...)
	var gate gateError
	if !errors.As(err, &gate) || len(gate) != 1 || !strings.Contains(gate[0], "CVE-2021-23214") {
		t.Fatalf("a new applicable CVE: %v", err)
	}

	now, err := runTo(t, args(cert)...)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runTo(t, args(cert, "-compare", write(t, "now.json", now), "-fail-on", "cve")...); err != nil {
		t.Errorf("the same CVE, already reported: %v", err)
	}
	if _, err := runTo(t, args(scram, "-compare", earlier, "-fail-on", "cve")...); err != nil {
		t.Errorf("does not apply: %v", err)
	}
}

func TestTheComparisonReadsAsText(t *testing.T) {
	settings := write(t, "settings.csv", "name,setting,unit\nserver_version,17.2,\nssl,off,\n")
	before, _ := runTo(t, "-pg-settings", settings, "-components", "postgres", "-output", "json", "-answer", "exposure=internet", "-pg-hba", write(t, "h", "local all all peer\n"))
	fixed := write(t, "fixed.csv", "name,setting,unit\nserver_version,17.2,\nssl,on,\n")
	out, err := runTo(t, "-pg-settings", fixed, "-components", "postgres", "-answer", "exposure=internet", "-pg-hba", write(t, "h", "local all all peer\n"),
		"-compare", write(t, "before.json", before))
	if err != nil || !strings.Contains(out, "resolved  postgres/ssl") || !strings.Contains(out, "1 resolved") {
		t.Errorf("%v\n%s", err, out)
	}
	if _, err := runTo(t, "-pg-settings", fixed, "-compare", write(t, "x.json", "[]")); err == nil {
		t.Error("a file that is not a report was compared")
	}
}
