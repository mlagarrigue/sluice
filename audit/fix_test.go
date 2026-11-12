package audit

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// apply does by hand what the header of each proposed file says: the
// sysctl lines written under the root as sysctl --system would, the ALTER
// SYSTEM statements into the saved pg_settings as a reload would show.
func apply(t *testing.T, files []ProposedFile, root, settingsPath string) {
	t.Helper()
	sysctl := regexp.MustCompile(`(?m)^([a-z0-9_.]+) = (\S+)$`)
	alter := regexp.MustCompile(`ALTER SYSTEM SET (\w+) = '([^']*)';`)
	for _, f := range files {
		switch f.Name {
		case "99-sluice-audit.conf":
			for _, m := range sysctl.FindAllStringSubmatch(f.Text, -1) {
				path := filepath.Join(root, "proc", "sys", strings.ReplaceAll(m[1], ".", "/"))
				if err := os.WriteFile(path, []byte(m[2]+"\n"), 0o600); err != nil { //nolint:gosec // G703: under the test's own root
					t.Fatal(err)
				}
			}
		case "sluice-audit.sql":
			b, err := os.ReadFile(settingsPath) //nolint:gosec // G304: a file of the test's own
			if err != nil {
				t.Fatal(err)
			}
			text := string(b)
			for _, m := range alter.FindAllStringSubmatch(f.Text, -1) {
				line := regexp.MustCompile(`(?m)^` + m[1] + `,[^,]*,`)
				text = line.ReplaceAllString(text, m[1]+","+m[2]+",")
			}
			if err := os.WriteFile(settingsPath, []byte(text), 0o600); err != nil { //nolint:gosec // G703: a file of the test's own
				t.Fatal(err)
			}
		default:
			t.Fatalf("an unexpected file %s", f.Name)
		}
	}
}

// TestAProposedFixAppliedByHandRemovesTheFinding: somaxconn at 128 and a
// server with md5 and no autovacuum; the files proposed, applied the way
// their header says, leave the next report without those findings.
func TestAProposedFixAppliedByHandRemovesTheFinding(t *testing.T) {
	root := seedRoot(t, map[string]string{"proc/sys/net/core/somaxconn": "128", "proc/sys/net/core/netdev_max_backlog": "300"})
	settings := seedFile(t, "settings.csv", defaultSettings)
	sel := Selection{Components: {"linux", "postgres"}, Sources: {"config"}}
	report := func() []string {
		target, err := Open(Spec{Root: root, PGSettings: settings})
		if err != nil {
			t.Fatal(err)
		}
		findings, err := Run(sel, nil, target)
		if err != nil {
			t.Fatal(err)
		}
		var names []string
		for _, f := range findings {
			names = append(names, f.Rule)
		}
		return names
	}
	fixable := []string{"linux/somaxconn", "linux/netdev-backlog", "postgres/password-encryption", "postgres/autovacuum"}
	before := report()
	for _, r := range fixable {
		if !slices.Contains(before, r) {
			t.Fatalf("%s not reported before: %v", r, before)
		}
	}
	target, _ := Open(Spec{Root: root, PGSettings: settings})
	findings, _ := Run(sel, nil, target)
	files := Propose(findings)
	if len(files) != 2 || !strings.Contains(files[0].Text, "net.core.somaxconn = 1024") ||
		!strings.Contains(files[0].Text, "net.core.netdev_max_backlog = 4096") ||
		!strings.Contains(files[1].Text, "ALTER SYSTEM SET password_encryption = 'scram-sha-256';") ||
		!strings.Contains(files[0].Text, "not applied by it") {
		t.Fatalf("%+v", files)
	}
	apply(t, files, root, settings)
	after := report()
	for _, r := range fixable {
		if slices.Contains(after, r) {
			t.Errorf("%s still reported after its fix was applied: %v", r, after)
		}
	}
	// ssl needs a certificate: reported, never written.
	if !slices.Contains(after, "postgres/ssl") && slices.Contains(before, "postgres/ssl") {
		t.Errorf("postgres/ssl went without a certificate: %v", after)
	}
	if strings.Contains(files[1].Text, "ssl") {
		t.Errorf("a fix that needs a certificate was written:\n%s", files[1].Text)
	}
}

func TestNothingToFixWritesNothing(t *testing.T) {
	if got := Propose([]Finding{{Rule: "postgres/ssl", Current: "ssl = off"}}); len(got) != 0 {
		t.Errorf("%+v", got)
	}
}
