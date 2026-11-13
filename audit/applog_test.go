package audit

import (
	"strings"
	"testing"
)

// The application's log names the descriptor it ran out of: EMFILE, the
// process's own limit, in the forms the stacks print it — sluice's warn line,
// net/http's Accept error, the errno's name alone — and ENFILE, the system's
// table, which is another setting. A log with neither reports nothing.
func TestTheApplicationLogNamesTheDescriptorsRunOut(t *testing.T) {
	log := `time=2026-11-13T09:00:00Z level=INFO msg=listening addr=:8443
time=2026-11-13T09:00:05Z level=WARN msg="httpstream: accepting failed; retrying with backoff until it succeeds" addr=[::]:8443 err="accept tcp [::]:8443: accept4: too many open files"
2026/11/13 09:00:06 http: Accept error: accept tcp [::]:8080: accept4: too many open files; retrying in 5ms
Error: accept EMFILE
`
	target, err := Open(Spec{AppLog: seedFile(t, "server.log", log)})
	if err != nil {
		t.Fatal(err)
	}
	findings, err := Run(Selection{Sources: {"logs"}}, nil, target)
	if err != nil {
		t.Fatal(err)
	}
	var got []Finding
	for _, f := range findings {
		if f.Rule == "app/out-of-descriptors" {
			got = append(got, f)
		}
	}
	if len(got) != 1 || !strings.HasPrefix(got[0].Current, "3 line(s) naming EMFILE") || !strings.Contains(got[0].Proposed, "nofile") {
		t.Fatalf("app/out-of-descriptors said %+v, want one finding: the three EMFILE lines, towards nofile", got)
	}

	system, err := Open(Spec{AppLog: seedFile(t, "server.log", "OSError: [Errno 23] Too many open files in system\n")})
	if err != nil {
		t.Fatal(err)
	}
	fs, _ := Run(Selection{Sources: {"logs"}}, nil, system)
	if len(fs) != 1 || !strings.Contains(fs[0].Current, "ENFILE") || !strings.Contains(fs[0].Proposed, "fs.file-max") {
		t.Errorf("an ENFILE line reported %+v, want one finding towards fs.file-max and none towards nofile", fs)
	}

	quiet, err := Open(Spec{AppLog: seedFile(t, "server.log", "time=2026-11-13T09:00:00Z level=INFO msg=listening addr=:8443\n")})
	if err != nil {
		t.Fatal(err)
	}
	if fs, _ := Run(Selection{Sources: {"logs"}}, nil, quiet); len(fs) != 0 {
		t.Errorf("a quiet application log was reported: %v", fs)
	}
}
