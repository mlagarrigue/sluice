package interop

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// scripts/interop-runner.sh --unpullable is the reader of UNPULLABLE's
// promise, that a peer leaves the list when its image is served again. The
// weekly pass only ever sees mvfst still refused, so this plays the three
// answers with curl and docker stubbed at the head of PATH: the runner list
// is a fixed document, and docker pull succeeds or fails on demand.
func TestUnpullableNamesAnImageServedAgain(t *testing.T) {
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq is not installed")
	}
	cases := []struct {
		name       string
		unpullable string
		pull       int // the stub docker's exit status
		fails      bool
		says       string
	}{
		{name: "an image served again", unpullable: "mvfst", pull: 0, fails: true, says: "served again: mvfst"},
		{name: "an image still refused", unpullable: "mvfst", pull: 1, says: "mvfst: x/mvfst-interop:latest is still not served"},
		{name: "a peer the list does not hold", unpullable: "nobody", pull: 1, fails: true, says: "UNPULLABLE names nobody"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bin := t.TempDir()
			stub(t, bin, "curl", `printf '%s' '{"mvfst":{"image":"x/mvfst-interop:latest"}}'`)
			stub(t, bin, "docker", "exit "+strconv.Itoa(tc.pull))
			cmd := exec.Command("sh", "../scripts/interop-runner.sh", "--unpullable")
			cmd.Env = append(os.Environ(), "PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"), "UNPULLABLE="+tc.unpullable)
			out, err := cmd.CombinedOutput()
			if (err != nil) != tc.fails {
				t.Fatalf("exit %v, want a failure %v:\n%s", err, tc.fails, out)
			}
			if !strings.Contains(string(out), tc.says) {
				t.Fatalf("the output does not say %q:\n%s", tc.says, out)
			}
		})
	}
}

func stub(t *testing.T, dir, name, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte("#!/bin/sh\n"+body+"\n"), 0o700); err != nil { //nolint:gosec // G306: an executable stub, this test's alone
		t.Fatal(err)
	}
}
