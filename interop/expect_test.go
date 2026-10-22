package interop

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The interop runner's pass is judged by scripts/interop-expect.sh against
// expected-results.json. A guard that has never refused anything proves
// nothing, so this replays it on the recorded quic-go pass in testdata/ and
// on that pass altered four ways, each of which it must refuse.
func TestExpectedResultsGuard(t *testing.T) {
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq is not installed")
	}
	asServer := readResults(t, "testdata/results_sluice_quic-go.json")
	asClient := readResults(t, "testdata/results_quic-go_sluice.json")

	cases := []struct {
		name   string
		peer   string
		edit   func(server, client map[string]any)
		refuse string // empty: the pass must be accepted
	}{
		{name: "the recorded pass", peer: "quic-go"},
		{
			name: "a case expected to succeed fails", peer: "quic-go",
			edit:   func(s, _ map[string]any) { setResult(s, "handshake", "failed") },
			refuse: "sluice server, handshake: failed, expected succeeded",
		},
		{
			name: "a case expected to fail succeeds", peer: "quic-go",
			edit:   func(s, _ map[string]any) { setResult(s, "connectionmigration", "succeeded") },
			refuse: "sluice server, connectionmigration: succeeded, expected failed",
		},
		{
			name: "a case the file does not list", peer: "quic-go",
			edit:   func(_, c map[string]any) { addResult(c, "multiconnect", "succeeded") },
			refuse: "sluice client, multiconnect: succeeded, which the file does not list",
		},
		{name: "a peer the file does not list", peer: "no-such-peer", refuse: "no expected results for no-such-peer"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, c := clone(t, asServer), clone(t, asClient)
			if tc.edit != nil {
				tc.edit(s, c)
			}
			dir := t.TempDir()
			sf, cf := writeResults(t, dir, "server.json", s), writeResults(t, dir, "client.json", c)
			out, err := exec.Command("sh", "../scripts/interop-expect.sh", tc.peer, sf, cf).CombinedOutput() //nolint:gosec // G204: the script is the repository's, the arguments this test's
			if tc.refuse == "" {
				if err != nil {
					t.Fatalf("the recorded pass was refused: %v\n%s", err, out)
				}
				return
			}
			if err == nil {
				t.Fatalf("accepted a pass it should refuse:\n%s", out)
			}
			if !strings.Contains(string(out), tc.refuse) {
				t.Fatalf("refused, but without %q:\n%s", tc.refuse, out)
			}
		})
	}
}

func readResults(t *testing.T, path string) map[string]any {
	t.Helper()
	b, err := os.ReadFile(path) //nolint:gosec // G304: a testdata path, not input
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func clone(t *testing.T, m map[string]any) map[string]any {
	t.Helper()
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func writeResults(t *testing.T, dir, name string, m map[string]any) string {
	t.Helper()
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// cases is the runner's results[0]: the one server/client pair of a pass.
func cases(m map[string]any) []any {
	return m["results"].([]any)[0].([]any)
}

func setResult(m map[string]any, name, result string) {
	for _, c := range cases(m) {
		if c := c.(map[string]any); c["name"] == name {
			c["result"] = result
		}
	}
}

func addResult(m map[string]any, name, result string) {
	r := m["results"].([]any)
	r[0] = append(cases(m), map[string]any{"name": name, "result": result})
}
