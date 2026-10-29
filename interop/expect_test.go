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

// A case marked random in the file accepts either outcome the losses can
// draw, and still refuses one the case never yields: unsupported means the
// image changed, not the dice.
func TestExpectedResultsGuardTakesRandom(t *testing.T) {
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq is not installed")
	}
	var want map[string]map[string]map[string]string
	b, err := os.ReadFile("expected-results.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &want); err != nil {
		t.Fatal(err)
	}
	want["quic-go"]["server"]["handshake"] = "random: the test's own"
	dir := t.TempDir()
	expected := filepath.Join(dir, "expected.json")
	b, err = json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(expected, b, 0o600); err != nil {
		t.Fatal(err)
	}
	asServer := readResults(t, "testdata/results_sluice_quic-go.json")
	asClient := readResults(t, "testdata/results_quic-go_sluice.json")
	for _, tc := range []struct {
		result string
		accept bool
	}{{"succeeded", true}, {"failed", true}, {"unsupported", false}} {
		t.Run(tc.result, func(t *testing.T) {
			s := clone(t, asServer)
			setResult(s, "handshake", tc.result)
			sf := writeResults(t, t.TempDir(), "server.json", s)
			cf := writeResults(t, t.TempDir(), "client.json", asClient)
			cmd := exec.Command("sh", "../scripts/interop-expect.sh", "quic-go", sf, cf) //nolint:gosec // G204: the script is the repository's, the arguments this test's
			cmd.Env = append(os.Environ(), "EXPECTED="+expected)
			out, err := cmd.CombinedOutput()
			if tc.accept && err != nil {
				t.Fatalf("a random case that %s was refused: %v\n%s", tc.result, err, out)
			}
			if !tc.accept && err == nil {
				t.Fatalf("a random case that came out %s was accepted:\n%s", tc.result, out)
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

// The gate walked the cases the runner had written and never asked whether
// one it was told to run was missing, so a pass that ran none of them
// answered "every case as expected" with a nul status. The runner at ab039a4
// skips a whole pair when its compliance check fails and exports its results
// anyway, so that pass is not hypothetical. CASES_SERVER and CASES_CLIENT are
// the floor: the cases the pass was asked for.
func TestPartialPassIsRefused(t *testing.T) {
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq is not installed")
	}
	asServer := readResults(t, "testdata/results_sluice_quic-go.json")
	asClient := readResults(t, "testdata/results_quic-go_sluice.json")
	full := strings.Join(caseNames(t, asServer), ",")

	for _, tc := range []struct {
		name           string
		server, client map[string]any
		askedServer    string
		askedClient    string
		refuse         string
	}{
		{
			name:   "the recorded pass, every case asked for",
			server: asServer, client: asClient,
			askedServer: full, askedClient: full,
		},
		{
			name:   "a pass with no case at all",
			server: emptyResults(), client: emptyResults(),
			askedServer: full, askedClient: full,
			refuse: "sluice server, handshake: asked for and not in the pass",
		},
		{
			name:   "a pass stopped after the first case",
			server: firstCaseOnly(t, asServer), client: asClient,
			askedServer: full, askedClient: full,
			refuse: "asked for and not in the pass",
		},
		{
			name:   "a side that was not run is not judged",
			server: asServer, client: emptyResults(),
			askedServer: full, askedClient: "",
		},
		{
			name:   "no floor given at all keeps the old behaviour",
			server: emptyResults(), client: emptyResults(),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			sf := writeResults(t, dir, "server.json", tc.server)
			cf := writeResults(t, dir, "client.json", tc.client)
			cmd := exec.Command("sh", "../scripts/interop-expect.sh", "quic-go", sf, cf) //nolint:gosec // G204: the script is the repository's, the arguments this test's
			cmd.Env = append(os.Environ(),
				"CASES_SERVER="+tc.askedServer, "CASES_CLIENT="+tc.askedClient)
			out, err := cmd.CombinedOutput()
			if tc.refuse == "" {
				if err != nil {
					t.Fatalf("refused a pass it should accept: %v\n%s", err, out)
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

// caseNames lists the case names a recorded pass holds, in its own order.
func caseNames(t *testing.T, results map[string]any) []string {
	t.Helper()
	var names []string
	for _, c := range resultList(t, results) {
		m, ok := c.(map[string]any)
		if !ok {
			t.Fatalf("a case of the recorded pass is %T, not an object", c)
		}
		name, ok := m["name"].(string)
		if !ok {
			t.Fatalf("a case of the recorded pass has no name: %v", m)
		}
		names = append(names, name)
	}
	if len(names) == 0 {
		t.Fatal("the recorded pass holds no case; this test has nothing to narrow")
	}
	return names
}

// resultList is the single server/client pair's list of cases.
func resultList(t *testing.T, results map[string]any) []any {
	t.Helper()
	outer, ok := results["results"].([]any)
	if !ok || len(outer) == 0 {
		t.Fatalf("the pass has no results array: %v", results["results"])
	}
	inner, ok := outer[0].([]any)
	if !ok {
		t.Fatalf("results[0] is %T, not a list", outer[0])
	}
	return inner
}

// emptyResults is a pass that ran nothing, which is what the runner exports
// when it skips a pair.
func emptyResults() map[string]any {
	return map[string]any{"results": []any{[]any{}}}
}

// firstCaseOnly is a pass stopped after its first case.
func firstCaseOnly(t *testing.T, results map[string]any) map[string]any {
	t.Helper()
	cut := map[string]any{}
	for k, v := range results {
		cut[k] = v
	}
	cut["results"] = []any{[]any{resultList(t, results)[0]}}
	return cut
}

// A random case is accepted either way pass by pass; over its history it
// is not. scripts/interop-random-history.sh appends each pass and refuses a
// random case that succeeded in none of the last six passes that ran it —
// and only then: five failures are not enough, one success among six is,
// and a pass that did not run the case does not count against it.
func TestRandomCasesAreJudgedOverTheirHistory(t *testing.T) {
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq is not installed")
	}
	dir := t.TempDir()
	expected := filepath.Join(dir, "expected.json")
	if err := os.WriteFile(expected, []byte(`{"quic-go": {"server": {"handshake": "random: the test's own", "transfer": "succeeded"}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	run := func(args ...string) (string, error) {
		cmd := exec.Command("sh", append([]string{"../scripts/interop-random-history.sh"}, args...)...) //nolint:gosec // G204: the script is the repository's, the arguments this test's
		cmd.Env = append(os.Environ(), "EXPECTED="+expected)
		out, err := cmd.CombinedOutput()
		return string(out), err
	}
	// history appends one pass per letter: s or f for the random case, - for
	// a pass that did not run it.
	history := func(t *testing.T, passes string) string {
		t.Helper()
		h := filepath.Join(t.TempDir(), "history.jsonl")
		for i, p := range passes {
			line := `{"server":"sluice","client":"quic-go","name":"transfer","result":"succeeded"}` + "\n"
			if p != '-' {
				result := map[rune]string{'s': "succeeded", 'f': "failed"}[p]
				line += `{"server":"sluice","client":"quic-go","name":"handshake","result":"` + result + `"}` + "\n"
			}
			cases := filepath.Join(t.TempDir(), "cases.jsonl")
			if err := os.WriteFile(cases, []byte(line), 0o600); err != nil {
				t.Fatal(err)
			}
			if out, err := run("append", h, cases, "pass-"+string(rune('a'+i)), "abc1234"); err != nil {
				t.Fatalf("append: %v\n%s", err, out)
			}
		}
		return h
	}
	for _, tc := range []struct {
		passes string
		refuse bool
	}{
		{"ffffff", true},
		{"fffff", false},
		{"sfffff", false},
		{"sffffff", true},
		{"ff-ff-ff", true},
		{"fffffs", false},
	} {
		t.Run(tc.passes, func(t *testing.T) {
			out, err := run("check", history(t, tc.passes))
			switch {
			case tc.refuse && err == nil:
				t.Fatalf("passes %s: accepted\n%s", tc.passes, out)
			case tc.refuse && !strings.Contains(out, "quic-go server handshake: no success in the last 6 passes"):
				t.Fatalf("passes %s: refused without naming the case\n%s", tc.passes, out)
			case !tc.refuse && err != nil:
				t.Fatalf("passes %s: refused: %v\n%s", tc.passes, err, out)
			}
		})
	}

	// The history keeps the last eight passes.
	b, err := os.ReadFile(history(t, "sssssssssss"))
	if err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(string(b), "\n"); n != 8 {
		t.Fatalf("the history holds %d passes, want the last 8", n)
	}
}
