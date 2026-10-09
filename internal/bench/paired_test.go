package bench

import (
	"fmt"
	"os/exec"
	"strings"
	"testing"
)

// pairedInput renders rounds in ab.sh's "side round name value" shape. Round 1
// is a warm-up paired.awk discards, so it is emitted as a tie that must not
// count either way.
func pairedInput(rounds [][2]float64) string {
	var b strings.Builder
	fmt.Fprintf(&b, "before 1 BenchmarkX 10\nafter 1 BenchmarkX 10\n")
	for i, r := range rounds {
		fmt.Fprintf(&b, "before %d BenchmarkX %g\nafter %d BenchmarkX %g\n", i+2, r[0], i+2, r[1])
	}
	return b.String()
}

func runPaired(t *testing.T, input string) string {
	t.Helper()
	awk, err := exec.LookPath("awk")
	if err != nil {
		t.Skip("no awk on PATH")
	}
	cmd := exec.Command(awk, "-v", "MINEFFECT=0", "-f", "scripts/paired.awk")
	cmd.Stdin = strings.NewReader(input)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("paired.awk: %v\n%s", err, out)
	}
	return string(out)
}

// The sign test drops ties. Scored as losses, they turn identical rounds into
// a SLOWER verdict that fails the regression gate on code that did not move,
// and they hide a speedup that won every round that was not a tie.
func TestPairedAwkDropsTies(t *testing.T) {
	var mostlyTies, tiesAndWins [][2]float64
	for range 10 {
		mostlyTies = append(mostlyTies, [2]float64{10, 10})
	}
	mostlyTies = append(mostlyTies, [2]float64{10, 9})
	for range 8 {
		tiesAndWins = append(tiesAndWins, [2]float64{10, 9})
	}
	for range 3 {
		tiesAndWins = append(tiesAndWins, [2]float64{10, 10})
	}

	cases := []struct {
		name   string
		rounds [][2]float64
		want   string
		reject string
	}{
		{"ten ties and one win", mostlyTies, "noise", "SLOWER"},
		{"eight wins and three ties", tiesAndWins, "faster", "noise"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out := runPaired(t, pairedInput(c.rounds))
			if !strings.Contains(out, c.want) || strings.Contains(out, c.reject) {
				t.Errorf("want %q, not %q, in:\n%s", c.want, c.reject, out)
			}
		})
	}
}
