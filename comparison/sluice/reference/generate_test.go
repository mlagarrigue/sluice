package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// order_hydrate.go is what sluicegen writes for the order type today, byte
// for byte: R is measured on the code the guide teaches, and a file
// committed once and touched by hand, or left behind by a generator that
// moved on, would measure something else without a word. The generator is
// the one of this tree (go.mod's replace), run as go:generate runs it.
func TestGeneratedHydratorIsWhatSluicegenWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs sluicegen")
	}
	out := filepath.Join(t.TempDir(), "order_hydrate.go")
	cmd := exec.Command("go", "run", "github.com/mlagarrigue/sluice/cmd/sluicegen", //nolint:gosec // G204: a temporary path of the test's own
		"-type", "order", "-in", "main.go", "-out", out)
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("sluicegen: %v\n%s", err, b)
	}
	want, err := os.ReadFile(out) //nolint:gosec // G304: the test's own temporary file
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile("order_hydrate.go")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Error("order_hydrate.go is not what sluicegen writes for order: run go generate in comparison/sluice/reference")
	}
}
