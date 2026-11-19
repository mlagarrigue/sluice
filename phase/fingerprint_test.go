package phase

import (
	"context"
	"testing"
)

// TestFingerprintIsStable pins the fingerprint of one job. It is stored in
// every record and compared on every run, so a change to what it hashes —
// a field renamed in the struct it marshals, as Job.Logic becoming
// LogicVersion would have done — makes every stored record look like
// another job's and reruns every phase. Changing it is a decision, made by
// changing this value.
func TestFingerprintIsStable(t *testing.T) {
	j := Job{
		Name:         "ingest",
		Input:        Input{"day": "2026-11-14"},
		LogicVersion: 3,
		Phases: []Phase{
			{Name: "extract", Run: func(context.Context) (Counts, error) { return nil, nil }},
			{Name: "load", Run: func(context.Context) (Counts, error) { return nil, nil }},
		},
	}
	const want = "3badb68555cb547f0b1d014549f438525587981d41c91a8603026dd2c1c9458a"
	if got := j.fresh().Fingerprint; got != want {
		t.Errorf("fingerprint %s, want %s", got, want)
	}
}
