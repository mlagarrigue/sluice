package phase_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/mlagarrigue/sluice/phase"
)

// A nightly job of two phases. The first run dies in its second phase;
// the run after it skips the first phase, once its Verify has checked the
// staging table still holds what was recorded, and runs the second.
func Example() {
	dir, err := os.MkdirTemp("", "etl")
	if err != nil {
		panic(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	staging, loaded := 0, 0 // stand-ins for two tables
	sourceUp := false
	job := phase.Job{
		Name:         "orders-nightly",
		Input:        phase.Input{"day": "2026-11-15"},
		LogicVersion: 1,
		Store:        phase.Dir(dir),
		Phases: []phase.Phase{
			{
				Name: "extract",
				Run: func(context.Context) (phase.Counts, error) {
					staging = 1000 // emptied, then filled: it can run again
					return phase.Counts{"rows": int64(staging)}, nil
				},
				Verify: func(_ context.Context, recorded phase.Counts) error {
					if int64(staging) != recorded["rows"] {
						return fmt.Errorf("staging holds %d rows, %d recorded", staging, recorded["rows"])
					}
					return nil
				},
			},
			{
				Name: "load",
				Run: func(context.Context) (phase.Counts, error) {
					if !sourceUp {
						return nil, errors.New("the warehouse is down")
					}
					loaded = staging
					return phase.Counts{"rows": int64(loaded)}, nil
				},
			},
		},
	}

	_, err = job.Run(context.Background())
	fmt.Println(err)

	sourceUp = true
	report, err := job.Run(context.Background())
	if err != nil {
		panic(err)
	}
	for _, p := range report.Phases {
		fmt.Println(p.Name, p.Outcome, p.Counts["rows"])
	}
	// Output:
	// phase: job "orders-nightly", phase "load": the warehouse is down
	// extract skipped 1000
	// load ran 1000
}

// A directory holds each job's record and lease; a lease longer than the
// default keeps a job whose process stalls for a while from being taken
// over.
func ExampleDir() {
	dir, err := os.MkdirTemp("", "etl")
	if err != nil {
		panic(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	store := phase.Dir(dir)
	store.LeaseTTL = 2 * time.Minute
	job := phase.Job{
		Name:  "reindex",
		Store: store,
		Phases: []phase.Phase{{
			Name: "rebuild",
			Run:  func(context.Context) (phase.Counts, error) { return phase.Counts{"indexes": 3}, nil },
		}},
	}
	report, err := job.Run(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Println(report.Phases[0].Name, report.Phases[0].Outcome)
	// Output:
	// rebuild ran
}
