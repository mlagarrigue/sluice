package phase

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// A lock lost while a phase runs ends that phase's context: here its lease
// file goes, as an expiry leaves it, and a second Run of the same job takes
// the lock and starts the same phase. The first phase must learn it is no
// longer the job's while the second runs, not at its own next Save, or the
// two fill the same working tables side by side.
func TestALostLeaseEndsThePhaseRunning(t *testing.T) {
	d := &DirStore{Path: t.TempDir(), LeaseTTL: 300 * time.Millisecond}
	successorIn := make(chan struct{})
	releaseSuccessor := make(chan struct{})
	cancelled := make(chan error, 1)
	successor := make(chan error, 1)

	first := &Job{Name: "lost", Store: d, Phases: []Phase{{
		Name: "load",
		Run: func(ctx context.Context) (Counts, error) {
			if err := os.Remove(filepath.Join(d.Path, "lost.lease")); err != nil {
				return nil, err
			}
			go func() {
				second := &Job{Name: "lost", Store: d, Phases: []Phase{{
					Name: "load",
					Run: func(context.Context) (Counts, error) {
						close(successorIn)
						<-releaseSuccessor
						return Counts{"rows": 2}, nil
					},
				}}}
				_, err := second.Run(t.Context()) // not the first's: it is cancelled
				successor <- err
			}()
			<-successorIn
			select {
			case <-ctx.Done(): // the renewal looks every 100 ms
				cancelled <- context.Cause(ctx)
			case <-time.After(10 * time.Second):
				cancelled <- nil
			}
			close(releaseSuccessor)
			return Counts{"rows": 1}, nil
		},
	}}}
	_, err := first.Run(t.Context())
	if !errors.Is(err, ErrLeaseLost) {
		t.Errorf("the first Run returned %v, want ErrLeaseLost", err)
	}
	if cause := <-cancelled; !errors.Is(cause, ErrLeaseLost) {
		t.Errorf("the first phase's context ended with %v, want ErrLeaseLost: it ran beside its successor", cause)
	}
	if err := <-successor; err != nil {
		t.Errorf("the successor: %v", err)
	}
}

// Release is not a loss: a job that finishes leaves Lost open.
func TestReleaseDoesNotCloseLost(t *testing.T) {
	d := &DirStore{Path: t.TempDir(), LeaseTTL: 300 * time.Millisecond}
	l, err := d.Lock(t.Context(), "job")
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Release(t.Context()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond) // past a renewal tick
	select {
	case <-l.Lost():
		t.Error("Release closed Lost")
	default:
	}
}
