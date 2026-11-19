package phase

import (
	"context"
	"errors"
	"time"
)

// A Store holds jobs' locks and records. [Dir] is one; an implementation
// over a database keeps them beside the data the phases write.
type Store interface {
	// Lock takes the job's lock for this process, or fails at once with
	// an error wrapping ErrBusy when another holds it. It never waits.
	Lock(ctx context.Context, job string) (Lease, error)
}

// A Lease is a job's lock, held: what the record is read and written
// through, so that a process that lost the lock cannot write.
type Lease interface {
	// Load reads the job's record; false when there is none.
	Load(ctx context.Context) (Record, bool, error)

	// Save writes the record whole, or not at all, and fails with an
	// error wrapping ErrLeaseLost when the lock is no longer this
	// lease's.
	Save(ctx context.Context, rec Record) error

	// Release gives the lock up. The record stays.
	Release(ctx context.Context) error

	// Lost is closed once the lock is found lost while it is held — its
	// expiry passed, another process took it, or the connection that held
	// it is gone — and never by Release. [Job.Run] ends the context of
	// the phase that is running then: the process that took the lock over
	// starts that phase again, and the two would otherwise write the same
	// working tables side by side until the first one's next Save.
	Lost() <-chan struct{}
}

var (
	// ErrBusy: another process holds the job's lock.
	ErrBusy = errors.New("phase: the job is held by another process")

	// ErrLeaseLost: the lock was lost while the job ran — taken over
	// after it expired, or with the connection that held it — and nothing
	// more is written under it.
	ErrLeaseLost = errors.New("phase: the job's lock was lost")
)

// Status is a phase's state in the record.
type Status string

const (
	// Pending: the phase has not started under this record.
	Pending Status = ""
	// Running: the phase started and did not finish; it counts as not
	// done.
	Running Status = "running"
	// Done: the phase finished; its counts are recorded.
	Done Status = "done"
	// Failed: the phase returned an error; its message is recorded.
	Failed Status = "failed"
)

// Record is a job's state in its store.
type Record struct {
	// Fingerprint is the SHA-256, in hex, of the job's canonical name,
	// input, logic version and phase names.
	Fingerprint string `json:"fingerprint"`

	// Input and Logic are kept in the clear, so that a run that discards
	// the record can say what changed.
	Input Input `json:"input"`
	Logic int   `json:"logic"`

	Phases []PhaseRecord `json:"phases"`
}

// PhaseRecord is one phase's state.
type PhaseRecord struct {
	Name   string    `json:"name"`
	Status Status    `json:"status,omitempty"`
	Counts Counts    `json:"counts,omitempty"`
	Error  string    `json:"error,omitempty"`
	At     time.Time `json:"at,omitzero"`
}
