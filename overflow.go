package sluice

import (
	"strconv"

	"github.com/mlagarrigue/sluice/internal/batch"
)

// ErrOverflow reports that a bounded operator reached its bound under the
// [Fail] policy.
//
// It is panicked rather than returned, for the reason [ErrSplitStalled] is: an
// iter.Seq has nowhere to put an error, and the alternative — yielding a silent
// prefix of the data — is what the Fail policy was chosen to avoid. Recover on
// it to convert it into whatever the caller's error handling expects.
//
// Reaching it is not a bug in the operator. It is the pipeline being told that
// the bound it declared was too small for the data it received, which is a fact
// worth learning rather than absorbing.
var ErrOverflow = batch.NewSentinel("sluice: bounded operator reached its limit under the Fail policy")

// Overflow says what a bounded operator does when its bound is reached.
//
// Back-pressure does not remove pressure, it moves it upstream until it reaches
// somewhere that can act on it. The O(k) operators are that somewhere: they are
// the only points in a pipeline holding state that can fill, so each one has to
// say what filling means. Leaving it implicit is how an unbounded queue gets
// built by accident.
//
// There is no default. A policy nobody chose is a policy nobody owns, and the
// right answer differs per pipeline: a metrics feed drops, a payment stream
// fails, a file read blocks. The caller states it.
type Overflow uint8

const (
	// DropOldest discards the oldest retained element to make room. The stream
	// stays current at the cost of completeness — the choice for a feed where
	// the latest value is the useful one.
	DropOldest Overflow = iota

	// DropNewest discards the arriving element and keeps what is held. The
	// choice when the first observation of something matters more than the
	// most recent.
	DropNewest

	// Fail stops the stream with a panic when the bound is reached. The choice
	// when dropping silently would be worse than stopping — a stream whose
	// completeness is the point.
	Fail
)

// String renders the policy for logs, with the same rule as [diagnostics.Severity.String]:
// a value outside the constants shows its number rather than a label that would
// misrepresent it.
func (o Overflow) String() string {
	switch o {
	case DropOldest:
		return "DropOldest"
	case DropNewest:
		return "DropNewest"
	case Fail:
		return "Fail"
	default:
		return "Overflow(" + strconv.Itoa(int(o)) + ")"
	}
}

// Valid reports whether the policy is one of the declared constants. An
// operator checks this at construction rather than on overflow: a typo in a
// configuration value must not lie dormant until the day the bound is reached,
// which is the day the pipeline is already under stress.
func (o Overflow) Valid() bool { return o <= Fail }
