package sluice

import (
	"errors"

	"github.com/mlagarrigue/sluice/internal/batch"
)

// Try runs consume and converts the library's sentinel panics into the error
// they carry. Any other panic — a bug in user code or in this package — is
// re-raised untouched.
//
//	err := Try(func() {
//	    ForEach(pipeline, handle)
//	})
//	if errors.Is(err, ErrOverflow) { ... }
//
// It exists because some sentinels are data-dependent: [Fail] as an Overflow
// policy means one pathological input batch panics the consuming goroutine,
// and [ErrUnsorted] fires on data that arrived out of order. In a batch job
// that crash is the right outcome. In a long-running server it is process
// death for every in-flight request, so every serious consumer ends up writing
// this boundary — each inventing alone which panics to convert. The library
// that raises the panics is the one place that knows the complete list, so it
// ships the boundary.
//
// The discriminating rule is the whole design: **sentinels only**. A Try that
// recovered every panic would convert user bugs into error returns and hide
// them — the silent-wrongness failure arriving through the exit. A nil pointer
// dereference in a Map function must keep crashing the process, loudly, with
// its stack.
//
// The re-raised panic unwinds from this frame, so the trace shows Try between
// the crash site and the caller; the panic value is untouched. The converted
// error is the panic value as raised — for [ErrParallelInit] that is the
// wrapping error, so errors.Is reaches both the sentinel and the cause.
//
// Try panics if consume is nil.
func Try(consume func()) (err error) {
	if consume == nil {
		panic("sluice: Try requires a non-nil consume function")
	}
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		if e, ok := r.(error); ok {
			var s *batch.Sentinel
			if errors.As(e, &s) {
				err = e
				return
			}
		}
		panic(r)
	}()
	consume()
	return nil
}
