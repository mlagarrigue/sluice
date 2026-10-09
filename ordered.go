package sluice

import "github.com/mlagarrigue/sluice/internal/batch"

// ErrUnsorted reports that an ordered-input operator saw its key go backwards
// — [Merge]'s join key, or [Interval]'s event time.
//
// These operators walk their inputs once and never look back, so an
// out-of-order key silently produces a wrong result — rows that should have
// matched are emitted as unmatched, and no error surfaces anywhere. Rather
// than document that as a caveat, the order each depends on is checked as
// elements are first seen, and this is the panic. The check costs one
// comparison per element against the several the operator already performs;
// it is not worth making optional.
//
// Recover on it only to improve the diagnostic — [sluice.Try] converts it, since
// out-of-order data can be a runtime condition and not only a wiring mistake.
var ErrUnsorted = batch.NewSentinel("sluice: ordered-input operator saw its key go backwards")
