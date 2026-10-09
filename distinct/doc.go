// Package distinct holds By, which drops elements whose key was among the
// last n distinct keys seen.
//
// It is a package of one function because it retains a set of keys between
// batches, bounded by n and governed by an Overflow policy, and the core's
// operators hold nothing beyond the batch in flight. Deduplication is local
// to that window by design: a global set over a stream grows without bound.
package distinct
