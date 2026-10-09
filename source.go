package sluice

// Source pairs a Stream with the errors that cannot travel inside it.
//
// A stream carries values. Two failures are not values, because they happen
// where no element exists to carry them:
//
//   - setup — the file did not open, the connection was refused. No element was
//     ever produced, so there is nothing to attach an error to.
//   - teardown — the Close after the last element failed, the final flush was
//     short. Every element was produced and consumed already.
//
// Both are reported by [Source.Err], consulted after consumption:
//
//	src := openCSV("orders.csv")
//	for b := range src.Stream() {
//	    // ...
//	}
//	if err := src.Err(); err != nil {
//	    return fmt.Errorf("reading orders: %w", err)
//	}
//
// This is deliberately not an error carried in the element type. An element
// that could not be produced is a different thing from a stream that could not
// be opened, and collapsing the two makes every consumer test for a case that
// cannot happen to it.
//
// # The error is only complete after consumption
//
// Err reports what is known at the moment it is called. Called before the
// stream is consumed it sees setup failures only; called before consumption
// finishes it cannot see a teardown failure that has not happened yet. Consume,
// then check — checking early is not wrong, it is just early, and a source that
// fails to open reports it straight away.
//
// # The zero value is usable
//
// A zero Source yields nothing and reports no error. That is the ordinary case
// for an in-memory stream that cannot fail to open, and it must not require a
// caller to supply a function that always returns nil — pass nil to
// [NewSource], or use the zero value.
//
// # One way to build one
//
// Both fields are unexported, so [NewSource] is the only route in. That is
// deliberate rather than defensive: with an exported stream field, replacing it
// after construction leaves the source describing data from one stream and an
// error from another, which is the exact failure this type exists to prevent —
// telling the caller why the stream was short — turned against itself. There is
// no partially built Source to reach.
type Source[T any] struct {
	// stream is the sequence of batches; a nil one yields nothing. Reached
	// through [Source.Stream], which keeps it composable with every operator
	// while leaving no way to swap it out from under err.
	stream Stream[T]

	// err reports the setup and teardown failure, or nil. Unexported so that
	// [Source.Err] can answer for a zero value: an exported func field would
	// make src.Err() a nil call on a source built without one.
	err func() error
}

// NewSource pairs a stream with the function reporting its setup and teardown
// failures.
//
// err is consulted by [Source.Err] and may be nil, which reports no error. It
// is called each time rather than cached, so a source that learns of a
// teardown failure during consumption reports it afterwards.
func NewSource[T any](s Stream[T], err func() error) Source[T] {
	return Source[T]{stream: s, err: err}
}

// Stream returns the sequence of batches, composable with every operator:
//
//	first := Collect(Take(src.Stream(), 10))
//
// A zero Source returns a stream that yields nothing, so the result is always
// safe to range over. The error stays on the Source — consuming a derived
// stream does not carry it along, and [Source.Err] is still the place to ask.
func (s Source[T]) Stream() Stream[T] {
	if s.stream == nil {
		return Empty[T]()
	}
	return s.stream
}

// Err reports the setup or teardown failure, or nil if there was none.
//
// It is safe on a zero Source and on one built without an error function: both
// report nil. See [Source] for when the answer is complete.
func (s Source[T]) Err() error {
	if s.err == nil {
		return nil
	}
	return s.err()
}

// Consume runs the stream to completion and reports [Source.Err].
//
// It is the shape most callers want — consume everything, then check — written
// once so that forgetting the check is a visible omission rather than the
// default. f is called per batch and may return false to stop early; the error
// is still reported, since a source that failed to open must not be silenced by
// a consumer that stopped.
//
// The return value says nothing about whether the stream was exhausted: a
// consumer that stopped early and one that read everything both get nil when
// the source is healthy. The caller that returned false already knows it did,
// so nothing is lost — the error is about the source, not about how far the
// caller chose to read.
//
// Consume panics if f is nil.
func (s Source[T]) Consume(f func(Batch[T]) bool) error {
	if f == nil {
		panic("sluice: Source.Consume requires a non-nil f function")
	}
	if s.stream != nil {
		s.stream(f)
	}
	return s.Err()
}

// Collect gathers every element of the source and reports [Source.Err].
//
// The elements are returned even when the error is non-nil: a teardown failure
// arrives after the data, and discarding what was read would lose it. Check the
// error before trusting the slice to be complete.
//
// It is O(n) in memory, like the package-level [Collect], and takes no bound
// for the same reason: compose it with [Take] on the stream when one is needed.
func (s Source[T]) Collect() ([]T, error) {
	var out []T
	err := s.Consume(func(b Batch[T]) bool {
		out = append(out, b.Items...)
		return true
	})
	return out, err
}
