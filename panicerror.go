package sluice

import "fmt"

// PanicError is a panic raised on one of an operator's goroutines — a worker
// of parallel.Ordered, the producer of parallel.Async or
// window.CoalesceWithin — and raised again on the consumer's, the one
// goroutine the caller can see. Raised again as it was, the value would crash
// the process from the consumer's stack, and the frame that failed — a nil
// dereference in a Map function — would be nowhere in the output: the
// envelope keeps the stack it was raised on, and its message prints it,
// so the crash stays loud and points at the faulty line (ADR 0006).
//
// The value the goroutine panicked with is Value. When it is an error,
// Unwrap returns it, so errors.Is and errors.As see through the envelope: a
// runtime.Error, a sentinel, the caller's own error. Any other value — a
// string, a struct — is read from the field:
//
//	defer func() {
//	    r := recover()
//	    if pe, ok := r.(*sluice.PanicError); ok {
//	        r = pe.Value
//	    }
//	    ...
//	}()
//
// A panic relayed twice — a Map worker under parallel.Async — is one
// PanicError: the deepest stack is the faulty one, and it is kept. [Try]
// looks inside the envelope and returns the sentinel as it was raised.
type PanicError struct {
	// Value is what the operator's goroutine panicked with.
	Value any
	// Stack is that goroutine's stack, taken as the panic was recovered,
	// with the panicking frames still below, as runtime/debug.Stack
	// formats it.
	Stack []byte
}

// Error returns the value as fmt prints it, then the stack it was raised on.
func (e *PanicError) Error() string {
	return fmt.Sprintf("%v\n\npanicked on an operator's goroutine:\n%s", e.Value, e.Stack)
}

// Unwrap returns Value when it is an error, nil otherwise.
func (e *PanicError) Unwrap() error {
	err, _ := e.Value.(error)
	return err
}
