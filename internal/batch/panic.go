package batch

import (
	"fmt"
	"runtime"
	"runtime/debug"
)

// Panic is a panic recovered on one of an operator's goroutines — a worker,
// a producer — to be raised again on the consumer's, the one goroutine the
// caller can see. The zero value is no panic.
//
// The stack is taken where the panic was recovered, with the panicking
// frames still below: raised again as it was, the value would crash the
// process from the consumer's stack, and the frame that failed — a nil
// dereference in a Map function — would be nowhere in the output, against
// ADR 0006's promise that a bug crashes "loudly, with its stack".
type Panic struct {
	value any
	stack []byte
}

// Recovered is the Panic for what recover returned: call it from the
// deferred function that recovers, so the stack is the panicking one. A nil
// value — no panic — is the zero Panic.
func Recovered(value any) Panic {
	if value == nil {
		return Panic{}
	}
	return Panic{value: value, stack: debug.Stack()}
}

// Occurred reports whether p holds a panic.
func (p Panic) Occurred() bool { return p.stack != nil }

// Raise panics with what p holds, if it holds anything. A runtime error —
// a nil dereference, an index out of range, the bugs ADR 0006 has crash
// "loudly, with its stack" — is raised as an error that carries the stack it
// was recovered with, and the runtime error itself through Unwrap, so
// errors.As still finds it. Any other value is raised as it was: a value the
// caller's code chose to panic with is compared by the caller — Try
// recovers a sentinel by value, a test or a handler matches its own — and
// its message is the caller's own account of the failure.
func (p Panic) Raise() {
	if p.stack == nil {
		return
	}
	if _, bug := p.value.(runtime.Error); bug {
		panic(&crossedPanic{value: p.value, stack: p.stack})
	}
	panic(p.value)
}

// crossedPanic is a runtime error raised again on another goroutine than the
// one it was raised on. Printed by the runtime as the process ends, its
// message is the original error and the original stack, above the
// consumer's own trace.
type crossedPanic struct {
	value any // a runtime.Error
	stack []byte
}

func (p *crossedPanic) Error() string {
	return fmt.Sprintf("%v\n\npanicked on an operator's goroutine:\n%s", p.value, p.stack)
}

func (p *crossedPanic) Unwrap() error { return p.value.(error) }
