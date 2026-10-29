package handoff

import (
	"runtime/debug"

	"github.com/mlagarrigue/sluice"
)

// Panic is a panic recovered on one of an operator's goroutines — a worker,
// a producer — to be raised again on the consumer's, as a
// [sluice.PanicError] that carries the stack it was raised on. The zero
// value is no panic.
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

// Raise panics with what p holds, if it holds anything: a
// *sluice.PanicError with the value and the stack it was recovered with. A
// value that already is one — relayed by an operator upstream, on a
// goroutine of its own — is raised as it is: its stack is the one with the
// faulty frame, the hop in between adds nothing to read.
func (p Panic) Raise() {
	if p.stack == nil {
		return
	}
	if relayed, ok := p.value.(*sluice.PanicError); ok {
		panic(relayed)
	}
	panic(&sluice.PanicError{Value: p.value, Stack: p.stack})
}
