// Package diagnostics is what can be said about a value without stopping the
// stream: what is wrong with it, and what can be done about it.
//
// It is deliberately outside the core. docs/design/architecture.md sets one test
// for anything that would live beside the core's Stream and Batch — if
// this type were removed, would the rest stop compiling? — and nothing here
// passes it. No type in this package mentions a Stream or a Batch, and the
// core never mentions a Diagnostic. Keeping them in one package said the
// opposite of what the architecture claims, which is that diagnostics are
// built on top of the core without the core knowing about them.
//
// What the separation buys is that the rule stays checkable: a dependency
// from here to the core would now be an import cycle rather than a comment
// nobody re-reads.
//
// # What is here, and why it is one package
//
//	Diagnostic           something worth reporting about one value
//	Affordance           what can be done about that value
//	Collector            a bounded set of diagnostics, counting what it dropped
//	Path                 where the value is, and what joins them
//
// Affordances have no collector of their own. A batch offers a handful of
// remedies where it may raise thousands of diagnostics, so the ceiling that
// [Collector] exists for (S12) has nothing to bound on that side, and the
// consumer — web/stream's Exchange — carries them as a plain slice.
//
// [Path] is the reason they belong together rather than in three packages: a
// diagnostic and an affordance at the same path are a problem and its remedy,
// and a client pairs them by comparing paths rather than by convention. That
// is the architecture's bet ("Affordances — the gap to fill"), and it only
// works if the same Path type is on both.
//
// A diagnostic is not an error. An error means the element could not be
// produced; a diagnostic means it exists and something about it is worth
// saying. A stream of a thousand orders can carry three thousand diagnostics
// and still deliver every order — which is why [Diagnostic] renders through
// String and not Error, and cannot be returned where a failure is expected.
package diagnostics

import (
	"fmt"
	"strconv"
)

// Severity says how serious a diagnostic is. It does not say whether to stop:
// "how bad is this" and "should the pipeline continue" are two independent
// questions, and merging them is what makes a validation model unable to report
// a warning without aborting. FHIR keeps fatal and error apart for the same
// reason; SARIF separates level from kind.
//
// The zero value is [Info], so a Diagnostic built without stating a severity
// reports the least alarming one rather than an unnamed number.
type Severity uint8

const (
	// Info is a remark that changes nothing: a default was applied, a value was
	// normalised.
	Info Severity = iota

	// Warning is a value that is suspicious but accepted — an unusual price, a
	// quantity far from the historical range.
	Warning

	// Error is a business rule that was violated. The element keeps flowing;
	// what to do about it is the caller's decision, not the operator's.
	Error

	// Critical is a violation that makes the element unusable — a referenced
	// entity that does not exist, a constraint the storage layer rejected.
	Critical
)

// String renders the severity for logs. An unknown value renders as its number
// rather than as a fixed label, so a value that escaped the constants above is
// visible instead of silently reading as Info.
func (s Severity) String() string {
	switch s {
	case Info:
		return "Info"
	case Warning:
		return "Warning"
	case Error:
		return "Error"
	case Critical:
		return "Critical"
	default:
		return "Severity(" + strconv.Itoa(int(s)) + ")"
	}
}

// Origin says which layer produced a diagnostic.
//
// It is what lets a reader go from "Critical ClientMustExists" back to what
// actually happened — a NOT NULL constraint at read time, a hydration failure, a
// business rule. Without it, every diagnostic looks like it came from the
// business layer and the real cause is a guess.
//
// The zero value is [OriginUnknown], which is honest about a diagnostic built
// without stating one.
type Origin uint8

const (
	// OriginUnknown is the zero value: the producer did not say.
	OriginUnknown Origin = iota

	// OriginBusinessRule is a rule expressed in the domain — a quantity that
	// must be positive.
	OriginBusinessRule

	// OriginSQL is a constraint enforced by the database — NOT NULL, a foreign
	// key, a check constraint.
	OriginSQL

	// OriginHydration is a failure turning a row into an entity: a column that
	// would not decode, a type that did not match.
	OriginHydration

	// OriginProtocol is a failure at the wire boundary — malformed input, a
	// limit exceeded, a frame that did not parse.
	OriginProtocol
)

// String renders the origin for logs, with the same rule as [Severity.String]:
// an unknown value shows its number rather than a misleading label.
func (o Origin) String() string {
	switch o {
	case OriginUnknown:
		return "Unknown"
	case OriginBusinessRule:
		return "BusinessRule"
	case OriginSQL:
		return "SQL"
	case OriginHydration:
		return "Hydration"
	case OriginProtocol:
		return "Protocol"
	default:
		return "Origin(" + strconv.Itoa(int(o)) + ")"
	}
}

// Diagnostic reports something worth saying about one value, without
// interrupting the stream.
//
// This is not an error. An error means the element could not be produced; a
// diagnostic means the element exists and something about it is worth
// reporting. A stream of a thousand orders can carry three thousand
// diagnostics and still deliver every order.
//
// The type is built so it cannot be mistaken for one: it renders through
// [Diagnostic.String], not an Error method, so it does not satisfy the error
// interface and cannot be returned where a failure is expected. A Warning that
// could fire an `if err != nil` would make the distinction decorative.
//
//	d := NewDiagnostic(Error, "Sales.Order.InvalidQty",
//	    Field("lines").Index(7).Field("saleQty"))
//
// # What travels is the key, not the text
//
// MessageID is an i18n key and Args holds its named parameters; neither is a
// rendered sentence. The rendering happens at the boundary that knows the
// reader's language, from a catalogue that is versioned separately. Args is
// named rather than positional because translations reorder — SARIF's {0}/{1}
// is the acknowledged weakness this avoids.
//
// # The cause never leaves
//
// A Diagnostic may wrap the internal error that produced it, reachable through
// [Diagnostic.Unwrap] for logging. The field is unexported, so a serializer
// cannot reach it by reflection over exported fields and cannot leak it by
// accident. That is guarantee S7 made structural rather than advisory: at the
// network boundary the cause is replaced by a correlation identifier that only
// the log resolves.
type Diagnostic struct {
	// Severity is how serious this is, not whether to stop.
	Severity Severity

	// Code is the hierarchical application code: "Sales.Order.InvalidQty". It
	// is the stable identifier a caller matches on.
	Code string

	// RuleID names the rule in the catalogue that produced this, when there is
	// one. It is separate from Code because several rules can raise the same
	// code, and knowing which one fired is what makes a report actionable.
	RuleID string

	// MessageID is the i18n key — "QteMustBeOverZero" — never a rendered
	// sentence.
	MessageID string

	// Args holds the named parameters of MessageID. Named rather than
	// positional: a translation reorders, and {0}/{1} cannot survive that
	// without ambiguity.
	Args map[string]any

	// Path locates the value in the business model.
	Path Path

	// Origin says which layer produced this.
	Origin Origin

	// cause is the internal error, if any. Unexported so that it cannot be
	// serialized by accident — see the type documentation.
	cause error
}

// NewDiagnostic builds a diagnostic for a value at a path.
//
// Severity, code and path are the three things a diagnostic cannot be useful
// without, so they are positional. Everything else is optional and set through
// the With methods, which return a copy:
//
//	NewDiagnostic(Error, "Sales.Order.InvalidQty", path).
//	    WithMessage("QteMustBeOverZero", map[string]any{"min": 1}).
//	    WithOrigin(OriginBusinessRule)
func NewDiagnostic(sev Severity, code string, path Path) Diagnostic {
	return Diagnostic{Severity: sev, Code: code, Path: path}
}

// WithMessage sets the i18n key and its named arguments, returning a copy.
//
// args may be nil for a message that takes no parameter. The map is used as
// given rather than copied: a caller that mutates it afterwards changes the
// diagnostic, which is the ordinary Go contract for a map in a struct.
func (d Diagnostic) WithMessage(messageID string, args map[string]any) Diagnostic {
	d.MessageID, d.Args = messageID, args
	return d
}

// WithOrigin sets the producing layer, returning a copy.
func (d Diagnostic) WithOrigin(o Origin) Diagnostic {
	d.Origin = o
	return d
}

// WithRule sets the catalogue rule that fired, returning a copy.
func (d Diagnostic) WithRule(ruleID string) Diagnostic {
	d.RuleID = ruleID
	return d
}

// WithCause attaches the internal error that produced this diagnostic,
// returning a copy.
//
// The error is reachable through [Diagnostic.Unwrap] and errors.Is, and is
// never exported: it exists for the log, not for the response. Replace it with
// a correlation identifier at the network boundary.
func (d Diagnostic) WithCause(err error) Diagnostic {
	d.cause = err
	return d
}

// Unwrap returns the internal cause, or nil. It makes errors.Is and errors.As
// work through a Diagnostic without exposing the cause to a serializer.
func (d Diagnostic) Unwrap() error { return d.cause }

// String renders the diagnostic for a log line: "Error Sales.Order.InvalidQty
// at lines[7].saleQty".
//
// It is deliberately String and not Error. A Diagnostic with an Error method
// would satisfy the error interface, and a Warning — "unusual price, accepted"
// — could then be returned where a failure is expected, firing every
// `if err != nil` it reaches. A diagnostic is not a processing error, and the
// type must not be able to pretend otherwise: the distinction is only worth
// stating if it is enforced.
//
// The internal cause is still reachable, through [Diagnostic.Unwrap], which
// reads as what it is — asking for the error behind the diagnostic rather than
// treating the diagnostic as one:
//
//	errors.Is(d.Unwrap(), sql.ErrNoRows)
//
// The rendered form omits Args and the cause: it identifies the diagnostic for
// a human scanning logs, it does not serialize it.
func (d Diagnostic) String() string {
	if d.Path.isRoot() {
		return fmt.Sprintf("%s %s", d.Severity, d.Code)
	}
	return fmt.Sprintf("%s %s at %s", d.Severity, d.Code, d.Path)
}
