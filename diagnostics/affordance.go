package diagnostics

// Affordance says what can be done about a value: which action, against which
// target, with what shape of input.
//
// It is the bet §4.4 describes, and the one part of this model with no
// precedent to copy. The building blocks exist separately — HAL-FORMS
// describes a property's constraints, Siren describes an action's method and
// fields — and no format joins them to the location a diagnostic points at.
// Here they share a [Path]: the same lines[7].saleQty carries both "this is
// why it is invalid" and "here is how to fix it".
//
// The join is the value. A client holding a diagnostic knows something is
// wrong and must then work out, from documentation or from convention, what
// to offer its user. A client holding a diagnostic and an affordance at the
// same path can render the remedy beside the problem without knowing what
// either means.
//
// # It describes, it does not execute
//
// An Affordance is data. Nothing here calls anything, and that is deliberate:
// the producer of a diagnostic knows what could be done, while whether it may
// be done is an authorisation question answered elsewhere, and how it is done
// belongs to the transport. A type that executed would have to know all three.
//
// # The zero value says nothing, usefully
//
// A zero Affordance is empty and reports [Affordance.IsZero]. That is the
// ordinary case for a diagnostic with no remedy to offer — most of them — so
// carrying one costs nothing and requires no decision.
type Affordance struct {
	// Path is where the affordance applies, and it is the same path the
	// diagnostic uses. That identity is the whole point: matching a remedy to
	// a problem is a comparison, not a lookup table.
	Path Path

	// Action names what can be done, as a stable application code in the
	// hierarchical style [Diagnostic.Code] uses — "Sales.Order.CorrectQty".
	// It is an identifier a client matches on, never a label it shows.
	Action string

	// Method is the verb the transport should use, when the transport has
	// verbs: "PATCH", "POST". Empty means the producer did not say, which is
	// the honest answer from a domain rule that does not know how it will be
	// exposed.
	Method string

	// Target is where the action is sent — a URI template, a queue name, a
	// procedure. Empty for the same reason Method may be empty.
	Target string

	// Inputs describes what the action accepts. Nil means it takes nothing.
	Inputs []Input
}

// Input is one value an [Affordance] accepts.
//
// It carries what a client needs to render an input and refuse an obviously
// bad one before sending it, which is the same information a diagnostic would
// have produced afterwards. The overlap is deliberate: an affordance is the
// diagnostic stated in advance.
type Input struct {
	// Name is the input's identifier in the action's payload.
	Name string

	// Kind is the shape of the value: "string", "number", "boolean", "date".
	// A small vocabulary rather than a type system — a client renders an
	// input from it, and anything richer belongs to the schema the Target
	// serves.
	Kind string

	// Required reports whether the action refuses without it.
	Required bool

	// ReadOnly marks a field a client should show and not offer to change.
	ReadOnly bool

	// Pattern is a regular expression the value must match, or empty. It is
	// carried, never compiled here: compiling it would make this type able to
	// fail, and a description of a constraint is not the place to enforce it.
	Pattern string

	// Value is the current or suggested value, as text. Empty means none is
	// suggested, which is different from suggesting an empty one — a
	// distinction this type cannot express and deliberately does not try to,
	// since an affordance describing a value's absence is describing the
	// schema's business rather than its own.
	Value string
}

// IsZero reports whether the affordance says nothing.
func (a Affordance) IsZero() bool {
	return a.Action == "" && a.Method == "" && a.Target == "" &&
		len(a.Inputs) == 0 && a.Path.isRoot()
}

// WithInput returns a copy of the affordance with one more input.
//
// Copying rather than appending in place, for the reason [Path.Field] copies:
// an affordance held as a template and extended along two branches would
// otherwise have the two branches overwrite each other through a shared
// backing array — the append-aliasing trap, and a description of a remedy is
// exactly the kind of value that gets held as a template.
func (a Affordance) WithInput(in Input) Affordance {
	out := make([]Input, len(a.Inputs)+1)
	copy(out, a.Inputs)
	out[len(a.Inputs)] = in
	a.Inputs = out
	return a
}
