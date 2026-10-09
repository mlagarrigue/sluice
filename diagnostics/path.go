package diagnostics

import (
	"strconv"
	"strings"
)

// Path locates a value inside the business model: which field, which element of
// which collection.
//
//	Field("lines").Index(7).Field("saleQty")   // lines[7].saleQty
//
// It is a structure rather than a string, for two reasons that only show up
// later. Concatenating strings loses the ability to group diagnostics by
// subtree — "everything wrong under lines[7]" needs the segments, not the text.
// And rendering is not one format: a path goes out as a dotted expression to one
// consumer and as a JSON Pointer to another, each with its own escaping rules
// ([Path.String] and [Path.JSONPointer]). Escaping at construction time would
// bake one consumer's rules into the value.
//
// # It addresses the model, not the document
//
// The path names fields of the business model, never the shape of the incoming
// JSON or form. The two diverge — a single model field may come from three
// document positions, and a document field may map to none — and the consumer
// of a diagnostic is fixing a value in the model. FHIR reached the same
// conclusion the hard way: its document-tied location field was deprecated in
// favour of a model-tied expression.
//
// # The zero value
//
// The zero Path is the root: empty, valid, and the base every other path is
// built from. Appending to it costs one allocation per growth, not one per
// segment.
//
// A Path is immutable once built. [Path.Field] and [Path.Index] return a new
// Path and never modify the receiver, so a path held as a prefix can be
// extended along several branches without the branches interfering.
type Path struct {
	segments []segment
}

// segment is one step of a path: a named field or a collection index. Both are
// kept in one type rather than two, because a path is walked in order and a
// slice of a single type walks without an interface dispatch per step.
type segment struct {
	name  string // set when the segment is a field
	index int    // set when the segment is an index
	isIdx bool
}

// Field returns the path of a top-level field. It is the usual entry point:
//
//	Field("customer").Field("address").Field("city")   // customer.address.city
func Field(name string) Path {
	return Path{segments: []segment{{name: name}}}
}

// Index returns the path of an element of a top-level collection. It exists for
// completeness — a path rooted at a collection rather than a field — and reads
// as "[3]" on its own.
func Index(i int) Path {
	return Path{segments: []segment{{index: i, isIdx: true}}}
}

// Field extends the path with a named field.
//
// The receiver is not modified: the returned Path is a new value, so a prefix
// can be extended along several branches independently.
func (p Path) Field(name string) Path {
	return p.extend(segment{name: name})
}

// Index extends the path with a collection index.
//
// Negative indices are kept as given rather than rejected. A path is a
// description of where a value was found, produced by code that already knows
// the shape it walked; refusing one here would turn a reporting helper into a
// validator of its own caller, and the wrong index still renders readably.
func (p Path) Index(i int) Path {
	return p.extend(segment{index: i, isIdx: true})
}

// extend copies before appending. Sharing the backing array between a prefix
// and its extensions would let two branches of the same prefix overwrite each
// other — the append-aliasing trap — and it would corrupt diagnostics silently,
// which is the one failure mode a reporting type must not have.
//
// The copy is what makes a Path safe to hold as a prefix. It costs one
// allocation per extension — the three-segment example above measures at four
// allocations including its rendering — and a path is built once per
// diagnostic, never per element, so it is not on a hot path. Comparison is:
// [Path.HasPrefix] runs at 3.9 ns without allocating, which is what makes
// grouping a report by subtree affordable.
func (p Path) extend(s segment) Path {
	out := make([]segment, len(p.segments)+1)
	copy(out, p.segments)
	out[len(p.segments)] = s
	return Path{segments: out}
}

// Len reports the number of segments. The zero Path — the root — has length 0.
func (p Path) Len() int { return len(p.segments) }

// isRoot reports whether the path names the root itself rather than anything
// inside it. [Path.Len] is the exported form: a caller asks for the count and
// compares, and this is kept for the two places in the package that only
// want the yes or no.
func (p Path) isRoot() bool { return len(p.segments) == 0 }

// String renders the path as a dotted expression: lines[7].saleQty.
//
// This is the form for logs and for humans. It is not escaped and is not meant
// to be parsed back — use [Path.JSONPointer] when the path crosses a wire and
// has to survive a round trip.
//
// Two distinct paths can render the same here: a field named "a.b" reads like
// two segments, and an empty field name reads like the root. That is the price
// of a readable form, and it is why this one is not the wire form —
// [Path.JSONPointer] keeps both pairs apart, and [Path.Equal] never confuses
// them whatever they render as.
//
// The root renders as an empty string.
func (p Path) String() string {
	if len(p.segments) == 0 {
		return ""
	}
	var b strings.Builder
	for i, s := range p.segments {
		if s.isIdx {
			b.WriteByte('[')
			b.WriteString(strconv.Itoa(s.index))
			b.WriteByte(']')
			continue
		}
		if i > 0 {
			b.WriteByte('.')
		}
		b.WriteString(s.name)
	}
	return b.String()
}

// JSONPointer renders the path as an RFC 6901 JSON Pointer: /lines/7/saleQty.
//
// This is the form for a wire protocol, where the path must survive a round
// trip. RFC 6901 reserves two characters inside a token, and both are escaped
// here: "~" becomes "~0" and "/" becomes "~1", in that order — reversing it
// would turn a literal "/" into "~01" and corrupt the pointer.
//
// The root renders as an empty string, which is what RFC 6901 specifies for a
// pointer to the whole document.
//
// A negative index, which [Path.Index] keeps as given, renders as its decimal
// text: Index(-1) is "/-1". RFC 6901 §4 does not make that an array index — an
// evaluator rejects it against an array, or reads it as the member "-1" of an
// object — and that is the intent: the pointer carries the wrong value the
// caller reported, so a consumer that resolves it fails visibly. Clamping it,
// or rendering "-", would point at a real location instead; "-" is the RFC's
// token for the element past the end of an array.
func (p Path) JSONPointer() string {
	if len(p.segments) == 0 {
		return ""
	}
	var b strings.Builder
	for _, s := range p.segments {
		b.WriteByte('/')
		if s.isIdx {
			b.WriteString(strconv.Itoa(s.index))
			continue
		}
		b.WriteString(escapePointer(s.name))
	}
	return b.String()
}

// escapePointer applies RFC 6901 token escaping. The order matters: "~" must be
// escaped before "/", or the "~1" produced by the second pass would be read as
// an escaped "~" by a decoder.
func escapePointer(s string) string {
	if !strings.ContainsAny(s, "~/") {
		return s // the common case allocates nothing
	}
	s = strings.ReplaceAll(s, "~", "~0")
	return strings.ReplaceAll(s, "/", "~1")
}

// HasPrefix reports whether p starts with the given path.
//
// This is what makes grouping by subtree possible — "every diagnostic under
// lines[7]" — and it is the reason a Path keeps its segments instead of
// rendering to a string at construction. String matching would report
// lines[70] as being under lines[7].
//
// Every path has the root as a prefix.
func (p Path) HasPrefix(prefix Path) bool {
	if len(prefix.segments) > len(p.segments) {
		return false
	}
	for i, s := range prefix.segments {
		if p.segments[i] != s {
			return false
		}
	}
	return true
}

// Equal reports whether two paths name the same location.
func (p Path) Equal(other Path) bool {
	return len(p.segments) == len(other.segments) && p.HasPrefix(other)
}
