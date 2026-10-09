package diagnostics

import "testing"

func TestPathString(t *testing.T) {
	tests := []struct {
		name string
		path Path
		want string
	}{
		{"root", Path{}, ""},
		{"one field", Field("customer"), "customer"},
		{"nested fields", Field("customer").Field("address").Field("city"), "customer.address.city"},
		{"field then index", Field("lines").Index(7), "lines[7]"},
		{"the §4.1 example", Field("lines").Index(7).Field("saleQty"), "lines[7].saleQty"},
		{"index at the root", Index(3), "[3]"},
		{"consecutive indices", Field("grid").Index(2).Index(5), "grid[2][5]"},
		{"index then field", Index(0).Field("id"), "[0].id"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.path.String(); got != tc.want {
				t.Errorf("String = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestPathJSONPointer(t *testing.T) {
	tests := []struct {
		name string
		path Path
		want string
	}{
		{"root is the whole document", Path{}, ""},
		{"one field", Field("customer"), "/customer"},
		{"the §4.1 example", Field("lines").Index(7).Field("saleQty"), "/lines/7/saleQty"},
		{"index at the root", Index(3), "/3"},
		// RFC 6901 reserves these two inside a token.
		{"tilde is escaped", Field("a~b"), "/a~0b"},
		{"slash is escaped", Field("a/b"), "/a~1b"},
		// Order matters: escaping "/" first would render this as "~01".
		{"both, tilde first", Field("~/"), "/~0~1"},
		{"a literal ~1 must survive", Field("~1"), "/~01"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.path.JSONPointer(); got != tc.want {
				t.Errorf("JSONPointer = %q, want %q", got, tc.want)
			}
		})
	}
}

// A Path held as a prefix must survive being extended twice. Sharing the
// backing array would make the second branch overwrite the first, and the
// corruption would be silent — the failure mode a diagnostic type cannot have.
func TestPathPrefixIsNotAliased(t *testing.T) {
	prefix := Field("order").Field("lines")

	a := prefix.Index(1)
	b := prefix.Index(2)

	if got, want := a.String(), "order.lines[1]"; got != want {
		t.Errorf("first branch = %q, want %q: the second extension overwrote it", got, want)
	}
	if got, want := b.String(), "order.lines[2]"; got != want {
		t.Errorf("second branch = %q, want %q", got, want)
	}
	if got, want := prefix.String(), "order.lines"; got != want {
		t.Errorf("prefix = %q, want %q: extending must not modify the receiver", got, want)
	}
}

func TestPathHasPrefix(t *testing.T) {
	line7 := Field("lines").Index(7)

	tests := []struct {
		name   string
		path   Path
		prefix Path
		want   bool
	}{
		{"under the subtree", Field("lines").Index(7).Field("saleQty"), line7, true},
		{"the subtree itself", line7, line7, true},
		{"a sibling index", Field("lines").Index(8), line7, false},
		// The reason Path keeps segments: string matching would say true here.
		{"lines[70] is not under lines[7]", Field("lines").Index(70), line7, false},
		{"a different field", Field("header").Index(7), line7, false},
		{"shorter than the prefix", Field("lines"), line7, false},
		{"everything is under the root", Field("anything"), Path{}, true},
		{"the root is under the root", Path{}, Path{}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.path.HasPrefix(tc.prefix); got != tc.want {
				t.Errorf("HasPrefix = %v, want %v", got, tc.want)
			}
		})
	}
}

// A field named "7" and the index 7 render the same in a JSON Pointer, but they
// are different locations and must not compare equal.
func TestPathFieldAndIndexAreDistinct(t *testing.T) {
	byField := Field("lines").Field("7")
	byIndex := Field("lines").Index(7)

	if byField.Equal(byIndex) {
		t.Error("a field named \"7\" must not equal the index 7")
	}
	if got, want := byField.String(), "lines.7"; got != want {
		t.Errorf("field form = %q, want %q", got, want)
	}
	if got, want := byIndex.String(), "lines[7]"; got != want {
		t.Errorf("index form = %q, want %q", got, want)
	}
}

func TestPathEqual(t *testing.T) {
	tests := []struct {
		name string
		a, b Path
		want bool
	}{
		{"same path", Field("a").Index(1), Field("a").Index(1), true},
		{"two roots", Path{}, Path{}, true},
		{"different length", Field("a"), Field("a").Index(1), false},
		{"different index", Field("a").Index(1), Field("a").Index(2), false},
		{"different field", Field("a"), Field("b"), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.a.Equal(tc.b); got != tc.want {
				t.Errorf("Equal = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestPathLenAndIsRoot(t *testing.T) {
	if p := (Path{}); p.Len() != 0 || !p.isRoot() {
		t.Errorf("zero Path: Len = %d, isRoot = %v, want 0 and true", p.Len(), p.isRoot())
	}
	if p := Field("a").Index(1).Field("b"); p.Len() != 3 || p.isRoot() {
		t.Errorf("three segments: Len = %d, isRoot = %v, want 3 and false", p.Len(), p.isRoot())
	}
}

// Negative indices are kept rather than rejected: a path describes where a
// value was found, and refusing one would make a reporting helper validate its
// own caller.
func TestPathNegativeIndex(t *testing.T) {
	if got, want := Field("lines").Index(-1).String(), "lines[-1]"; got != want {
		t.Errorf("String = %q, want %q", got, want)
	}
	// The pointer keeps the decimal text too: never clamped to an element, and
	// never "-", which RFC 6901 §4 reserves for the element past the end.
	tests := []struct {
		p    Path
		want string
	}{
		{Index(-1), "/-1"},
		{Field("lines").Index(-1).Field("saleQty"), "/lines/-1/saleQty"},
		{Field("lines").Index(-42), "/lines/-42"},
	}
	for _, tt := range tests {
		if got := tt.p.JSONPointer(); got != tt.want {
			t.Errorf("%s: JSONPointer = %q, want %q", tt.p, got, tt.want)
		}
	}
}

// String is for humans and is not injective: an empty field name renders like
// the root, and a field containing a dot renders like two segments. That is
// acceptable because String is documented as unparseable — but JSONPointer is
// the form that crosses a wire, and it must keep the two apart.
func TestPathStringIsLossyJSONPointerIsNot(t *testing.T) {
	tests := []struct {
		name         string
		a, b         Path
		sameString   bool
		wantPointerA string
		wantPointerB string
	}{
		{
			name:         "a dotted field name against two segments",
			a:            Field("a.b"),
			b:            Field("a").Field("b"),
			sameString:   true,
			wantPointerA: "/a.b",
			wantPointerB: "/a/b",
		},
		{
			name:         "an empty field name against the root",
			a:            Field(""),
			b:            Path{},
			sameString:   true,
			wantPointerA: "/",
			wantPointerB: "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if (tc.a.String() == tc.b.String()) != tc.sameString {
				t.Errorf("String: %q vs %q, expected same=%v", tc.a, tc.b, tc.sameString)
			}
			if got := tc.a.JSONPointer(); got != tc.wantPointerA {
				t.Errorf("JSONPointer(a) = %q, want %q", got, tc.wantPointerA)
			}
			if got := tc.b.JSONPointer(); got != tc.wantPointerB {
				t.Errorf("JSONPointer(b) = %q, want %q", got, tc.wantPointerB)
			}
			// The distinction that matters: the two are not the same location.
			if tc.a.Equal(tc.b) {
				t.Error("two distinct paths must not compare equal, whatever they render as")
			}
		})
	}
}
