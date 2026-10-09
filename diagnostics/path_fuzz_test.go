package diagnostics

import (
	"strings"
	"testing"
)

// unescapePointer reverses RFC 6901 token escaping, for the round-trip property
// below. It lives in the test rather than the package because nothing in the
// library decodes a pointer yet — the wire boundary that will is stage 5.
//
// The order mirrors the encoder's, reversed: "~1" back to "/" before "~0" back
// to "~", or the "~" produced by the second pass would re-trigger the first.
func unescapePointer(s string) string {
	s = strings.ReplaceAll(s, "~1", "/")
	return strings.ReplaceAll(s, "~0", "~")
}

// FuzzPathJSONPointerRoundTrip asserts that escaping is reversible for any
// field name at all.
//
// JSON Pointer is an interchange format, so its encoder is exactly what the
// project's conventions say must be fuzzed: a caller does not choose which
// characters a business field name contains, and an encoder that loses one
// produces a pointer addressing the wrong place — silently, on the wire, which
// is the worst combination available.
//
// The property is stated on the segment rather than on the rendered string,
// because that is what a decoder has to recover: split on "/", unescape each
// token, and the original names must come back byte for byte.
func FuzzPathJSONPointerRoundTrip(f *testing.F) {
	f.Add("saleQty")
	f.Add("")
	f.Add("~")
	f.Add("/")
	f.Add("~0")
	f.Add("~1")
	f.Add("~01")
	f.Add("a/b~c")
	f.Add("//~~")
	f.Add("é€\x00\n")

	f.Fuzz(func(t *testing.T, name string) {
		p := Field(name).Index(7).Field(name)

		ptr := p.JSONPointer()
		if !strings.HasPrefix(ptr, "/") {
			t.Fatalf("JSONPointer(%q) = %q, must start with a slash", name, ptr)
		}

		// RFC 6901: a pointer is a sequence of "/" followed by a token, so
		// splitting on "/" yields an empty first element and one token each.
		tokens := strings.Split(ptr, "/")[1:]
		if len(tokens) != 3 {
			t.Fatalf("JSONPointer(%q) = %q split into %d tokens, want 3: an escaped "+
				"name leaked a separator", name, ptr, len(tokens))
		}

		if got := unescapePointer(tokens[0]); got != name {
			t.Errorf("round trip lost the first segment: got %q, want %q (pointer %q)", got, name, ptr)
		}
		if tokens[1] != "7" {
			t.Errorf("index rendered as %q, want \"7\" (pointer %q)", tokens[1], ptr)
		}
		if got := unescapePointer(tokens[2]); got != name {
			t.Errorf("round trip lost the last segment: got %q, want %q (pointer %q)", got, name, ptr)
		}

		// An escaped token must not contain a raw separator, and must not leave
		// a bare "~": both would make the pointer ambiguous to a decoder.
		for i, tok := range []string{tokens[0], tokens[2]} {
			if strings.Contains(tok, "/") {
				t.Errorf("token %d of %q contains a raw slash: %q", i, ptr, tok)
			}
			for j := 0; j < len(tok); j++ {
				if tok[j] != '~' {
					continue
				}
				if j+1 >= len(tok) || (tok[j+1] != '0' && tok[j+1] != '1') {
					t.Errorf("token %d of %q has a tilde not followed by 0 or 1: %q", i, ptr, tok)
					break
				}
				j++ // the escape consumes two bytes
			}
		}
	})
}

// FuzzPathHasPrefix asserts the property the type exists for: a path is under a
// prefix if and only if it was built by extending it.
//
// The subtree grouping in §4.4 rests on this. A string-matching implementation
// passes the obvious cases and fails on the ones a fuzzer finds first — a field
// literally named "lines[7]" against the path lines[7], for instance.
func FuzzPathHasPrefix(f *testing.F) {
	f.Add("lines", 7, "saleQty")
	f.Add("lines[7]", 0, "")
	f.Add("", 0, "")
	f.Add("a.b", 70, "c")

	f.Fuzz(func(t *testing.T, head string, idx int, tail string) {
		prefix := Field(head).Index(idx)
		extended := prefix.Field(tail)

		if !extended.HasPrefix(prefix) {
			t.Errorf("a path built by extending %q is not under it", prefix)
		}
		if !prefix.HasPrefix(prefix) {
			t.Errorf("%q is not under itself", prefix)
		}
		if !extended.HasPrefix(Path{}) {
			t.Errorf("%q is not under the root", extended)
		}
		// The prefix is shorter, so it can never be under its own extension.
		if prefix.HasPrefix(extended) {
			t.Errorf("%q reported as under its own extension %q", prefix, extended)
		}
		// Extending must not disturb the prefix.
		if prefix.Len() != 2 {
			t.Errorf("prefix length became %d after extension, want 2", prefix.Len())
		}
		// A different index is a different subtree, whatever it renders as.
		if other := Field(head).Index(idx + 1); extended.HasPrefix(other) {
			t.Errorf("%q reported as under %q, a sibling index", extended, other)
		}
	})
}
