package pgtype

import "testing"

// A nil Value is NULL to the parameter that carries it, so an appender of a
// value that may be empty never returns nil: the documented idiom,
// AppendText(nil, ""), sent NULL for the empty string, and WHERE c = $1
// found no row where an empty one existed. Nothing is allocated for it.
func TestAnEmptyValueIsNotNil(t *testing.T) {
	for _, tc := range []struct {
		name string
		got  func() []byte
	}{
		{"AppendText(nil, \"\")", func() []byte { return AppendText(nil, "") }},
		{"AppendBytea(nil, nil)", func() []byte { return AppendBytea(nil, nil) }},
		{"AppendBytea(nil, []byte{})", func() []byte { return AppendBytea(nil, []byte{}) }},
		{"AppendJSON(nil, nil)", func() []byte { return AppendJSON(nil, nil) }},
	} {
		if v := tc.got(); v == nil || len(v) != 0 {
			t.Errorf("%s = %#v, want an empty slice that is not nil", tc.name, v)
		}
		if n := testing.AllocsPerRun(100, func() { _ = tc.got() }); n != 0 {
			t.Errorf("%s allocates %v times", tc.name, n)
		}
	}
}
