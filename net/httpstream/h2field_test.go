package httpstream

import "testing"

// validFieldValue guards the h2 and h3 inbound paths: RFC 9110 §5.5 keeps
// every C0 control but HTAB, and DEL, out of field content — not only the
// NUL/CR/LF smuggling bytes — and RFC 9113 §8.2.1 adds that a value MUST NOT
// start or end with SP or HTAB.
func TestValidFieldValueOctetRules(t *testing.T) {
	tests := []struct {
		value string
		ok    bool
	}{
		{"plain value", true},
		{"tab\tseparated", true},
		{"high bytes \x80\xff", true}, // obs-text is tolerated, controls are not
		{"", true},
		{"nul\x00", false},
		{"cr\r", false},
		{"lf\n", false},
		{"c0\x01", false},
		{"c0\x08", false},
		{"c0\x0b", false},
		{"c0\x0c", false},
		{"c0\x1f", false},
		{"del\x7f", false},
		{" leading space", false},
		{"trailing space ", false},
		{"\tleading tab", false},
		{"trailing tab\t", false},
		{" ", false},
	}
	for _, tt := range tests {
		if got := validFieldValue([]byte(tt.value)); got != tt.ok {
			t.Errorf("validFieldValue(%q) = %v, want %v", tt.value, got, tt.ok)
		}
		// The outbound twin holds response values to the same rules.
		if got := !hasFieldControl([]byte(tt.value)); got != tt.ok {
			t.Errorf("!hasFieldControl(%q) = %v, want %v", tt.value, got, tt.ok)
		}
	}
}
