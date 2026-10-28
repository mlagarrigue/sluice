package reqbody

import "testing"

// RFC 9110 §8.3.1: "media-type = type "/" subtype parameters", type and
// subtype each a token. A Content-Type is one media type, not a list: a
// value carrying a comma passed the JSON gate whenever its tail ended in a
// +json subtype, while a hop reading the first member read the body as
// whatever that member named — two hops reading one request differently.
func TestJSONMediaTypeTakesOneMediaType(t *testing.T) {
	for _, tc := range []struct {
		value string
		json  bool
	}{
		{"application/json", true},
		{"application/merge-patch+json", true},
		{"APPLICATION/JSON; charset=utf-8", true},
		{" application/problem+json ;charset=utf-8", true},
		{"text/plain, a/b+json", false},
		{"a/b+json, text/plain", false},
		{"application/x-www-form-urlencoded, a/b+json", false},
		{"application/+json", false},
		{"/x+json", false},
		{"application/json/x+json", false},
		{"application /json", false},
		{"text/plain", false},
		{"", false},
	} {
		if got := JSONMediaType(tc.value); got != tc.json {
			t.Errorf("JSONMediaType(%q) = %v, want %v", tc.value, got, tc.json)
		}
		if got := JSONMediaType([]byte(tc.value)); got != tc.json {
			t.Errorf("JSONMediaType([]byte(%q)) = %v, want %v", tc.value, got, tc.json)
		}
	}
	// It runs on every request that carries a body.
	v := []byte("application/merge-patch+json; charset=utf-8")
	if n := testing.AllocsPerRun(100, func() { _ = JSONMediaType(v) }); n != 0 {
		t.Errorf("JSONMediaType allocates %v times per call, want none", n)
	}
}
