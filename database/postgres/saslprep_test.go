package postgres

import "testing"

// The examples of RFC 4013 §3, plus the cases where pg_saslprep's callers
// fall back to the raw bytes. Where the RFC answers "Error", the password
// comes back unchanged: that is what libpq sends and what the server stored.
func TestSASLprep(t *testing.T) {
	for _, tt := range []struct {
		name, in, want string
	}{
		{"RFC 4013 #1, soft hyphen mapped to nothing", "I\u00adX", "IX"},
		{"RFC 4013 #2, no transformation", "user", "user"},
		{"RFC 4013 #3, case preserved", "USER", "USER"},
		{"RFC 4013 #4, output is NFKC", "ª", "a"},
		{"RFC 4013 #5, output is NFKC", "Ⅸ", "IX"},
		{"RFC 4013 #6, prohibited character", "\u0007", "\u0007"},
		{"RFC 4013 #7, bidirectional check", "ا1", "ا1"},
		{"fullwidth letters fold", "ｐａｓｓ", "pass"},
		{"non-ASCII space becomes U+0020", "a b", "a b"},
		{"right-to-left throughout", "אב", "אב"},
		{"right-to-left mixed with left-to-right", "אaב", "אaב"},
		{"prohibited after a mapping", "é", "é"},
		{"unassigned in Unicode 3.2", "éȡ", "éȡ"},
		{"maps to nothing at all", "\u00ad\u200c", "\u00ad\u200c"},
		{"U+200B is a space before it is nothing, as in pg_saslprep", "a\u200bb", "a b"},
		{"not UTF-8", "caf\xe9", "caf\xe9"},
		{"empty", "", ""},
	} {
		if got := saslprep(tt.in); got != tt.want {
			t.Errorf("%s: saslprep(%+q) = %+q, want %+q", tt.name, tt.in, got, tt.want)
		}
	}
}

// The derivation runs on the prepared password: two spellings SASLprep
// folds together give the same proof, which is what lets a client that
// typed "Ⅸ" authenticate against a key the server derived from "IX".
func TestSCRAMProofUsesSASLprep(t *testing.T) {
	salt := []byte("0123456789abcdef")
	a, _, err := scramProof("Ⅸ", salt, 4096, "auth", defaultMinSaltLen)
	if err != nil {
		t.Fatal(err)
	}
	b, _, err := scramProof("IX", salt, 4096, "auth", defaultMinSaltLen)
	if err != nil {
		t.Fatal(err)
	}
	if string(a) != string(b) {
		t.Error("scramProof gives different proofs for U+2168 and its NFKC form IX")
	}
}
