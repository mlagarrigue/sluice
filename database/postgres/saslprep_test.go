package postgres

import "testing"

// The examples of RFC 4013 §3, plus the cases where pg_saslprep's callers
// fall back to the raw bytes. Where the RFC answers "Error", the password
// comes back unchanged: that is what libpq sends and what the server stored.
//
// Every case a check of §2.3-§2.5 decides carries a character NFKC changes
// (U+00AA, a fullwidth digit), so that only that check keeps the password as
// it came: without one, the case fails. Examples 6 and 7 of §3 gain one for
// that reason — on their own, ASCII returns before any table, and NFKC
// leaves "ا1" alone.
func TestSASLprep(t *testing.T) {
	for _, tt := range []struct {
		name, in, want string
	}{
		{"RFC 4013 #1, soft hyphen mapped to nothing", "I\u00adX", "IX"},
		{"RFC 4013 #2, no transformation", "user", "user"},
		{"RFC 4013 #3, case preserved", "USER", "USER"},
		{"RFC 4013 #4, output is NFKC", "ª", "a"},
		{"RFC 4013 #5, output is NFKC", "Ⅸ", "IX"},
		{"RFC 4013 #6, prohibited character", "ª\u0007", "ª\u0007"},
		{"RFC 4013 #7, bidirectional check", "ا１", "ا１"},
		{"fullwidth letters fold", "ｐａｓｓ", "pass"},
		{"non-ASCII space becomes U+0020", "a b", "a b"},
		{"right-to-left throughout", "א１ב", "א1ב"},
		{"right-to-left mixed with left-to-right", "אa１ב", "אa１ב"},
		{"prohibited after a mapping", "\u00adª\ue000", "\u00adª\ue000"},
		{"unassigned in Unicode 3.2", "ªȡ", "ªȡ"},
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
