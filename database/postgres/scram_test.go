package postgres

import (
	"encoding/base64"
	"errors"
	"strings"
	"testing"
)

// The complete SCRAM-SHA-256 exchange published in RFC 7677 §3, with password
// "pencil". It is the reason this test exists at all.
//
// A round-trip test cannot catch a derivation that is consistently wrong,
// because the same mistake would compute both sides of it. These numbers come
// from the standard rather than from this package, so they disagree with a
// wrong implementation — which is the only property that makes a vector worth
// writing down. Nothing here was derived by eye: it is copied from the RFC,
// and that is where it says it came from.
const (
	rfcPassword        = "pencil"
	rfcClientFirstBare = "n=user,r=rOprNGfwEbeRWgbNEkqO"
	rfcServerFirst     = "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0," +
		"s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096"
	rfcClientFinalBare = "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0"
	rfcSalt            = "W22ZaJ0SNY7soEsUEjb6gQ=="
	rfcIterations      = 4096
	rfcProof           = "dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
	rfcServerSignature = "6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4="
)

func TestSCRAMProofMatchesRFC7677(t *testing.T) {
	salt, err := base64.StdEncoding.DecodeString(rfcSalt)
	if err != nil {
		t.Fatal(err)
	}
	authMessage := rfcClientFirstBare + "," + rfcServerFirst + "," + rfcClientFinalBare

	proof, serverSig, err := scramProof(rfcPassword, salt, rfcIterations, authMessage)
	if err != nil {
		t.Fatalf("scramProof: %v", err)
	}
	if got := base64.StdEncoding.EncodeToString(proof); got != rfcProof {
		t.Errorf("client proof = %s\n            want %s", got, rfcProof)
	}
	if got := base64.StdEncoding.EncodeToString(serverSig); got != rfcServerSignature {
		t.Errorf("server signature = %s\n                want %s", got, rfcServerSignature)
	}
}

// The gs2 header the client echoes back in c= must be the one it sent, and the
// RFC's "biws" is base64("n,,"). If these ever disagree the exchange still
// completes against a permissive server and fails against a strict one, which
// is the worst way to find out.
func TestGS2HeaderIsDeclinedChannelBinding(t *testing.T) {
	if got := base64.StdEncoding.EncodeToString([]byte(gs2Header)); got != "biws" {
		t.Errorf("base64(gs2 header) = %q, want %q", got, "biws")
	}
	if gs2Header[0] != 'n' {
		t.Errorf("gs2 header = %q, want it to start with 'n': 'y' would claim a channel-binding "+
			"capability this client does not have, in the field that detects a stripped mechanism list", gs2Header)
	}
}

// Every check in the server's first message is a downgrade this client
// refuses. They are listed together because each one, alone, looks like
// pedantry — and each one, missing, is an exchange that proves less than it
// appears to.
func TestParseServerFirstRefusals(t *testing.T) {
	const clientNonce = "abcdefgh"
	good := "r=" + clientNonce + "SERVER,s=" + rfcSalt + ",i=4096"

	tests := []struct {
		name string
		msg  string
		want string // a fragment of the reason
	}{
		{"the good case", good, ""},
		{
			"a nonce that replaces the client's rather than extending it",
			"r=somethingelse,s=" + rfcSalt + ",i=4096",
			"does not extend",
		},
		{
			"a nonce echoed back unchanged, which proves the server did nothing",
			"r=" + clientNonce + ",s=" + rfcSalt + ",i=4096",
			"does not extend",
		},
		{
			"an iteration count below what makes the derivation worth running",
			"r=" + clientNonce + "S,s=" + rfcSalt + ",i=1",
			"below the 4096",
		},
		{
			"an iteration count a hostile server would use to stall the client",
			"r=" + clientNonce + "S,s=" + rfcSalt + ",i=100000000",
			"above the 1048576",
		},
		{
			"a mandatory extension this client cannot honour",
			"m=unknown,r=" + clientNonce + "S,s=" + rfcSalt + ",i=4096",
			"requires an extension",
		},
		{"no salt", "r=" + clientNonce + "S,i=4096", "missing r=, s= or i="},
		{
			"a salt too short to be worth deriving against",
			"r=" + clientNonce + "S,s=" + base64.StdEncoding.EncodeToString([]byte("short")) + ",i=4096",
			"below the 16",
		},
		{"no iteration count", "r=" + clientNonce + "S,s=" + rfcSalt, "missing r=, s= or i="},
		{"a salt that is not base64", "r=" + clientNonce + "S,s=!!!,i=4096", "not base64"},
		{"an iteration count that is not a number", "r=" + clientNonce + "S,s=" + rfcSalt + ",i=many", "not a number"},
		{"an attribute with no value", "r,s=" + rfcSalt + ",i=4096", "malformed attribute"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, _, _, err := parseServerFirst(tc.msg, clientNonce)
			switch {
			case tc.want == "" && err != nil:
				t.Fatalf("parseServerFirst = %v, want it accepted", err)
			case tc.want == "":
				return
			case err == nil:
				t.Fatalf("parseServerFirst accepted %q", tc.msg)
			case !strings.Contains(err.Error(), tc.want):
				t.Errorf("error = %v, want it to mention %q", err, tc.want)
			case !errors.Is(err, ErrAuth):
				t.Errorf("error = %v, want it to wrap ErrAuth", err)
			}
		})
	}
}

// An unknown *optional* attribute is skipped, because the RFC reserves them
// for extension and a client that refuses one turns a server upgrade into an
// outage. This is the deliberate other side of the m= refusal above.
func TestParseServerFirstSkipsUnknownOptionalAttributes(t *testing.T) {
	const clientNonce = "abcdefgh"
	msg := "r=" + clientNonce + "S,s=" + rfcSalt + ",i=4096,x=something-new"
	if _, _, _, err := parseServerFirst(msg, clientNonce); err != nil {
		t.Errorf("parseServerFirst = %v, want the unknown attribute skipped", err)
	}
}

func TestParseServerFinal(t *testing.T) {
	sig, err := parseServerFinal("v=" + rfcServerSignature)
	if err != nil {
		t.Fatalf("parseServerFinal: %v", err)
	}
	if got := base64.StdEncoding.EncodeToString(sig); got != rfcServerSignature {
		t.Errorf("signature = %s, want %s", got, rfcServerSignature)
	}

	// The server may answer with a reason instead of a signature, and saying
	// which reason is more useful than the refusal that would follow anyway.
	_, err = parseServerFinal("e=invalid-proof")
	if err == nil || !strings.Contains(err.Error(), "invalid-proof") {
		t.Errorf("parseServerFinal(e=...) = %v, want the server's reason carried through", err)
	}

	// A final message with neither is not a success with a missing field.
	if _, err := parseServerFinal("x=1"); err == nil {
		t.Error("parseServerFinal accepted a final message with no signature")
	}
}

func TestParseMechanisms(t *testing.T) {
	got, err := parseMechanisms([]byte("SCRAM-SHA-256\x00SCRAM-SHA-256-PLUS\x00\x00"))
	if err != nil {
		t.Fatalf("parseMechanisms: %v", err)
	}
	want := []string{mechSCRAMSHA256, mechSCRAMSHA256Plus}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("mechanisms = %v, want %v", got, want)
	}

	if _, err := parseMechanisms([]byte("SCRAM-SHA-256")); err == nil {
		t.Error("parseMechanisms accepted an unterminated list")
	}
}

// A tampered signature must not authenticate, and it must say what it means:
// the far end does not know this role's password.
func TestSCRAMVerifyRejectsAWrongSignature(t *testing.T) {
	s := &scram{sig: []byte("0123456789abcdef0123456789abcdef")}

	wrong := base64.StdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdeg"))
	err := s.verify([]byte("v=" + wrong))
	if err == nil {
		t.Fatal("verify accepted a signature that does not match")
	}
	if !errors.Is(err, ErrAuth) {
		t.Errorf("error = %v, want it to wrap ErrAuth", err)
	}
	if s.verified {
		t.Error("verified is set after a failed verification")
	}
}

// The steps refuse to run out of order. A SASLContinue before any exchange
// started, or a SASLFinal before the challenge was answered, means the client
// is being walked through a conversation it did not begin.
func TestSCRAMStepsRefuseOutOfOrder(t *testing.T) {
	var s scram
	if _, err := s.respond([]byte(rfcServerFirst), rfcPassword); !errors.Is(err, ErrProtocol) {
		t.Errorf("respond before begin = %v, want ErrProtocol", err)
	}
	if err := s.verify([]byte("v=" + rfcServerSignature)); !errors.Is(err, ErrProtocol) {
		t.Errorf("verify before respond = %v, want ErrProtocol", err)
	}
}

func TestSCRAMBeginRefusals(t *testing.T) {
	tests := []struct {
		name       string
		mechanisms string
		password   string
		want       string
	}{
		{
			"a server offering only channel binding, which is not silently downgraded",
			mechSCRAMSHA256Plus + "\x00\x00", rfcPassword, "channel binding",
		},
		{
			"a server offering nothing this package speaks",
			"GSSAPI\x00\x00", rfcPassword, "none of which",
		},
		{
			"no password to run the exchange with",
			mechSCRAMSHA256 + "\x00\x00", "", "Password is empty",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var s scram
			_, _, err := s.begin([]byte(tc.mechanisms), tc.password, nil)
			if err == nil {
				t.Fatal("begin accepted it")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %v, want it to mention %q", err, tc.want)
			}
		})
	}
}

// scramProof reports a derivation it should not run rather than returning a
// proof nobody should trust. The exchange judges the salt before it gets here,
// but the function is callable on its own and its contract is its own.
func TestSCRAMProofRefusesAShortSalt(t *testing.T) {
	_, _, err := scramProof(rfcPassword, []byte("salty"), 4096, "auth")
	if err == nil {
		t.Fatal("scramProof accepted a five-byte salt")
	}
	if !errors.Is(err, ErrAuth) {
		t.Errorf("error = %v, want it to wrap ErrAuth", err)
	}
}

// A final message whose attributes are not attributes is not a success with a
// field missing.
func TestParseServerFinalRefusesMalformedAttributes(t *testing.T) {
	for _, msg := range []string{"v", "v=!!!", "vv=x"} {
		if _, err := parseServerFinal(msg); err == nil {
			t.Errorf("parseServerFinal(%q) accepted it", msg)
		}
	}
}
