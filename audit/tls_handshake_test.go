package audit

import (
	_ "embed"
	"strings"
	"testing"
)

// sClient is what openssl s_client prints around a handshake, its session
// lines given as "New, <version>, Cipher is <cipher>": the session block
// after each says that version on its Protocol line. OpenSSL's own line
// names the version that defined the cipher instead (see
// TestTheProtocolLineIsTheVersion, on a real transcript).
func sClient(sessions ...string) string {
	var b strings.Builder
	b.WriteString("CONNECTED(00000003)\n---\nCertificate chain\n 0 s:CN = studychain-web\n---\n")
	for _, s := range sessions {
		b.WriteString(s + "\nServer public key is 256 bit\nThis TLS version forbids renegotiation.\n")
		if f := strings.Split(s, ", "); len(f) == 3 && f[1] != "(NONE)" {
			b.WriteString("SSL-Session:\n    Protocol  : " + f[1] + "\n    Cipher    : " + strings.TrimPrefix(f[2], "Cipher is ") + "\n")
		}
		b.WriteString("---\n")
	}
	b.WriteString("DONE\n")
	return b.String()
}

// tls12CBC is a real s_client transcript, its session keys and ticket cut.
//
//go:embed testdata/s_client_tls12_cbc.txt
var tls12CBC string

// The version is the session block's Protocol line. OpenSSL 3.0's s_client
// against s_server -tls1_2 -cipher AES128-SHA printed "New, SSLv3, Cipher
// is AES128-SHA" and "Protocol  : TLSv1.2" (testdata/s_client_tls12_cbc.txt,
// the session's keys and ticket cut): read off the cipher line, that
// session was reported as SSLv3 accepted. A cipher line with no Protocol
// line after it is refused, unless it is TLS 1.3's.
func TestTheProtocolLineIsTheVersion(t *testing.T) {
	got := handshakeFindings(t, "production", tls12CBC)
	if len(got["tls/legacy-version"]) != 0 {
		t.Errorf("a TLS 1.2 session reported as a legacy version: %+v", got["tls/legacy-version"])
	}
	if len(got["tls/weak-cipher"]) != 1 || !strings.Contains(got["tls/weak-cipher"][0].Current, "TLSv1.2") {
		t.Errorf("AES128-SHA under TLS 1.2: %+v, want one weak-cipher finding naming TLSv1.2", got["tls/weak-cipher"])
	}
	bare := "CONNECTED(00000003)\n---\nNew, SSLv3, Cipher is AES128-SHA\n---\n"
	if _, err := Open(Spec{TLSHandshake: seedFile(t, "bare.txt", bare)}); err == nil {
		t.Error("a cipher line with no Protocol line was read")
	}
	v13 := "CONNECTED(00000003)\n---\nNew, TLSv1.3, Cipher is TLS_AES_128_GCM_SHA256\n---\n"
	if _, err := Open(Spec{TLSHandshake: seedFile(t, "v13.txt", v13)}); err != nil {
		t.Errorf("a TLS 1.3 cipher line alone: %v", err)
	}
}

func handshakeFindings(t *testing.T, profile string, transcripts ...string) map[string][]Finding {
	t.Helper()
	var paths []string
	for i, tr := range transcripts {
		paths = append(paths, seedFile(t, "s_client"+strings.Repeat("x", i)+".txt", tr))
	}
	target, err := Open(Spec{TLSHandshake: strings.Join(paths, ",")})
	if err != nil {
		t.Fatal(err)
	}
	findings, err := Run(Selection{Components: {"app"}, Sources: {"config"}, Profile: {profile}}, nil, target)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string][]Finding{}
	for i := range findings {
		f := &findings[i]
		out[f.Rule] = append(out[f.Rule], *f)
	}
	return out
}

func TestNegotiatedVersions(t *testing.T) {
	v13 := sClient("New, TLSv1.3, Cipher is TLS_AES_128_GCM_SHA256")
	v12 := sClient("New, TLSv1.2, Cipher is ECDHE-ECDSA-AES128-GCM-SHA256")
	v11 := sClient("New, TLSv1.1, Cipher is ECDHE-ECDSA-AES128-SHA")
	refused := sClient("New, (NONE), Cipher is (NONE)")

	if got := handshakeFindings(t, "comparative", v13, v12); len(got["tls/version-in-comparison"]) != 1 {
		t.Errorf("TLS 1.2 accepted in a comparison: %+v", got)
	}
	if got := handshakeFindings(t, "production", v13, v12); len(got) != 0 {
		t.Errorf("TLS 1.2 with an AEAD suite in production: %+v", got)
	}
	got := handshakeFindings(t, "production", v11)
	if len(got["tls/legacy-version"]) != 1 || !strings.Contains(got["tls/legacy-version"][0].Current, "TLSv1.1") {
		t.Errorf("TLS 1.1 accepted: %+v", got)
	}
	if got := handshakeFindings(t, "comparative", v13, refused); len(got) != 0 {
		t.Errorf("TLS 1.2 refused: %+v", got)
	}
}

func TestNegotiatedCiphers(t *testing.T) {
	for cipher, weak := range map[string]bool{
		"ECDHE-RSA-AES128-SHA":          true, // CBC
		"AES128-GCM-SHA256":             true, // static RSA
		"ECDHE-RSA-DES-CBC3-SHA":        true,
		"ADH-AES128-GCM-SHA256":         true,
		"ECDHE-RSA-AES128-GCM-SHA256":   false,
		"ECDHE-ECDSA-CHACHA20-POLY1305": false,
		"TLS_CHACHA20_POLY1305_SHA256":  false,
	} {
		got := handshakeFindings(t, "production", sClient("New, TLSv1.2, Cipher is "+cipher))
		if (len(got["tls/weak-cipher"]) == 1) != weak {
			t.Errorf("%s: %+v, weak %t", cipher, got["tls/weak-cipher"], weak)
		}
	}
}

func TestSessionResumption(t *testing.T) {
	const suite = ", TLSv1.3, Cipher is TLS_AES_256_GCM_SHA384"
	off := sClient("New"+suite, "New"+suite, "New"+suite)
	on := sClient("New"+suite, "Reused"+suite, "Reused"+suite)
	if got := handshakeFindings(t, "production", off); len(got["tls/resumption-off"]) != 1 {
		t.Errorf("-reconnect, every connection new, in production: %+v", got)
	}
	if got := handshakeFindings(t, "comparative", off); len(got) != 0 {
		t.Errorf("no resumption in a comparison: %+v", got)
	}
	if got := handshakeFindings(t, "comparative", on); len(got["tls/resumption-in-comparison"]) != 1 {
		t.Errorf("resumed in a comparison: %+v", got)
	}
	if got := handshakeFindings(t, "production", on); len(got) != 0 {
		t.Errorf("resumed in production: %+v", got)
	}
	if got := handshakeFindings(t, "production", sClient("New"+suite)); len(got) != 0 {
		t.Errorf("one connection says nothing of the next: %+v", got)
	}
}

func TestHandshakeTranscriptRefused(t *testing.T) {
	if _, err := Open(Spec{TLSHandshake: seedFile(t, "cut.txt", "CONNECTED(00000003)\n")}); err == nil {
		t.Error("a transcript cut before its handshake was read")
	}
}
