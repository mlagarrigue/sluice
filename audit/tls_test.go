package audit

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"slices"
	"strings"
	"testing"
	"time"
)

// now is the instant the tests judge the chains at.
var now = time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)

// issue makes a certificate for cn valid until now + days, signed by
// parent's key (itself when parent is nil), and returns it as PEM with its
// key.
func issue(t *testing.T, cn string, days int, key crypto.Signer, parent *x509.Certificate, parentKey crypto.Signer) (*x509.Certificate, string) {
	t.Helper()
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             now.Add(-24 * time.Hour),
		NotAfter:              now.Add(time.Duration(days) * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  parent == nil,
	}
	if parent == nil {
		parent, parentKey = tmpl, key
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, parent, key.Public(), parentKey)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return cert, string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
}

func p256(t *testing.T) crypto.Signer {
	t.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// tlsFindings audits the chain text, saved to a file, at now.
func tlsFindings(t *testing.T, chain string) map[string][]Finding {
	t.Helper()
	target, err := Open(Spec{TLSChain: seedFile(t, "chain.pem", chain), Now: now})
	if err != nil {
		t.Fatal(err)
	}
	findings, err := Run(Selection{Components: {"app"}, Sources: {"config"}}, nil, target)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string][]Finding{}
	for _, f := range findings {
		if strings.HasPrefix(f.Rule, "tls/") {
			out[f.Rule] = append(out[f.Rule], f)
		}
	}
	return out
}

// TestTheServedCertificateExpiry: seeded leaves at 10 days, at 90 days and
// expired; only the first and the last are reported, each with its date.
func TestTheServedCertificateExpiry(t *testing.T) {
	for _, tc := range []struct {
		days int
		want string
	}{{10, "in 10 day(s)"}, {90, ""}, {-2, "expired on"}} {
		_, leaf := issue(t, "study", tc.days, p256(t), nil, nil)
		got := tlsFindings(t, leaf)["tls/cert-expiry"]
		switch {
		case tc.want == "" && len(got) != 0:
			t.Errorf("%d days: %v, want nothing", tc.days, got)
		case tc.want != "" && (len(got) != 1 || !strings.Contains(got[0].Current, tc.want)):
			t.Errorf("%d days: %v, want one finding saying %q", tc.days, got, tc.want)
		}
	}
}

// TestTheServedCertificateKey: an RSA 1024 key is weak, RSA 2048 and
// ECDSA P-256 are not.
func TestTheServedCertificateKey(t *testing.T) {
	for _, tc := range []struct {
		name string
		key  func() crypto.Signer
		weak bool
	}{
		{"RSA 1024", func() crypto.Signer { k, _ := rsa.GenerateKey(rand.Reader, 1024); return k }, true}, //nolint:gosec // G403: the weak key is the seed
		{"RSA 2048", func() crypto.Signer { k, _ := rsa.GenerateKey(rand.Reader, 2048); return k }, false},
		{"ECDSA P-256", func() crypto.Signer { return p256(t) }, false},
	} {
		key := tc.key()
		if key == nil {
			t.Fatalf("%s: no key", tc.name)
		}
		_, leaf := issue(t, "study", 90, key, nil, nil)
		if got := tlsFindings(t, leaf)["tls/cert-key"]; (len(got) == 1) != tc.weak {
			t.Errorf("%s: %v, weak %v", tc.name, got, tc.weak)
		}
	}
}

// TestTheServedChain: what openssl s_client -showcerts prints — text
// around the blocks — reads as the PEM it holds; a leaf followed by its
// issuer holds together, a leaf followed by another CA does not.
func TestTheServedChain(t *testing.T) {
	caKey, otherKey, leafKey := p256(t), p256(t), p256(t)
	ca, caPEM := issue(t, "study CA", 365, caKey, nil, nil)
	_, otherPEM := issue(t, "other CA", 365, otherKey, nil, nil)
	_, leafPEM := issue(t, "study", 90, leafKey, ca, caKey)
	transcript := "CONNECTED(00000003)\n---\nCertificate chain\n 0 s:CN = study\n   i:CN = study CA\n" +
		leafPEM + " 1 s:CN = study CA\n   i:CN = study CA\n" + caPEM + "---\nServer certificate\nsubject=CN = study\n"
	if got := tlsFindings(t, transcript); len(got) != 0 {
		t.Errorf("a leaf and its issuer, in a transcript: %v, want nothing", got)
	}
	got := tlsFindings(t, leafPEM+otherPEM)["tls/cert-chain"]
	if len(got) != 1 || !strings.Contains(got[0].Current, `certificate 1 ("study") is not signed by certificate 2 ("other CA")`) {
		t.Errorf("a leaf and a CA that did not sign it: %v", got)
	}
	if _, err := Open(Spec{TLSChain: seedFile(t, "empty.pem", "no block here\n")}); err == nil {
		t.Error("a file with no certificate opened")
	}
	names := []string{}
	for _, r := range rules {
		if strings.HasPrefix(r.Name, "tls/") {
			names = append(names, r.Name)
		}
	}
	if !slices.Equal(names, []string{"tls/cert-expiry", "tls/cert-key", "tls/cert-chain"}) {
		t.Errorf("tls rules registered %v", names)
	}
}
