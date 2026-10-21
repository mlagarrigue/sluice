package quic

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"strings"
	"testing"
	"time"
)

// mintCert signs tmpl with the issuer given (itself when parent is nil) and
// returns the DER and the leaf's key.
func mintCert(t *testing.T, tmpl, parent *x509.Certificate, parentKey *ecdsa.PrivateKey) ([]byte, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer := parentKey
	if parent == nil {
		parent, signer = tmpl, key
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, parent, &key.PublicKey, signer)
	if err != nil {
		t.Fatal(err)
	}
	return der, key
}

func serverTemplate(name string, usage x509.KeyUsage) *x509.Certificate {
	return &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     usage,
	}
}

func serverConfig(der []byte, key *ecdsa.PrivateKey, chain ...[]byte) *tls.Config {
	return &tls.Config{
		Certificates: []tls.Certificate{{Certificate: append([][]byte{der}, chain...), PrivateKey: key}},
		MinVersion:   tls.VersionTLS13,
		NextProtos:   []string{"h3"},
	}
}

// RFC 8446 §4.4.2.2: a leaf with a Key Usage extension must carry
// digitalSignature; §4.2.3: a TLS 1.3 server does not offer a SHA-1 signed
// certificate. Both are refused where the operator configured them,
// NewListener and Accept, with the section named.
func TestServerRefusesAChainTLS13Forbids(t *testing.T) {
	pcA, _ := newLoopbackPair(t)
	refused := func(t *testing.T, cfg *tls.Config, want string) {
		t.Helper()
		l, err := NewListener(pcA, cfg, DefaultParameters(), ListenerConfig{})
		if err == nil {
			_ = l.Close()
			t.Fatalf("NewListener accepted the chain; want a refusal naming %s", want)
		}
		if !strings.Contains(err.Error(), want) {
			t.Errorf("NewListener: %v, want %s named", err, want)
		}
		if _, err := Accept(pcA, Datagram{}, cfg, DefaultParameters()); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("Accept: %v, want the same refusal", err)
		}
	}

	der, key := mintCert(t, serverTemplate("no-ds", x509.KeyUsageKeyEncipherment), nil, nil)
	refused(t, serverConfig(der, key), "§4.4.2.2")

	caTmpl := serverTemplate("ca", x509.KeyUsageCertSign|x509.KeyUsageDigitalSignature)
	caTmpl.IsCA, caTmpl.BasicConstraintsValid = true, true
	caDER, caKey := mintCert(t, caTmpl, nil, nil)
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	sha1Tmpl := serverTemplate("sha1-leaf", x509.KeyUsageDigitalSignature)
	sha1Tmpl.SignatureAlgorithm = x509.ECDSAWithSHA1
	sha1DER, sha1Key := mintCert(t, sha1Tmpl, ca, caKey)
	refused(t, serverConfig(sha1DER, sha1Key, caDER), "§4.2.3")

	// Accepted: digitalSignature set, SHA-256 throughout; and a Key Usage
	// extension absent altogether.
	for name, usage := range map[string]x509.KeyUsage{"digitalSignature": x509.KeyUsageDigitalSignature, "no key usage": 0} {
		der, key := mintCert(t, serverTemplate(name, usage), ca, caKey)
		l, err := NewListener(pcA, serverConfig(der, key, caDER), DefaultParameters(), ListenerConfig{})
		if err != nil {
			t.Fatalf("%s: NewListener refused a chain TLS 1.3 allows: %v", name, err)
		}
		_ = l.Close()
	}
}

// The self-signed anchor is exempt from the SHA-1 rule: no client verifies
// its signature. Checked on the rule alone, since a chain is sent without
// its anchor in the ordinary case.
func TestSHA1SelfSignedAnchorIsNotRefused(t *testing.T) {
	tmpl := serverTemplate("anchor", x509.KeyUsageCertSign|x509.KeyUsageDigitalSignature)
	tmpl.SignatureAlgorithm = x509.ECDSAWithSHA1
	der, _ := mintCert(t, tmpl, nil, nil)
	c, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkServedCertificate(c, false); err != nil {
		t.Errorf("a self-signed SHA-1 anchor was refused: %v", err)
	}
}
