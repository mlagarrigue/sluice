package audit

import (
	"crypto/ecdsa"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"time"
)

// The certificate a contender serves: the chain it sends in its TLS
// handshake, read from a file the caller saved — what `openssl s_client
// -showcerts` printed, the text around the blocks included, or the PEM file
// the server is configured with. The audit never dials the server: like
// every other source, the reading is a file that can be seeded. The
// versions, suites and resumption it negotiates need a handshake, and are
// not read here.

// TLSChain is the chain as served, the leaf first.
type TLSChain struct {
	Path  string
	Certs []*x509.Certificate
}

// parseChain reads every CERTIFICATE block of b, in order, and skips the
// text between them. A file with none is an error: a chain that read as
// empty would pass every rule.
func parseChain(path string, b []byte) (*TLSChain, error) {
	c := &TLSChain{Path: path}
	for {
		var block *pem.Block
		block, b = pem.Decode(b)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" {
			continue // a key, parameters: not the chain
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("%s: certificate %d: %w", path, len(c.Certs)+1, err)
		}
		c.Certs = append(c.Certs, cert)
	}
	if len(c.Certs) == 0 {
		return nil, errors.New(path + ": no CERTIFICATE block")
	}
	return c, nil
}

// expiryMargin is how long before its end a certificate is reported: a
// renewal that fails gets a month to be noticed and done by hand.
const expiryMargin = 30 * 24 * time.Hour

// init registers the rules on the served chain, in this order: when the
// leaf stops being valid, how strong its key is, whether the chain holds
// together. Each is one call, read on its own.
func init() {
	register := func(name, doc string, check func(*TLSChain, time.Time) []Finding) {
		Register(Rule{
			Name:  "tls/" + name,
			Needs: Selection{Components: {"app"}, Goals: {"security"}, Sources: {"config"}},
			Doc:   doc,
			Check: func(t *Target, _ Answers) []Finding {
				if t.TLSChain == nil {
					return nil
				}
				return check(t.TLSChain, t.Now)
			},
		})
	}

	register("cert-expiry", "https://www.rfc-editor.org/rfc/rfc5280#section-4.1.2.5",
		func(c *TLSChain, now time.Time) []Finding {
			leaf := c.Certs[0]
			switch {
			case now.Before(leaf.NotBefore):
				return []Finding{{
					Current:  fmt.Sprintf("the certificate %q of %s is valid from %s only", leaf.Subject.CommonName, c.Path, leaf.NotBefore.UTC().Format(time.DateOnly)),
					Proposed: "a certificate valid now: the host's clock, or the one it was issued with",
					Why:      "a client refuses a certificate before its validity starts, as it refuses one after it ends",
				}}
			case !now.Before(leaf.NotAfter):
				return []Finding{{
					Current:  fmt.Sprintf("the certificate %q of %s expired on %s", leaf.Subject.CommonName, c.Path, leaf.NotAfter.UTC().Format(time.DateOnly)),
					Proposed: "a new certificate, and its renewal automated",
					Why:      "every client that checks it refuses the handshake: the service is down for them",
				}}
			case leaf.NotAfter.Sub(now) < expiryMargin:
				days := int(leaf.NotAfter.Sub(now).Hours() / 24)
				return []Finding{{
					Current:  fmt.Sprintf("the certificate %q of %s expires on %s, in %d day(s)", leaf.Subject.CommonName, c.Path, leaf.NotAfter.UTC().Format(time.DateOnly), days),
					Proposed: "renewed now, and its renewal automated with a margin of a month or more",
					Why:      "past that date every client that checks it refuses the handshake; a month is the time a failed renewal needs to be noticed and done by hand",
				}}
			}
			return nil
		})

	register("cert-key", "https://csrc.nist.gov/pubs/sp/800/57/pt1/r5/final",
		func(c *TLSChain, _ time.Time) []Finding {
			leaf := c.Certs[0]
			var weak string
			switch k := leaf.PublicKey.(type) {
			case *rsa.PublicKey:
				if n := k.N.BitLen(); n < 2048 {
					weak = fmt.Sprintf("RSA %d", n)
				}
			case *ecdsa.PublicKey:
				if n := k.Curve.Params().BitSize; n < 256 {
					weak = "ECDSA " + k.Curve.Params().Name
				}
			}
			if weak == "" {
				return nil
			}
			return []Finding{{
				Current:  fmt.Sprintf("the certificate %q of %s has a %s key", leaf.Subject.CommonName, c.Path, weak),
				Proposed: "ECDSA P-256, or RSA 2048 at least",
				Why:      "below 112 bits of security a key is no longer approved (NIST SP 800-57), and clients and CAs refuse it",
			}}
		})

	register("cert-chain", "https://www.rfc-editor.org/rfc/rfc8446#section-4.4.2",
		func(c *TLSChain, _ time.Time) []Finding {
			for i := 0; i+1 < len(c.Certs); i++ {
				if err := c.Certs[i].CheckSignatureFrom(c.Certs[i+1]); err != nil {
					return []Finding{{
						Current: fmt.Sprintf("in %s, certificate %d (%q) is not signed by certificate %d (%q): %v",
							c.Path, i+1, c.Certs[i].Subject.CommonName, i+2, c.Certs[i+1].Subject.CommonName, err),
						Proposed: "the chain served in order, each certificate followed by its issuer's",
						Why:      "TLS 1.3 wants each certificate to certify the one before it; a client that cannot build the path refuses the handshake, or fetches what is missing and pays a round trip",
					}}
				}
			}
			return nil
		})
}
