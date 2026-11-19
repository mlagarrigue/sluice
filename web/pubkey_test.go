package web

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math/big"
	"strings"
	"sync"
	"testing"
	"time"
)

// The examples of RFC 7515 Appendix A.2 (RS256) and A.3 (ES256), copied from
// the text: the public key of each, and the complete JWS it validates. Their
// payload is {"iss":"joe","exp":1300819380,"http://example.com/is_root":true},
// so the clock is set just before that expiry.
const (
	rfc7515RSAModulus = "ofgWCuLjybRlzo0tZWJjNiuSfb4p4fAkd_wWJcyQoTbji9k0l8W26mPddxHmfHQp-Vaw-4qPCJrcS2mJPMEzP1Pt0Bm4d4QlL-yRT-SFd2lZS-pCgNMsD1W_YpRPEwOWvG6b32690r2jZ47soMZo9wGzjb_7OMg0LOL-bSf63kpaSHSXndS5z5rexMdbBYUsLA9e-KXBdQOS-UTo7WTBEMa2R2CapHg665xsmtdVMTBQY4uDZlxvb3qCo5ZwKh9kG4LT6_I5IhlJH7aGhyxXFvUK-DWNmoudF8NAco9_h9iaGNj8q2ethFkMLs91kzk2PAcDTW9gb54h4FRWyuXpoQ"
	rfc7515RS256      = "eyJhbGciOiJSUzI1NiJ9.eyJpc3MiOiJqb2UiLA0KICJleHAiOjEzMDA4MTkzODAsDQogImh0dHA6Ly9leGFtcGxlLmNvbS9pc19yb290Ijp0cnVlfQ.cC4hiUPoj9Eetdgtv3hF80EGrhuB__dzERat0XF9g2VtQgr9PJbu3XOiZj5RZmh7AAuHIm4Bh-0Qc_lF5YKt_O8W2Fp5jujGbds9uJdbF9CUAr7t1dnZcAcQjbKBYNX4BAynRFdiuB--f_nZLgrnbyTyWzO75vRK5h6xBArLIARNPvkSjtQBMHlb1L07Qe7K0GarZRmB_eSN9383LcOLn6_dO--xi12jzDwusC-eOkHWEsqtFZESc6BfI7noOPqvhJ1phCnvWh6IeYI2w9QOYEUipUTI8np6LbgGY9Fs98rqVt5AXLIhWkWywlVmtVrBp0igcN_IoypGlUPQGe77Rw"
	rfc7515ECX        = "f83OJ3D2xF1Bg8vub9tLe1gHMzV76e8Tus9uPHvRVEU"
	rfc7515ECY        = "x_FEzRu9m36HLN_tue659LNpXW6pCyStikYjKIWI5a0"
	rfc7515ES256      = "eyJhbGciOiJFUzI1NiJ9.eyJpc3MiOiJqb2UiLA0KICJleHAiOjEzMDA4MTkzODAsDQogImh0dHA6Ly9leGFtcGxlLmNvbS9pc19yb290Ijp0cnVlfQ.DtEhU3ljbEg8L38VWAfUAqOyKAM6-Xx-F4GawxaepmXFCgfTjDxw5djxLa8ISlSApmWQxfKTUJqPP3-Kg6NU1Q"

	// RFC 8037 Appendix A.2 and A.4: an Ed25519 public key and a JWS whose
	// payload is the text "Example of Ed25519 signing", not a claims set.
	rfc8037X     = "11qYAYKxCrfVS_7TyWQHOg7hcvPapiMlrwIaaPcHURo"
	rfc8037EdDSA = "eyJhbGciOiJFZERTQSJ9.RXhhbXBsZSBvZiBFZDI1NTE5IHNpZ25pbmc.hgyY0il_MGCjP0JzlnLWG1PPOt7-09PGcvMg3AIbQR6dWbhijcNR4ki4iylGjg5BhVsPt9g7sVvpAr_MuM0KAg"
)

func b64int(t *testing.T, s string) *big.Int {
	t.Helper()
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return new(big.Int).SetBytes(b)
}

func beforeRFCExpiry() time.Time { return time.Unix(1300819380-60, 0) }

func TestPublicKeyVerifierAcceptsTheRFC7515Examples(t *testing.T) {
	rsaKey := &rsa.PublicKey{N: b64int(t, rfc7515RSAModulus), E: 65537}
	ecKey := &ecdsa.PublicKey{Curve: elliptic.P256(), X: b64int(t, rfc7515ECX), Y: b64int(t, rfc7515ECY)}
	for _, tc := range []struct {
		alg, token string
		key        crypto.PublicKey
	}{
		{"RS256", rfc7515RS256, rsaKey},
		{"ES256", rfc7515ES256, ecKey},
	} {
		v, err := NewPublicKeyVerifier(PublicKeyConfig{
			Keys:   map[string]PublicKey{"only": {Key: tc.key, Algorithm: tc.alg}},
			Issuer: "joe", Now: beforeRFCExpiry,
		})
		if err != nil {
			t.Fatal(err)
		}
		p, err := v.Verify(tc.token)
		if err != nil {
			t.Fatalf("%s: the RFC's own example is refused: %v", tc.alg, err)
		}
		if root, _ := p.Claims["http://example.com/is_root"].(bool); !root {
			t.Errorf("%s: claims %v", tc.alg, p.Claims)
		}
		// One byte of the signature changed is a signature that fails.
		forged := []byte(tc.token)
		forged[len(forged)-2] ^= 'A' ^ 'B'
		if _, err := v.Verify(string(forged)); !errors.Is(err, ErrTokenSignature) && !errors.Is(err, ErrTokenMalformed) {
			t.Errorf("%s: a changed signature: %v", tc.alg, err)
		}
	}
}

// The RFC 8037 example's signature verifies, and its payload, a sentence
// rather than a claims set, is then refused as not JSON: the refusal that
// comes after the signature, which proves the signature passed. Its alg is
// EdDSA, and a key bound to RFC 9864's Ed25519 verifies it as well.
func TestPublicKeyVerifierVerifiesTheRFC8037Signature(t *testing.T) {
	x, err := base64.RawURLEncoding.DecodeString(rfc8037X)
	if err != nil {
		t.Fatal(err)
	}
	for _, bound := range []string{"EdDSA", "Ed25519"} {
		v, err := NewPublicKeyVerifier(PublicKeyConfig{Keys: map[string]PublicKey{"k": {Key: ed25519.PublicKey(x), Algorithm: bound}}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := v.Verify(rfc8037EdDSA); !errors.Is(err, ErrTokenMalformed) || !strings.Contains(err.Error(), "not JSON") {
			t.Errorf("the RFC 8037 example, key bound to %s: %v, want its payload refused as not JSON", bound, err)
		}
		forged := strings.Replace(rfc8037EdDSA, ".hgyY", ".hgyZ", 1)
		if _, err := v.Verify(forged); !errors.Is(err, ErrTokenSignature) {
			t.Errorf("a changed Ed25519 signature, key bound to %s: %v", bound, err)
		}
	}
}

// testKeys are one private key per algorithm — an RSA key verifies one
// algorithm only, so each RS and PS has its own — generated once for the
// package's tests: RSA generation is slow.
type testKeys struct {
	rsa map[string]*rsa.PrivateKey
	ec  map[string]*ecdsa.PrivateKey
	ed  ed25519.PrivateKey
}

var sharedTestKeys = sync.OnceValue(func() testKeys {
	k := testKeys{rsa: map[string]*rsa.PrivateKey{}, ec: map[string]*ecdsa.PrivateKey{}}
	for _, alg := range []string{"RS256", "RS384", "RS512", "PS256", "PS384", "PS512"} {
		priv, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			panic(err)
		}
		k.rsa[alg] = priv
	}
	for alg, c := range map[string]elliptic.Curve{"ES256": elliptic.P256(), "ES384": elliptic.P384(), "ES512": elliptic.P521()} {
		priv, err := ecdsa.GenerateKey(c, rand.Reader)
		if err != nil {
			panic(err)
		}
		k.ec[alg] = priv
	}
	_, ed, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		panic(err)
	}
	k.ed = ed
	return k
})

// public is the configuration entry for the key that signs alg, under the
// key ID alg names.
func (k testKeys) public(alg string) PublicKey {
	switch {
	case alg == "EdDSA" || alg == "Ed25519":
		return PublicKey{Key: k.ed.Public(), Algorithm: alg}
	case alg[:2] == "ES":
		return PublicKey{Key: &k.ec[alg].PublicKey, Algorithm: alg}
	}
	return PublicKey{Key: &k.rsa[alg].PublicKey, Algorithm: alg}
}

// keys is the configuration for the algorithms given, each key under its
// algorithm's name as kid.
func (k testKeys) keys(algs ...string) map[string]PublicKey {
	m := make(map[string]PublicKey, len(algs))
	for _, alg := range algs {
		m[alg] = k.public(alg)
	}
	return m
}

// sign builds a token the way an issuer does, with alg's own key.
func (k testKeys) sign(t *testing.T, alg, kid string, claims map[string]any) string {
	t.Helper()
	return k.signBy(t, alg, alg, kid, claims)
}

// signBy signs an alg token with the key that belongs to signer's
// algorithm: the way to make a token whose key and alg disagree.
func (k testKeys) signBy(t *testing.T, alg, signer, kid string, claims map[string]any) string {
	t.Helper()
	hdr := map[string]string{"alg": alg, "typ": "JWT"}
	if kid != "" {
		hdr["kid"] = kid
	}
	h, _ := json.Marshal(hdr)
	c, _ := json.Marshal(claims)
	signed := base64.RawURLEncoding.EncodeToString(h) + "." + base64.RawURLEncoding.EncodeToString(c)
	var sig []byte
	var err error
	switch {
	case alg == "EdDSA" || alg == "Ed25519":
		sig = ed25519.Sign(k.ed, []byte(signed))
	case alg[:2] == "RS":
		sig, err = rsa.SignPKCS1v15(rand.Reader, k.rsa[signer], hashOf(alg), digestOf(hashOf(alg), signed))
	case alg[:2] == "PS":
		sig, err = rsa.SignPSS(rand.Reader, k.rsa[signer], hashOf(alg), digestOf(hashOf(alg), signed), &rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthEqualsHash})
	case alg[:2] == "ES":
		priv := k.ec[signer]
		r, s, serr := ecdsa.Sign(rand.Reader, priv, digestOf(hashOf(alg), signed))
		err = serr
		size := (priv.Curve.Params().BitSize + 7) / 8
		sig = make([]byte, 2*size)
		r.FillBytes(sig[:size])
		s.FillBytes(sig[size:])
	}
	if err != nil {
		t.Fatal(err)
	}
	return signed + "." + base64.RawURLEncoding.EncodeToString(sig)
}

var allPublicKeyAlgs = []string{"RS256", "RS384", "RS512", "PS256", "PS384", "PS512", "ES256", "ES384", "ES512", "EdDSA", "Ed25519"}

func TestPublicKeyVerifierAcceptsEveryAlgorithm(t *testing.T) {
	k := sharedTestKeys()
	now := time.Unix(1_800_000_000, 0)
	v, err := NewPublicKeyVerifier(PublicKeyConfig{
		Keys: k.keys(allPublicKeyAlgs...), Audience: "api", Issuer: "idp",
		Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	claims := map[string]any{"sub": "u1", "iss": "idp", "aud": "api", "exp": now.Unix() + 60}
	for _, alg := range allPublicKeyAlgs {
		p, err := v.Verify(k.sign(t, alg, alg, claims))
		if err != nil || p.Subject != "u1" {
			t.Errorf("%s: %+v, %v", alg, p, err)
		}
	}
	// The claim checks are HMACVerifier's: an expired token is refused.
	expired := map[string]any{"sub": "u1", "iss": "idp", "aud": "api", "exp": now.Unix() - 1}
	if _, err := v.Verify(k.sign(t, "ES256", "ES256", expired)); !errors.Is(err, ErrTokenExpired) {
		t.Errorf("an expired token: %v", err)
	}
}

// RFC 8725 §3.1: each key is used with exactly one algorithm, checked when
// the signature is verified. A verifier that accepts RS256 and PS256 still
// refuses a PS256 token under the RS256 key — a valid PS256 signature by
// that very key — and refuses at construction one key bound to both.
func TestPublicKeyVerifierBindsEachKeyToOneAlgorithm(t *testing.T) {
	k := sharedTestKeys()
	now := time.Unix(1_800_000_000, 0)
	v, err := NewPublicKeyVerifier(PublicKeyConfig{Keys: k.keys("RS256", "PS256"), Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	claims := map[string]any{"sub": "u1", "exp": now.Unix() + 60}
	for _, alg := range []string{"RS256", "PS256"} {
		if _, err := v.Verify(k.sign(t, alg, alg, claims)); err != nil {
			t.Errorf("%s under its own key: %v", alg, err)
		}
	}
	if _, err := v.Verify(k.signBy(t, "PS256", "RS256", "RS256", claims)); !errors.Is(err, ErrTokenSignature) {
		t.Errorf("a PS256 token under the RS256 key: %v, want ErrTokenSignature", err)
	}
	if _, err := v.Verify(k.signBy(t, "RS256", "PS256", "PS256", claims)); !errors.Is(err, ErrTokenSignature) {
		t.Errorf("an RS256 token under the PS256 key: %v, want ErrTokenSignature", err)
	}
	one := &k.rsa["RS256"].PublicKey
	if _, err := NewPublicKeyVerifier(PublicKeyConfig{Keys: map[string]PublicKey{
		"a": {Key: one, Algorithm: "RS256"}, "b": {Key: one, Algorithm: "PS256"},
	}}); err == nil {
		t.Error("one key under two IDs with two algorithms: accepted")
	}
	// Two IDs for one key and one algorithm are an alias, not a second use.
	if _, err := NewPublicKeyVerifier(PublicKeyConfig{Keys: map[string]PublicKey{
		"a": {Key: one, Algorithm: "RS256"}, "b": {Key: one, Algorithm: "RS256"},
	}}); err != nil {
		t.Errorf("one key under two IDs with one algorithm: %v", err)
	}
}

// RFC 9864 deprecates EdDSA for Ed25519, the same verification under a
// fully-specified name: a key bound to either accepts a token naming either.
func TestPublicKeyVerifierAcceptsTheFullySpecifiedEd25519(t *testing.T) {
	k := sharedTestKeys()
	now := time.Unix(1_800_000_000, 0)
	claims := map[string]any{"sub": "u1", "exp": now.Unix() + 60}
	for _, bound := range []string{"Ed25519", "EdDSA"} {
		v, err := NewPublicKeyVerifier(PublicKeyConfig{
			Keys: map[string]PublicKey{"ed": {Key: k.ed.Public(), Algorithm: bound}},
			Now:  func() time.Time { return now },
		})
		if err != nil {
			t.Fatalf("a key bound to %s: %v", bound, err)
		}
		for _, alg := range []string{"Ed25519", "EdDSA"} {
			if _, err := v.Verify(k.sign(t, alg, "ed", claims)); err != nil {
				t.Errorf("key bound to %s, token %s: %v", bound, alg, err)
			}
		}
	}
}

func TestPublicKeyVerifierRefusals(t *testing.T) {
	k := sharedTestKeys()
	now := time.Unix(1_800_000_000, 0)
	claims := map[string]any{"sub": "u1", "exp": now.Unix() + 60}
	v, err := NewPublicKeyVerifier(PublicKeyConfig{Keys: k.keys("RS256", "ES256"), Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	unsigned := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none"}`)) + "." +
		base64.RawURLEncoding.EncodeToString([]byte(`{"sub":"u1"}`)) + "."
	// An ES256 token whose signature is the ASN.1 form some libraries emit.
	es := k.sign(t, "ES256", "ES256", claims)
	asn1Sig, err := ecdsa.SignASN1(rand.Reader, k.ec["ES256"], digestOf(crypto.SHA256, es[:strings.LastIndexByte(es, '.')]))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, token string
		want        error
	}{
		{"alg none", unsigned, ErrTokenMalformed},
		{"an algorithm no key signs with", k.sign(t, "PS256", "PS256", claims), ErrTokenAlgorithm},
		{"an ES256 alg on the RSA key", reKid(t, k.sign(t, "ES256", "ES256", claims), "RS256"), ErrTokenSignature},
		{"an unknown kid", k.sign(t, "RS256", "gone", claims), ErrTokenSignature},
		{"no kid among several keys", k.sign(t, "RS256", "", claims), ErrTokenSignature},
		{"an ASN.1 ECDSA signature", es[:strings.LastIndexByte(es, '.')+1] + base64.RawURLEncoding.EncodeToString(asn1Sig), ErrTokenSignature},
	} {
		if _, err := v.Verify(tc.token); !errors.Is(err, tc.want) {
			t.Errorf("%s: %v, want %v", tc.name, err, tc.want)
		}
	}
}

// reKid re-signs nothing: it swaps the kid of a token's header, which breaks
// the signature unless the verifier never reaches it — and a verifier that
// let an ES256 alg run over an RSA key would fail some other way first.
func reKid(t *testing.T, token, kid string) string {
	t.Helper()
	parts := strings.SplitN(token, ".", 3)
	h, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		t.Fatal(err)
	}
	var hdr map[string]string
	if err := json.Unmarshal(h, &hdr); err != nil {
		t.Fatal(err)
	}
	hdr["kid"] = kid
	h, _ = json.Marshal(hdr)
	return base64.RawURLEncoding.EncodeToString(h) + "." + parts[1] + "." + parts[2]
}

func TestNewPublicKeyVerifierRefusesWhatCannotBeSafe(t *testing.T) {
	small, err := rsa.GenerateKey(rand.Reader, 1024) //nolint:gosec // G403: the key under 2048 bits is what the test needs refused
	if err != nil {
		t.Fatal(err)
	}
	k := sharedTestKeys()
	ed := k.ed.Public()
	for _, tc := range []struct {
		name string
		cfg  PublicKeyConfig
	}{
		{"no key", PublicKeyConfig{}},
		{"no algorithm", PublicKeyConfig{Keys: map[string]PublicKey{"ed": {Key: ed}}}},
		{"HS256, which needs a secret", PublicKeyConfig{Keys: map[string]PublicKey{"ed": {Key: ed, Algorithm: "HS256"}}}},
		{"none", PublicKeyConfig{Keys: map[string]PublicKey{"ed": {Key: ed, Algorithm: "none"}}}},
		{"RS256 on an Ed25519 key", PublicKeyConfig{Keys: map[string]PublicKey{"ed": {Key: ed, Algorithm: "RS256"}}}},
		{"ES384 on a P-256 key", PublicKeyConfig{Keys: map[string]PublicKey{"ec": {Key: &k.ec["ES256"].PublicKey, Algorithm: "ES384"}}}},
		{"EdDSA on an RSA key", PublicKeyConfig{Keys: map[string]PublicKey{"r": {Key: &k.rsa["RS256"].PublicKey, Algorithm: "EdDSA"}}}},
		{"an RSA key of 1024 bits", PublicKeyConfig{Keys: map[string]PublicKey{"r": {Key: &small.PublicKey, Algorithm: "RS256"}}}},
		{"a key of no known type", PublicKeyConfig{Keys: map[string]PublicKey{"x": {Key: []byte("secret"), Algorithm: "RS256"}}}},
		{"a negative leeway", PublicKeyConfig{Keys: map[string]PublicKey{"ed": {Key: ed, Algorithm: "EdDSA"}}, Leeway: -time.Second}},
	} {
		if _, err := NewPublicKeyVerifier(tc.cfg); err == nil {
			t.Errorf("%s: accepted", tc.name)
		}
	}
}
