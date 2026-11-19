package web

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/sha512"
	"errors"
	"fmt"
	"maps"
	"math/big"
	"slices"
	"strings"
	"time"
)

// PublicKeyConfig states what a token signed with a private key must
// satisfy: the keys it may be signed with, each with the one algorithm it
// signs with, and the same claim checks as [HMACConfig], with the same
// readings of the texts.
//
// It is the configuration for tokens an identity provider issues — an
// OpenID Connect provider signs with RS256 by default — where the verifier
// holds only the public half and the issuer alone can sign.
type PublicKeyConfig struct {
	// Keys are the issuer's public keys by key ID, the `kid` a token's header
	// names, each bound to its algorithm (see [PublicKey]). At least one is
	// required. A token without `kid` is verified with the only key when
	// there is one, and refused when there are several. The kid only ever
	// picks among these: it is never a path, a URL or a query, so a token
	// cannot bring its own key.
	//
	// The algorithms accepted are the keys' own: RFC 8725 §3.1 has the
	// caller state the set, and has each key used with exactly one
	// algorithm, checked when the signature is verified. A token whose `alg`
	// is not its key's is refused, and the same key under two IDs with two
	// algorithms is refused by [NewPublicKeyVerifier].
	//
	// Rotation is a key added under a new ID before the issuer signs with it,
	// and the old one removed once the longest-lived token it signed has
	// expired. [NewPublicKeyVerifier] copies the map, not the keys.
	Keys map[string]PublicKey

	// Audience, Issuer, TenantClaim, Leeway, MaxTokenBytes and Now are
	// [HMACConfig]'s, with the same zero values and the same refusals.
	Audience      string
	Issuer        string
	TenantClaim   string
	Leeway        time.Duration
	MaxTokenBytes int
	Now           func() time.Time
}

// PublicKey is one of an issuer's public keys and the one algorithm it
// signs with, the pair a JWK states with its key and its `alg` member.
type PublicKey struct {
	// Key is an *rsa.PublicKey of 2048 bits or more (RFC 7518 §3.3), an
	// *ecdsa.PublicKey on P-256, P-384 or P-521, or an ed25519.PublicKey.
	Key crypto.PublicKey

	// Algorithm is the one `alg` the key verifies: RS256, RS384, RS512,
	// PS256, PS384 or PS512 for an RSA key; the ES of its curve for an ECDSA
	// key (ES256 on P-256, ES384 on P-384, ES512 on P-521); Ed25519 or EdDSA
	// for an Ed25519 key. RFC 9864 names Ed25519 and deprecates the
	// polymorphic EdDSA; both name the same verification, so a key bound to
	// either accepts a token that says either.
	Algorithm string
}

// PublicKeyVerifier verifies tokens signed with RSA (PKCS #1 v1.5 or PSS),
// ECDSA or Ed25519 against the public keys of a [PublicKeyConfig].
//
// It is safe for concurrent use.
type PublicKeyVerifier struct {
	cfg  PublicKeyConfig
	algs []string // the keys' algorithms, sorted, for the refusal's message
}

// String prints the configuration: the key IDs with their algorithms and
// types, and the claim checks. Public keys are not secrets; their values
// are left out because a log has no use for them.
func (c PublicKeyConfig) String() string {
	ids := slices.Sorted(maps.Keys(c.Keys))
	keys := make([]string, len(ids))
	for i, id := range ids {
		keys[i] = fmt.Sprintf("%q: %s %T", id, c.Keys[id].Algorithm, c.Keys[id].Key)
	}
	return fmt.Sprintf("web.PublicKeyConfig{Keys: {%s}, Audience: %q, Issuer: %q, TenantClaim: %q, Leeway: %v, MaxTokenBytes: %d}",
		strings.Join(keys, ", "), c.Audience, c.Issuer, c.TenantClaim, c.Leeway, c.MaxTokenBytes)
}

// String prints the verifier's configuration (see [PublicKeyConfig.String]).
func (v *PublicKeyVerifier) String() string { return "web.PublicKeyVerifier{" + v.cfg.String() + "}" }

// NewPublicKeyVerifier builds a verifier, refusing a configuration that
// cannot be safe: no key, a key of a type or size RFC 7518 does not allow,
// an algorithm it does not know or that does not fit its key, one key
// under two IDs with two algorithms.
func NewPublicKeyVerifier(cfg PublicKeyConfig) (*PublicKeyVerifier, error) {
	if len(cfg.Keys) == 0 {
		return nil, errors.New("web: PublicKeyConfig.Keys is empty — a verifier needs the issuer's public key")
	}
	ids := slices.Sorted(maps.Keys(cfg.Keys)) // sorted, so the same mistake gives the same message
	var algs []string
	for i, id := range ids {
		k := cfg.Keys[id]
		if err := checkPublicKey(k.Key); err != nil {
			return nil, fmt.Errorf("web: PublicKeyConfig.Keys[%q]: %w", id, err)
		}
		alg := canonicalAlg(k.Algorithm)
		if hashOf(alg) == 0 {
			return nil, fmt.Errorf("web: PublicKeyConfig.Keys[%q]: algorithm %q is not one of RS256, RS384, RS512, PS256, PS384, PS512, ES256, ES384, ES512, Ed25519, EdDSA", id, k.Algorithm)
		}
		if !algFitsKey(alg, k.Key) {
			return nil, fmt.Errorf("web: PublicKeyConfig.Keys[%q]: algorithm %s does not fit a %T", id, k.Algorithm, k.Key)
		}
		// RFC 8725 §3.1: each key used with exactly one algorithm. Two IDs
		// for one key are a legitimate alias; two algorithms for it are not.
		for _, other := range ids[:i] {
			o := cfg.Keys[other]
			if eq, ok := o.Key.(interface{ Equal(crypto.PublicKey) bool }); ok && eq.Equal(k.Key) && canonicalAlg(o.Algorithm) != alg {
				return nil, fmt.Errorf("web: PublicKeyConfig.Keys[%q] and [%q] are one key with two algorithms, %s and %s — RFC 8725 §3.1 has a key used with exactly one", other, id, o.Algorithm, k.Algorithm)
			}
		}
		if !slices.Contains(algs, alg) {
			algs = append(algs, alg)
		}
	}
	slices.Sort(algs)
	if cfg.MaxTokenBytes < 0 {
		return nil, fmt.Errorf("web: PublicKeyConfig.MaxTokenBytes %d is negative — zero means 8 KiB", cfg.MaxTokenBytes)
	}
	if cfg.Leeway < 0 {
		return nil, fmt.Errorf("web: PublicKeyConfig.Leeway %v is negative — zero means none", cfg.Leeway)
	}
	if cfg.MaxTokenBytes == 0 {
		cfg.MaxTokenBytes = 8 << 10
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	cfg.Keys = maps.Clone(cfg.Keys)
	return &PublicKeyVerifier{cfg: cfg, algs: algs}, nil
}

// canonicalAlg is the name an algorithm is checked under: RFC 9864's
// fully-specified Ed25519 and the EdDSA it deprecates are one verification
// (its §5: the same key, only the `alg` value differs), so they are one
// algorithm here. Every other name is its own.
func canonicalAlg(alg string) string {
	if alg == "Ed25519" {
		return "EdDSA"
	}
	return alg
}

// algFitsKey reports whether alg, a canonical name hashOf knows, can verify
// with key: RS and PS on an RSA key, the ES of an ECDSA key's curve
// (RFC 7518 §3.4), EdDSA on an Ed25519 key (RFC 8037 §3.1).
func algFitsKey(alg string, key crypto.PublicKey) bool {
	switch k := key.(type) {
	case *rsa.PublicKey:
		return strings.HasPrefix(alg, "RS") || strings.HasPrefix(alg, "PS")
	case *ecdsa.PublicKey:
		return curveAlg(k.Curve) == alg
	case ed25519.PublicKey:
		return alg == "EdDSA"
	}
	return false
}

// checkPublicKey refuses a key no accepted algorithm could use safely.
func checkPublicKey(k crypto.PublicKey) error {
	switch k := k.(type) {
	case *rsa.PublicKey:
		if k == nil || k.N == nil || k.N.BitLen() < 2048 {
			return errors.New("an RSA key under 2048 bits, which RFC 7518 §3.3 forbids")
		}
	case *ecdsa.PublicKey:
		if k == nil || curveAlg(k.Curve) == "" {
			return errors.New("an ECDSA key on a curve other than P-256, P-384 or P-521")
		}
	case ed25519.PublicKey:
		if len(k) != ed25519.PublicKeySize {
			return fmt.Errorf("an Ed25519 key of %d bytes, not %d", len(k), ed25519.PublicKeySize)
		}
	default:
		return fmt.Errorf("a %T, which is none of *rsa.PublicKey, *ecdsa.PublicKey and ed25519.PublicKey", k)
	}
	return nil
}

// Verify checks a token and returns who it is from. As for [HMACVerifier],
// the signature is verified before any claim is read.
func (v *PublicKeyVerifier) Verify(token string) (Principal, error) {
	t, err := parseJWT(token, v.cfg.MaxTokenBytes, func(alg string) error {
		if !slices.Contains(v.algs, canonicalAlg(alg)) {
			return fmt.Errorf("%w: %q, and this verifier's keys sign with %q", ErrTokenAlgorithm, alg, v.algs)
		}
		return nil
	})
	if err != nil {
		return Principal{}, err
	}
	key, ok := v.cfg.Keys[t.kid]
	if t.kid == "" && len(v.cfg.Keys) == 1 {
		for _, k := range v.cfg.Keys {
			key, ok = k, true
		}
	}
	if !ok {
		return Principal{}, fmt.Errorf("%w: no key with the token's kid %q", ErrTokenSignature, t.kid)
	}
	// RFC 8725 §3.1, checked here, at the operation: the key verifies its
	// own algorithm and no other, whatever else the verifier accepts.
	alg := canonicalAlg(t.alg)
	if alg != canonicalAlg(key.Algorithm) {
		return Principal{}, fmt.Errorf("%w: the key %q signs with %s, and the token says %s", ErrTokenSignature, t.kid, key.Algorithm, t.alg)
	}
	if !verifySignature(alg, key.Key, t.signed, t.sig) {
		return Principal{}, ErrTokenSignature
	}
	claims, err := readClaims(t.payload)
	if err != nil {
		return Principal{}, err
	}
	return claimRules{
		audience: v.cfg.Audience, issuer: v.cfg.Issuer, tenantClaim: v.cfg.TenantClaim,
		leeway: v.cfg.Leeway, now: v.cfg.Now,
	}.principalFrom(claims)
}

// hashOf is the hash an algorithm signs over, 0 for an algorithm this
// package does not verify. EdDSA signs the input itself; its entry only
// says the algorithm is known.
func hashOf(alg string) crypto.Hash {
	switch alg {
	case "RS256", "PS256", "ES256":
		return crypto.SHA256
	case "RS384", "PS384", "ES384":
		return crypto.SHA384
	case "RS512", "PS512", "ES512":
		return crypto.SHA512
	case "EdDSA":
		return crypto.SHA512 // unused: Ed25519 hashes internally
	}
	return 0
}

// curveAlg is the one ES algorithm a curve signs with (RFC 7518 §3.4).
func curveAlg(c elliptic.Curve) string {
	switch c {
	case elliptic.P256():
		return "ES256"
	case elliptic.P384():
		return "ES384"
	case elliptic.P521():
		return "ES512"
	}
	return ""
}

// verifySignature reports whether sig is alg's signature of signed under
// key. The algorithm must fit the key: an RSA key verifies only RS and PS,
// an ECDSA key only the ES of its curve, an Ed25519 key only EdDSA — the
// token's alg is never allowed to reinterpret the key (RFC 8725 §3.1).
func verifySignature(alg string, key crypto.PublicKey, signed string, sig []byte) bool {
	if alg == "EdDSA" {
		k, ok := key.(ed25519.PublicKey)
		return ok && ed25519.Verify(k, []byte(signed), sig)
	}
	h := hashOf(alg)
	digest := digestOf(h, signed)
	switch alg[:2] {
	case "RS":
		k, ok := key.(*rsa.PublicKey)
		return ok && rsa.VerifyPKCS1v15(k, h, digest, sig) == nil
	case "PS":
		// RFC 7518 §3.5: the salt is as long as the hash output.
		k, ok := key.(*rsa.PublicKey)
		return ok && rsa.VerifyPSS(k, h, digest, sig, &rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthEqualsHash, Hash: h}) == nil
	case "ES":
		k, ok := key.(*ecdsa.PublicKey)
		if !ok || curveAlg(k.Curve) != alg {
			return false
		}
		// RFC 7518 §3.4: R and S, each the curve's byte size, end to end —
		// not the ASN.1 form; any other length fails.
		size := (k.Curve.Params().BitSize + 7) / 8
		if len(sig) != 2*size {
			return false
		}
		r := new(big.Int).SetBytes(sig[:size])
		s := new(big.Int).SetBytes(sig[size:])
		return ecdsa.Verify(k, digest, r, s)
	}
	return false
}

// digestOf hashes the signing input with one of the three SHA-2 sizes.
func digestOf(h crypto.Hash, signed string) []byte {
	switch h {
	case crypto.SHA384:
		d := sha512.Sum384([]byte(signed))
		return d[:]
	case crypto.SHA512:
		d := sha512.Sum512([]byte(signed))
		return d[:]
	}
	d := sha256.Sum256([]byte(signed))
	return d[:]
}
