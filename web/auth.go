package web

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/mlagarrigue/sluice/diagnostics"
)

// # Authentication is a boundary here, not an identity provider
//
// This package verifies a token that already exists. It does not issue one, it
// does not talk to a directory, and it does not know what a login is. Those
// belong to whatever already owns identity in a system, and a framework that
// grew its own would be competing with it badly.
//
// What it does own is the **boundary**: the point where an untrusted string
// becomes a [Principal] that later code is entitled to believe. That point is
// worth writing carefully, because every known JWT vulnerability lives in it
// rather than in the cryptography underneath — `alg: none`, algorithm
// confusion, an expiry nobody checked. Those are policy failures, and policy
// written out where it can be read is safer than policy delegated to a
// dependency nobody opens.
//
// The cryptography itself is [crypto/hmac] and [crypto/sha256] from the
// standard library. Nothing here reimplements a primitive, which is the
// project's standing rule and the one place it is not negotiable.
//
// # Only HS256, and that is a real limit
//
// [HMACVerifier] verifies HS256 and refuses everything else. It does not do
// RS256 or ES256, which is what most identity providers actually issue: those
// need public-key handling, a key set fetched over the network, and rotation —
// a component with a lifecycle, not a function.
//
// So a system using an external provider implements [Verifier] itself, in its
// own module, with whatever library it trusts. This package stays at zero
// dependencies and stops claiming to be the thing that verifies your
// provider's tokens. HS256 is here because a shared secret between two
// services this small is a real case, not because it is the general answer.

// Token errors. They are distinct because they need distinct responses: an
// expired token means refresh and retry, a bad signature means something is
// wrong that retrying will not fix.
var (
	// ErrTokenMalformed reports a token that is not a well-formed JWT.
	ErrTokenMalformed = errors.New("web: the bearer token is not a well-formed token")

	// ErrTokenAlgorithm reports a token whose algorithm this verifier will not
	// accept — including `none`, and including any algorithm other than the
	// one the verifier was built for.
	ErrTokenAlgorithm = errors.New("web: the token's algorithm is not accepted")

	// ErrTokenSignature reports a token whose signature does not verify. It is
	// deliberately indistinguishable from a token signed with the wrong key:
	// telling those apart tells an attacker which half to keep guessing.
	ErrTokenSignature = errors.New("web: the token's signature does not verify")

	// ErrTokenExpired reports a token past its `exp`, or not yet valid by its
	// `nbf`.
	ErrTokenExpired = errors.New("web: the token is outside its validity window")

	// ErrTokenClaims reports a token whose claims are missing or do not match
	// what the verifier requires — a wrong audience, a wrong issuer, no expiry.
	ErrTokenClaims = errors.New("web: the token's claims are not acceptable")

	// ErrCredentialsMissing is what [Authenticate] returns when the request
	// carries no bearer token it could hand to a verifier: no Authorization
	// field, two of them, another scheme, or a malformed value. The verifier
	// was never called. A caller that admits anonymous requests checks for
	// it with errors.Is; every other caller refuses on err != nil as usual.
	ErrCredentialsMissing = errors.New("web: the request carries no usable bearer credentials")
)

// Principal is who a request is from, once a token has been believed.
//
// It carries only what an authorisation decision needs. In particular it
// carries no token, no signature and no secret: a Principal is meant to travel
// through a pipeline and into logs, and anything in it will eventually be
// somewhere it was not meant to be.
type Principal struct {
	// Subject is the `sub` claim: who the token is about.
	Subject string

	// Tenant is the claim naming the security principal for row-level
	// security, when the verifier was told which claim that is. It is the
	// value that becomes part of the batching key — see the brief's §0.3 —
	// so getting it from a *verified* token rather than from a header is the
	// whole point of this type existing.
	Tenant string

	// Scopes are the space-separated `scope` claim, split.
	Scopes []string

	// ExpiresAt is when the token stops being valid. Kept so a caller can
	// decide how long a downstream decision may be cached — a cached
	// authorisation must not outlive the token it was based on.
	ExpiresAt time.Time

	// Claims is everything else the token carried. Read it when a rule needs a
	// claim this type does not model; do not put a secret in it.
	Claims map[string]any
}

// HasScope reports whether the principal carries a scope.
func (p Principal) HasScope(s string) bool {
	return slices.Contains(p.Scopes, s)
}

// Verifier turns a bearer token into a principal, or refuses it.
//
// It is an interface because the library's own implementation covers one
// case and a real system usually has another. Implement it against your
// provider; the rest of this package does not care how a token was checked,
// only that something took responsibility for saying it was.
type Verifier interface {
	Verify(token string) (Principal, error)
}

// HMACConfig states what a token must satisfy. Everything in it is a refusal
// the verifier will make, which is why there are no clever defaults.
type HMACConfig struct {
	// Secret is the shared HMAC key. It must be at least 32 bytes: shorter
	// than the hash it feeds, and the security of the whole thing rests on
	// guessing difficulty this package cannot restore. [NewHMACVerifier]
	// copies it, so the caller may zero its own slice afterwards.
	Secret []byte

	// Previous are retired secrets still accepted while a rotation is under
	// way: tokens signed before the issuer switched to Secret keep verifying
	// until they expire, so a rotation needs no flag day. They are tried
	// after Secret, in order, and each obeys Secret's 32-byte floor and is
	// copied the same way. Remove one once the longest-lived token it could
	// have signed has expired; until then it is as good as the current key.
	Previous [][]byte

	// Audience, when set, is the `aud` claim a token must carry. Leaving it
	// empty accepts any audience, which means accepting a token minted for a
	// different service that happens to share the secret.
	Audience string

	// Issuer, when set, is the `iss` claim a token must carry.
	Issuer string

	// TenantClaim names the claim holding the security principal — often
	// "tenant" or "org". Empty means [Principal.Tenant] stays empty, and any
	// row-level-security decision downstream has nothing to key on.
	TenantClaim string

	// Leeway absorbs clock skew between whoever signed the token and this
	// machine. Zero means none, which is correct and occasionally brutal: two
	// machines a second apart will reject each other's freshly minted tokens.
	// A minute is the usual answer; more than a few minutes means an expiry is
	// no longer an expiry.
	Leeway time.Duration

	// MaxTokenBytes bounds what will be parsed at all. Zero means 8 KiB. A
	// token is presented by whoever dialled the socket, so its size is theirs
	// to choose until something says otherwise.
	MaxTokenBytes int

	// Now is the clock, for tests. Nil means [time.Now].
	Now func() time.Time
}

// HMACVerifier verifies HS256 tokens against a shared secret.
//
// It is safe for concurrent use.
type HMACVerifier struct {
	cfg HMACConfig

	// macs recycles keyed HMAC states across verifications, one pool per
	// accepted key: macs[0] is Secret, the rest are Previous in order. Reset
	// restores the keyed initial state without re-deriving it from the key,
	// where hmac.New re-hashes the key pads and builds two fresh digest
	// states on every call. The keys are fixed for the life of the verifier,
	// which is what makes pooling the keyed state sound. Each New is set at
	// construction so there is exactly one place a mac is built.
	macs []*sync.Pool
}

// NewHMACVerifier builds a verifier, refusing a configuration that cannot be
// safe rather than accepting it and being unsafe quietly.
func NewHMACVerifier(cfg HMACConfig) (*HMACVerifier, error) {
	if len(cfg.Secret) < 32 {
		return nil, fmt.Errorf("web: the HMAC secret is %d bytes; HS256 needs at least 32, and a shorter one is guessable in a way no amount of care here can fix", len(cfg.Secret))
	}
	for i, old := range cfg.Previous {
		if len(old) < 32 {
			return nil, fmt.Errorf("web: previous HMAC secret %d is %d bytes; HS256 needs at least 32 for a retired key as much as for the current one", i, len(old))
		}
	}
	if cfg.MaxTokenBytes <= 0 {
		cfg.MaxTokenBytes = 8 << 10
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	// Copied, not referenced: a pool's New re-reads its key whenever a GC
	// has emptied the pool, so a caller that zeroes or reuses its slice after
	// construction would otherwise change the key under live verifications.
	cfg.Secret = bytes.Clone(cfg.Secret)
	prev := make([][]byte, len(cfg.Previous))
	for i, old := range cfg.Previous {
		prev[i] = bytes.Clone(old)
	}
	cfg.Previous = prev
	v := &HMACVerifier{cfg: cfg, macs: make([]*sync.Pool, 0, 1+len(prev))}
	for _, key := range append([][]byte{cfg.Secret}, prev...) {
		v.macs = append(v.macs, &sync.Pool{New: func() any { return hmac.New(sha256.New, key) }})
	}
	return v, nil
}

// jwtHeader is the part of a token's header this verifier reads. `kid` is
// deliberately absent: selecting a key by a value the token supplies is how a
// verifier is talked into using the attacker's key.
type jwtHeader struct {
	Alg string `json:"alg"`
	Typ string `json:"typ"`

	// Crit is read only to be refused. RFC 7515 §4.1.11: a verifier MUST
	// reject a token whose `crit` lists an extension it does not implement —
	// and this verifier implements none, so any `crit` at all means the token
	// demands semantics that will not be enforced here.
	Crit []json.RawMessage `json:"crit"`
}

// Verify checks a token and returns who it is from.
//
// The order of the checks is part of the design: the signature is verified
// **before** any claim is read. A verifier that reads `exp` first has parsed
// attacker-controlled JSON and made a decision on it before establishing that
// the attacker could not have written it.
func (v *HMACVerifier) Verify(token string) (Principal, error) {
	if len(token) > v.cfg.MaxTokenBytes {
		return Principal{}, fmt.Errorf("%w: %d bytes, over the %d-byte limit", ErrTokenMalformed, len(token), v.cfg.MaxTokenBytes)
	}
	// One spelling per token. The decoder skips CR and LF and, unless
	// strict, ignores non-zero padding bits, so without this a single valid
	// token has many string forms that all verify — and a revocation list,
	// a replay cache or a rate limit keyed on the string is bypassed by
	// re-spelling it. Every byte must be base64url or a separator, and the
	// decodes below are Strict.
	for i := range len(token) {
		if !isTokenByte(token[i]) {
			return Principal{}, fmt.Errorf("%w: byte %d is outside the base64url alphabet", ErrTokenMalformed, i)
		}
	}

	// Exactly three segments. Two is an unsigned token; four is something else
	// entirely — a JWE, or a forgery hoping a splitter takes the first three.
	first := strings.IndexByte(token, '.')
	last := strings.LastIndexByte(token, '.')
	if first <= 0 || last <= first || last == len(token)-1 {
		return Principal{}, fmt.Errorf("%w: expected three segments", ErrTokenMalformed)
	}
	if strings.IndexByte(token[first+1:last], '.') >= 0 {
		return Principal{}, fmt.Errorf("%w: expected three segments", ErrTokenMalformed)
	}
	signed, sigPart := token[:last], token[last+1:]

	headerJSON, err := jwtEncoding.DecodeString(token[:first])
	if err != nil {
		return Principal{}, fmt.Errorf("%w: the header is not base64url", ErrTokenMalformed)
	}
	var hdr jwtHeader
	if err := json.Unmarshal(headerJSON, &hdr); err != nil {
		return Principal{}, fmt.Errorf("%w: the header is not JSON", ErrTokenMalformed)
	}

	// The algorithm is checked against a constant, never used to *choose*
	// anything. `alg: none` is refused by the same comparison that refuses
	// RS256, which is what makes both impossible rather than one of them
	// remembered.
	if hdr.Alg != "HS256" {
		return Principal{}, fmt.Errorf("%w: %q, and this verifier accepts only HS256", ErrTokenAlgorithm, hdr.Alg)
	}
	// Case-insensitive on purpose: typ is a media type, and RFC 7515 §4.1.9
	// makes its comparison case-insensitive; RFC 7519 §5.1 only RECOMMENDS
	// the uppercase spelling "JWT", so "jwt" is a conforming token.
	if hdr.Typ != "" && !strings.EqualFold(hdr.Typ, "JWT") {
		return Principal{}, fmt.Errorf("%w: type %q", ErrTokenMalformed, hdr.Typ)
	}
	if hdr.Crit != nil {
		return Principal{}, fmt.Errorf("%w: the header carries crit, and this verifier implements no extension it could name (RFC 7515 §4.1.11)", ErrTokenMalformed)
	}

	sig, err := jwtEncoding.DecodeString(sigPart)
	if err != nil {
		return Principal{}, fmt.Errorf("%w: the signature is not base64url", ErrTokenMalformed)
	}
	if !v.signedByAnyKey(signed, sig) {
		return Principal{}, ErrTokenSignature
	}

	// Only now is the payload worth reading.
	payloadJSON, err := jwtEncoding.DecodeString(token[first+1 : last])
	if err != nil {
		return Principal{}, fmt.Errorf("%w: the payload is not base64url", ErrTokenMalformed)
	}
	var claims map[string]any
	if err := json.Unmarshal(payloadJSON, &claims); err != nil {
		return Principal{}, fmt.Errorf("%w: the payload is not JSON", ErrTokenMalformed)
	}
	return v.principalFrom(claims)
}

// jwtEncoding is base64url without padding, strict about the trailing bits:
// the lenient decoder maps several spellings of the last character to the
// same bytes.
var jwtEncoding = base64.RawURLEncoding.Strict()

func isTokenByte(c byte) bool {
	return 'A' <= c && c <= 'Z' || 'a' <= c && c <= 'z' || '0' <= c && c <= '9' ||
		c == '-' || c == '_' || c == '.'
}

// signedByAnyKey reports whether sig is the HS256 of signed under the current
// secret or one of the previous ones. The current key is tried first, so a
// verifier with no rotation in progress does the one HMAC it always did.
func (v *HMACVerifier) signedByAnyKey(signed string, sig []byte) bool {
	var buf [sha256.Size]byte
	for _, pool := range v.macs {
		mac := pool.Get().(hash.Hash)
		mac.Reset() // a no-op on a fresh mac, and what makes a recycled one fresh
		mac.Write([]byte(signed))
		digest := mac.Sum(buf[:0])
		pool.Put(mac)
		// hmac.Equal, not bytes.Equal: a comparison that returns early tells
		// an attacker how many bytes were right, one request at a time.
		if hmac.Equal(sig, digest) {
			return true
		}
	}
	return false
}

func (v *HMACVerifier) principalFrom(claims map[string]any) (Principal, error) {
	now := v.cfg.Now()

	// An expiry is required. A signed token with no `exp` is a credential that
	// never stops working, and accepting one because the standard calls the
	// claim optional is how a leaked token stays useful for years.
	exp, ok := numericDate(claims["exp"])
	if !ok {
		return Principal{}, fmt.Errorf("%w: no usable exp claim, and a token that never expires is not one this package will believe", ErrTokenClaims)
	}
	if now.After(exp.Add(v.cfg.Leeway)) {
		return Principal{}, fmt.Errorf("%w: expired at %s", ErrTokenExpired, exp.UTC().Format(time.RFC3339))
	}
	// nbf and iat are optional, but one that is present and unreadable is
	// refused rather than skipped: skipping it accepted a token its issuer
	// meant to be not yet valid.
	nbf, ok := numericDate(claims["nbf"])
	if _, present := claims["nbf"]; present && !ok {
		return Principal{}, fmt.Errorf("%w: unusable nbf claim", ErrTokenClaims)
	}
	if ok && now.Add(v.cfg.Leeway).Before(nbf) {
		return Principal{}, fmt.Errorf("%w: not valid before %s", ErrTokenExpired, nbf.UTC().Format(time.RFC3339))
	}
	// An `iat` in the future is a clock problem or a forgery; either way it is
	// not a token to act on.
	iat, ok := numericDate(claims["iat"])
	if _, present := claims["iat"]; present && !ok {
		return Principal{}, fmt.Errorf("%w: unusable iat claim", ErrTokenClaims)
	}
	if ok && now.Add(v.cfg.Leeway).Before(iat) {
		return Principal{}, fmt.Errorf("%w: issued in the future, at %s", ErrTokenClaims, iat.UTC().Format(time.RFC3339))
	}

	if v.cfg.Issuer != "" {
		if iss, _ := claims["iss"].(string); iss != v.cfg.Issuer {
			return Principal{}, fmt.Errorf("%w: issuer %q", ErrTokenClaims, iss)
		}
	}
	if v.cfg.Audience != "" && !audienceContains(claims["aud"], v.cfg.Audience) {
		return Principal{}, fmt.Errorf("%w: audience does not include %q", ErrTokenClaims, v.cfg.Audience)
	}

	p := Principal{ExpiresAt: exp, Claims: claims}
	p.Subject, _ = claims["sub"].(string)
	if s, ok := claims["scope"].(string); ok && s != "" {
		p.Scopes = strings.Fields(s)
	}
	if v.cfg.TenantClaim != "" {
		p.Tenant, _ = claims[v.cfg.TenantClaim].(string)
		if p.Tenant == "" {
			return Principal{}, fmt.Errorf("%w: no %q claim, and this verifier was told that claim carries the security principal", ErrTokenClaims, v.cfg.TenantClaim)
		}
	}
	return p, nil
}

// numericDate reads a JWT NumericDate. JSON gives it as a float64, and seconds
// since the Unix epoch is what the specification says it is — not
// milliseconds, which is the mistake that makes every token look valid until
// the year 56000.
const (
	minNumericDate = -(1 << 53)
	maxNumericDate = 1 << 53
)

func numericDate(v any) (time.Time, bool) {
	f, ok := v.(float64)
	// Bounded to ±2^53, where every float64 is an exact integer and the int64
	// conversion is defined on every platform; past it the result depends on
	// the CPU, and NaN fails every bound.
	if !ok || !(f >= minNumericDate && f <= maxNumericDate) {
		return time.Time{}, false
	}
	sec, frac := int64(f), f-float64(int64(f))
	return time.Unix(sec, int64(frac*float64(time.Second))), true
}

// audienceContains handles `aud` being either a string or an array of them,
// which the specification allows and which a verifier that assumes one shape
// gets wrong in the direction of accepting too much.
func audienceContains(v any, want string) bool {
	switch aud := v.(type) {
	case string:
		return aud == want
	case []any:
		for _, item := range aud {
			if s, ok := item.(string); ok && s == want {
				return true
			}
		}
	}
	return false
}

// CodeUnauthenticated is the diagnostic code for a request whose credentials
// were absent or rejected.
const CodeUnauthenticated = "Transport.Request.Unauthenticated"

// Bearer extracts the token from an Authorization header.
//
// The scheme is matched case-insensitively, as RFC 7235 requires, and exactly
// one space separates it from the token — a header this package will not parse
// creatively, because creative parsing at an authentication boundary is how
// two components disagree about what the credential was.
func Bearer(r *http.Request) (string, diagnostics.Diagnostic, bool) {
	// Two Authorization fields is two credentials for one request: Get would
	// quietly authenticate on the first while another hop may pick the
	// other. Refused rather than resolved, the same stance the httpstream
	// transport takes on a Host/:authority disagreement.
	if len(r.Header.Values("Authorization")) > 1 {
		return "", unauthenticated("CredentialsDuplicated", nil), false
	}
	h := r.Header.Get("Authorization")
	if h == "" {
		return "", unauthenticated("CredentialsMissing", nil), false
	}
	const prefix = "bearer "
	if len(h) <= len(prefix) || !strings.EqualFold(h[:len(prefix)], prefix) {
		return "", unauthenticated("CredentialsNotBearer", nil), false
	}
	// Never empty here: the length check above already excludes an h of
	// exactly len(prefix), which is the only way this slice could be.
	token := h[len(prefix):]
	if strings.ContainsAny(token, " \t") {
		return "", unauthenticated("CredentialsMalformed", nil), false
	}
	return token, diagnostics.Diagnostic{}, true
}

// Authenticate is [Bearer] followed by verification: the whole boundary in one
// call, returning a principal or the diagnostic that says why not.
//
// The diagnostic never carries the reason in a form a client can learn from.
// "Expired" and "wrong signature" are different errors internally — the caller
// gets them from err and can log them — but what goes back to the requester is
// one code, because telling an attacker which half of a forgery was right is
// telling them how to fix it.
//
// err is non-nil whenever no principal was established, so the usual
// `if err != nil` refuses. When the request carried no usable bearer token —
// absent, duplicated, another scheme, malformed — err is
// [ErrCredentialsMissing] and the verifier was not called; a route that
// admits anonymous callers tells that case apart with errors.Is. Any other
// error came from the verifier.
func Authenticate(r *http.Request, v Verifier) (Principal, diagnostics.Diagnostic, error) {
	token, d, ok := Bearer(r)
	if !ok {
		return Principal{}, d, ErrCredentialsMissing
	}
	p, err := v.Verify(token)
	if err != nil {
		return Principal{}, unauthenticated("CredentialsRejected", nil), err
	}
	return p, diagnostics.Diagnostic{}, nil
}

func unauthenticated(messageID string, args map[string]any) diagnostics.Diagnostic {
	return diagnostics.NewDiagnostic(diagnostics.Error, CodeUnauthenticated, diagnostics.Path{}).
		WithMessage(messageID, args).
		WithOrigin(diagnostics.OriginProtocol)
}
