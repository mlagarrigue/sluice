package web_test

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/web"
)

var testSecret = []byte("a-secret-of-at-least-thirty-two-bytes!!")

// mint builds a token the way an attacker would have to: by hand, so that a
// forgery is expressible. A test that can only produce tokens through the
// verifier's own helpers cannot forge one, and therefore cannot test the
// boundary at all.
func mint(t *testing.T, header, claims map[string]any, secret []byte) string {
	t.Helper()
	enc := func(v any) string {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		return base64.RawURLEncoding.EncodeToString(b)
	}
	signed := enc(header) + "." + enc(claims)
	if secret == nil {
		return signed + "." // the unsigned shape, with an empty signature
	}
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(signed))
	return signed + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func hs256(t *testing.T, claims map[string]any) string {
	t.Helper()
	return mint(t, map[string]any{"alg": "HS256", "typ": "JWT"}, claims, testSecret)
}

func verifier(t *testing.T, cfg web.HMACConfig) *web.HMACVerifier {
	t.Helper()
	if cfg.Secret == nil {
		cfg.Secret = testSecret
	}
	if cfg.Now == nil {
		cfg.Now = func() time.Time { return time.Unix(1_700_000_000, 0) }
	}
	v, err := web.NewHMACVerifier(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestVerifyAcceptsAWellFormedToken(t *testing.T) {
	v := verifier(t, web.HMACConfig{TenantClaim: "tenant"})
	token := hs256(t, map[string]any{
		"sub": "user-1", "tenant": "acme", "scope": "orders:read orders:write",
		"exp": 1_700_000_600,
	})

	p, err := v.Verify(token)
	if err != nil {
		t.Fatalf("a valid token was refused: %v", err)
	}
	if p.Subject != "user-1" || p.Tenant != "acme" {
		t.Errorf("principal = %+v", p)
	}
	if !p.HasScope("orders:write") || p.HasScope("orders:delete") {
		t.Errorf("scopes = %v", p.Scopes)
	}
	if p.ExpiresAt.Unix() != 1_700_000_600 {
		t.Errorf("ExpiresAt = %v", p.ExpiresAt)
	}
}

// The forgeries. Each one is a published JWT attack, and each must be refused
// by a rule rather than by luck.
func TestVerifyRefusesForgeries(t *testing.T) {
	valid := map[string]any{"sub": "user-1", "exp": 1_700_000_600}

	// The unsigned shape — `header.payload.` with nothing after the last dot —
	// is caught on its form, before the header is even read. Two defences
	// catch `alg: none`: this one on the empty signature, and the algorithm
	// comparison below for a token that supplies one anyway. The assertion
	// here is on the refusal, not on which defence fired, because relying on
	// the order would make the test fail when a stricter check moves earlier.
	t.Run("alg none with an empty signature", func(t *testing.T) {
		v := verifier(t, web.HMACConfig{})
		token := mint(t, map[string]any{"alg": "none", "typ": "JWT"}, valid, nil)
		p, err := v.Verify(token)
		if err == nil {
			t.Fatal("an unsigned token was accepted")
		}
		if p.Subject != "" {
			t.Error("a principal came back from an unsigned token")
		}
	})

	t.Run("alg none with a signature anyway", func(t *testing.T) {
		v := verifier(t, web.HMACConfig{})
		token := mint(t, map[string]any{"alg": "none"}, valid, testSecret)
		if _, err := v.Verify(token); !errors.Is(err, web.ErrTokenAlgorithm) {
			t.Fatalf("err = %v, want ErrTokenAlgorithm", err)
		}
	})

	// RFC 7515 §4.1.11: a token whose `crit` names an extension the verifier
	// does not implement MUST be rejected — and this verifier implements
	// none. Accepting it would mean enforcing a token minus the semantics its
	// issuer declared non-optional.
	t.Run("crit demanding an unimplemented extension", func(t *testing.T) {
		v := verifier(t, web.HMACConfig{})
		header := map[string]any{"alg": "HS256", "typ": "JWT", "crit": []string{"exp2"}, "exp2": true}
		token := mint(t, header, valid, testSecret)
		if _, err := v.Verify(token); !errors.Is(err, web.ErrTokenMalformed) {
			t.Fatalf("err = %v, want ErrTokenMalformed", err)
		}
	})

	// An empty crit array is itself malformed per the RFC, and there is no
	// reason to look inside: any crit at all is refused.
	t.Run("crit present but empty", func(t *testing.T) {
		v := verifier(t, web.HMACConfig{})
		header := map[string]any{"alg": "HS256", "typ": "JWT", "crit": []string{}}
		token := mint(t, header, valid, testSecret)
		if _, err := v.Verify(token); !errors.Is(err, web.ErrTokenMalformed) {
			t.Fatalf("err = %v, want ErrTokenMalformed", err)
		}
	})

	t.Run("algorithm confusion: RS256 declared", func(t *testing.T) {
		// The classic: declare an asymmetric algorithm and sign with the
		// public key as an HMAC secret. Refused because the algorithm is
		// compared against a constant and never used to choose anything.
		v := verifier(t, web.HMACConfig{})
		token := mint(t, map[string]any{"alg": "RS256", "typ": "JWT"}, valid, testSecret)
		if _, err := v.Verify(token); !errors.Is(err, web.ErrTokenAlgorithm) {
			t.Fatalf("err = %v, want ErrTokenAlgorithm", err)
		}
	})

	t.Run("signed with the wrong secret", func(t *testing.T) {
		v := verifier(t, web.HMACConfig{})
		token := mint(t, map[string]any{"alg": "HS256"}, valid,
			[]byte("another-secret-of-at-least-32-bytes!!"))
		if _, err := v.Verify(token); !errors.Is(err, web.ErrTokenSignature) {
			t.Fatalf("err = %v, want ErrTokenSignature", err)
		}
	})

	t.Run("payload edited after signing", func(t *testing.T) {
		v := verifier(t, web.HMACConfig{TenantClaim: "tenant"})
		token := hs256(t, map[string]any{"sub": "user-1", "tenant": "acme", "exp": 1_700_000_600})

		parts := strings.Split(token, ".")
		tampered, err := json.Marshal(map[string]any{
			"sub": "user-1", "tenant": "globex", "exp": 1_700_000_600,
		})
		if err != nil {
			t.Fatal(err)
		}
		parts[1] = base64.RawURLEncoding.EncodeToString(tampered)

		if _, err := v.Verify(strings.Join(parts, ".")); !errors.Is(err, web.ErrTokenSignature) {
			t.Fatalf("a token whose tenant was rewritten verified: %v", err)
		}
	})

	t.Run("segment counts", func(t *testing.T) {
		v := verifier(t, web.HMACConfig{})
		good := hs256(t, valid)
		parts := strings.Split(good, ".")
		for _, tc := range []struct{ name, token string }{
			{"two segments", parts[0] + "." + parts[1]},
			{"four segments", good + "." + parts[2]},
			{"empty", ""},
			{"no dots", "not-a-token"},
			{"trailing dot", good + "."},
			{"leading dot", "." + good},
		} {
			t.Run(tc.name, func(t *testing.T) {
				if _, err := v.Verify(tc.token); err == nil {
					t.Fatal("accepted")
				}
			})
		}
	})

	t.Run("oversized", func(t *testing.T) {
		v := verifier(t, web.HMACConfig{MaxTokenBytes: 64})
		if _, err := v.Verify(hs256(t, valid)); !errors.Is(err, web.ErrTokenMalformed) {
			t.Fatalf("err = %v, want ErrTokenMalformed", err)
		}
	})
}

// A signed token with no expiry is a credential that never stops working.
func TestVerifyRequiresAnExpiry(t *testing.T) {
	v := verifier(t, web.HMACConfig{})
	if _, err := v.Verify(hs256(t, map[string]any{"sub": "user-1"})); !errors.Is(err, web.ErrTokenClaims) {
		t.Fatalf("err = %v, want ErrTokenClaims — a token with no exp was believed", err)
	}
	// And an exp of the wrong JSON type is not an exp.
	if _, err := v.Verify(hs256(t, map[string]any{"sub": "u", "exp": "1700000600"})); !errors.Is(err, web.ErrTokenClaims) {
		t.Fatalf("err = %v, want ErrTokenClaims for a string exp", err)
	}
}

// An optional date claim that is present but unreadable is refused, not
// skipped: skipping an unusable nbf accepted a token its issuer meant to be
// not yet valid. Out-of-range numbers count as unreadable — past 2^53 the
// conversion to int64 is platform-defined.
func TestVerifyRefusesUnusableDateClaims(t *testing.T) {
	v := verifier(t, web.HMACConfig{})
	for name, claims := range map[string]map[string]any{
		"string nbf":        {"sub": "u", "exp": 1_700_000_600, "nbf": "later"},
		"huge nbf":          {"sub": "u", "exp": 1_700_000_600, "nbf": 1e300},
		"string iat":        {"sub": "u", "exp": 1_700_000_600, "iat": "now"},
		"huge exp":          {"sub": "u", "exp": 1e300},
		"negative huge exp": {"sub": "u", "exp": -1e300},
	} {
		if _, err := v.Verify(hs256(t, claims)); !errors.Is(err, web.ErrTokenClaims) {
			t.Errorf("%s: err = %v, want ErrTokenClaims", name, err)
		}
	}
}

func TestVerifyValidityWindow(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)

	t.Run("expired", func(t *testing.T) {
		v := verifier(t, web.HMACConfig{Now: func() time.Time { return now }})
		token := hs256(t, map[string]any{"sub": "u", "exp": now.Add(-time.Second).Unix()})
		if _, err := v.Verify(token); !errors.Is(err, web.ErrTokenExpired) {
			t.Fatalf("err = %v, want ErrTokenExpired", err)
		}
	})

	t.Run("expired but inside the leeway", func(t *testing.T) {
		v := verifier(t, web.HMACConfig{
			Leeway: time.Minute,
			Now:    func() time.Time { return now },
		})
		token := hs256(t, map[string]any{"sub": "u", "exp": now.Add(-30 * time.Second).Unix()})
		if _, err := v.Verify(token); err != nil {
			t.Fatalf("a token thirty seconds past its expiry was refused under a minute of leeway: %v", err)
		}
	})

	t.Run("not yet valid", func(t *testing.T) {
		v := verifier(t, web.HMACConfig{Now: func() time.Time { return now }})
		token := hs256(t, map[string]any{
			"sub": "u", "exp": now.Add(time.Hour).Unix(), "nbf": now.Add(time.Minute).Unix(),
		})
		if _, err := v.Verify(token); !errors.Is(err, web.ErrTokenExpired) {
			t.Fatalf("err = %v, want ErrTokenExpired for an nbf in the future", err)
		}
	})

	t.Run("issued in the future", func(t *testing.T) {
		v := verifier(t, web.HMACConfig{Now: func() time.Time { return now }})
		token := hs256(t, map[string]any{
			"sub": "u", "exp": now.Add(time.Hour).Unix(), "iat": now.Add(time.Hour).Unix(),
		})
		if _, err := v.Verify(token); !errors.Is(err, web.ErrTokenClaims) {
			t.Fatalf("err = %v, want ErrTokenClaims for an iat in the future", err)
		}
	})

	// Seconds, not milliseconds. Reading a NumericDate as milliseconds makes
	// every token look valid until the year 56000.
	t.Run("exp is in seconds", func(t *testing.T) {
		v := verifier(t, web.HMACConfig{Now: func() time.Time { return now }})
		token := hs256(t, map[string]any{"sub": "u", "exp": now.Add(-time.Hour).UnixMilli()})
		if _, err := v.Verify(token); err != nil {
			t.Fatalf("an expiry given in milliseconds should read as a far-future second count, not fail: %v", err)
		}
		p, _ := v.Verify(token)
		if p.ExpiresAt.Year() < 50000 {
			t.Errorf("ExpiresAt = %v; a millisecond value was read as seconds, which is the point of this test", p.ExpiresAt)
		}
	})
}

func TestVerifyChecksIssuerAndAudience(t *testing.T) {
	base := map[string]any{"sub": "u", "exp": 1_700_000_600}

	t.Run("wrong issuer", func(t *testing.T) {
		v := verifier(t, web.HMACConfig{Issuer: "https://issuer.example"})
		claims := map[string]any{"sub": "u", "exp": 1_700_000_600, "iss": "https://evil.example"}
		if _, err := v.Verify(hs256(t, claims)); !errors.Is(err, web.ErrTokenClaims) {
			t.Fatalf("err = %v, want ErrTokenClaims", err)
		}
	})

	t.Run("missing audience", func(t *testing.T) {
		v := verifier(t, web.HMACConfig{Audience: "orders"})
		if _, err := v.Verify(hs256(t, base)); !errors.Is(err, web.ErrTokenClaims) {
			t.Fatalf("err = %v, want ErrTokenClaims", err)
		}
	})

	// aud may be a string or an array, and a verifier that assumes one shape
	// gets the other wrong in the direction of accepting too much.
	t.Run("audience as a string", func(t *testing.T) {
		v := verifier(t, web.HMACConfig{Audience: "orders"})
		claims := map[string]any{"sub": "u", "exp": 1_700_000_600, "aud": "orders"}
		if _, err := v.Verify(hs256(t, claims)); err != nil {
			t.Fatalf("a string audience was refused: %v", err)
		}
	})

	// The gap a "missing audience" case does not cover: an audience that is
	// present, is a string, and is somebody else's.
	t.Run("audience as a string, and the wrong one", func(t *testing.T) {
		v := verifier(t, web.HMACConfig{Audience: "orders"})
		claims := map[string]any{"sub": "u", "exp": 1_700_000_600, "aud": "billing"}
		if _, err := v.Verify(hs256(t, claims)); !errors.Is(err, web.ErrTokenClaims) {
			t.Fatalf("err = %v, want ErrTokenClaims — a token minted for another service was accepted", err)
		}
	})

	t.Run("audience as an array", func(t *testing.T) {
		v := verifier(t, web.HMACConfig{Audience: "orders"})
		claims := map[string]any{"sub": "u", "exp": 1_700_000_600, "aud": []any{"billing", "orders"}}
		if _, err := v.Verify(hs256(t, claims)); err != nil {
			t.Fatalf("an array audience was refused: %v", err)
		}
	})

	t.Run("audience array without ours", func(t *testing.T) {
		v := verifier(t, web.HMACConfig{Audience: "orders"})
		claims := map[string]any{"sub": "u", "exp": 1_700_000_600, "aud": []any{"billing"}}
		if _, err := v.Verify(hs256(t, claims)); !errors.Is(err, web.ErrTokenClaims) {
			t.Fatalf("err = %v, want ErrTokenClaims", err)
		}
	})
}

// The tenant is what becomes part of the batching key, so a verifier told to
// find it must refuse a token that does not carry it — rather than hand back a
// principal with an empty tenant that row-level security would read as nobody.
func TestVerifyRefusesAMissingTenantWhenOneIsRequired(t *testing.T) {
	v := verifier(t, web.HMACConfig{TenantClaim: "tenant"})
	if _, err := v.Verify(hs256(t, map[string]any{"sub": "u", "exp": 1_700_000_600})); !errors.Is(err, web.ErrTokenClaims) {
		t.Fatalf("err = %v, want ErrTokenClaims", err)
	}
}

// A short secret is guessable in a way no care inside the verifier can fix.
func TestNewHMACVerifierRefusesAShortSecret(t *testing.T) {
	for _, n := range []int{0, 1, 16, 31} {
		if _, err := web.NewHMACVerifier(web.HMACConfig{Secret: make([]byte, n)}); err == nil {
			t.Errorf("a %d-byte secret was accepted", n)
		}
	}
	if _, err := web.NewHMACVerifier(web.HMACConfig{Secret: make([]byte, 32)}); err != nil {
		t.Errorf("a 32-byte secret was refused: %v", err)
	}
}

// The verifier's pool builds keyed MACs from the secret whenever a GC has
// emptied it, long after construction. A caller zeroing its key material once
// the verifier exists — the hygiene this package should reward — must not
// change the key those later MACs are built with.
func TestNewHMACVerifierCopiesTheSecret(t *testing.T) {
	secret := bytes.Clone(testSecret)
	v := verifier(t, web.HMACConfig{Secret: secret})
	clear(secret)
	// Two cycles: the first moves pooled MACs to the victim cache, the second
	// drops them, so the next Get has to build a fresh one from the key.
	runtime.GC()
	runtime.GC()

	token := hs256(t, map[string]any{"sub": "alice", "exp": float64(1_700_000_600)})
	if _, err := v.Verify(token); err != nil {
		t.Fatalf("Verify after the caller cleared its secret: %v", err)
	}
}

func TestBearer(t *testing.T) {
	for _, tc := range []struct {
		name, header, want string
		ok                 bool
	}{
		{"absent", "", "", false},
		{"bearer", "Bearer abc.def.ghi", "abc.def.ghi", true},
		{"lower case scheme", "bearer abc.def.ghi", "abc.def.ghi", true},
		{"mixed case scheme", "BeArEr abc.def.ghi", "abc.def.ghi", true},
		{"basic", "Basic dXNlcjpwYXNz", "", false},
		{"scheme only", "Bearer ", "", false},
		{"no space", "Bearerabc", "", false},
		{"two tokens", "Bearer abc def", "", false},
		{"tab in the token", "Bearer abc\tdef", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
			if tc.header != "" {
				r.Header.Set("Authorization", tc.header)
			}
			got, d, ok := web.Bearer(r)
			if ok != tc.ok {
				t.Fatalf("ok = %v, want %v (diagnostic %+v)", ok, tc.ok, d)
			}
			if got != tc.want {
				t.Errorf("token = %q, want %q", got, tc.want)
			}
			if !ok && d.Code != web.CodeUnauthenticated {
				t.Errorf("diagnostic code = %q", d.Code)
			}
		})
	}
}

// What goes back to the requester must not say which half of a forgery was
// right. Expired and wrongly-signed are different errors for the log and the
// same diagnostic for the client.
func TestAuthenticateDoesNotLeakWhyItFailed(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	v := verifier(t, web.HMACConfig{Now: func() time.Time { return now }})

	expired := hs256(t, map[string]any{"sub": "u", "exp": now.Add(-time.Hour).Unix()})
	forged := mint(t, map[string]any{"alg": "HS256"},
		map[string]any{"sub": "u", "exp": now.Add(time.Hour).Unix()},
		[]byte("another-secret-of-at-least-32-bytes!!"))

	var diags []string
	for _, token := range []string{expired, forged} {
		r := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
		r.Header.Set("Authorization", "Bearer "+token)

		p, d, err := web.Authenticate(r, v)
		if err == nil {
			t.Fatal("a bad token was accepted")
		}
		if p.Subject != "" {
			t.Error("a principal came back from a refused token")
		}
		diags = append(diags, d.Code+"/"+d.MessageID)
	}
	if diags[0] != diags[1] {
		t.Errorf("the client can tell an expired token from a forged one: %q vs %q", diags[0], diags[1])
	}
	// And the internal errors are still distinguishable, for the log.
	if errors.Is(errFor(t, v, expired), web.ErrTokenSignature) {
		t.Error("an expired token was reported as a signature failure")
	}
	if !errors.Is(errFor(t, v, forged), web.ErrTokenSignature) {
		t.Error("a forged token was not reported as a signature failure")
	}
}

func errFor(t *testing.T, v web.Verifier, token string) error {
	t.Helper()
	_, err := v.Verify(token)
	return err
}

// Authenticate must report a missing credential without calling the verifier
// at all — there is nothing to verify, and a verifier asked to check "" is a
// verifier one bug away from saying yes.
func TestAuthenticateWithNoHeaderDoesNotCallTheVerifier(t *testing.T) {
	called := false
	v := verifierFunc(func(string) (web.Principal, error) {
		called = true
		return web.Principal{}, nil
	})

	r := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	p, d, err := web.Authenticate(r, v)
	// Non-nil, so `if err != nil` refuses; distinguishable, so a route that
	// admits anonymous callers can tell "none" from "rejected".
	if !errors.Is(err, web.ErrCredentialsMissing) {
		t.Fatalf("err = %v, want ErrCredentialsMissing: a nil error would fail open for the usual err != nil check", err)
	}
	if called {
		t.Error("the verifier was asked to check an absent credential")
	}
	if d.Code != web.CodeUnauthenticated || p.Subject != "" {
		t.Errorf("diagnostic = %+v, principal = %+v", d, p)
	}
}

type verifierFunc func(string) (web.Principal, error)

func (f verifierFunc) Verify(token string) (web.Principal, error) { return f(token) }

// Two Authorization fields are two credentials, and Header.Get would quietly
// authenticate on the first while another hop may pick the other. Refused,
// even when both lines carry the same bytes — whether they "agree" is not a
// question an authentication boundary should answer creatively.
func TestBearerRefusesDuplicateAuthorization(t *testing.T) {
	for _, second := range []string{"Bearer other.token.x", "Bearer abc.def.ghi"} {
		r := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
		r.Header.Add("Authorization", "Bearer abc.def.ghi")
		r.Header.Add("Authorization", second)
		got, d, ok := web.Bearer(r)
		if ok || got != "" {
			t.Fatalf("Bearer accepted one of two Authorization fields: token %q", got)
		}
		if d.Code != web.CodeUnauthenticated {
			t.Errorf("diagnostic code = %q", d.Code)
		}
	}
}

// A token has exactly one spelling. The lenient base64 decoder skipped CR and
// LF and ignored the unused low bits of the last character, so one signed
// token had many strings that all verified — and anything keyed on the string
// (a revocation list, a replay cache) was bypassed by re-spelling it.
func TestVerifyRefusesAlternateSpellingsOfAToken(t *testing.T) {
	v := verifier(t, web.HMACConfig{})
	token := hs256(t, map[string]any{"sub": "alice", "exp": 1_700_000_600})
	if _, err := v.Verify(token); err != nil {
		t.Fatalf("the canonical spelling was refused: %v", err)
	}

	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
	// A 32-byte signature is 43 characters carrying 258 bits: the last
	// character's two low bits are padding. Flipping one leaves the decoded
	// signature unchanged under the lenient decoder.
	last := strings.IndexByte(alphabet, token[len(token)-1])
	trailing := token[:len(token)-1] + string(alphabet[last^1])
	first := strings.IndexByte(token, '.')

	for name, spelling := range map[string]string{
		"padding bits":   trailing,
		"LF in header":   token[:2] + "\n" + token[2:],
		"CRLF in claims": token[:first+3] + "\r\n" + token[first+3:],
		"trailing LF":    token + "\n",
		"padding char":   token + "=",
	} {
		if _, err := v.Verify(spelling); !errors.Is(err, web.ErrTokenMalformed) {
			t.Errorf("%s: err = %v, want ErrTokenMalformed", name, err)
		}
	}
}

// A rotation keeps tokens signed under the retired key valid until they
// expire, and only the keys named are believed.
func TestVerifyAcceptsPreviousSecretsDuringARotation(t *testing.T) {
	next := []byte("the-new-secret-that-is-also-32-bytes-long")
	claims := map[string]any{"sub": "alice", "exp": 1_700_000_600}
	header := map[string]any{"alg": "HS256", "typ": "JWT"}
	old := bytes.Clone(testSecret)
	v := verifier(t, web.HMACConfig{Secret: next, Previous: [][]byte{old}})
	clear(old) // copied at construction, like Secret
	runtime.GC()
	runtime.GC()

	if _, err := v.Verify(mint(t, header, claims, next)); err != nil {
		t.Errorf("a token under the current secret was refused: %v", err)
	}
	if _, err := v.Verify(mint(t, header, claims, testSecret)); err != nil {
		t.Errorf("a token under the previous secret was refused: %v", err)
	}
	other := []byte("a-third-secret-nobody-configured-here!!!")
	if _, err := v.Verify(mint(t, header, claims, other)); !errors.Is(err, web.ErrTokenSignature) {
		t.Errorf("a token under an unknown secret: err = %v, want ErrTokenSignature", err)
	}
}

func TestNewHMACVerifierRefusesAShortPreviousSecret(t *testing.T) {
	_, err := web.NewHMACVerifier(web.HMACConfig{Secret: testSecret, Previous: [][]byte{testSecret, make([]byte, 31)}})
	if err == nil {
		t.Fatal("a 31-byte previous secret was accepted")
	}
}
