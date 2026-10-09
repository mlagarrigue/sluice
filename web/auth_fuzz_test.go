package web_test

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"strings"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/web"
)

// FuzzVerify feeds the verifier arbitrary tokens. A bearer token is whatever
// the caller dialled in with, so the parser must never panic, and whatever it
// accepts must be a three-segment token whose signature the secret really
// produced and whose expiry is still ahead.
func FuzzVerify(f *testing.F) {
	now := time.Unix(1_700_000_000, 0)
	sign := func(header, payload string) string {
		signed := base64.RawURLEncoding.EncodeToString([]byte(header)) + "." +
			base64.RawURLEncoding.EncodeToString([]byte(payload))
		mac := hmac.New(sha256.New, testSecret)
		mac.Write([]byte(signed))
		return signed + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	}
	f.Add(sign(`{"alg":"HS256","typ":"JWT"}`, `{"sub":"a","exp":1700000600}`))
	f.Add(sign(`{"alg":"HS256"}`, `{"sub":"a","exp":1700000600,"nbf":1699999000,"iat":1699999000,"aud":["x","api"],"iss":"me","tenant":"t1","scope":"a b"}`))
	f.Add(sign(`{"alg":"none"}`, `{"exp":1700000600}`))
	f.Add(sign(`{"alg":"HS256","crit":["x"]}`, `{"exp":1700000600}`))
	f.Add(sign(`{"alg":"HS256"}`, `{"exp":1e300}`))
	f.Add(sign(`{"alg":"HS256"}`, `{"exp":-1e300}`))
	f.Add(sign(`{"alg":"HS256"}`, `[]`))
	f.Add("a.b.c")
	f.Add("..")
	f.Add("a.b.c.d")
	f.Add("")

	v, err := web.NewHMACVerifier(web.HMACConfig{
		Secret: testSecret, Audience: "api", Leeway: time.Minute,
		Now: func() time.Time { return now },
	})
	if err != nil {
		f.Fatal(err)
	}
	f.Fuzz(func(t *testing.T, token string) {
		p, err := v.Verify(token)
		if err != nil {
			if p.Subject != "" || p.Claims != nil {
				t.Fatalf("a refused token returned a principal: %+v", p)
			}
			return
		}
		if strings.Count(token, ".") != 2 {
			t.Fatalf("accepted a token without exactly three segments: %q", token)
		}
		last := strings.LastIndexByte(token, '.')
		mac := hmac.New(sha256.New, testSecret)
		mac.Write([]byte(token[:last]))
		sig, derr := base64.RawURLEncoding.DecodeString(token[last+1:])
		if derr != nil || !hmac.Equal(sig, mac.Sum(nil)) {
			t.Fatalf("accepted a token the secret did not sign: %q", token)
		}
		if now.After(p.ExpiresAt.Add(time.Minute)) {
			t.Fatalf("accepted a token that expired at %v: %q", p.ExpiresAt, token)
		}
	})
}
