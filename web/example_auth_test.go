package web_test

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/web"
)

// signHS256 mints the token a client would present, as an identity provider
// signs it: base64url header and claims, then their HMAC-SHA256.
func signHS256(secret []byte, claims map[string]any) string {
	enc := base64.RawURLEncoding
	header, _ := json.Marshal(map[string]string{"alg": "HS256", "typ": "JWT"})
	body, _ := json.Marshal(claims)
	unsigned := enc.EncodeToString(header) + "." + enc.EncodeToString(body)
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(unsigned))
	return unsigned + "." + enc.EncodeToString(mac.Sum(nil))
}

// tenantRule grants a principal its own tenant, and nothing to a principal
// that has none.
type tenantRule struct{}

func (tenantRule) Authorize(p web.Principal) (web.Grant, error) {
	if p.Tenant == "" {
		return web.Grant{}, errors.New("no tenant claim")
	}
	return web.Grant{Tenant: p.Tenant}, nil
}

// The boundary of a request, in its two steps: who is calling
// (Authenticate, with a verifier built once by NewHMACVerifier), then what
// they may do (Authorize). The tenant comes from the verified token, never
// from a header the client chose, and it is what the request's batch is
// keyed by downstream.
func ExampleAuthenticate() {
	secret := []byte("a 32-byte secret drawn from crypto/rand!")
	now := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	verifier, err := web.NewHMACVerifier(web.HMACConfig{
		Secret:      secret,
		TenantClaim: "tenant",
		Now:         func() time.Time { return now },
	})
	if err != nil {
		fmt.Println(err)
		return
	}

	r := httptest.NewRequest("GET", "/orders", nil)
	r.Header.Set("Authorization", "Bearer "+signHS256(secret, map[string]any{
		"sub": "ada", "tenant": "acme", "exp": now.Add(time.Hour).Unix(),
	}))

	p, d, err := web.Authenticate(r, verifier)
	if err != nil {
		fmt.Println(web.BearerChallenge("orders", d), err)
		return
	}
	g, _, err := web.Authorize(p, tenantRule{})
	if err != nil {
		fmt.Println("refused:", err)
		return
	}
	fmt.Println(p.Subject, "may act for", g.Tenant)
	// Output: ada may act for acme
}

// A handler's bug answers 500 instead of closing the connection, and the
// observer sees the panic first, to log it with what the request was.
func ExampleRecover() {
	h := web.Recover(func(v any) { fmt.Println("observed:", v) },
		http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			panic("nil order")
		}))

	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/orders/7", nil))
	fmt.Println(w.Code)
	// Output:
	// observed: nil order
	// 500
}

// A sentinel the library raised — here an overflow under the Fail policy —
// is answered by the caller's own function, with the status it chooses; a
// bug would still reach Recover, outside.
func ExampleTryHandler() {
	h := web.Recover(func(v any) { fmt.Println("a bug:", v) },
		web.TryHandler(
			func(err error) { fmt.Println("sentinel:", errors.Is(err, sluice.ErrOverflow)) },
			func(w http.ResponseWriter, _ *http.Request, _ error) {
				w.WriteHeader(http.StatusServiceUnavailable)
			},
			http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				panic(sluice.ErrOverflow)
			})))

	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("POST", "/orders", nil))
	fmt.Println(w.Code)
	// Output:
	// sentinel: true
	// 503
}
