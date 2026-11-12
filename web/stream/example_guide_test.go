package stream_test

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/diagnostics"
	"github.com/mlagarrigue/sluice/net/httpstream"
	"github.com/mlagarrigue/sluice/web"
	"github.com/mlagarrigue/sluice/web/stream"
)

// One envelope per request of the batch, in the batch's order: the
// position each answer must be written at.
func ExampleExchanges() {
	b := sluice.Batch[httpstream.Request]{Items: []httpstream.Request{
		{Method: []byte("GET"), Target: []byte("/a")},
		{Method: []byte("GET"), Target: []byte("/b")},
	}}
	ex := stream.Exchanges(nil, b)
	for i := range ex {
		fmt.Println(string(ex[i].Request.Target), ex[i].Refused())
	}
	// Output:
	// /a false
	// /b false
}

// A wildcard read as an integer; a value that is not one refuses its
// exchange with 400 and leaves the others alone.
func ExamplePathInt64() {
	rt := stream.NewRouter(stream.Route{Method: "GET", Pattern: "/orders/{order}"})
	ex := stream.Exchanges(nil, sluice.Batch[httpstream.Request]{Items: []httpstream.Request{
		{Method: []byte("GET"), Target: []byte("/orders/7")},
		{Method: []byte("GET"), Target: []byte("/orders/x")},
	}})
	rt.Route(ex)
	for i := range ex {
		order, ok := stream.PathInt64(&ex[i], 0, "order")
		fmt.Println(order, ok, ex[i].Status)
	}
	// Output:
	// 7 true 0
	// 0 false 400
}

// Every exchange answered at its position: the refused ones with their
// status and report, the others with what serve returns.
func ExampleRender() {
	ex := stream.Exchanges(nil, sluice.Batch[httpstream.Request]{Items: []httpstream.Request{
		{Method: []byte("GET"), Target: []byte("/a")},
		{Method: []byte("GET"), Target: []byte("/b")},
	}})
	ex[1].Refuse(409, diagnostics.NewDiagnostic(diagnostics.Error, "Sales.Order.Locked", diagnostics.Path{}))
	out := stream.Render(ex, func(i int) (int, any) { return 200, map[string]int{"i": i} })
	for _, r := range out.Items {
		fmt.Println(r.Status)
	}
	// Output:
	// 200
	// 409
}

// Every exchange's principal from its bearer token, along the batch: one
// without a valid token is refused with 401 and its challenge.
func ExampleAuthenticate() {
	secret := []byte("a 32-byte secret drawn from crypto/rand!")
	now := time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC)
	v, err := web.NewHMACVerifier(web.HMACConfig{Secret: secret, TenantClaim: "tenant", Now: func() time.Time { return now }})
	if err != nil {
		fmt.Println(err)
		return
	}
	token := signHS256(secret, map[string]any{"sub": "ada", "tenant": "acme", "exp": now.Add(time.Hour).Unix()})
	ex := stream.Exchanges(nil, sluice.Batch[httpstream.Request]{Items: []httpstream.Request{
		{Method: []byte("GET"), Target: []byte("/a"), Headers: []httpstream.Header{{Name: []byte("authorization"), Value: []byte("Bearer " + token)}}},
		{Method: []byte("GET"), Target: []byte("/b")},
	}})
	stream.Authenticate(ex, v, "orders")
	for i := range ex {
		fmt.Printf("%q %d\n", ex[i].Principal.Subject, ex[i].Status)
	}
	// Output:
	// "ada" 0
	// "" 401
}

// tenantRule grants a principal its own tenant, and nothing to a principal
// without one.
type tenantRule struct{}

func (tenantRule) Authorize(p web.Principal) (web.Grant, error) {
	if p.Tenant == "" {
		return web.Grant{}, errors.New("no tenant claim")
	}
	return web.Grant{Tenant: p.Tenant}, nil
}

// Every exchange's grant from its principal; the authorizer's refusal is a
// 403 on that exchange alone.
func ExampleAuthorize() {
	ex := stream.Exchanges(nil, sluice.Batch[httpstream.Request]{Items: []httpstream.Request{
		{Method: []byte("GET"), Target: []byte("/a")},
		{Method: []byte("GET"), Target: []byte("/b")},
	}})
	ex[0].Principal = web.Principal{Subject: "ada", Tenant: "acme"}
	ex[1].Principal = web.Principal{Subject: "bob"}
	stream.Authorize(ex, tenantRule{})
	for i := range ex {
		fmt.Printf("%s %q %d\n", ex[i].Principal.Subject, ex[i].Grant.Tenant, ex[i].Status)
	}
	// Output:
	// ada "acme" 0
	// bob "" 403
}

// signHS256 mints the token a client would present, as an identity provider
// signs it.
func signHS256(secret []byte, claims map[string]any) string {
	enc := base64.RawURLEncoding
	header, _ := json.Marshal(map[string]string{"alg": "HS256", "typ": "JWT"})
	body, _ := json.Marshal(claims)
	unsigned := enc.EncodeToString(header) + "." + enc.EncodeToString(body)
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(unsigned))
	return unsigned + "." + enc.EncodeToString(mac.Sum(nil))
}
