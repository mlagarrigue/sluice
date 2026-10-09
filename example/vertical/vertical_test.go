package vertical_test

import (
	"bufio"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/database/postgres"
	"github.com/mlagarrigue/sluice/example/vertical"
	"github.com/mlagarrigue/sluice/net/httpstream"
	"github.com/mlagarrigue/sluice/web"
)

var secret = []byte("a-vertical-example-secret-of-32-bytes!")

func dial(t *testing.T) *postgres.Conn {
	t.Helper()
	addr := os.Getenv("SLUICE_PG")
	if addr == "" {
		t.Skip("SLUICE_PG is not set: no server to run the vertical against")
	}
	user := os.Getenv("SLUICE_PG_USER")
	if user == "" {
		user = "postgres"
	}
	db := os.Getenv("SLUICE_PG_DB")
	if db == "" {
		db = user
	}
	nc, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := postgres.Startup(nc, postgres.StartupConfig{
		User: user, Database: db, Password: os.Getenv("SLUICE_PG_PASSWORD"),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// fixture builds the table and the row-level-security policy the vertical runs
// under. FORCE matters: without it the table's owner — which is whoever just
// created it — is exempt from its own policy, and every assertion below would
// pass by seeing everything.
func fixture(t *testing.T, conn *postgres.Conn) {
	t.Helper()
	if superuser(t, conn) {
		t.Skip("the test role is a superuser, which bypasses row-level security entirely")
	}
	for _, s := range []string{
		`DROP TABLE IF EXISTS order_lines`,
		`CREATE TABLE order_lines (
		   order_id bigint NOT NULL, line_index int NOT NULL,
		   tenant text NOT NULL, sale_qty bigint NOT NULL, price bigint NOT NULL,
		   PRIMARY KEY (order_id, line_index))`,
		`INSERT INTO order_lines VALUES
		   (1, 0, 'acme',   5, 1000), (1, 1, 'acme',   3,  500),
		   (2, 0, 'globex', 7, 2000), (2, 1, 'globex', 1,  250)`,
		`ALTER TABLE order_lines ENABLE ROW LEVEL SECURITY`,
		`ALTER TABLE order_lines FORCE ROW LEVEL SECURITY`,
		`CREATE POLICY tenant_isolation ON order_lines
		   USING (tenant = current_setting('app.tenant_id', true))`,
	} {
		if err := conn.Exec(t.Context(), s); err != nil {
			t.Fatalf("fixture %q: %v", s, err)
		}
	}
	t.Cleanup(func() { _ = conn.Exec(t.Context(), `DROP TABLE IF EXISTS order_lines`) })
}

func superuser(t *testing.T, conn *postgres.Conn) bool {
	t.Helper()
	var is bool
	src := conn.Query(t.Context(), `SELECT current_setting('is_superuser')`, nil,
		postgres.QueryConfig{BatchRows: 2, AllRows: true})
	src.Stream()(func(b sluice.Batch[postgres.Row]) bool {
		for _, r := range b.Items {
			v, _ := r.Value(0)
			is = strings.EqualFold(string(v), "on")
		}
		return true
	})
	if err := src.Err(); err != nil {
		t.Fatal(err)
	}
	return is
}

func token(t *testing.T, tenant string) string {
	t.Helper()
	enc := func(v any) string {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		return base64.RawURLEncoding.EncodeToString(b)
	}
	signed := enc(map[string]any{"alg": "HS256", "typ": "JWT"}) + "." +
		enc(map[string]any{
			"sub": "user-1", "tenant": tenant,
			"exp": time.Now().Add(time.Hour).Unix(),
		})
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(signed))
	return signed + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

// build starts the whole experimental path: the service over the real
// connector, the stream handler over the service, and httpstream over a
// loopback socket. What the tests dial is the same protocol stack a client
// would.
func build(t *testing.T) (*vertical.Service, string) {
	t.Helper()
	conn := dial(t)
	fixture(t, conn)

	svc := vertical.NewService(conn, 64, 2*time.Millisecond)
	t.Cleanup(func() { _ = svc.Close() })

	v, err := web.NewHMACVerifier(web.HMACConfig{Secret: secret, TenantClaim: "tenant"})
	if err != nil {
		t.Fatal(err)
	}
	h, err := vertical.StreamHandler(svc, v, web.ClaimAuthorizer{}, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = httpstream.Serve(ctx, ln, httpstream.Config{}, h) }()
	t.Cleanup(func() { cancel(); <-done })
	return svc, ln.Addr().String()
}

// rawPatch renders one request as the bytes a client would send. The tests
// speak wire-level HTTP/1.1 on purpose: pipelining — several requests written
// before any answer is read — is the shape this transport exists for, and no
// high-level client produces it.
func rawPatch(tok string, order int64, line int, qty int64) string {
	body := fmt.Sprintf(`{"saleQty":%d}`, qty)
	req := fmt.Sprintf("PATCH /orders/%d/lines/%d HTTP/1.1\r\nHost: vertical\r\n", order, line)
	if tok != "" {
		req += "Authorization: Bearer " + tok + "\r\n"
	}
	return req + fmt.Sprintf("Content-Type: application/json\r\nContent-Length: %d\r\n\r\n%s",
		len(body), body)
}

// readResponse reads one response off the connection: the status, and the
// body decoded as JSON when there is one.
func readResponse(t *testing.T, br *bufio.Reader) (int, map[string]any) {
	t.Helper()
	line, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("reading a status line: %v", err)
	}
	parts := strings.SplitN(strings.TrimRight(line, "\r\n"), " ", 3)
	if len(parts) < 2 {
		t.Fatalf("not a status line: %q", line)
	}
	status, err := strconv.Atoi(parts[1])
	if err != nil {
		t.Fatalf("not a status in %q: %v", line, err)
	}
	length := 0
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatalf("reading a header: %v", err)
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			break
		}
		if name, value, ok := strings.Cut(line, ": "); ok && strings.EqualFold(name, "content-length") {
			if length, err = strconv.Atoi(value); err != nil {
				t.Fatalf("content-length %q: %v", value, err)
			}
		}
	}
	if length == 0 {
		return status, nil
	}
	raw := make([]byte, length)
	for read := 0; read < length; {
		n, err := br.Read(raw[read:])
		read += n
		if err != nil {
			t.Fatalf("reading a body: %v", err)
		}
	}
	var body map[string]any
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("body is not JSON: %v\n%s", err, raw)
	}
	return status, body
}

// patch sends one amendment on its own connection and reads the answer.
func patch(t *testing.T, addr, tenant string, order int64, line int, qty int64) (int, map[string]any) {
	t.Helper()
	tok := ""
	if tenant != "" {
		tok = token(t, tenant)
	}
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	_ = c.SetDeadline(time.Now().Add(30 * time.Second))
	if _, err := c.Write([]byte(rawPatch(tok, order, line, qty))); err != nil {
		t.Fatal(err)
	}
	return readResponse(t, bufio.NewReader(c))
}

// buildOffline is build without the database: the service's pipeline only
// touches the connection when a batch reaches it, and these tests assert on
// requests that are refused before that point.
func buildOffline(t *testing.T) (*vertical.Service, string) {
	t.Helper()
	svc := vertical.NewService(nil, 64, 2*time.Millisecond)
	t.Cleanup(func() { _ = svc.Close() })

	v, err := web.NewHMACVerifier(web.HMACConfig{Secret: secret, TenantClaim: "tenant"})
	if err != nil {
		t.Fatal(err)
	}
	h, err := vertical.StreamHandler(svc, v, web.ClaimAuthorizer{}, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = httpstream.Serve(ctx, ln, httpstream.Config{}, h) }()
	t.Cleanup(func() { cancel(); <-done })
	return svc, ln.Addr().String()
}

// A refused request is answered at the boundary that refused it and never
// costs a round trip — which is why none of this needs a database to assert.
func TestVerticalRefusalsNeverReachThePipeline(t *testing.T) {
	svc, addr := buildOffline(t)

	send := func(t *testing.T, raw string) (int, map[string]any) {
		t.Helper()
		c, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = c.Close() }()
		_ = c.SetDeadline(time.Now().Add(30 * time.Second))
		if _, err := c.Write([]byte(raw)); err != nil {
			t.Fatal(err)
		}
		return readResponse(t, bufio.NewReader(c))
	}

	t.Run("no token is 401", func(t *testing.T) {
		if status, _ := send(t, rawPatch("", 1, 0, 9)); status != 401 {
			t.Errorf("status = %d, want 401", status)
		}
	})

	t.Run("no route is 404", func(t *testing.T) {
		status, _ := send(t, "GET /nothing HTTP/1.1\r\nHost: v\r\n\r\n")
		if status != 404 {
			t.Errorf("status = %d, want 404", status)
		}
	})

	t.Run("a wrong method is 405", func(t *testing.T) {
		status, _ := send(t, "GET /orders/1/lines/0 HTTP/1.1\r\nHost: v\r\n\r\n")
		if status != 405 {
			t.Errorf("status = %d, want 405", status)
		}
	})

	t.Run("a parameter that is not a number is 400", func(t *testing.T) {
		body := `{"saleQty":9}`
		raw := "PATCH /orders/x/lines/0 HTTP/1.1\r\nHost: v\r\n" +
			"Authorization: Bearer " + token(t, "acme") + "\r\n" +
			fmt.Sprintf("Content-Type: application/json\r\nContent-Length: %d\r\n\r\n%s", len(body), body)
		status, report := send(t, raw)
		if status != 400 {
			t.Errorf("status = %d, body %v, want 400", status, report)
		}
	})

	if batches, roundTrips := svc.Stats(); batches != 0 || roundTrips != 0 {
		t.Errorf("refusals cost %d batches and %d round trips; they must never reach the pipeline",
			batches, roundTrips)
	}
}

// The vertical, end to end: a token, a grant, a batch, a rule, a policy, an
// answer — over the experimental transport, socket included.
func TestVerticalEndToEnd(t *testing.T) {
	_, addr := build(t)

	t.Run("accepted", func(t *testing.T) {
		status, body := patch(t, addr, "acme", 1, 0, 9)
		if status != 200 {
			t.Fatalf("status = %d, body %v", status, body)
		}
		if body["accepted"] != true || body["saleQty"] != float64(9) {
			t.Errorf("body = %v", body)
		}
	})

	// A refusal carries the action that fixes it, at the same path as the
	// problem — which is what makes a 422 actionable rather than merely
	// correct.
	t.Run("refused with its remedy", func(t *testing.T) {
		status, body := patch(t, addr, "acme", 1, 0, 0)
		if status != 422 {
			t.Fatalf("status = %d, body %v", status, body)
		}
		problems, _ := body["problems"].([]any)
		remedies, _ := body["remedies"].([]any)
		if len(problems) != 1 || len(remedies) != 1 {
			t.Fatalf("%d problems and %d remedies", len(problems), len(remedies))
		}
		p := problems[0].(map[string]any)
		rm := remedies[0].(map[string]any)
		if p["path"] != rm["path"] {
			t.Errorf("the remedy is at %v and the problem at %v; a client pairs them by path",
				rm["path"], p["path"])
		}
		if rm["action"] != "Sales.Order.CorrectQty" {
			t.Errorf("remedy = %v", rm)
		}
	})

	// The boundary is the database's. This code never filters by tenant — there
	// is no predicate to delete — so the only thing keeping globex's rows away
	// from acme is the policy.
	t.Run("another tenant's row does not exist", func(t *testing.T) {
		status, body := patch(t, addr, "acme", 2, 0, 9)
		if status != 422 {
			t.Fatalf("status = %d, body %v", status, body)
		}
		problems, _ := body["problems"].([]any)
		if len(problems) != 1 {
			t.Fatalf("problems = %v", problems)
		}
		if code := problems[0].(map[string]any)["code"]; code != "Sales.Order.LineNotFound" {
			t.Errorf("code = %v, want LineNotFound", code)
		}

		// And it is genuinely there for its owner, so the previous assertion is
		// about the policy rather than about a missing row.
		if status, body := patch(t, addr, "globex", 2, 0, 9); status != 200 {
			t.Errorf("globex cannot reach its own row: status %d, body %v", status, body)
		}
	})

	t.Run("no token", func(t *testing.T) {
		if status, _ := patch(t, addr, "", 1, 0, 9); status != 401 {
			t.Errorf("status = %d, want 401", status)
		}
	})

	t.Run("no route", func(t *testing.T) {
		c, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = c.Close() }()
		_ = c.SetDeadline(time.Now().Add(30 * time.Second))
		req := "GET /nothing HTTP/1.1\r\nHost: vertical\r\n\r\n"
		if _, err := c.Write([]byte(req)); err != nil {
			t.Fatal(err)
		}
		if status, _ := readResponse(t, bufio.NewReader(c)); status != 404 {
			t.Errorf("status = %d, want 404", status)
		}
	})
}

// The objective's acceptance test: requests pipelined on one connection reach
// the gateway as the batch they already were — one gateway batch per
// transport batch, one query per tenant in it — with the round trips counted
// rather than asserted in prose.
func TestVerticalPipelinedBatchCostsOneQueryPerTenant(t *testing.T) {
	svc, addr := build(t)

	const requests = 16
	acme, globex := token(t, "acme"), token(t, "globex")

	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	_ = c.SetDeadline(time.Now().Add(30 * time.Second))

	// All the requests go out before any answer is read: two tenants
	// interleaved, so the batch exercises the per-tenant partition too.
	var out strings.Builder
	for i := range requests {
		if i%2 == 0 {
			out.WriteString(rawPatch(acme, 1, 0, int64(i%5+1)))
		} else {
			out.WriteString(rawPatch(globex, 2, 0, int64(i%5+1)))
		}
	}
	if _, err := c.Write([]byte(out.String())); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(c)
	for i := range requests {
		status, body := readResponse(t, br)
		if status != 200 {
			t.Fatalf("response %d: status %d, body %v", i, status, body)
		}
	}

	batches, roundTrips := svc.Stats()
	t.Logf("%d pipelined requests → %d gateway batches → %d round trips (2 tenants)",
		requests, batches, roundTrips)

	// The socket may split the burst across a couple of reads, so the bounds
	// are on the mechanism rather than on scheduling luck: every transport
	// batch is one gateway batch costing three round trips for its frame
	// (BEGIN, its statement_timeout, ROLLBACK) plus two per tenant (SET
	// LOCAL, the query), and nothing approaches the five a request served
	// alone would cost.
	if batches == 0 {
		t.Fatal("nothing was served")
	}
	if batches > 4 {
		t.Errorf("%d gateway batches for one pipelined burst; the transport batch is not reaching the gateway whole", batches)
	}
	if roundTrips > batches*(3+2*2) {
		t.Errorf("%d round trips for %d batches; a batch should cost its frame plus two per tenant", roundTrips, batches)
	}
	if roundTrips >= requests*5 {
		t.Errorf("%d round trips for %d requests; batching did not happen", roundTrips, requests)
	}
}

// The claim, printed rather than asserted in prose: concurrent callers become
// one batch, and a batch costs one round trip per distinct tenant. This is
// the gateway's own shape — separate connections — kept working behind the
// stream transport.
func TestVerticalBatchesByTenant(t *testing.T) {
	svc, addr := build(t)

	const callers = 32
	var wg sync.WaitGroup
	for i := range callers {
		for _, tenant := range []string{"acme", "globex"} {
			wg.Go(func() {
				order := int64(1)
				if tenant == "globex" {
					order = 2
				}
				status, body := patch(t, addr, tenant, order, 0, int64(i%5+1))
				if status != 200 {
					t.Errorf("%s: status %d — %v", tenant, status, body)
				}
			})
		}
	}
	wg.Wait()

	batches, roundTrips := svc.Stats()
	t.Logf("%d requests → %d batches → %d round trips (%d tenants)",
		callers*2, batches, roundTrips, 2)

	if batches == 0 || roundTrips == 0 {
		t.Fatal("nothing was served")
	}
	// A batch costs three round trips for its frame plus two per distinct
	// tenant, so the ratio must be small and bounded by the tenants rather
	// than by the callers.
	if roundTrips > batches*(3+2*2) {
		t.Errorf("%d round trips for %d batches; a batch should cost its frame plus two per tenant", roundTrips, batches)
	}
	if roundTrips >= int64(callers*2)*5 {
		t.Errorf("%d round trips for %d requests; batching did not happen", roundTrips, callers*2)
	}
}
