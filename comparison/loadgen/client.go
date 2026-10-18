package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/http/httptrace"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
)

// The protocols the generator speaks. One virtual client is one connection
// whatever the protocol (§3.5): HTTP/1.1 carries one request at a time on
// it, HTTP/2 and HTTP/3 at most streamsPerConn.
const (
	protoH1 = "h1"
	protoH2 = "h2"
	protoH3 = "h3"
)

// streamsPerConn is the published ceiling on what one HTTP/2 or HTTP/3
// connection carries at once (§3.5): a real client keeps few streams in
// flight, and a hundred of them would measure the server's parallelism per
// connection, not its cost.
const streamsPerConn = 8

// inFlightPerConn is how many requests one connection carries at once
// under the protocol: the workers a stage runs are C times this.
func (st stage) inFlightPerConn() int {
	if st.proto == protoH1 {
		return 1
	}
	return streamsPerConn
}

// client is the generator's HTTP side: keep-alive connections to one
// server, nothing shared with Sluice.go. Each connection of the budget C
// is a transport of its own, so that the transport can never coalesce two
// virtual clients onto one connection (net/http's HTTP/2 and quic-go's
// HTTP/3 both share a connection per host) and never opens a second one:
// what the budget says is what is on the wire. Worker w of a stage uses
// connection w / inFlightPerConn. A ramp grows the budget between two
// stages: `warm` opens the connections a stage lacks, and the ones a
// smaller stage leaves idle never age out. A fresh connection (§3.6) is
// a transport that lives for one request, opened inside its delays and
// closed by the client, so that TIME_WAIT lands on the tester.
type client struct {
	url     string
	proto   string
	timeout time.Duration
	tls     *tls.Config

	mu    sync.Mutex
	conns []conn
}

// conn is one virtual client: an http.Client over a transport that holds
// exactly one connection.
type conn struct {
	hc    *http.Client
	close func()
}

func (c campaign) newClient() (*client, error) {
	st := c.stage
	cfg := &tls.Config{MinVersion: tls.VersionTLS13}
	switch st.proto {
	case protoH1:
		cfg.NextProtos = []string{"http/1.1"}
	case protoH2:
		cfg.NextProtos = []string{"h2"}
	case protoH3:
		// quic-go sets "h3" itself.
	}
	if st.ca == "" {
		// The apparatus talks to a server it started itself, often under a
		// certificate generated at startup; -ca is for the campaign, whose
		// certificate is fixed and published.
		cfg.InsecureSkipVerify = true //nolint:gosec // G402: see above
	} else {
		pem, err := os.ReadFile(st.ca)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("-ca %s: no certificate in it", st.ca)
		}
		cfg.RootCAs = pool
	}
	return &client{url: st.url, proto: st.proto, timeout: st.timeout, tls: cfg}, nil
}

// dial makes one connection's transport; the connection itself opens on
// the first request. Each transport gets its own copy of the TLS config:
// net/http's HTTP/2 setup writes NextProtos into the one it is given.
func (c *client) dial() conn {
	cfg := c.tls.Clone()
	if c.proto == protoH3 {
		// No Dial of our own: quic-go's default one is the only path that
		// fires the httptrace hooks (ConnectDone, TLSHandshakeDone). It
		// dials early, which is a plain 1-RTT handshake with no session
		// cache in the config.
		tr := &http3.Transport{
			TLSClientConfig:    cfg,
			QUICConfig:         &quic.Config{MaxIdleTimeout: 24 * time.Hour}, // a connection of the budget never ages out
			DisableCompression: true,
		}
		return conn{hc: &http.Client{Transport: tr, Timeout: c.timeout}, close: func() { _ = tr.Close() }}
	}
	tr := &http.Transport{
		TLSClientConfig:    cfg,
		MaxConnsPerHost:    1, // the virtual client, one connection
		IdleConnTimeout:    0, // a connection of the budget never ages out
		ForceAttemptHTTP2:  c.proto == protoH2,
		DisableCompression: true,
	}
	if c.proto == protoH1 {
		tr.MaxIdleConnsPerHost = 1
	}
	return conn{hc: &http.Client{Transport: tr, Timeout: c.timeout}, close: tr.CloseIdleConnections}
}

func (c *client) close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, cn := range c.conns {
		cn.close()
	}
	c.conns = nil
}

// freshDraw says which requests of a stage open a fresh connection: a
// function of (seed, index) like the full validation, so that every
// contender of a round sees the same ones.
func (st stage) freshDraw() func(i int) bool {
	if st.newConns == 0 {
		return func(int) bool { return false }
	}
	return func(i int) bool {
		return rand.New(rand.NewPCG(st.seed^freshSalt, uint64(i))).IntN(100) < st.newConns //nolint:gosec // G404: a seeded draw is the point
	}
}

// freshSalt keeps the fresh draw apart from the full-validation draw of
// the same (seed, index).
const freshSalt = 0x66726573685f636f

// warm opens the budget before the stage: one request on each connection
// the budget lacks, all at once, so every TLS handshake happens now, never
// inside the measurement (§3.5). The connections an earlier stage opened
// are kept. The answers are discarded.
func (c *client) warm(ctx context.Context, conns int) error {
	c.mu.Lock()
	from := len(c.conns)
	for len(c.conns) < conns {
		c.conns = append(c.conns, c.dial())
	}
	c.mu.Unlock()
	errs := make([]error, conns-from)
	var wg sync.WaitGroup
	for i := from; i < conns; i++ {
		wg.Go(func() {
			r := c.do(ctx, c.conns[i].hc, job{req: request{kind: static, method: "GET", path: "/static"}, due: time.Now()})
			errs[i-from] = r.err
		})
	}
	wg.Wait()
	return errors.Join(errs...)
}

// result is one request's outcome: the two delays from the due instant,
// and what went wrong if anything.
type result struct {
	ttfb, ttlb time.Duration
	wait       time.Duration // due → taken by a worker
	status     int
	err        error
	connect    bool // err is a failed connect or handshake (§3.6)
	badKey     bool // err is the response contract broken (§4)
	handshake  bool // a TLS handshake happened inside this request's delays
}

// send runs one job on connection `cn` of the budget, or on a fresh one.
func (c *client) send(ctx context.Context, cn int, j job) result {
	if !j.fresh {
		return c.do(ctx, c.conns[cn].hc, j)
	}
	f := c.dial()
	defer f.close()
	return c.do(ctx, f.hc, j)
}

func (c *client) do(ctx context.Context, hc *http.Client, j job) result {
	res := result{wait: time.Since(j.due)}
	req, err := http.NewRequestWithContext(ctx, j.req.method, c.url+j.req.path, bytes.NewReader(j.req.body)) //nolint:gosec // G704: the URL is the -url flag, a load generator's whole purpose
	if err != nil {
		res.err = err
		return res
	}
	if j.req.body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	var firstByte time.Time
	// A dial the transport started for this request can outlive it, when
	// another connection became free first: these two are written from
	// that goroutine, hence the atomics.
	var connectErr, handshake atomic.Bool
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), &httptrace.ClientTrace{
		GotFirstResponseByte: func() { firstByte = time.Now() },
		ConnectDone: func(_, _ string, err error) {
			if err != nil {
				connectErr.Store(true)
			}
		},
		TLSHandshakeDone: func(_ tls.ConnectionState, err error) {
			if err != nil {
				connectErr.Store(true)
			}
			handshake.Store(true)
		},
	}))
	resp, err := hc.Do(req) //nolint:gosec // G704: same
	res.handshake = handshake.Load()
	if err != nil {
		res.err = err
		res.connect = connectErr.Load()
		return res
	}
	defer resp.Body.Close() //nolint:errcheck // the body was read to the end or the error is already reported
	body, err := io.ReadAll(resp.Body)
	res.ttlb = time.Since(j.due)
	res.ttfb = firstByte.Sub(j.due)
	res.status = resp.StatusCode
	if err != nil {
		res.err = fmt.Errorf("reading the body: %w", err)
		return res
	}
	if resp.ContentLength >= 0 && int64(len(body)) != resp.ContentLength {
		res.err = fmt.Errorf("body of %d bytes, Content-Length %d", len(body), resp.ContentLength)
		res.badKey = true
		return res
	}
	if resp.StatusCode != http.StatusOK {
		res.err = fmt.Errorf("status %d", resp.StatusCode)
		return res
	}
	if err := check(j.req, body); err != nil {
		res.err = err
		res.badKey = true
	}
	return res
}
