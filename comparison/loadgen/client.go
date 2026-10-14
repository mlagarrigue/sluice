package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net/http"
	"net/http/httptrace"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// inFlightPerConn is how many requests one connection carries at once.
// HTTP/1.1 without pipelining carries one; HTTP/2 and HTTP/3 will carry
// at most 8 streams (§3.5, the published ceiling), never more.
const inFlightPerConn = 1

// client is the generator's HTTP side: keep-alive connections to one
// server, HTTP/1.1 over TLS, nothing shared with Sluice.go. The budget C is
// held by the number of workers, not by the transport, so that a ramp can
// grow it between two stages: `warm` opens the connections a stage lacks,
// and the ones a smaller stage leaves idle never age out. The share of
// fresh connections goes through `fresh`, a transport that keeps nothing:
// each such request opens its connection, inside its own delays, and the
// client closes it, so that TIME_WAIT lands on the tester (§3.6).
type client struct {
	url   string
	tr    *http.Transport
	hc    *http.Client
	fresh *http.Client
}

func (c campaign) newClient() (*client, error) {
	st := c.stage
	cfg := &tls.Config{
		MinVersion: tls.VersionTLS13,
		NextProtos: []string{"http/1.1"},
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
	tr := &http.Transport{
		TLSClientConfig:     cfg,
		MaxIdleConnsPerHost: math.MaxInt32, // the budget, whatever a ramp grows it to
		IdleConnTimeout:     0,             // a connection of the budget never ages out
		ForceAttemptHTTP2:   false,
		DisableCompression:  true,
	}
	fresh := tr.Clone()
	fresh.DisableKeepAlives = true
	return &client{
		url:   st.url,
		tr:    tr,
		hc:    &http.Client{Transport: tr, Timeout: st.timeout},
		fresh: &http.Client{Transport: fresh, Timeout: st.timeout},
	}, nil
}

func (c *client) close() { c.tr.CloseIdleConnections() }

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

// warm opens the budget before the stage: `conns` requests in flight at
// once, so every TLS handshake happens now, never inside the measurement
// (§3.5). The connections an earlier stage left idle are reused, the
// missing ones opened. The answers are discarded.
func (c *client) warm(ctx context.Context, conns int) error {
	errs := make([]error, conns)
	var wg sync.WaitGroup
	for i := range conns {
		wg.Go(func() {
			r := c.do(ctx, job{req: request{kind: static, method: "GET", path: "/static"}, due: time.Now()})
			errs[i] = r.err
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

func (c *client) do(ctx context.Context, j job) result {
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
	hc := c.hc
	if j.fresh {
		hc = c.fresh
	}
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
