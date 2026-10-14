package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"os"
	"sync"
	"time"
)

// inFlightPerConn is how many requests one connection carries at once.
// HTTP/1.1 without pipelining carries one; HTTP/2 and HTTP/3 will carry
// at most 8 streams (§3.5, the published ceiling), never more.
const inFlightPerConn = 1

// client is the generator's HTTP side: `conns` keep-alive connections to
// one server, HTTP/1.1 over TLS, nothing shared with Sluice.go.
type client struct {
	url string
	tr  *http.Transport
	hc  *http.Client
}

func (st stage) newClient() (*client, error) {
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
		MaxConnsPerHost:     st.conns,
		MaxIdleConnsPerHost: st.conns,
		IdleConnTimeout:     0, // a connection of the budget never ages out
		ForceAttemptHTTP2:   false,
		DisableCompression:  true,
	}
	return &client{
		url: st.url,
		tr:  tr,
		hc:  &http.Client{Transport: tr, Timeout: st.timeout},
	}, nil
}

func (c *client) close() { c.tr.CloseIdleConnections() }

// warm opens the budget before the stage: `conns` requests in flight at
// once, so every TLS handshake happens now, never inside the measurement
// (§3.5). The answers are discarded.
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
	var connectErr error
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), &httptrace.ClientTrace{
		GotFirstResponseByte: func() { firstByte = time.Now() },
		ConnectDone: func(_, _ string, err error) {
			if err != nil {
				connectErr = err
			}
		},
		TLSHandshakeDone: func(_ tls.ConnectionState, err error) {
			if err != nil {
				connectErr = err
			}
		},
	}))
	resp, err := c.hc.Do(req) //nolint:gosec // G704: same
	if err != nil {
		res.err = err
		res.connect = connectErr != nil
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
