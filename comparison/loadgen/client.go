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
	"net"
	"net/http"
	"net/http/httptrace"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
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
	url      string
	proto    string
	encoding string        // the content coding asked for, "" for identity (§2.3 bis)
	deadline time.Duration // a request's whole life from its due instant (§1.2)
	tls      *tls.Config

	// What the server did to the budget's connections: every dial after
	// `warm` is a reconnection, and every connection that ends while the
	// generator holds it was ended by the server, since the generator
	// closes none before the end, except an HTTP/1.1 connection under a
	// request it gave up on (see `abandon`). A fresh connection is the
	// generator's own and counts in neither.
	dials, closes atomic.Int64

	mu    sync.Mutex
	conns []conn
	// negotiated is every distinct TLS setting a handshake ended on: one,
	// unless the server is not what it says.
	negotiated []tlsSetting
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
	if st.resume {
		// One cache for every connection: the budget's handshakes, before
		// the clock, leave the tickets the fresh ones resume with.
		cfg.ClientSessionCache = copyingCache{tls.NewLRUClientSessionCache(1024)}
	}
	return &client{url: st.url, proto: st.proto, encoding: st.encoding, deadline: st.deadline(), tls: cfg}, nil
}

// dial makes one connection's transport; the connection itself opens on
// the first request. Each transport gets its own copy of the TLS config:
// net/http's HTTP/2 setup writes NextProtos into the one it is given.
// The connections of the budget are counted when they open and when they
// end; a fresh one is not.
func (c *client) dial(budget bool) conn {
	cfg := c.tls.Clone()
	if c.proto == protoH3 {
		tr := &http3.Transport{
			TLSClientConfig:    cfg,
			QUICConfig:         &quic.Config{MaxIdleTimeout: 24 * time.Hour}, // a connection of the budget never ages out
			DisableCompression: true,
			Dial: func(ctx context.Context, addr string, tlsCfg *tls.Config, qcfg *quic.Config) (*quic.Conn, error) {
				return c.dialQUIC(ctx, addr, tlsCfg, qcfg, budget)
			},
		}
		return conn{hc: &http.Client{Transport: tr}, close: func() { _ = tr.Close() }}
	}
	var d net.Dialer
	tr := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			nc, err := d.DialContext(ctx, network, addr)
			if err != nil || !budget {
				return nc, err
			}
			c.dials.Add(1)
			return &watched{Conn: nc, closes: &c.closes}, nil
		},
		TLSClientConfig:    cfg,
		MaxConnsPerHost:    1, // the virtual client, one connection
		IdleConnTimeout:    0, // a connection of the budget never ages out
		ForceAttemptHTTP2:  c.proto == protoH2,
		DisableCompression: true,
	}
	if c.proto == protoH1 {
		tr.MaxIdleConnsPerHost = 1
	}
	return conn{hc: &http.Client{Transport: tr}, close: tr.CloseIdleConnections}
}

// dialQUIC is quic-go's own dial, which an http3.Transport with no Dial
// uses, with the same httptrace hooks (ConnectStart, TLSHandshakeDone,
// ConnectDone), on a UDP socket of the connection's own, closed with it;
// a connection of the budget is counted when it opens and when it ends.
// It waits for the handshake to complete, never sending in 0-RTT, which
// the study does not measure: with a session cache, an early dial would
// hand the connection over before the handshake said what it negotiated.
func (c *client) dialQUIC(ctx context.Context, addr string, tlsCfg *tls.Config, cfg *quic.Config, budget bool) (*quic.Conn, error) {
	ua, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	trace := httptrace.ContextClientTrace(ctx)
	if trace != nil && trace.ConnectStart != nil {
		trace.ConnectStart("udp", ua.String())
	}
	var qc *quic.Conn
	pc, err := net.ListenUDP("udp", nil)
	if err == nil {
		tr := &quic.Transport{Conn: pc}
		qc, err = tr.Dial(ctx, ua, tlsCfg, cfg)
		if err != nil {
			_ = tr.Close()
			_ = pc.Close()
		} else {
			if budget {
				c.dials.Add(1)
			}
			context.AfterFunc(qc.Context(), func() {
				if budget {
					c.closes.Add(1)
				}
				_ = tr.Close()
				_ = pc.Close()
			})
		}
	}
	if trace != nil && trace.TLSHandshakeDone != nil {
		var state tls.ConnectionState
		if qc != nil {
			state = qc.ConnectionState().TLS
		}
		trace.TLSHandshakeDone(state, err)
	}
	if trace != nil && trace.ConnectDone != nil {
		trace.ConnectDone("udp", ua.String(), err)
	}
	return qc, err
}

// watched is one TCP connection of the budget. It ends once: on its first
// failed read (the server's FIN or RST) or on its close (net/http closes a
// connection after a `Connection: close` answer under HTTP/1.1, and an
// idle one after a GOAWAY under HTTP/2). An end is counted as the
// server's unless the generator abandoned a request on it, which under
// HTTP/1.1 closes the connection: whichever of the two comes first, the
// count is right once both have happened.
type watched struct {
	net.Conn
	closes *atomic.Int64
	state  atomic.Int32 // live, ended (counted), abandoned
}

const (
	live int32 = iota
	ended
	abandoned
)

func (w *watched) Read(p []byte) (int, error) {
	n, err := w.Conn.Read(p)
	if err != nil {
		w.end()
	}
	return n, err
}

func (w *watched) Close() error {
	w.end()
	return w.Conn.Close()
}

func (w *watched) end() {
	if w.state.CompareAndSwap(live, ended) {
		w.closes.Add(1)
	}
}

func (w *watched) abandon() {
	if w.state.CompareAndSwap(live, abandoned) {
		return
	}
	if w.state.CompareAndSwap(ended, abandoned) {
		w.closes.Add(-1)
	}
}

// watchedOf is the watched connection under what httptrace's GotConn
// hands out: itself in clear, net/http's TLS connection over it under
// HTTP/1.1 and HTTP/2. quic-go hands out a stand-in, and abandoning a request under
// HTTP/3 resets its stream, never the connection.
func watchedOf(nc net.Conn) *watched {
	if w, ok := nc.(*watched); ok { // HTTP/1.1 in clear
		return w
	}
	if tc, ok := nc.(*tls.Conn); ok {
		w, _ := tc.NetConn().(*watched)
		return w
	}
	return nil
}

// tlsSetting is what one handshake negotiated.
type tlsSetting struct{ version, suite, alpn string }

// copyingCache hands every Get a session state of its own. quic-go v0.63
// writes into the state it resumes with (EarlyData, in
// handshake.cryptoSetup.handleEvent), and two fresh connections resuming
// the one cached session at once would write into the same one: a data
// race the detector catches under -tls-resume -proto h3.
type copyingCache struct{ tls.ClientSessionCache }

func (c copyingCache) Get(key string) (*tls.ClientSessionState, bool) {
	cs, ok := c.ClientSessionCache.Get(key)
	if !ok || cs == nil {
		return cs, ok
	}
	ticket, st, err := cs.ResumptionState()
	if err != nil || st == nil {
		return cs, ok
	}
	own := *st
	cp, err := tls.NewResumptionState(ticket, &own)
	if err != nil {
		return cs, ok
	}
	return cp, true
}

// note records the TLS setting a handshake ended on.
func (c *client) note(cs tls.ConnectionState) {
	n := tlsSetting{tls.VersionName(cs.Version), tls.CipherSuiteName(cs.CipherSuite), cs.NegotiatedProtocol}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !slices.Contains(c.negotiated, n) {
		c.negotiated = append(c.negotiated, n)
	}
}

// tlsSettings is what the handshakes so far negotiated, for the report:
// "none" in clear, the distinct values joined by " | " if they differ.
func (c *client) tlsSettings() (version, suite, alpn string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.negotiated) == 0 {
		return "none", "none", ""
	}
	var vs, ss, as []string
	for _, n := range c.negotiated {
		vs, ss, as = appendNew(vs, n.version), appendNew(ss, n.suite), appendNew(as, n.alpn)
	}
	return strings.Join(vs, " | "), strings.Join(ss, " | "), strings.Join(as, " | ")
}

func appendNew(xs []string, x string) []string {
	if slices.Contains(xs, x) {
		return xs
	}
	return append(xs, x)
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
// are kept. The answers are discarded; the opening is bounded by
// warmDeadline, not by a request's deadline, since a few thousand
// handshakes at once is no measurement.
func (c *client) warm(ctx context.Context, conns int) error {
	c.mu.Lock()
	from := len(c.conns)
	for len(c.conns) < conns {
		c.conns = append(c.conns, c.dial(true))
	}
	c.mu.Unlock()
	errs := make([]error, conns-from)
	var wg sync.WaitGroup
	for i := from; i < conns; i++ {
		wg.Go(func() {
			r := c.do(ctx, c.conns[i].hc, job{req: request{kind: static, method: "GET", path: "/static"}, due: time.Now()}, warmDeadline)
			errs[i-from] = r.err
		})
	}
	wg.Wait()
	return errors.Join(errs...)
}

// warmDeadline bounds the opening of the budget.
const warmDeadline = 30 * time.Second

// result is one request's outcome: the two delays from the due instant,
// and what went wrong if anything, by nature.
type result struct {
	ttfb, ttlb time.Duration
	wait       time.Duration // due → taken by a worker
	status     int
	err        error
	nature     nature
	wire, size int  // an answer's body on the wire and decoded
	compressed bool // in the coding asked for
	raw        bool // deflate without its zlib wrapper
	handshake  bool // a TLS handshake happened inside this request's delays
	resumed    bool // and it resumed a session
}

// nature is what became of a request (§1.2): answered right, answered
// wrong, not answered in time, an error of the contender by kind, or a
// failure of the tester. Each request has exactly one.
type nature uint8

const (
	natureOK             nature = iota
	natureBadKey                // a 200 whose document breaks the contract (§4): a wrong answer, never an error
	natureTimedOut              // no complete answer by the deadline 10 X + 4 RTT from the due instant
	natureTester                // the tester could not open a connection: no local port or descriptor (§4)
	nature4xx                   // a 4xx status
	nature5xx                   // a 5xx status
	natureRefused               // ECONNREFUSED: nothing listens
	natureReset                 // the connection ended under a request already sent: ECONNRESET, EOF, a stream reset
	natureConnectTimeout        // the deadline passed while the connection was still opening
	natureOther                 // any other failure: a failed handshake, a status out of 200, 4xx and 5xx
)

// send runs one job on connection `cn` of the budget, or on a fresh one.
func (c *client) send(ctx context.Context, cn int, j job) result {
	if !j.fresh {
		return c.do(ctx, c.conns[cn].hc, j, c.deadline)
	}
	f := c.dial(false)
	defer f.close()
	return c.do(ctx, f.hc, j, c.deadline)
}

// do sends one request and reads its answer to the end, all of it within
// `deadline` of the instant it was due.
func (c *client) do(ctx context.Context, hc *http.Client, j job, deadline time.Duration) result {
	res := result{wait: time.Since(j.due)}
	ctx, cancel := context.WithDeadline(ctx, j.due.Add(deadline))
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, j.req.method, c.url+j.req.path, bytes.NewReader(j.req.body)) //nolint:gosec // G704: the URL is the -url flag, a load generator's whole purpose
	if err != nil {
		res.err, res.nature = err, natureOther
		return res
	}
	if j.req.body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.encoding != "" {
		req.Header.Set("Accept-Encoding", c.encoding)
	}
	var firstByte time.Time
	// A dial the transport started for this request can outlive it, when
	// another connection became free first: these are written from that
	// goroutine, hence the atomics.
	var connecting, gotConn, handshake, resumed atomic.Bool
	var on atomic.Pointer[watched]
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), &httptrace.ClientTrace{
		GotFirstResponseByte: func() { firstByte = time.Now() },
		ConnectStart:         func(_, _ string) { connecting.Store(true) },
		GotConn: func(info httptrace.GotConnInfo) {
			if w := watchedOf(info.Conn); w != nil {
				on.Store(w)
			}
			gotConn.Store(true)
		},
		TLSHandshakeDone: func(cs tls.ConnectionState, err error) {
			handshake.Store(true)
			if err == nil {
				resumed.Store(cs.DidResume)
				c.note(cs)
			}
		},
	}))
	failed := func(err error) result {
		res.err = err
		res.nature = classify(ctx, err, connecting.Load() && !gotConn.Load())
		if res.nature == natureTimedOut && c.proto == protoH1 {
			if w := on.Load(); w != nil {
				w.abandon()
			}
		}
		return res
	}
	resp, err := hc.Do(req) //nolint:gosec // G704: same
	res.handshake, res.resumed = handshake.Load(), resumed.Load()
	if err != nil {
		return failed(err)
	}
	defer resp.Body.Close() //nolint:errcheck // the body was read to the end or the error is already reported
	body, err := io.ReadAll(resp.Body)
	res.ttlb = time.Since(j.due)
	res.ttfb = firstByte.Sub(j.due)
	res.status = resp.StatusCode
	if err != nil {
		return failed(fmt.Errorf("reading the body: %w", err))
	}
	if resp.ContentLength >= 0 && int64(len(body)) != resp.ContentLength {
		res.err = fmt.Errorf("body of %d bytes, Content-Length %d", len(body), resp.ContentLength)
		res.nature = natureBadKey
		return res
	}
	if resp.StatusCode != http.StatusOK {
		res.err = fmt.Errorf("status %d", resp.StatusCode)
		switch resp.StatusCode / 100 {
		case 4:
			res.nature = nature4xx
		case 5:
			res.nature = nature5xx
		default:
			res.nature = natureOther
		}
		return res
	}
	d, err := decode(c.encoding, resp.Header.Get("Content-Encoding"), body)
	if err != nil {
		res.err, res.nature = err, natureBadKey
		return res
	}
	res.wire, res.size, res.compressed, res.raw = len(body), len(d.doc), d.compressed, d.raw
	if err := check(j.req, d.doc); err != nil {
		res.err, res.nature = err, natureBadKey
	}
	return res
}

// classify says what a failed request is. The tester's own failures come
// first, then a refusal, then the deadline, whatever error the transport
// made of it: a request whose deadline passed while its connection was
// still opening timed out at the opening (§1.2), any other timed out. A
// connection that ended under the request comes next; the rest is other.
// `ctx` is the request's: its deadline passed if it reports
// DeadlineExceeded (an interruption reports Canceled).
func classify(ctx context.Context, err error, opening bool) nature {
	switch {
	case errors.Is(err, syscall.EADDRNOTAVAIL), errors.Is(err, syscall.EMFILE), errors.Is(err, syscall.ENFILE):
		return natureTester
	case errors.Is(err, syscall.ECONNREFUSED):
		return natureRefused
	case errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded):
		if opening {
			return natureConnectTimeout
		}
		return natureTimedOut
	case errors.Is(err, syscall.ECONNRESET), errors.Is(err, syscall.EPIPE),
		errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF), isStreamReset(err):
		return natureReset
	}
	return natureOther
}

// isStreamReset says whether the peer reset an HTTP/3 stream or closed its
// QUIC connection under the request. HTTP/2's stream errors are net/http's
// own unexported types; it retries a request a GOAWAY refused by itself.
func isStreamReset(err error) bool {
	var se *quic.StreamError
	var ae *quic.ApplicationError
	var he *http3.Error
	return errors.As(err, &se) || errors.As(err, &ae) || errors.As(err, &he)
}
