package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/quic-go/quic-go/http3"
)

// The trivial server of §4: it answers the four routes of the study with
// documents of the shape and size the scenario expects, serialised once at
// startup, so that a stage against it measures the generator alone, its
// validation at 100 % included. The campaign opens with a ramp against it
// per scenario: the last stage held is the generator's own ceiling, and a
// capacity measured against a contender is interpretable only when it sits
// well under that ceiling (the study asks for a factor of two).
//
// It serves the three protocols at once on one address: TCP for HTTP/1.1
// and HTTP/2 (ALPN chooses), UDP on the same port for HTTP/3, under a
// certificate generated at startup. It runs as a process of its own
// (`loadgen -trivial 127.0.0.1:8444`), so the generator's figures never
// include the time the server's goroutines steal from the same scheduler.
//
// What it costs per request is a few appends into a pooled buffer and one
// write: the rows of the table, 1..keyspace+1000, are serialised once, and a
// list is a slice of that array between two offsets. Nothing is parsed
// beyond the key the path or the body carries.
type trivial struct {
	rows   []byte // every row, `{"id":i,"total":7i,"label":"order-i"},`, back to back
	off    []int  // off[i] is where row i starts, 1 ≤ i ≤ n; off[n+1] = len(rows)
	n      int    // rows in the table
	static []byte
}

func newTrivial(keyspace int) *trivial {
	t := &trivial{n: keyspace + listLimit}
	t.off = make([]int, t.n+2)
	t.rows = make([]byte, 0, t.n*48)
	for i := 1; i <= t.n; i++ {
		t.off[i] = len(t.rows)
		t.rows = appendRow(t.rows, int64(i))
		t.rows = append(t.rows, ',')
	}
	t.off[t.n+1] = len(t.rows)
	t.static = staticDoc()
	return t
}

// appendRow is one row of bench_orders as the fixtures predict it (§4):
// total 7·id, label "order-<id>".
func appendRow(b []byte, id int64) []byte {
	b = append(b, `{"id":`...)
	b = strconv.AppendInt(b, id, 10)
	b = append(b, `,"total":`...)
	b = strconv.AppendInt(b, id*7, 10)
	b = append(b, `,"label":"order-`...)
	b = strconv.AppendInt(b, id, 10)
	return append(b, `"}`...)
}

// staticDoc is the ~1 KB document of the static scenario, the shape of the
// study's server's own: a status and forty lines of padding.
func staticDoc() []byte {
	var lines []string
	for i := range 40 {
		lines = append(lines, fmt.Sprintf("line-%02d-abcdefghijklmnop", i))
	}
	b, err := json.Marshal(map[string]any{"status": "ok", "service": "trivial", "version": "0", "padding": lines})
	if err != nil {
		panic(err)
	}
	return b
}

// bufPool holds the response buffers: a list is ~100 KB, everything else
// under a kilobyte.
var bufPool = sync.Pool{New: func() any { b := make([]byte, 0, 128<<10); return &b }}

func (t *trivial) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	bp := bufPool.Get().(*[]byte) //nolint:errcheck // the pool holds nothing else
	defer bufPool.Put(bp)
	b := (*bp)[:0]
	status := http.StatusOK
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/static":
		b = append(b, t.static...)
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/orders/"):
		id, err := strconv.ParseInt(r.URL.Path[len("/orders/"):], 10, 64)
		if err != nil || id < 1 || int64(t.n) < id {
			status, b = http.StatusNotFound, append(b, `{"error":"no such order"}`...)
			break
		}
		b = appendRow(b, id)
	case r.Method == http.MethodGet && r.URL.Path == "/orders":
		from, limit, ok := t.listArgs(r.URL.RawQuery)
		if !ok {
			status, b = http.StatusBadRequest, append(b, `{"error":"bad from or limit"}`...)
			break
		}
		b = append(b, `{"from":`...)
		b = strconv.AppendInt(b, from, 10)
		b = append(b, `,"limit":`...)
		b = strconv.AppendInt(b, int64(limit), 10)
		b = append(b, `,"items":[`...)
		// Rows from+1 .. from+limit, the trailing comma of the last dropped.
		b = append(b, t.rows[t.off[from+1]:t.off[from+1+int64(limit)]-1]...)
		b = append(b, `]}`...)
	case r.Method == http.MethodPost && r.URL.Path == "/orders":
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<10))
		key, ok := writeKey(body)
		if err != nil || !ok {
			status, b = http.StatusBadRequest, append(b, `{"error":"bad body"}`...)
			break
		}
		b = append(b, `{"key":`...)
		b = strconv.AppendInt(b, key, 10)
		b = append(b, '}')
	default:
		status, b = http.StatusNotFound, append(b, `{"error":"no such route"}`...)
	}
	h := w.Header()
	h["Content-Type"] = jsonType
	h["Content-Length"] = []string{strconv.Itoa(len(b))}
	w.WriteHeader(status)
	_, _ = w.Write(b) //nolint:gosec // G705: the body is JSON the server built, never a request's bytes
	*bp = b
}

var jsonType = []string{"application/json"}

// listArgs reads `from=K&limit=N` without url.Values: a list serves rows
// K+1..K+N, N at most 1 000 and never past the table.
func (t *trivial) listArgs(query string) (from int64, limit int, ok bool) {
	limit = -1
	for query != "" {
		var kv string
		kv, query, _ = strings.Cut(query, "&")
		k, v, _ := strings.Cut(kv, "=")
		switch k {
		case "from":
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil || n < 0 {
				return 0, 0, false
			}
			from = n
		case "limit":
			n, err := strconv.Atoi(v)
			if err != nil || n <= 0 || n > listLimit {
				return 0, 0, false
			}
			limit = n
		}
	}
	if limit < 0 || from+int64(limit) > int64(t.n) {
		return 0, 0, false
	}
	return from, limit, true
}

// writeKey finds the key of `{"key":K,...}` without decoding the document.
func writeKey(body []byte) (int64, bool) {
	_, after, found := bytes.Cut(body, []byte(`"key":`))
	if !found {
		return 0, false
	}
	end := bytes.IndexAny(after, ",}")
	if end < 0 {
		return 0, false
	}
	key, err := strconv.ParseInt(string(bytes.TrimSpace(after[:end])), 10, 64)
	return key, err == nil
}

// trivialServer is the trivial handler listening: TCP and UDP on one
// address, one certificate.
type trivialServer struct {
	URL  string
	tcp  *http.Server
	udp  *http3.Server
	ln   net.Listener
	pc   net.PacketConn
	errs chan error
}

// listenTrivial binds the address and generates the certificate; serve
// starts answering.
func listenTrivial(addr string, keyspace int) (*trivialServer, error) {
	cert, err := selfSigned()
	if err != nil {
		return nil, err
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	pc, err := net.ListenPacket("udp", ln.Addr().String())
	if err != nil {
		_ = ln.Close()
		return nil, err
	}
	handler := newTrivial(keyspace)
	tlsCfg := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13}
	s := &trivialServer{
		URL: "https://" + ln.Addr().String(),
		ln:  ln, pc: pc,
		tcp: &http.Server{
			Handler:           handler,
			TLSConfig:         tlsCfg, // ServeTLS adds "h2" and "http/1.1" to NextProtos
			ReadHeaderTimeout: 10 * time.Second,
		},
		udp: &http3.Server{
			Handler:   handler,
			TLSConfig: http3.ConfigureTLSConfig(tlsCfg.Clone()),
		},
		errs: make(chan error, 2),
	}
	return s, nil
}

// serve answers until ctx ends or a listener fails, then closes both.
func (s *trivialServer) serve(ctx context.Context) error {
	go func() { s.errs <- s.tcp.ServeTLS(s.ln, "", "") }()
	go func() { s.errs <- s.udp.Serve(s.pc) }()
	var err error
	select {
	case <-ctx.Done():
	case err = <-s.errs:
	}
	s.close()
	if errors.Is(err, http.ErrServerClosed) || errors.Is(err, net.ErrClosed) {
		err = nil
	}
	return err
}

func (s *trivialServer) close() {
	shutdown, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = s.tcp.Shutdown(shutdown)
	_ = s.udp.Close()
	_ = s.ln.Close()
	_ = s.pc.Close()
}

// selfSigned is a certificate for 127.0.0.1 and localhost, valid a day: the
// generator trusts any certificate when -ca is empty.
func selfSigned() (tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: "loadgen trivial"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, nil
}

// runTrivial is `loadgen -trivial addr`: it prints one JSON line once it
// listens — the script that drives the self-test waits for it — and serves
// until interrupted.
func runTrivial(ctx context.Context, addr string, keyspace int, out io.Writer) error {
	s, err := listenTrivial(addr, keyspace)
	if err != nil {
		return err
	}
	if err := json.NewEncoder(out).Encode(map[string]any{"trivial": s.URL, "keyspace": keyspace, "rows": keyspace + listLimit}); err != nil {
		s.close()
		return err
	}
	return s.serve(ctx)
}
