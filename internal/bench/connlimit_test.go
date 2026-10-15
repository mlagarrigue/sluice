package bench

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"io"
	"math"
	"math/big"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/net/httpstream"
)

// What httpstream's Config.MaxRequestsPerConn costs a client that does not
// pipeline — net/http's own client, and the shape of most load generators.
//
// When an HTTP/1.1 connection reaches its budget, its last response says
// Connection: close and the client dials again, paying TCP and a full TLS
// handshake that net/http's server never asks for. The question is whether
// the default of 1024 makes that visible in a measurement of the transport.
//
// The budget of 16 is not a candidate default: it is there to make the
// handshake large enough to measure, so that the cost at 1024 can be predicted
// from it — (t16 − tUnbounded) × 16 / 1024 — and the measured gap checked
// against the prediction rather than read on its own.
//
// Client and server share this machine, so a handshake is paid twice here,
// once per side; on a deployment only the server's half lands on its CPU.
// Sessions are not resumed (net/http's client keeps no session cache unless
// told to), which is the expensive case.

const connLimitClients = 8

func BenchmarkConnLimitTLS16(b *testing.B)            { benchConnLimit(b, 16, false) }
func BenchmarkConnLimitTLS1024(b *testing.B)          { benchConnLimit(b, 1024, false) }
func BenchmarkConnLimitTLS1024Staggered(b *testing.B) { benchConnLimit(b, 1024, true) }
func BenchmarkConnLimitTLSUnbounded(b *testing.B)     { benchConnLimit(b, math.MaxInt, false) }

// startTLSStream serves httpstream over TLS 1.3 (HTTP/1.1 by ALPN) and
// returns the address, the certificate to trust and the accept count.
func startTLSStream(b *testing.B, cfg httpstream.Config) (string, tls.Certificate, *countingListener) {
	b.Helper()
	cert := connLimitCert(b)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	counted := &countingListener{Listener: ln}
	tlsLn := tls.NewListener(counted, &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{"http/1.1"},
	})

	body := []byte(`{"status":"ok","service":"sluice"}`)
	h := func(_ context.Context, batch sluice.Batch[httpstream.Request]) sluice.Batch[httpstream.Response] {
		out := make([]httpstream.Response, batch.Len())
		for i := range out {
			out[i] = httpstream.Response{Status: 200, Body: body}
		}
		return sluice.Batch[httpstream.Response]{Items: out}
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = httpstream.Serve(ctx, tlsLn, cfg, h)
	}()
	b.Cleanup(func() { cancel(); <-done })
	return ln.Addr().String(), cert, counted
}

func benchConnLimit(b *testing.B, perConn int, stagger bool) {
	b.Helper()
	addr, cert, counted := startTLSStream(b, httpstream.Config{MaxRequestsPerConn: perConn})
	pool := x509.NewCertPool()
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		b.Fatal(err)
	}
	pool.AddCert(leaf)
	url := "https://" + addr + "/status"

	// One transport per client, so each client is one connection, as a load
	// generator's virtual user is: a shared pool lends connections across
	// clients and, past its idle bound, closes and redials them itself.
	clients := make([]*http.Client, connLimitClients)
	for i := range clients {
		tr := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}}
		b.Cleanup(tr.CloseIdleConnections)
		clients[i] = &http.Client{Transport: tr, Timeout: 30 * time.Second}
	}
	get := func(c *http.Client) error {
		resp, err := c.Get(url)
		if err != nil {
			return err
		}
		_, err = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if err == nil && resp.StatusCode != 200 {
			err = fmt.Errorf("status %d", resp.StatusCode)
		}
		return err
	}
	// The first connections are opened outside the timer: every variant pays
	// them. Staggered, client i also spends i/connLimitClients of a budget
	// first, so the connections reach it one after another instead of all
	// redialling at the same moment.
	var warm sync.WaitGroup
	for i, c := range clients {
		n := 1
		if stagger && perConn < math.MaxInt {
			n += i * perConn / connLimitClients
		}
		warm.Go(func() {
			for range n {
				if err := get(c); err != nil {
					b.Error(err)
					return
				}
			}
		})
	}
	warm.Wait()
	before := counted.accepted.Load()

	b.ResetTimer()
	var next atomic.Int64
	var wg sync.WaitGroup
	for _, c := range clients {
		wg.Go(func() {
			for next.Add(1) <= int64(b.N) {
				if err := get(c); err != nil {
					b.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
	b.StopTimer()
	b.ReportMetric(float64(counted.accepted.Load()-before)*1000/float64(b.N), "conns/1000req")
}

// What Config.MaxRequestsPerBatch changes for a client that does pipeline:
// one connection, 256 requests written before any answer is read, over TLS.
// With a client that sends one request at a time the bound never binds — a
// batch is whatever one read returned — so this is its only measurement.

const batchLimitDepth = 256

func BenchmarkBatchLimitTLS8(b *testing.B)   { benchBatchLimit(b, 8) }
func BenchmarkBatchLimitTLS64(b *testing.B)  { benchBatchLimit(b, 64) }
func BenchmarkBatchLimitTLS256(b *testing.B) { benchBatchLimit(b, 256) }

func benchBatchLimit(b *testing.B, perBatch int) {
	b.Helper()
	addr, cert, _ := startTLSStream(b, httpstream.Config{
		MaxRequestsPerBatch: perBatch,
		MaxRequestsPerConn:  math.MaxInt,
	})
	pool := x509.NewCertPool()
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		b.Fatal(err)
	}
	pool.AddCert(leaf)
	c, err := tls.Dial("tcp", addr, &tls.Config{RootCAs: pool, NextProtos: []string{"http/1.1"}})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = c.Close() })
	_ = c.SetDeadline(time.Now().Add(5 * time.Minute))

	var out strings.Builder
	for range batchLimitDepth {
		out.WriteString("GET /status HTTP/1.1\r\nHost: h\r\n\r\n")
	}
	burst := []byte(out.String())
	br := bufio.NewReader(c)

	b.ResetTimer()
	for sent := 0; sent < b.N; sent += batchLimitDepth {
		if _, err := c.Write(burst); err != nil {
			b.Fatal(err)
		}
		for range batchLimitDepth {
			if err := drainOneResponse(br); err != nil {
				b.Fatal(err)
			}
		}
	}
}

type countingListener struct {
	net.Listener
	accepted atomic.Int64
}

func (l *countingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err == nil {
		l.accepted.Add(1)
	}
	return c, err
}

func connLimitCert(b *testing.B) tls.Certificate {
	b.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		b.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "connlimit bench"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		b.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}
