// Command endpoint is sluice's QUIC endpoint for the public QUIC Interop
// Runner (https://github.com/quic-interop/quic-interop-runner), which
// crosses many implementations two by two inside a simulated network.
//
// The runner drives an implementation through environment variables and
// mounted directories, nothing else: ROLE is client or server; TESTCASE
// names the case; REQUESTS (client) lists the URLs to fetch, space
// separated; the server serves /www on port 443 with /certs/cert.pem and
// /certs/priv.key, the client writes what it fetched to /downloads;
// SSLKEYLOGFILE, when set, receives the TLS secrets the runner decrypts the
// capture with. Unless the case says otherwise the transfer is HTTP/0.9 over
// QUIC: "GET /path\r\n" on a bidirectional stream, the file as the whole
// answer, FIN at the end. A case the implementation does not support must
// exit with status 127, so the runner records it as unsupported rather than
// failed; anything else that goes wrong exits with 1.
//
// What is refused here, and why, is one table (supported); the cases it
// refuses are the features net/quic documents as not there (0-RTT, session
// resumption, ECN, QUIC v2) or that crypto/tls offers no knob for (an
// endpoint that offers only ChaCha20 — Go does not let a TLS 1.3 cipher
// suite list be restricted — and a key update forced inside the first
// megabyte, which net/quic performs at the AEAD's confidentiality limit
// and not before). The HTTP/3 case is served, by net/httpstream, but not
// fetched: the module has no HTTP/3 client.
package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/net/httpstream"
	"github.com/mlagarrigue/sluice/net/quic"
	"github.com/mlagarrigue/sluice/net/quic/udp"
)

const (
	alpnHQ = "hq-interop"
	alpnH3 = "h3"

	wwwDir       = "/www"
	downloadsDir = "/downloads"
	certFile     = "/certs/cert.pem"
	keyFile      = "/certs/priv.key"

	serverPort = "443"
	// preferredPort is the second port a server announces as its preferred
	// address in the connectionmigration case (RFC 9000 §9.6).
	preferredPort = "4434"

	// exitUnsupported is the status the runner reads as "this case is not
	// implemented", as opposed to "it failed".
	exitUnsupported = 127

	// maxStreamsInFlight bounds how many requests a client keeps open on one
	// connection at a time, under the server's default MAX_STREAMS grant
	// (quic.DefaultParameters: 100), so the multiplexing case's 1999 files
	// flow through the grant as it is replenished.
	maxStreamsInFlight = 64
)

var errUnsupported = errors.New("unsupported test case")

// supported says, per role, which case names this endpoint implements. The
// runner names a case to each side separately (keyupdate reaches the server
// as "transfer", connectionmigration reaches the client as "transfer"), so
// the two lists differ and both are closed: an unknown name exits 127,
// which is also how the runner checks that an implementation is compliant.
var supported = map[string]map[string]bool{
	"server": {
		"handshake": true, "transfer": true, "retry": true, "multiconnect": true,
		"http3": true, "connectionmigration": true,
	},
	"client": {
		"handshake": true, "transfer": true, "retry": true, "multiconnect": true,
		"ipv6": true,
	},
}

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	role, testcase := os.Getenv("ROLE"), os.Getenv("TESTCASE")
	if !supported[role][testcase] {
		fmt.Printf("%s: %s %s\n", errUnsupported, role, testcase)
		os.Exit(exitUnsupported)
	}
	var err error
	switch role {
	case "server":
		err = runServer(testcase)
	case "client":
		err = runClient(testcase, strings.Fields(os.Getenv("REQUESTS")))
	}
	if err != nil {
		log.Printf("%s %s: %v", role, testcase, err)
		os.Exit(1)
	}
}

// keyLog opens SSLKEYLOGFILE for the TLS secrets, or returns nil when the
// runner did not ask for them.
func keyLog() (io.Writer, error) {
	name := os.Getenv("SSLKEYLOGFILE")
	if name == "" {
		return nil, nil
	}
	f, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("SSLKEYLOGFILE: %w", err)
	}
	return f, nil
}

// --- server -----------------------------------------------------------------

func runServer(testcase string) error {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return err
	}
	kl, err := keyLog()
	if err != nil {
		return err
	}
	alpn := alpnHQ
	if testcase == "http3" {
		alpn = alpnH3
	}
	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS13,
		NextProtos:   []string{alpn},
		KeyLogWriter: kl,
	}

	// Dual-stack: the runner reaches the server as server4 or server6 and
	// names the ipv6 case "transfer" on this side, so the family cannot be
	// chosen per case.
	pc, err := udp.Listen("udp", net.JoinHostPort("", serverPort))
	if err != nil {
		return err
	}
	lcfg := quic.ListenerConfig{AlwaysRetry: testcase == "retry"}
	if testcase == "connectionmigration" {
		ip, err := localIPv4()
		if err != nil {
			return err
		}
		lcfg.PreferredAddress, err = udp.Listen("udp4", net.JoinHostPort(ip.String(), preferredPort))
		if err != nil {
			return err
		}
	}
	l, err := quic.NewListener(pc, cfg, quic.DefaultParameters(), lcfg)
	if err != nil {
		return err
	}
	log.Printf("server %s on %s, ALPN %s", testcase, pc.LocalAddr(), alpn)
	for {
		c, err := l.Accept()
		if err != nil {
			return err
		}
		log.Printf("accepted a connection")
		if alpn == alpnH3 {
			go serveH3(c)
		} else {
			go serveHQ(c)
		}
	}
}

// localIPv4 is the address the preferred-address socket binds to: the one
// global unicast IPv4 address the simulated server has.
func localIPv4() (net.IP, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}
	for _, a := range addrs {
		if n, ok := a.(*net.IPNet); ok && n.IP.To4() != nil && n.IP.IsGlobalUnicast() {
			return n.IP.To4(), nil
		}
	}
	return nil, errors.New("no global IPv4 address to announce as preferred")
}

// serveHQ answers HTTP/0.9 requests, one per stream, until the connection
// ends.
func serveHQ(c *quic.Conn) {
	for {
		s, err := c.AcceptStream()
		if err != nil {
			log.Printf("connection ended: %v (%v)", err, c.Err())
			return
		}
		go func() {
			if err := serveHQStream(s); err != nil {
				log.Printf("stream %d: %v", s.ID(), err)
			}
		}()
	}
}

// serveHQStream reads "GET /path\r\n" and answers with the file, then FIN.
// A path it cannot serve is answered with an empty body: HTTP/0.9 has no
// status line, and a reset would read as a transport failure.
func serveHQStream(s *quic.Stream) error {
	line, err := bufio.NewReaderSize(s, 4096).ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	fields := strings.Fields(line)
	if len(fields) < 2 || fields[0] != "GET" {
		return fmt.Errorf("not an HTTP/0.9 request: %q", line)
	}
	name, err := wwwPath(fields[1])
	if err != nil {
		_ = s.CloseWrite()
		return err
	}
	f, err := os.Open(name)
	if err != nil {
		_ = s.CloseWrite()
		return err
	}
	defer f.Close() //nolint:errcheck // read side; nothing to recover
	if _, err := io.Copy(s, f); err != nil {
		return err
	}
	return s.CloseWrite()
}

// wwwPath maps a request target onto /www, refusing anything that would
// leave it.
func wwwPath(target string) (string, error) {
	clean := path.Clean("/" + target)
	if strings.Contains(clean, "..") {
		return "", fmt.Errorf("refused target %q", target)
	}
	return filepath.Join(wwwDir, filepath.FromSlash(clean)), nil
}

// serveH3 serves HTTP/3 with net/httpstream: the runner's files from /www,
// a 404 with an empty body otherwise.
func serveH3(c *quic.Conn) {
	err := httpstream.ServeH3(context.Background(), c, httpstream.Config{}, func(b sluice.Batch[httpstream.Request]) sluice.Batch[httpstream.Response] {
		out := make([]httpstream.Response, 0, b.Len())
		for _, r := range b.Items {
			out = append(out, h3File(string(r.Target)))
		}
		return sluice.Batch[httpstream.Response]{Items: out}
	})
	log.Printf("connection ended: %v", err)
}

func h3File(target string) httpstream.Response {
	name, err := wwwPath(target)
	if err != nil {
		return httpstream.Response{Status: 400}
	}
	body, err := os.ReadFile(name)
	if err != nil {
		log.Printf("%s: %v", target, err)
		return httpstream.Response{Status: 404}
	}
	return httpstream.Response{Status: 200, Body: body}
}

// --- client -----------------------------------------------------------------

func runClient(testcase string, requests []string) error {
	if len(requests) == 0 {
		return errors.New("REQUESTS is empty")
	}
	kl, err := keyLog()
	if err != nil {
		return err
	}
	if testcase == "multiconnect" {
		// One connection per file, one after the other: the case counts
		// handshakes under heavy loss.
		for _, r := range requests {
			if err := fetchAll([]string{r}, kl); err != nil {
				return err
			}
		}
		return nil
	}
	return fetchAll(requests, kl)
}

// fetchAll opens one connection to the host of the first URL and fetches
// every URL over it, concurrently within maxStreamsInFlight.
func fetchAll(requests []string, kl io.Writer) error {
	first, err := url.Parse(requests[0])
	if err != nil {
		return err
	}
	host := first.Hostname()
	port := first.Port()
	if port == "" {
		port = serverPort
	}
	addr, err := net.ResolveUDPAddr("udp", net.JoinHostPort(host, port))
	if err != nil {
		return err
	}
	network := "udp4"
	if addr.IP.To4() == nil {
		network = "udp6"
	}
	pc, err := udp.Listen(network, ":0")
	if err != nil {
		return err
	}
	defer pc.Close() //nolint:errcheck // process exits right after

	cfg := &tls.Config{
		// The runner mints its own certificate per run; what it checks is
		// the bytes that arrive, not the chain.
		InsecureSkipVerify: true, //nolint:gosec // interop endpoint, see above
		ServerName:         host,
		MinVersion:         tls.VersionTLS13,
		NextProtos:         []string{alpnHQ},
		KeyLogWriter:       kl,
	}
	c, err := quic.Dial(pc, addr, cfg, quic.DefaultParameters())
	if err != nil {
		return fmt.Errorf("dial %s: %w", addr, err)
	}
	log.Printf("connected to %s, %d request(s)", addr, len(requests))

	var (
		wg       sync.WaitGroup
		sem      = make(chan struct{}, maxStreamsInFlight)
		mu       sync.Mutex
		firstErr error
	)
	fail := func(err error) {
		mu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		mu.Unlock()
	}
	for _, r := range requests {
		u, err := url.Parse(r)
		if err != nil {
			return err
		}
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if err := fetch(c, u.Path); err != nil {
				fail(fmt.Errorf("%s: %w", u.Path, err))
			}
		}()
	}
	wg.Wait()
	_ = c.Close()
	return firstErr
}

// fetch requests one path on a new stream and writes the answer under
// /downloads. OpenStream refuses rather than waits when the peer's
// MAX_STREAMS grant is spent, so a refusal is retried while the connection
// lives.
func fetch(c *quic.Conn, p string) error {
	var (
		s   *quic.Stream
		err error
	)
	for deadline := time.Now().Add(30 * time.Second); ; {
		s, err = c.OpenStream()
		if err == nil {
			break
		}
		if c.Err() != nil || time.Now().After(deadline) {
			return fmt.Errorf("open stream: %w", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := io.WriteString(s, "GET "+p+"\r\n"); err != nil {
		return err
	}
	if err := s.CloseWrite(); err != nil {
		return err
	}
	name := filepath.Join(downloadsDir, filepath.FromSlash(path.Clean("/"+p)))
	f, err := os.Create(name)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, s); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}
