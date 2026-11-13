package gonethttp_test

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
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestConformsToR is CMP-09's check for this contender: both versions, on
// every protocol they serve, answer the generator's conformance requests as
// the study's server in R does, after parsing (comparison/README.md,
// "Checking a contender against R"). It builds the two versions, R and the
// generator from this tree, starts R and the contender with the same
// contract environment, and runs `loadgen -conformance` between them.
//
// It needs the PostgreSQL of the repository's tests (SLUICE_PG and its
// siblings, the tables of `go run ./internal/bench/cmd/pgfixtures`) and
// skips without it.
func TestConformsToR(t *testing.T) {
	if os.Getenv("SLUICE_PG") == "" {
		t.Skip("SLUICE_PG is not set")
	}
	dir := t.TempDir()
	build(t, dir, "standard", ".", "./standard")
	build(t, dir, "optimised", ".", "./optimised")
	build(t, dir, "dataloader", ".", "./dataloader")
	build(t, dir, "r", "../sluice", ".")
	build(t, dir, "loadgen", "../loadgen", ".")
	cert, key := certificate(t, dir)
	env := contractEnv(t, cert, key)

	cases := []struct {
		version, proto, compression string
	}{
		{"standard", "h1", "off"},
		{"standard", "h2", "off"},
		{"optimised", "h1", "off"},
		{"optimised", "h2", "off"},
		{"optimised", "h3", "off"},
		{"optimised", "h1", "gzip"},
		{"optimised", "h2", "gzip"},
		{"dataloader", "h1", "off"},
		{"dataloader", "h2", "off"},
	}
	for _, c := range cases {
		t.Run(c.version+"-"+c.proto+"-"+c.compression, func(t *testing.T) {
			run := append(env, "STUDY_PROTOCOL="+c.proto, "STUDY_COMPRESSION="+c.compression)
			ref := start(t, filepath.Join(dir, "r"), run, c.proto)
			got := start(t, filepath.Join(dir, c.version), run, c.proto)
			args := []string{"-no-verify", "-proto", c.proto}
			if c.compression != "off" {
				args = append(args, "-encoding", c.compression)
			}
			loadgen := filepath.Join(dir, "loadgen")
			for _, u := range []string{ref, got} {
				generate(t, loadgen, append(args, "-url", u, "-ready", "20s")...)
			}
			out := generate(t, loadgen, append(args, "-url", got, "-conformance", ref, "-checks", "300")...)
			if !strings.Contains(out, `"conformant":true`) {
				t.Fatalf("not conformant:\n%s", out)
			}
		})
	}
}

// TestRefusesWhatItDoesNotServe: the standard version has no HTTP/3 and no
// compression, and says so before it listens, rather than serving R's
// configuration under another name (study §2.4).
func TestRefusesWhatItDoesNotServe(t *testing.T) {
	dir := t.TempDir()
	build(t, dir, "standard", ".", "./standard")
	build(t, dir, "optimised", ".", "./optimised")
	cert, key := certificate(t, dir)
	base := []string{"STUDY_TLS_CERT=" + cert, "STUDY_TLS_KEY=" + key, "STUDY_LISTEN=127.0.0.1:0"}
	for _, c := range []struct {
		version string
		env     []string
	}{
		{"standard", []string{"STUDY_PROTOCOL=h3"}},
		{"standard", []string{"STUDY_COMPRESSION=gzip"}},
		{"optimised", []string{"STUDY_COMPRESSION=br"}},
		{"optimised", []string{"STUDY_TLS=off", "STUDY_PROTOCOL=h2"}},
	} {
		cmd := exec.Command(filepath.Join(dir, c.version)) //nolint:gosec // G204: a binary this test built
		cmd.Env = append(append(os.Environ(), base...), c.env...)
		out, err := cmd.CombinedOutput()
		if err == nil || !bytes.Contains(out, []byte("STUDY_")) {
			t.Errorf("%s %v: started, or refused without naming the variable: %v %s", c.version, c.env, err, out)
		}
	}
}

func build(t *testing.T, dir, name, module, pkg string) {
	t.Helper()
	cmd := exec.Command("go", "build", "-C", module, "-o", filepath.Join(dir, name), pkg) //nolint:gosec // G204: paths of this repository
	cmd.Env = append(os.Environ(), "GOWORK=off")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build %s: %v\n%s", name, err, out)
	}
}

// contractEnv is the database under libpq's names, from the repository's
// SLUICE_PG* (R reads the latter, pgx the former), and the certificate.
func contractEnv(t *testing.T, cert, key string) []string {
	t.Helper()
	host, port, err := net.SplitHostPort(os.Getenv("SLUICE_PG"))
	if err != nil {
		t.Fatalf("SLUICE_PG: %v", err)
	}
	return append(os.Environ(),
		"PGHOST="+host, "PGPORT="+port,
		"PGUSER="+os.Getenv("SLUICE_PG_USER"),
		"PGDATABASE="+os.Getenv("SLUICE_PG_DB"),
		"PGPASSWORD="+os.Getenv("SLUICE_PG_PASSWORD"),
		"STUDY_TLS_CERT="+cert, "STUDY_TLS_KEY="+key,
	)
}

// start runs a server on a free port of the protocol's kind and returns its
// URL; the process is killed when the test ends.
func start(t *testing.T, bin string, env []string, proto string) string {
	t.Helper()
	addr := freePort(t, proto == "h3")
	cmd := exec.Command(bin)
	cmd.Env = append(env, "STUDY_LISTEN="+addr)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		if t.Failed() {
			t.Logf("%s:\n%s", filepath.Base(bin), stderr.String())
		}
	})
	return "https://" + addr
}

func freePort(t *testing.T, udp bool) string {
	t.Helper()
	if udp {
		pc, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer pc.Close()
		return pc.LocalAddr().String()
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

func generate(t *testing.T, loadgen string, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	out, err := exec.CommandContext(ctx, loadgen, args...).CombinedOutput() //nolint:gosec // G204: the generator this test built
	if err != nil {
		t.Fatalf("loadgen %v: %v\n%s", args, err, out)
	}
	return string(out)
}

// certificate writes the ECDSA P-256 certificate the contract names, for
// 127.0.0.1, and its key.
func certificate(t *testing.T, dir string) (cert, key string) {
	t.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "go-nethttp"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &k.PublicKey, k)
	if err != nil {
		t.Fatal(err)
	}
	kder, err := x509.MarshalECPrivateKey(k)
	if err != nil {
		t.Fatal(err)
	}
	cert, key = filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	if err := os.WriteFile(cert, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(key, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kder}), 0o600); err != nil {
		t.Fatal(err)
	}
	return cert, key
}

// TestDataLoaderGroups is what the variant exists for: unit reads that
// arrive together leave in fewer queries than reads, and the metrics point
// says so. 64 concurrent reads over HTTP/2, one connection, inside the
// loader's 16 ms window: at least one batch of more than one key.
func TestDataLoaderGroups(t *testing.T) {
	if os.Getenv("SLUICE_PG") == "" {
		t.Skip("SLUICE_PG is not set")
	}
	dir := t.TempDir()
	build(t, dir, "dataloader", ".", "./dataloader")
	cert, key := certificate(t, dir)
	url := start(t, filepath.Join(dir, "dataloader"), append(contractEnv(t, cert, key), "STUDY_PROTOCOL=h2"), "h2")
	tr := &http.Transport{
		TLSClientConfig:   &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS13}, //nolint:gosec // G402: the certificate this test made
		ForceAttemptHTTP2: true,
		Protocols:         new(http.Protocols),
	}
	tr.Protocols.SetHTTP2(true)
	client := &http.Client{Transport: tr, Timeout: 5 * time.Second}
	defer tr.CloseIdleConnections()

	get := func(path string) ([]byte, error) {
		resp, err := client.Get(url + path)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		if err == nil && resp.StatusCode != http.StatusOK {
			err = fmt.Errorf("%s: %d %s", path, resp.StatusCode, b)
		}
		return b, err
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		if _, err := get("/orders/1"); err == nil {
			break
		} else if time.Now().After(deadline) {
			t.Fatalf("not ready: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}

	const reads = 64
	var wg sync.WaitGroup
	errs := make(chan error, reads)
	for i := range reads {
		wg.Go(func() {
			if _, err := get(fmt.Sprintf("/orders/%d", 1000+i)); err != nil {
				errs <- err
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	b, err := get("/study/metrics")
	if err != nil {
		t.Fatal(err)
	}
	var m struct {
		Batches *struct {
			Count, Keys uint64
		} `json:"batches"`
	}
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	if m.Batches == nil || m.Batches.Keys < reads || m.Batches.Count >= m.Batches.Keys {
		t.Fatalf("no grouping in the metrics point: %s", b)
	}
	t.Logf("%d keys in %d batches", m.Batches.Keys, m.Batches.Count)
}
