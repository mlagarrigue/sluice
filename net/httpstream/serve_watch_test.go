package httpstream

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
)

// What the peer watch reads while a response streams is a pipelining
// client's next request, and it must reach the request reader as if it had
// never been read ahead. net.Pipe makes this deterministic: a client write
// returns only once the server side has read it, and while the producer
// holds the serving goroutine the watch is the only reader.
func TestPeerWatchPutsBackAPipelinedRequest(t *testing.T) {
	server, client := net.Pipe()
	defer func() { _ = client.Close() }()
	if err := client.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}

	release := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() { _ = server.Close() }()
		serveConn(&prefixConn{Conn: server}, Config{}.withDefaults(), func(b sluice.Batch[Request]) sluice.Batch[Response] {
			out := make([]Response, 0, b.Len())
			for _, r := range b.Items {
				if string(r.Target) == "/stream" {
					out = append(out, Response{Status: 200, Stream: func(yield func(sluice.Batch[[]byte]) bool) {
						<-release
						yield(sluice.Batch[[]byte]{Items: [][]byte{[]byte("streamed")}})
					}})
					continue
				}
				out = append(out, Response{Status: 200, Body: append([]byte(nil), r.Target...)})
			}
			return sluice.Batch[Response]{Items: out}
		})
	}()

	if _, err := io.WriteString(client, "GET /stream HTTP/1.1\r\nHost: h\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(client)
	head := readHead(t, br)
	if !strings.HasPrefix(head, "HTTP/1.1 200") {
		t.Fatalf("first head = %q", head)
	}
	// Returns once read: by the watch, since the producer holds the
	// connection's goroutine until release.
	if _, err := io.WriteString(client, "GET /second HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	close(release)

	// The streamed body, chunked, then the second answer behind it.
	var body strings.Builder
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		var n int
		if _, err := fmt.Sscanf(line, "%x", &n); err != nil {
			t.Fatalf("chunk size %q: %v", line, err)
		}
		chunk := make([]byte, n+2)
		if _, err := io.ReadFull(br, chunk); err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			break
		}
		body.Write(chunk[:n])
	}
	if body.String() != "streamed" {
		t.Errorf("streamed body = %q", body.String())
	}
	head = readHead(t, br)
	if !strings.HasPrefix(head, "HTTP/1.1 200") {
		t.Fatalf("second head = %q", head)
	}
	rest, _ := io.ReadAll(br)
	if string(rest) != "/second" {
		t.Errorf("second body = %q, want the pipelined request's target", rest)
	}
	<-done
}

// readHead reads a response's status line and header section, returning the
// status line.
func readHead(t *testing.T, br *bufio.Reader) string {
	t.Helper()
	status, err := br.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if line == "\r\n" {
			return strings.TrimRight(status, "\r\n")
		}
	}
}
