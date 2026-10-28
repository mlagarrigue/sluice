package httpstream

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"
)

// A head delivered a byte per read is parsed once, when its blank line
// arrives, and gives the request one read of it would. Every read used to
// parse the request line and every field line received so far again: 51,423
// field lines for a head of 60 fields, work in the square of the head's
// length for one byte of the peer's. The head goes through reader.next
// itself, over net.Pipe, whose every Read returns what one Write gave it.
func TestAHeadDrippedByteByByteIsParsedOnce(t *testing.T) {
	cfg := Config{BodyDelivery: BodyWhole, MaxHeaders: 1024}.mustResolve()
	var b strings.Builder
	b.WriteString("\r\nGET /x HTTP/1.1\r\nHost: h\r\n") // the empty line §2.2 lets a client send first
	for i := 0; b.Len() < cfg.MaxHeaderBytes-64; i++ {
		fmt.Fprintf(&b, "X-Pad-%04d: %s\r\n", i, strings.Repeat("v", 24))
	}
	b.WriteString("\r\n")
	raw := []byte(b.String())

	want, wantUsed, _, err := parseRequest(raw, cfg, nil, nil)
	if err != nil || wantUsed != len(raw) {
		t.Fatalf("the head in one read: used %d of %d, %v", wantUsed, len(raw), err)
	}

	server, client := net.Pipe()
	defer func() { _ = server.Close() }()
	defer func() { _ = client.Close() }()
	go func() {
		for i := range raw {
			if _, err := client.Write(raw[i : i+1]); err != nil {
				return
			}
		}
	}()
	r := &h1Conn{conn: server, cfg: cfg}
	batch, ok := r.next()
	if !ok || len(batch) != 1 || r.pending != len(raw) {
		t.Fatalf("the dripped head: %d requests over %d of %d bytes, %v", len(batch), r.pending, len(raw), r.err)
	}
	// The first short reads parse what they have, until the request line
	// is whole; every later one is a scan.
	if r.shortParses > 2 {
		t.Errorf("reader.next parsed the head %d times before its blank line arrived, want two at most", r.shortParses)
	}
	got := batch[0]
	if string(got.Method) != string(want.Method) || string(got.Target) != string(want.Target) || len(got.Headers) != len(want.Headers) {
		t.Fatalf("dripped %s %s with %d fields, one read %s %s with %d", got.Method, got.Target, len(got.Headers), want.Method, want.Target, len(want.Headers))
	}
	for i := range want.Headers {
		if string(got.Headers[i].Name) != string(want.Headers[i].Name) || string(got.Headers[i].Value) != string(want.Headers[i].Value) {
			t.Fatalf("field %d differs: %q: %q, want %q: %q", i, got.Headers[i].Name, got.Headers[i].Value, want.Headers[i].Name, want.Headers[i].Value)
		}
	}
}

// A head dripped a byte per read is refused at the byte, and with the
// error, of the first prefix one parse refuses: the scan hands the head to
// the parse as soon as its request line is whole, checks each field line
// with parseHeaders as it ends, and leaves the bounds to the parse.
func TestADrippedHeadIsRefusedWhereOneParseIs(t *testing.T) {
	cfg := Config{BodyDelivery: BodyWhole, MaxHeaders: 3, MaxHeaderBytes: 1 << 10, MaxRequestLineBytes: 256}.mustResolve()
	heads := map[string]string{
		"request line":       "GET /x HTTP/9.1\r\nHost: h\r\nX-A: b\r\n",
		"field line":         "GET /x HTTP/1.1\r\nHost: h\r\nNoColon\r\nX-A: b\r\n",
		"folded line":        "GET /x HTTP/1.1\r\nHost: h\r\n folded\r\nX-A: b\r\n",
		"control character":  "\r\nGET /x HTTP/1.1\r\nHost: h\r\nX-A: b\x01c\r\nX-B: d\r\n",
		"field count":        "GET /x HTTP/1.1\r\nHost: h\r\nA: 1\r\nB: 2\r\nC: 3\r\nD: 4\r\n",
		"request line bound": "GET /" + strings.Repeat("a", cfg.MaxRequestLineBytes+8),
		"head bound":         "GET /x HTTP/1.1\r\nX-A: " + strings.Repeat("b", cfg.MaxHeaderBytes+8),
	}
	for name, head := range heads {
		raw := []byte(head)
		want, wantErr := -1, error(nil)
		for n := 1; n <= len(raw) && want < 0; n++ {
			if _, _, _, err := parseRequest(raw[:n], cfg, nil, nil); !errors.Is(err, errIncomplete) {
				want, wantErr = n, err
			}
		}
		if wantErr == nil {
			t.Fatalf("%s: one parse never refuses it", name)
		}
		var progress chunkProgress
		var scan headScan
		got, gotErr := -1, error(nil)
		for n := 1; n <= len(raw) && got < 0; n++ {
			if headStillShort(raw[:n], &cfg, &scan) {
				continue
			}
			_, _, _, err := parseRequest(raw[:n], cfg, nil, &progress)
			if errors.Is(err, errIncomplete) {
				noteShortHead(raw[:n], &cfg, &progress, &scan)
				continue
			}
			got, gotErr = n, err
		}
		if got != want || gotErr == nil || gotErr.Error() != wantErr.Error() {
			t.Errorf("%s dripped: refused at byte %d with %v, one parse at byte %d with %v", name, got, gotErr, want, wantErr)
		}
	}
}

// Through the reader itself: a head written a byte per segment is served,
// and the same connection serves the request behind it — the scan resets
// with each request.
func TestADrippedHeadIsServedByTheReader(t *testing.T) {
	addr, _ := serveTest(t, Config{}, echoTarget)
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.SetDeadline(time.Now().Add(20 * time.Second)); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(c)
	for _, target := range []string{"/dripped", "/next"} {
		head := "GET " + target + " HTTP/1.1\r\nHost: x\r\nX-Pad: " + strings.Repeat("p", 64) + "\r\n\r\n"
		for i := range len(head) {
			if _, err := c.Write([]byte{head[i]}); err != nil {
				t.Fatal(err)
			}
			time.Sleep(time.Millisecond)
		}
		status, body := readResponse(t, br)
		if !strings.Contains(status, "200") || !strings.Contains(body, target) {
			t.Fatalf("%s dripped a byte at a time: %s %q", target, status, body)
		}
	}
}
