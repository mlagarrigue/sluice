package nats

import (
	"bufio"
	"errors"
	"io"
	"strings"
	"testing"
)

func readOp(s string, bound int) (op, error) {
	r := reader{br: bufio.NewReaderSize(strings.NewReader(s), 16), maxPayload: bound}
	return r.read()
}

func TestReadEachOperation(t *testing.T) {
	for in, check := range map[string]func(op) bool{
		"PING\r\n": func(o op) bool { return o.kind == opPing },
		"pong\r\n": func(o op) bool { return o.kind == opPong },
		"+OK\r\n":  func(o op) bool { return o.kind == opOK },
		"-ERR 'Authorization Violation'\r\n": func(o op) bool {
			return o.kind == opErr && string(o.arg) == "Authorization Violation"
		},
		"INFO {\"headers\":true}\r\n": func(o op) bool { return o.kind == opInfo && string(o.arg) == `{"headers":true}` },
		"MSG a.b 7 5\r\nhello\r\n": func(o op) bool {
			return o.kind == opMsg && o.subject == "a.b" && o.sid == "7" && o.reply == "" && string(o.data) == "hello" && o.header == nil
		},
		"MSG a 1 r.s 0\r\n\r\n": func(o op) bool { return o.reply == "r.s" && len(o.data) == 0 },
		"HMSG a 1 $JS.ACK.x 12 14\r\nNATS/1.0\r\n\r\nhi\r\n": func(o op) bool {
			return o.reply == "$JS.ACK.x" && string(o.header) == "NATS/1.0\r\n\r\n" && string(o.data) == "hi"
		},
		"HMSG _INBOX.p 2 28 28\r\nNATS/1.0 408 Request Timeout\r\n\r\n": func(o op) bool {
			c, d := status(o.header)
			return c == 408 && d == "Request Timeout" && len(o.data) == 0
		},
	} {
		o, err := readOp(in, 1<<20)
		if err != nil || !check(o) {
			t.Errorf("%q: %+v, %v", in, o, err)
		}
	}
}

func TestReadRefusesWhatIsNotNATS(t *testing.T) {
	for _, in := range []string{
		"HELLO\r\n", "PING\n", "PING x\r\n", "MSG a 1\r\n", "MSG a 1 2 3 4\r\n", "MSG a 1 x\r\n",
		"MSG a 1 -1\r\n", "MSG a 1 1234567890\r\n", "HMSG a 1 5 3\r\nabc\r\n", "MSG a 1 3\r\nabcXY",
	} {
		if _, err := readOp(in, 1<<20); !errors.Is(err, ErrProtocol) {
			t.Errorf("%q: err %v, want ErrProtocol", in, err)
		}
	}
}

func TestReadBound(t *testing.T) {
	if _, err := readOp("MSG a 1 100\r\n"+strings.Repeat("x", 100)+"\r\n", 64); !errors.Is(err, ErrTooLarge) {
		t.Errorf("a payload past the bound: %v", err)
	}
	if _, err := readOp("INFO "+strings.Repeat("x", maxControlLine), 1<<20); !errors.Is(err, ErrTooLarge) {
		t.Errorf("a line with no end: %v", err)
	}
	if o, err := readOp("MSG a 1 64\r\n"+strings.Repeat("x", 64)+"\r\n", 64); err != nil || len(o.data) != 64 {
		t.Errorf("a payload at the bound: %v", err)
	}
}

func TestReadTruncated(t *testing.T) {
	for _, in := range []string{"", "PI", "MSG a 1 5\r\nhel"} {
		if _, err := readOp(in, 1<<20); !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Errorf("%q: %v", in, err)
		}
	}
}

func TestStatusLine(t *testing.T) {
	for in, want := range map[string]int{
		"NATS/1.0 404 No Messages\r\n\r\n": 404,
		"NATS/1.0\r\nX: y\r\n\r\n":         0,
		"NATS/1.0 40 Short\r\n\r\n":        0,
		"HTTP/1.1 404 Nope\r\n\r\n":        0,
		"":                                 0,
	} {
		if c, _ := status([]byte(in)); c != want {
			t.Errorf("%q: %d, want %d", in, c, want)
		}
	}
}

func TestPublishLineAndSubjects(t *testing.T) {
	if got := string(appendPub(nil, "a.b", "r", []byte("hi"))); got != "PUB a.b r 2\r\nhi\r\n" {
		t.Fatalf("got %q", got)
	}
	for _, s := range []string{"", "a b", "a\r\nPUB x 0", "a\tb"} {
		if validSubject(s) {
			t.Errorf("%q accepted as a subject", s)
		}
	}
}

func TestConsumerConfig(t *testing.T) {
	_, err := (&Conn{}).Consume(ConsumerConfig{Stream: "a.b", Consumer: "", BatchMessages: -1, Expires: -1})
	for _, f := range []string{"Stream", "Consumer", "BatchMessages", "Expires"} {
		if err == nil || !strings.Contains(err.Error(), f) {
			t.Errorf("err %v does not name %s", err, f)
		}
	}
	k, err := (&Conn{}).Consume(ConsumerConfig{Stream: "S", Consumer: "C"})
	if err != nil || k.cfg.BatchMessages != DefaultBatchMessages || k.cfg.Expires.Seconds() != 1 {
		t.Fatalf("defaults: %+v %v", k.cfg, err)
	}
}

// Whatever the bytes, the reader returns or fails, and never hands out a
// payload past its bound.
func FuzzServerOp(f *testing.F) {
	for _, s := range []string{"PING\r\n", "MSG a 1 2\r\nhi\r\n", "HMSG a 1 r 12 12\r\nNATS/1.0\r\n\r\n\r\n", "INFO {}\r\n", "-ERR 'x'\r\n"} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		const bound = 256
		r := reader{br: bufio.NewReaderSize(strings.NewReader(string(b)), 16), maxPayload: bound}
		for range 4 {
			o, err := r.read()
			if err != nil {
				return
			}
			if len(o.header)+len(o.data) > bound {
				t.Fatalf("a message of %d bytes, bound %d", len(o.header)+len(o.data), bound)
			}
		}
	})
}
