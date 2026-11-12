package nats

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
)

var (
	// ErrConnBroken reports a connection whose place in the protocol is no
	// longer known: a write that failed, a read that stopped mid-message, a
	// server operation this package cannot parse or will not hold. Every
	// later call returns it; close the connection and open another.
	ErrConnBroken = errors.New("nats: connection is out of sync and cannot be reused")
	// ErrTooLarge reports a received message past
	// [Config.MaxPayloadBytes], or a control line past 64 KiB, which break
	// the connection; and a message to send past the server's max_payload,
	// refused before anything is sent, the connection kept.
	ErrTooLarge = errors.New("nats: message exceeds the bound")
	// ErrProtocol reports bytes that are not a NATS server operation. It
	// breaks the connection.
	ErrProtocol = errors.New("nats: malformed server operation")
)

// Error is an -ERR the server sent. The server closes the connection after
// most of them (an authorization violation, a parser error, a stale
// connection); after a permissions violation or an invalid subject it keeps
// it, and so does this package.
type Error struct {
	Message string
}

func (e *Error) Error() string { return "nats: server error: " + e.Message }

// maxControlLine bounds a line of the server: an INFO, a MSG's subject,
// reply and sizes. The server's own INFO lists its cluster's URLs and is
// the longest it sends; 64 KiB holds it with room.
const maxControlLine = 64 << 10

// opKind is a server operation.
type opKind uint8

const (
	opMsg opKind = iota + 1 // MSG or HMSG
	opPing
	opPong
	opOK
	opErr
	opInfo
)

// op is one server operation as read: for a message, its subject, sid,
// reply subject, raw headers (HMSG) and payload; for INFO and -ERR, the
// rest of the line.
type op struct {
	kind    opKind
	subject string
	sid     string
	reply   string
	header  []byte
	data    []byte
	arg     []byte
}

// reader parses the operations of one connection. Its messages' bytes are
// fresh allocations: what a caller keeps stays valid.
type reader struct {
	br         *bufio.Reader
	maxPayload int
}

// read parses one server operation.
func (r *reader) read() (op, error) {
	line, err := r.line()
	if err != nil {
		return op{}, err
	}
	verb, rest, _ := bytes.Cut(line, []byte{' '})
	switch {
	case eqFold(verb, "MSG"):
		return r.msg(rest, false)
	case eqFold(verb, "HMSG"):
		return r.msg(rest, true)
	case eqFold(verb, "PING") && len(rest) == 0:
		return op{kind: opPing}, nil
	case eqFold(verb, "PONG") && len(rest) == 0:
		return op{kind: opPong}, nil
	case eqFold(verb, "+OK") && len(rest) == 0:
		return op{kind: opOK}, nil
	case eqFold(verb, "-ERR"):
		return op{kind: opErr, arg: bytes.Trim(bytes.TrimSpace(rest), "'")}, nil
	case eqFold(verb, "INFO"):
		return op{kind: opInfo, arg: bytes.Clone(rest)}, nil
	}
	return op{}, fmt.Errorf("%w: operation %q", ErrProtocol, verb)
}

// msg reads a MSG or HMSG after its verb: subject, sid, an optional reply
// subject, the header size (HMSG) and the total size, then the bytes.
//
//	MSG <subject> <sid> [reply-to] <#bytes>
//	HMSG <subject> <sid> [reply-to] <#header bytes> <#total bytes>
func (r *reader) msg(args []byte, headers bool) (op, error) {
	f := bytes.Fields(args)
	sizes := 1
	if headers {
		sizes = 2
	}
	if len(f) != 2+sizes && len(f) != 3+sizes {
		return op{}, fmt.Errorf("%w: a message line of %d fields", ErrProtocol, len(f))
	}
	m := op{kind: opMsg, subject: string(f[0]), sid: string(f[1])}
	if len(f) == 3+sizes {
		m.reply = string(f[2])
	}
	total, err := size(f[len(f)-1])
	if err != nil {
		return op{}, err
	}
	hdr := 0
	if headers {
		if hdr, err = size(f[len(f)-2]); err != nil {
			return op{}, err
		}
		if hdr > total {
			return op{}, fmt.Errorf("%w: headers of %d bytes in a message of %d", ErrProtocol, hdr, total)
		}
	}
	if total > r.maxPayload {
		return op{}, fmt.Errorf("%w: a message of %d bytes, bound %d", ErrTooLarge, total, r.maxPayload)
	}
	buf := make([]byte, total+2)
	if _, err := io.ReadFull(r.br, buf); err != nil {
		return op{}, err
	}
	if buf[total] != '\r' || buf[total+1] != '\n' {
		return op{}, fmt.Errorf("%w: a message not ended by CRLF", ErrProtocol)
	}
	m.header, m.data = buf[:hdr:hdr], buf[hdr:total:total]
	if !headers {
		m.header = nil
	}
	return m, nil
}

// line reads one CRLF-terminated control line, without the CRLF; the slice
// is valid until the next read.
func (r *reader) line() ([]byte, error) {
	b, err := r.br.ReadSlice('\n')
	if errors.Is(err, bufio.ErrBufferFull) {
		long := append([]byte(nil), b...)
		for errors.Is(err, bufio.ErrBufferFull) && len(long) <= maxControlLine {
			b, err = r.br.ReadSlice('\n')
			long = append(long, b...)
		}
		b = long
	}
	if len(b) > maxControlLine {
		return nil, fmt.Errorf("%w: a control line of %d bytes", ErrTooLarge, len(b))
	}
	if err != nil {
		return nil, err
	}
	if len(b) < 2 || b[len(b)-2] != '\r' {
		return nil, fmt.Errorf("%w: a line not ended by CRLF", ErrProtocol)
	}
	return b[:len(b)-2], nil
}

// size reads a byte count: decimal digits, at most nine.
func size(b []byte) (int, error) {
	if len(b) == 0 || len(b) > 9 {
		return 0, fmt.Errorf("%w: a size of %q", ErrProtocol, b)
	}
	n := 0
	for _, c := range b {
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("%w: a size of %q", ErrProtocol, b)
		}
		n = n*10 + int(c-'0')
	}
	return n, nil
}

func eqFold(b []byte, s string) bool { return bytes.EqualFold(b, []byte(s)) }

// status reads the status a header-only message carries — "NATS/1.0 404 No
// Messages" — and returns its code and description; code 0 when the
// headers carry none.
func status(header []byte) (code int, desc string) {
	first, _, _ := bytes.Cut(header, []byte("\r\n"))
	v, rest, ok := bytes.Cut(first, []byte{' '})
	if !ok || !bytes.Equal(v, []byte("NATS/1.0")) {
		return 0, ""
	}
	c, d, _ := bytes.Cut(rest, []byte{' '})
	n, err := strconv.Atoi(string(c))
	if err != nil || len(c) != 3 {
		return 0, ""
	}
	return n, string(bytes.TrimSpace(d))
}
