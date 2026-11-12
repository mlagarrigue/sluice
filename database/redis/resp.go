package redis

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
)

// Kind is the RESP2 type of a [Reply].
type Kind uint8

// The RESP2 types. A null bulk string and a null array are both [KindNil]:
// RESP2 has two spellings of one absent value, and RESP3 merges them.
const (
	KindString Kind = iota + 1 // a simple string, +OK
	KindError                  // a simple error, -ERR …
	KindInt                    // an integer, :42
	KindBulk                   // a bulk string, binary-safe
	KindNil                    // $-1 or *-1: no value
	KindArray                  // an array of replies
)

func (k Kind) String() string {
	switch k {
	case KindString:
		return "string"
	case KindError:
		return "error"
	case KindInt:
		return "integer"
	case KindBulk:
		return "bulk"
	case KindNil:
		return "nil"
	case KindArray:
		return "array"
	}
	return fmt.Sprintf("Kind(%d)", uint8(k))
}

// Reply is one reply of the server, as RESP2 writes it.
//
// Str holds a simple string, an error's text or a bulk string's bytes; Int
// an integer; Array an array's elements. Its bytes belong to the caller: they
// are not reused by a later call.
type Reply struct {
	Kind  Kind
	Str   []byte
	Int   int64
	Array []Reply
}

// Err is the server's error when the reply is one, as an [*Error], and nil
// otherwise. A server error answers one command: the connection goes on.
func (r Reply) Err() error {
	if r.Kind != KindError {
		return nil
	}
	return newError(string(r.Str))
}

// Error is an error reply of the server. Prefix is its first word — ERR,
// WRONGTYPE, NOAUTH… — which the server uses as the error's kind, by
// convention rather than by protocol; Message is the whole text.
type Error struct {
	Prefix  string
	Message string
}

func newError(msg string) *Error {
	prefix, _, _ := strings.Cut(msg, " ")
	return &Error{Prefix: prefix, Message: msg}
}

func (e *Error) Error() string { return "redis: " + e.Message }

var (
	// ErrConnBroken reports a connection whose place in the protocol is no
	// longer known: a write that failed, a read that stopped mid-reply, a
	// reply this package cannot parse or will not hold. Every later call on
	// the connection returns it; close it and open another.
	ErrConnBroken = errors.New("redis: connection is out of sync and cannot be reused")
	// ErrReplyTooLarge reports a reply past [Config.MaxReplyBytes]. It
	// breaks the connection, whose stream is left mid-reply.
	ErrReplyTooLarge = errors.New("redis: reply exceeds MaxReplyBytes")
	// ErrProtocol reports bytes that are not a RESP2 reply. It breaks the
	// connection.
	ErrProtocol = errors.New("redis: malformed reply")
)

// maxDepth bounds how deeply arrays nest in one reply. No command of the
// server nests past a few levels (XREADGROUP: four); a peer that sends a
// thousand opening arrays would otherwise make the reader recurse a
// thousand times.
const maxDepth = 32

// reader parses the replies of one connection. Its bytes go to arena, which
// a call hands out with its replies and never reuses: the replies a caller
// holds stay valid.
type reader struct {
	br     *bufio.Reader
	budget int // bytes the reply being read may still take
	arena  []byte
}

// read parses one complete reply, which may take at most limit bytes of the
// stream, counted as they stand on the wire.
func (r *reader) read(limit int) (Reply, error) {
	r.budget = limit
	return r.reply(0)
}

func (r *reader) reply(depth int) (Reply, error) {
	line, err := r.line()
	if err != nil {
		return Reply{}, err
	}
	if len(line) == 0 {
		return Reply{}, fmt.Errorf("%w: an empty line", ErrProtocol)
	}
	body := line[1:]
	switch line[0] {
	case '+':
		return Reply{Kind: KindString, Str: r.keep(body)}, nil
	case '-':
		return Reply{Kind: KindError, Str: r.keep(body)}, nil
	case ':':
		n, err := parseInt(body)
		if err != nil {
			return Reply{}, err
		}
		return Reply{Kind: KindInt, Int: n}, nil
	case '$':
		n, err := parseInt(body)
		if err != nil {
			return Reply{}, err
		}
		if n == -1 {
			return Reply{Kind: KindNil}, nil
		}
		if n < 0 {
			return Reply{}, fmt.Errorf("%w: a bulk string of length %d", ErrProtocol, n)
		}
		if n+2 > int64(r.budget) {
			return Reply{}, fmt.Errorf("%w: a bulk string of %d bytes", ErrReplyTooLarge, n)
		}
		r.budget -= int(n) + 2
		at := len(r.arena)
		r.arena = slices.Grow(r.arena, int(n)+2)[:at+int(n)+2]
		if _, err := io.ReadFull(r.br, r.arena[at:]); err != nil {
			return Reply{}, err
		}
		if r.arena[len(r.arena)-2] != '\r' || r.arena[len(r.arena)-1] != '\n' {
			return Reply{}, fmt.Errorf("%w: a bulk string not ended by CRLF", ErrProtocol)
		}
		r.arena = r.arena[:len(r.arena)-2]
		return Reply{Kind: KindBulk, Str: r.arena[at:len(r.arena):len(r.arena)]}, nil
	case '*':
		n, err := parseInt(body)
		if err != nil {
			return Reply{}, err
		}
		if n == -1 {
			return Reply{Kind: KindNil}, nil
		}
		if n < 0 {
			return Reply{}, fmt.Errorf("%w: an array of length %d", ErrProtocol, n)
		}
		if depth == maxDepth {
			return Reply{}, fmt.Errorf("%w: arrays nested past %d levels", ErrProtocol, maxDepth)
		}
		// Each element takes at least three bytes (":0" would be four, an
		// empty line is refused): an announced count is held to what the
		// budget could carry before anything is allocated for it.
		if n > int64(r.budget/3) {
			return Reply{}, fmt.Errorf("%w: an array of %d elements", ErrReplyTooLarge, n)
		}
		elems := make([]Reply, n)
		for i := range elems {
			if elems[i], err = r.reply(depth + 1); err != nil {
				return Reply{}, err
			}
		}
		return Reply{Kind: KindArray, Array: elems}, nil
	}
	return Reply{}, fmt.Errorf("%w: type byte %q (RESP3 is not negotiated)", ErrProtocol, line[0])
}

// line reads one CRLF-terminated line and returns it without the CRLF,
// charging it to the budget. The slice is valid until the next read.
func (r *reader) line() ([]byte, error) {
	b, err := r.br.ReadSlice('\n')
	if errors.Is(err, bufio.ErrBufferFull) {
		// Longer than the buffer: gathered, within the budget.
		long := append([]byte(nil), b...)
		for errors.Is(err, bufio.ErrBufferFull) && len(long) <= r.budget {
			b, err = r.br.ReadSlice('\n')
			long = append(long, b...)
		}
		b = long
	}
	if len(b) > r.budget {
		return nil, fmt.Errorf("%w: a line of %d bytes", ErrReplyTooLarge, len(b))
	}
	if err != nil {
		return nil, err
	}
	r.budget -= len(b)
	if len(b) < 2 || b[len(b)-2] != '\r' {
		return nil, fmt.Errorf("%w: a line not ended by CRLF", ErrProtocol)
	}
	return b[:len(b)-2], nil
}

// keep copies b into the arena.
func (r *reader) keep(b []byte) []byte {
	at := len(r.arena)
	r.arena = append(r.arena, b...)
	return r.arena[at:len(r.arena):len(r.arena)]
}

// parseInt reads a RESP integer: an optional sign and decimal digits, in
// the range of an int64.
func parseInt(b []byte) (int64, error) {
	neg := false
	if len(b) > 0 && (b[0] == '-' || b[0] == '+') {
		neg = b[0] == '-'
		b = b[1:]
	}
	if len(b) == 0 || len(b) > 19 {
		return 0, fmt.Errorf("%w: an integer of %d digits", ErrProtocol, len(b))
	}
	var n uint64
	for _, c := range b {
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("%w: %q in an integer", ErrProtocol, c)
		}
		n = n*10 + uint64(c-'0')
	}
	if neg {
		if n > 1<<63 {
			return 0, fmt.Errorf("%w: an integer below the int64 range", ErrProtocol)
		}
		return -int64(n), nil //nolint:gosec // G115: checked against 2^63 just above
	}
	if n > 1<<63-1 {
		return 0, fmt.Errorf("%w: an integer past the int64 range", ErrProtocol)
	}
	return int64(n), nil //nolint:gosec // G115: checked against the int64 range just above
}

// appendCommand writes one command as RESP: an array of bulk strings.
func appendCommand(dst []byte, args []string) []byte {
	dst = append(dst, '*')
	dst = appendUint(dst, uint64(len(args)))
	dst = append(dst, '\r', '\n')
	for _, a := range args {
		dst = append(dst, '$')
		dst = appendUint(dst, uint64(len(a)))
		dst = append(dst, '\r', '\n')
		dst = append(dst, a...)
		dst = append(dst, '\r', '\n')
	}
	return dst
}

func appendUint(dst []byte, n uint64) []byte {
	var buf [20]byte
	i := len(buf)
	for {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
		if n == 0 {
			break
		}
	}
	return append(dst, buf[i:]...)
}
