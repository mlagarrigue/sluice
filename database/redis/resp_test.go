package redis

import (
	"bufio"
	"errors"
	"io"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

func parse(s string, limit int) (Reply, error) {
	r := reader{br: bufio.NewReaderSize(strings.NewReader(s), 16)}
	return r.read(limit)
}

func show(r Reply) string {
	switch r.Kind {
	case KindInt:
		return strconv.FormatInt(r.Int, 10)
	case KindNil:
		return "nil"
	case KindArray:
		parts := make([]string, len(r.Array))
		for i, e := range r.Array {
			parts[i] = show(e)
		}
		return "[" + strings.Join(parts, " ") + "]"
	}
	return r.Kind.String() + ":" + string(r.Str)
}

func TestReadEachType(t *testing.T) {
	for in, want := range map[string]string{
		"+OK\r\n":                               "string:OK",
		"-ERR unknown command\r\n":              "error:ERR unknown command",
		":0\r\n":                                "0",
		":-42\r\n":                              "-42",
		":+7\r\n":                               "7",
		":9223372036854775807\r\n":              "9223372036854775807",
		":-9223372036854775808\r\n":             "-9223372036854775808",
		"$5\r\nhello\r\n":                       "bulk:hello",
		"$0\r\n\r\n":                            "bulk:",
		"$4\r\na\r\nb\r\n":                      "bulk:a\r\nb",
		"$-1\r\n":                               "nil",
		"*-1\r\n":                               "nil",
		"*0\r\n":                                "[]",
		"*3\r\n$1\r\na\r\n$-1\r\n:3\r\n":        "[bulk:a nil 3]",
		"*2\r\n*1\r\n+x\r\n-y z\r\n":            "[[string:x] error:y z]",
		"+" + strings.Repeat("s", 100) + "\r\n": "string:" + strings.Repeat("s", 100),
	} {
		r, err := parse(in, 1<<20)
		if err != nil || show(r) != want {
			t.Errorf("%q: got %s, %v; want %s", in, show(r), err, want)
		}
	}
}

func TestReadRefusesWhatIsNotRESP2(t *testing.T) {
	for _, in := range []string{
		"OK\r\n", "\r\n", "+OK\n", ":12a\r\n", ":\r\n", ":-\r\n",
		":9223372036854775808\r\n", ":-9223372036854775809\r\n", ":12345678901234567890\r\n",
		"$-2\r\n", "*-2\r\n", "$3\r\nabcd\r\n", "$3\r\nabcXY",
		"_\r\n", "%1\r\n+a\r\n+b\r\n", ",1.5\r\n", ">1\r\n+x\r\n",
		strings.Repeat("*1\r\n", maxDepth+1) + ":1\r\n",
	} {
		if _, err := parse(in, 1<<20); !errors.Is(err, ErrProtocol) {
			t.Errorf("%q: err %v, want ErrProtocol", in, err)
		}
	}
	// Nested to the bound is fine.
	if r, err := parse(strings.Repeat("*1\r\n", maxDepth)+":1\r\n", 1<<20); err != nil || r.Kind != KindArray {
		t.Errorf("nested %d deep: %v", maxDepth, err)
	}
}

func TestReadTruncated(t *testing.T) {
	for _, in := range []string{"", "+OK", "$5\r\nhel", "*2\r\n:1\r\n"} {
		_, err := parse(in, 1<<20)
		if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Errorf("%q: err %v, want an EOF", in, err)
		}
	}
}

// The bound holds before anything is allocated or read for what a peer
// announces: a bulk string, an array's count, a line with no end.
func TestReadBound(t *testing.T) {
	const bound = 64
	for name, in := range map[string]string{
		"bulk announced":  "$1000000000\r\n",
		"bulk just over":  "$61\r\n" + strings.Repeat("x", 61) + "\r\n", // a header of five, 61 of data, two of CRLF: sixty-eight
		"array count":     "*1000000000\r\n",
		"elements summed": "*3\r\n$20\r\n" + strings.Repeat("x", 20) + "\r\n$20\r\n" + strings.Repeat("y", 20) + "\r\n$20\r\n" + strings.Repeat("z", 20) + "\r\n",
		"endless line":    "+" + strings.Repeat("a", 10_000),
	} {
		if _, err := parse(in, bound); !errors.Is(err, ErrReplyTooLarge) {
			t.Errorf("%s: err %v, want ErrReplyTooLarge", name, err)
		}
	}
	at := "$56\r\n" + strings.Repeat("x", 56) + "\r\n" // a header of five, 56 of data, two of CRLF: sixty-three
	if r, err := parse(at, bound); err != nil || len(r.Str) != 56 {
		t.Errorf("a reply within the bound: %v", err)
	}
}

// A length at the end of the int64 range is refused by the bound: adding
// the CRLF to it must not wrap around and let it through.
func TestReadRefusesALengthAtTheEndOfTheInt64Range(t *testing.T) {
	for _, n := range []string{"9223372036854775806", "9223372036854775807"} {
		if _, err := parse("$"+n+"\r\n", 64); !errors.Is(err, ErrReplyTooLarge) {
			t.Errorf("$%s: err %v, want ErrReplyTooLarge", n, err)
		}
	}
}

// Every announced element owes the bound at least three bytes until it
// arrives, so nested arrays cannot each claim the whole bound: headers
// announcing more elements than the bound could carry are refused.
func TestReadChargesAnnouncedElements(t *testing.T) {
	const bound = 1 << 16
	var b strings.Builder
	left := bound
	for range maxDepth {
		n := left / 3
		for n > 0 && n > (left-len("*"+strconv.Itoa(n)+"\r\n"))/3 {
			n--
		}
		if n == 0 {
			break
		}
		h := "*" + strconv.Itoa(n) + "\r\n"
		b.WriteString(h)
		left -= len(h)
	}
	if _, err := parse(b.String(), bound); !errors.Is(err, ErrReplyTooLarge) {
		t.Errorf("nested headers announcing past the bound: err %v, want ErrReplyTooLarge", err)
	}

	// Elements of three bytes, the least one takes, fill the bound exactly.
	const n = 19 // a header of five, nineteen elements of three: sixty-two
	in := "*" + strconv.Itoa(n) + "\r\n" + strings.Repeat("+\r\n", n)
	if r, err := parse(in, 62); err != nil || len(r.Array) != n {
		t.Errorf("an array within the bound: %d elements, err %v", len(r.Array), err)
	}
}

// An announced count is not allocated for whole before its elements arrive:
// a header of eight bytes cannot make the reader allocate many times the
// bound.
func TestReadDoesNotAllocateForWhatIsOnlyAnnounced(t *testing.T) {
	const bound = 300_000
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err := parse("*99000\r\n", bound)
	runtime.ReadMemStats(&after)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("err %v, want the input to end", err)
	}
	if got := after.TotalAlloc - before.TotalAlloc; got > 4*bound {
		t.Errorf("an eight-byte header allocated %d bytes under a bound of %d", got, bound)
	}

	// A long array still reads whole.
	const long = 5000
	r, err := parse("*"+strconv.Itoa(long)+"\r\n"+strings.Repeat(":7\r\n", long), 1<<20)
	if err != nil || len(r.Array) != long || r.Array[long-1].Int != 7 {
		t.Fatalf("a long array: %d elements, err %v", len(r.Array), err)
	}
}

// The replies of one call share an arena that the next call does not reuse.
func TestRepliesOutliveTheNextRead(t *testing.T) {
	r := reader{br: bufio.NewReader(strings.NewReader("$3\r\nabc\r\n+OK\r\n$3\r\nxyz\r\n"))}
	a, _ := r.read(1 << 10)
	b, _ := r.read(1 << 10)
	r.arena = nil
	c, _ := r.read(1 << 10)
	if string(a.Str) != "abc" || string(b.Str) != "OK" || string(c.Str) != "xyz" {
		t.Fatalf("got %q %q %q", a.Str, b.Str, c.Str)
	}
}

func TestAppendCommand(t *testing.T) {
	got := string(appendCommand(nil, []string{"SET", "k", "a\r\nb", ""}))
	want := "*4\r\n$3\r\nSET\r\n$1\r\nk\r\n$4\r\na\r\nb\r\n$0\r\n\r\n"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestErrorPrefix(t *testing.T) {
	r, _ := parse("-WRONGTYPE Operation against a key\r\n", 1<<10)
	var e *Error
	if !errors.As(r.Err(), &e) || e.Prefix != "WRONGTYPE" {
		t.Fatalf("got %v", r.Err())
	}
	if (Reply{Kind: KindString}).Err() != nil {
		t.Fatal("a string reply has an error")
	}
}

// Whatever the bytes, the reader returns or fails, and never holds more
// than the bound.
func FuzzReply(f *testing.F) {
	for _, s := range []string{"+OK\r\n", "*2\r\n$1\r\na\r\n:1\r\n", "$-1\r\n", "*1\r\n*1\r\n-e\r\n", "$3\r\nab"} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		const bound = 256
		r := reader{br: bufio.NewReaderSize(strings.NewReader(string(b)), 16)}
		for range 4 {
			r.arena = nil
			if _, err := r.read(bound); err != nil {
				return
			}
			if len(r.arena) > bound {
				t.Fatalf("a reply held %d bytes, bound %d", len(r.arena), bound)
			}
		}
	})
}
