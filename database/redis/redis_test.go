package redis

import (
	"bufio"
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"
)

// fakeServer answers each command it reads with what answer returns for
// its arguments, over one end of a pipe.
func fakeServer(t *testing.T, answer func(args []string) string) DialFunc {
	t.Helper()
	return func(context.Context) (net.Conn, error) {
		client, server := net.Pipe()
		t.Cleanup(func() { _ = server.Close() })
		go func() {
			br := bufio.NewReader(server)
			r := reader{br: br}
			for {
				r.arena = nil
				cmd, err := r.read(1 << 20)
				if err != nil {
					return
				}
				args := make([]string, len(cmd.Array))
				for i, a := range cmd.Array {
					args[i] = string(a.Str)
				}
				if _, err := server.Write([]byte(answer(args))); err != nil {
					return
				}
			}
		}()
		return client, nil
	}
}

func kv(args []string) string {
	switch strings.ToUpper(args[0]) {
	case "PING":
		return "+PONG\r\n"
	case "GET":
		if args[1] == "missing" {
			return "$-1\r\n"
		}
		return "$" + itoa(len(args[1])) + "\r\n" + args[1] + "\r\n"
	case "INCR":
		return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
	case "HANG":
		time.Sleep(time.Hour)
	}
	return "-ERR unknown command\r\n"
}

func itoa(n int) string { return string(appendUint(nil, uint64(n))) } //nolint:gosec // G115: test lengths

func TestPipelineRepliesInOrderWithAnErrorInPlace(t *testing.T) {
	c, err := Connect(t.Context(), fakeServer(t, kv), Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	rs, err := c.Pipeline(t.Context(), [][]string{{"GET", "a"}, {"INCR", "a"}, {"GET", "missing"}, {"GET", "bc"}})
	if err != nil {
		t.Fatal(err)
	}
	if got := show(Reply{Kind: KindArray, Array: rs}); got != "[bulk:a error:WRONGTYPE Operation against a key holding the wrong kind of value nil bulk:bc]" {
		t.Fatalf("got %s", got)
	}
	var e *Error
	if !errors.As(rs[1].Err(), &e) || e.Prefix != "WRONGTYPE" {
		t.Fatalf("the error reply: %v", rs[1].Err())
	}
	// The connection goes on after a server error.
	if r, err := c.Do(t.Context(), "GET", "z"); err != nil || string(r.Str) != "z" {
		t.Fatalf("after the error: %v, %v", show(r), err)
	}
}

func TestPipelineEndedByContextBreaksTheConnection(t *testing.T) {
	c, err := Connect(t.Context(), fakeServer(t, kv), Config{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	_, err = c.Do(ctx, "HANG")
	if !errors.Is(err, ErrConnBroken) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err %v, want ErrConnBroken and the deadline", err)
	}
	if _, err := c.Do(t.Context(), "PING"); !errors.Is(err, ErrConnBroken) {
		t.Fatalf("a call after the break: %v", err)
	}
}

func TestConnectReportsARefusal(t *testing.T) {
	denied := func([]string) string { return "-DENIED Redis is running in protected mode\r\n" }
	_, err := Connect(t.Context(), fakeServer(t, denied), Config{})
	var e *Error
	if !errors.As(err, &e) || e.Prefix != "DENIED" {
		t.Fatalf("err %v, want the server's DENIED", err)
	}
	var sent [][]string
	record := func(args []string) string {
		sent = append(sent, args)
		return "+OK\r\n"
	}
	if _, err := Connect(t.Context(), fakeServer(t, record), Config{User: "u", Password: "p", Database: 2}); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join([]string{strings.Join(sent[0], " "), strings.Join(sent[1], " "), strings.Join(sent[2], " ")}, "|"); got != "AUTH u p|SELECT 2|PING" {
		t.Fatalf("sent %s", got)
	}
}

func TestConfigRefusesNegatives(t *testing.T) {
	_, err := Connect(t.Context(), func(context.Context) (net.Conn, error) {
		t.Fatal("dialled with a refused config")
		return nil, nil
	}, Config{Database: -1, MaxReplyBytes: -1})
	if err == nil || !strings.Contains(err.Error(), "Database") || !strings.Contains(err.Error(), "MaxReplyBytes") {
		t.Fatalf("err %v, want both fields named", err)
	}
}

func TestReplyPastTheBoundBreaksTheConnection(t *testing.T) {
	big := func(args []string) string {
		if args[0] == "PING" {
			return "+PONG\r\n"
		}
		return "$100\r\n" + strings.Repeat("x", 100) + "\r\n"
	}
	c, err := Connect(t.Context(), fakeServer(t, big), Config{MaxReplyBytes: 64})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Do(t.Context(), "GET", "k"); !errors.Is(err, ErrReplyTooLarge) || !errors.Is(err, ErrConnBroken) {
		t.Fatalf("err %v", err)
	}
}

// A server that announces a bulk string at the end of the int64 range gets
// the bound's refusal, not a panic out of Do.
func TestALengthAtTheEndOfTheInt64RangeIsRefused(t *testing.T) {
	huge := func(args []string) string {
		if args[0] == "PING" {
			return "+PONG\r\n"
		}
		return "$9223372036854775807\r\n"
	}
	c, err := Connect(t.Context(), fakeServer(t, huge), Config{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Do(t.Context(), "GET", "k"); !errors.Is(err, ErrReplyTooLarge) || !errors.Is(err, ErrConnBroken) {
		t.Fatalf("err %v", err)
	}
}

func TestPipelineRefusesAnEmptyCommand(t *testing.T) {
	c, err := Connect(t.Context(), fakeServer(t, kv), Config{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Pipeline(t.Context(), [][]string{{"PING"}, {}}); err == nil || errors.Is(err, ErrConnBroken) {
		t.Fatalf("err %v: an empty command is refused before anything is sent", err)
	}
	if _, err := c.Do(t.Context(), "PING"); err != nil {
		t.Fatalf("the connection after a refused batch: %v", err)
	}
}

func TestParseEntries(t *testing.T) {
	if es, err := parseEntries(Reply{Kind: KindNil}); err != nil || es != nil {
		t.Fatalf("a BLOCK that timed out: %v %v", es, err)
	}
	bulk := func(s string) Reply { return Reply{Kind: KindBulk, Str: []byte(s)} }
	arr := func(rs ...Reply) Reply { return Reply{Kind: KindArray, Array: rs} }
	ok := arr(arr(bulk("s"), arr(
		arr(bulk("1-0"), arr(bulk("a"), bulk("x"), bulk("b"), bulk("y"))),
		arr(bulk("2-0"), Reply{Kind: KindNil}),
	)))
	es, err := parseEntries(ok)
	if err != nil || len(es) != 2 || es[0].ID != "1-0" || string(es[0].Fields[1].Value) != "y" || !es[1].Deleted || es[0].Deleted {
		t.Fatalf("got %+v, %v", es, err)
	}
	for name, r := range map[string]Reply{
		"two streams":  arr(arr(bulk("s"), arr()), arr(bulk("t"), arr())),
		"no entries":   arr(arr(bulk("s"))),
		"odd fields":   arr(arr(bulk("s"), arr(arr(bulk("1-0"), arr(bulk("a")))))),
		"integer id":   arr(arr(bulk("s"), arr(arr(Reply{Kind: KindInt}, arr())))),
		"string reply": {Kind: KindString, Str: []byte("OK")},
	} {
		if _, err := parseEntries(r); !errors.Is(err, ErrProtocol) {
			t.Errorf("%s: err %v, want ErrProtocol", name, err)
		}
	}
}

func TestReadGroupConfig(t *testing.T) {
	_, err := (&Conn{}).ReadGroup(GroupConfig{BatchEntries: -1, Block: -time.Second})
	for _, field := range []string{"Stream", "Group", "Consumer", "BatchEntries", "Block"} {
		if err == nil || !strings.Contains(err.Error(), field) {
			t.Errorf("err %v does not name %s", err, field)
		}
	}
	g, err := (&Conn{}).ReadGroup(GroupConfig{Stream: "s", Group: "g", Consumer: "c"})
	if err != nil || g.cfg.BatchEntries != 1024 || g.cfg.Block != time.Second {
		t.Fatalf("defaults: %+v, %v", g.cfg, err)
	}
}
