package redis_test

import (
	"context"
	"crypto/rand"
	"errors"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/database/redis"
)

// dial connects to the server SLUICE_REDIS names ("host:port"). Each test
// works under a key prefix of its own, so runs against one server do not
// meet.
func dial(tb testing.TB) (*redis.Conn, string) {
	tb.Helper()
	addr := os.Getenv("SLUICE_REDIS")
	if addr == "" {
		tb.Skip("SLUICE_REDIS is not set: no server to run against")
	}
	ctx, cancel := context.WithTimeout(tb.Context(), 10*time.Second)
	defer cancel()
	c, err := redis.Connect(ctx, redis.DialTCP(addr), redis.Config{})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = c.Close() })
	return c, "sluice_t_" + rand.Text() + ":"
}

func TestPipelineAgainstAServer(t *testing.T) {
	c, p := dial(t)
	const n = 1000
	sets := make([][]string, n)
	gets := make([][]string, n+1)
	for i := range n {
		k := p + strconv.Itoa(i)
		sets[i] = []string{"SET", k, "v" + strconv.Itoa(i), "EX", "60"}
		gets[i] = []string{"GET", k}
	}
	// One command of the batch the server refuses, in the middle of it.
	gets = append(gets[:n/2], append([][]string{{"INCR", p + "0"}}, gets[n/2:n]...)...)
	rs, err := c.Pipeline(t.Context(), sets)
	if err != nil {
		t.Fatal(err)
	}
	for i, r := range rs {
		if r.Kind != redis.KindString || string(r.Str) != "OK" {
			t.Fatalf("SET %d: %v %q", i, r.Kind, r.Str)
		}
	}
	rs, err = c.Pipeline(t.Context(), gets)
	if err != nil {
		t.Fatal(err)
	}
	if len(rs) != n+1 {
		t.Fatalf("%d replies for %d commands", len(rs), n+1)
	}
	var e *redis.Error
	if !errors.As(rs[n/2].Err(), &e) || e.Prefix != "ERR" {
		t.Fatalf("the INCR of a non-integer: %v", rs[n/2].Err())
	}
	for i, r := range append(rs[:n/2:n/2], rs[n/2+1:]...) {
		if want := "v" + strconv.Itoa(i); r.Kind != redis.KindBulk || string(r.Str) != want {
			t.Fatalf("GET %d: %v %q, want %q", i, r.Kind, r.Str, want)
		}
	}
	if r, err := c.Do(t.Context(), "GET", p+"absent"); err != nil || r.Kind != redis.KindNil {
		t.Fatalf("a missing key: %v %v", r.Kind, err)
	}
}

func TestConnectWithAWrongPassword(t *testing.T) {
	addr := os.Getenv("SLUICE_REDIS")
	if addr == "" {
		t.Skip("SLUICE_REDIS is not set: no server to run against")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	_, err := redis.Connect(ctx, redis.DialTCP(addr), redis.Config{Password: "not-the-password"})
	var e *redis.Error
	if !errors.As(err, &e) {
		t.Fatalf("err %v, want the server's refusal", err)
	}
}
