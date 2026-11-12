package redis_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/database/redis"
)

// stream adds n entries to a stream of the test's own and creates the
// group at its start; a second connection checks the server from outside.
func stream(t *testing.T, n int) (c, check *redis.Conn, cfg redis.GroupConfig) {
	t.Helper()
	c, p := dial(t)
	check, _ = dial(t)
	cfg = redis.GroupConfig{Stream: p + "s", Group: "g", Consumer: "c1", BatchEntries: 1000, Block: 100 * time.Millisecond}
	t.Cleanup(func() { _, _ = check.Do(context.Background(), "DEL", cfg.Stream) })
	if r, err := check.Do(t.Context(), "XGROUP", "CREATE", cfg.Stream, cfg.Group, "0", "MKSTREAM"); err != nil || r.Err() != nil {
		t.Fatalf("XGROUP CREATE: %v %v", err, r.Err())
	}
	adds := make([][]string, n)
	for i := range adds {
		adds[i] = []string{"XADD", cfg.Stream, "*", "n", strconv.Itoa(i)}
	}
	if _, err := check.Pipeline(t.Context(), adds); err != nil {
		t.Fatal(err)
	}
	return c, check, cfg
}

func pending(t *testing.T, check *redis.Conn, cfg redis.GroupConfig) int64 {
	t.Helper()
	r, err := check.Do(t.Context(), "XPENDING", cfg.Stream, cfg.Group)
	if err != nil || r.Err() != nil || r.Kind != redis.KindArray {
		t.Fatalf("XPENDING: %v %v", err, r.Err())
	}
	return r.Array[0].Int
}

// 2,500 entries in batches of 1,000; each batch acknowledged once
// processed, and nothing pending before that.
func TestGroupReadsInBatchesAndAcknowledgesOnlyWhatItIsTold(t *testing.T) {
	c, check, cfg := stream(t, 2500)
	g, err := c.ReadGroup(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	src := g.Entries(ctx)
	var sizes []int
	seen := 0
	for b := range src.Stream() {
		sizes = append(sizes, len(b.Items))
		for i, e := range b.Items {
			if string(e.Fields[0].Name) != "n" || string(e.Fields[0].Value) != strconv.Itoa(seen+i) {
				t.Fatalf("entry %d: %s %q=%q", seen+i, e.ID, e.Fields[0].Name, e.Fields[0].Value)
			}
		}
		seen += len(b.Items)
		// This batch delivered and not yet acknowledged, the ones before it
		// acknowledged with the read that brought this one: exactly this
		// batch is pending.
		if p := pending(t, check, cfg); p != int64(len(b.Items)) {
			t.Fatalf("after %d delivered, %d pending; want this batch's %d", seen, p, len(b.Items))
		}
		ids := make([]string, len(b.Items))
		for i, e := range b.Items {
			ids[i] = e.ID
		}
		g.Ack(ids...)
		if seen == 2500 {
			cancel()
		}
	}
	if err := src.Err(); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(sizes) != "[1000 1000 500]" {
		t.Fatalf("batches %v", sizes)
	}
	if p := pending(t, check, cfg); p != 0 {
		t.Fatalf("%d pending after every batch was acknowledged", p)
	}
	// The connection is still usable after the stream stopped on ctx.
	if _, err := c.Do(t.Context(), "PING"); err != nil {
		t.Fatal(err)
	}
}

// A consumer that stops in the middle of its first batch, having
// acknowledged nothing, finds the batch again first the next time it reads,
// before the entries never delivered.
func TestGroupRedeliversWhatWasNotAcknowledged(t *testing.T) {
	c, check, cfg := stream(t, 2500)
	g, _ := c.ReadGroup(cfg)
	var first []string
	for b := range g.Entries(t.Context()).Stream() {
		for _, e := range b.Items[:10] {
			first = append(first, e.ID)
		}
		g.Ack(first[:3]...) // three processed before the stop
		break
	}
	if p := pending(t, check, cfg); p != 997 {
		t.Fatalf("%d pending after a stop with three acknowledged, want 997", p)
	}

	g, _ = c.ReadGroup(cfg)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	src := g.Entries(ctx)
	var sizes []int
	for b := range src.Stream() {
		if len(sizes) == 0 && (b.Items[0].ID != first[3] || len(b.Items) != 997) {
			t.Fatalf("first batch after the restart: %d entries from %s, want the 997 pending from %s", len(b.Items), b.Items[0].ID, first[3])
		}
		sizes = append(sizes, len(b.Items))
		for _, e := range b.Items {
			g.Ack(e.ID)
		}
		if total(sizes) == 2497 {
			cancel()
		}
	}
	if err := src.Err(); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(sizes) != "[997 1000 500]" {
		t.Fatalf("batches %v", sizes)
	}
}

func total(ns []int) int {
	s := 0
	for _, n := range ns {
		s += n
	}
	return s
}

func TestGroupThatDoesNotExist(t *testing.T) {
	c, p := dial(t)
	g, _ := c.ReadGroup(redis.GroupConfig{Stream: p + "none", Group: "g", Consumer: "c"})
	_, err := g.Entries(t.Context()).Collect()
	var e *redis.Error
	if !errors.As(err, &e) || e.Prefix != "NOGROUP" {
		t.Fatalf("err %v, want NOGROUP", err)
	}
}
