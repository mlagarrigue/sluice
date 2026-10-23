package gateway

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/mlagarrigue/sluice"
)

// recorder is a slog.Handler that keeps every record, for a test to read.
type recorder struct {
	mu      sync.Mutex
	records []slog.Record
}

func (r *recorder) Enabled(context.Context, slog.Level) bool { return true }

func (r *recorder) Handle(_ context.Context, rec slog.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = append(r.records, rec.Clone())
	return nil
}

func (r *recorder) WithAttrs([]slog.Attr) slog.Handler { return r }
func (r *recorder) WithGroup(string) slog.Handler      { return r }

func (r *recorder) all() []slog.Record {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]slog.Record(nil), r.records...)
}

// attr is the value of the record's attribute named key, as a string.
func attr(rec slog.Record, key string) string {
	var v string
	rec.Attrs(func(a slog.Attr) bool {
		if a.Key == key {
			v = a.Value.String()
			return false
		}
		return true
	})
	return v
}

// panickingPipeline is named so that its frame can be looked for in the
// stack the gateway logs.
func panickingPipeline(calls sluice.Stream[*Call[int, string]]) {
	calls(func(sluice.Batch[*Call[int, string]]) bool {
		panic("pipeline boom")
	})
}

// A pipeline's panic is the one failure a gateway cannot recover from, and
// the error every caller gets says nothing of its cause on purpose. The
// cause goes to Config.Logger, value and stack, and is there by the time a
// caller sees the error: an operator who reads the log after the first 503
// finds why.
func TestGatewayPipelinePanicIsLogged(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rec := &recorder{}
		gw := New(Config{Size: 1, Within: 5 * time.Millisecond, Logger: slog.New(rec)}, panickingPipeline)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := gw.Do(ctx, 1); !errors.Is(err, ErrPipelinePanicked) {
			t.Fatalf("err = %v, want ErrPipelinePanicked", err)
		}
		got := rec.all()
		if len(got) != 1 {
			t.Fatalf("the logger holds %d records when the caller is released, want 1", len(got))
		}
		r := got[0]
		if r.Level != slog.LevelError {
			t.Errorf("level %v, want error", r.Level)
		}
		if v := attr(r, "panic"); v != "pipeline boom" {
			t.Errorf("panic attribute %q, want the panic value", v)
		}
		if s := attr(r, "stack"); !strings.Contains(s, "gateway.panickingPipeline") {
			t.Errorf("the stack does not show the pipeline's frame:\n%s", s)
		}
		if err := gw.Close(); !errors.Is(err, ErrPipelinePanicked) {
			t.Errorf("Close on the dead gateway = %v, want ErrPipelinePanicked", err)
		}
	})
}

// Without a Logger, the panic goes to slog.Default, as httpstream's does.
func TestGatewayPipelinePanicDefaultLogger(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	gw := New(Config{Size: 1, Within: 5 * time.Millisecond}, panickingPipeline)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := gw.Do(ctx, 1); !errors.Is(err, ErrPipelinePanicked) {
		t.Fatalf("err = %v, want ErrPipelinePanicked", err)
	}
	_ = gw.Close()
	if !strings.Contains(buf.String(), "pipeline boom") {
		t.Errorf("slog.Default received %q, want the panic value", buf.String())
	}
}
