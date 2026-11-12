package httpstream

import (
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// busyConn is a socket whose every write leaves the writer a control frame
// to send next — a peer's PING to answer, say — so the write goroutine
// always has a round to run and never parks. Once over is set, writes fail
// and the goroutine ends.
type busyConn struct {
	net.Conn // nil: only Write, SetWriteDeadline and Close are called
	w        *h2Writer
	over     atomic.Bool
}

func (c *busyConn) SetWriteDeadline(time.Time) error { return nil }
func (c *busyConn) Close() error                     { return nil }

func (c *busyConn) Write(p []byte) (int, error) {
	if c.over.Load() {
		return 0, net.ErrClosed
	}
	c.w.mu.Lock()
	_ = c.w.appendCtrl32(frameWindowUpdate, 0, 1)
	c.w.mu.Unlock()
	return len(p), nil
}

// A stream the peer grants no credit is reset once WriteTimeout passes. The
// expiry was only ever looked at by park, so a writer kept busy — a control
// frame waiting at the end of every round, or another stream always
// runnable — held the stalled stream past its deadline for as long as the
// work lasted.
func TestH2StalledStreamExpiresUnderAWriterThatNeverParks(t *testing.T) {
	cfg := Config{WriteTimeout: 50 * time.Millisecond}.mustResolve()
	conn := &busyConn{}
	w := newH2Writer(conn, cfg)
	conn.w = w
	j := &h2Job{id: 1, headSent: true, data: []byte("no credit for this"), deadline: time.Now().Add(cfg.WriteTimeout), queued: true}
	w.mu.Lock()
	w.windows[1] = 0
	w.jobs = append(w.jobs, j)
	_ = w.appendCtrl32(frameWindowUpdate, 0, 1) // a first round to run
	w.mu.Unlock()
	go w.run()
	defer func() {
		conn.over.Store(true)
		<-w.done
	}()

	limit := time.Now().Add(40 * cfg.WriteTimeout)
	for {
		w.mu.Lock()
		done, err := j.done, j.err
		w.mu.Unlock()
		if done {
			if !errors.Is(err, errWriteStalled) {
				t.Fatalf("the stalled job ended with %v, want errWriteStalled", err)
			}
			return
		}
		if time.Now().After(limit) {
			t.Fatalf("the stalled stream is still queued %v after its %v WriteTimeout, under a writer that never parks", 40*cfg.WriteTimeout, cfg.WriteTimeout)
		}
		time.Sleep(cfg.WriteTimeout / 5)
	}
}
