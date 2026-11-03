package postgres

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"testing/synctest"
	"time"

	"github.com/mlagarrigue/sluice/internal/pgwire"
)

// A server that reads the copy's last message — CopyDone for Close, CopyFail
// for Abort — and never answers. Close and Abort waited for it as long as the
// socket lived: neither armed a watch, and Abort had no bound at all where
// Tx.Rollback, in the same position, has CancelTimeout. Close is now bounded
// by the ctx it takes, as Tx.Commit is, and Abort by CancelTimeout.
func TestTheEndOfACopyIsBounded(t *testing.T) {
	for _, tc := range []struct {
		name string
		last byte
		end  func(cp *Copier) error
	}{
		{"Close", pgwire.FrontendCopyDone, func(cp *Copier) error {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			return cp.Close(ctx)
		}},
		{"Abort", pgwire.FrontendCopyFail, func(cp *Copier) error { return cp.Abort("silent server") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				conn, backend := newCopyBackend(t, func(rd *pgwire.Reader, w net.Conn) error {
					if err := openCopy(rd, w); err != nil {
						return err
					}
					for { // nothing is ever answered once the COPY is open
						if _, err := rd.Next(); errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) {
							return nil
						} else if err != nil {
							return err
						}
					}
				})
				conn.cancelKey = CancelKey{PID: 1, Secret: testSecret}
				packets := make(chan []byte, 4)
				conn.cancelDial = cancelHolding(0, packets) // confirms at once
				conn.cancelTimeout = time.Second
				cp, err := conn.CopyFrom(t.Context(), copyTestSQL, CopyConfig{RowsPerTx: 100})
				if err != nil {
					t.Fatal(err)
				}
				if err := cp.WriteTuples(tuple(new(int64(1)), nil), 1); err != nil {
					t.Fatal(err)
				}
				res := make(chan error, 1)
				go func() { res <- tc.end(cp) }()
				select {
				case err = <-res:
				case <-time.After(time.Minute):
					// A guard, not a measurement: the bounds are 100 ms and a
					// second, so a bounded end is back long before.
					_ = conn.rw.Close()
					<-res
					<-backend
					t.Fatalf("Copier.%s still blocked a minute after the server went silent", tc.name)
				}
				if !errors.Is(err, ErrConnBroken) {
					t.Errorf("Copier.%s = %v, want ErrConnBroken: the server never answered", tc.name, err)
				}
				_ = conn.rw.Close()
				<-backend
			})
		})
	}
}

// Close with a ctx already done sends nothing, as Tx.Commit does not, and
// leaves the copy for Abort to end.
func TestCloseWithADoneContextLeavesTheCopyToAbort(t *testing.T) {
	conn, backend := newCopyBackend(t, func(rd *pgwire.Reader, w net.Conn) error {
		if err := openCopy(rd, w); err != nil {
			return err
		}
		for {
			m, err := rd.Next()
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) {
				return nil // the client closed the connection: the end of the script
			} else if err != nil {
				return err
			}
			if m.Type == pgwire.FrontendCopyDone {
				return errors.New("Close sent CopyDone under a done ctx")
			}
			if m.Type == pgwire.FrontendCopyFail {
				return nil
			}
		}
	})
	cp, err := conn.CopyFrom(t.Context(), copyTestSQL, CopyConfig{RowsPerTx: 100})
	if err != nil {
		t.Fatal(err)
	}
	if err := cp.WriteTuples(tuple(new(int64(1)), nil), 1); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := cp.Close(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Close = %v, want ctx's error", err)
	}
	if !cp.inTx {
		t.Error("the copy's transaction is no longer Abort's to end")
	}
	_ = conn.rw.Close()
	if err := <-backend; err != nil {
		t.Error(err)
	}
}
