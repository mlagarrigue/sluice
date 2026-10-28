package vertical

import (
	"context"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/database/postgres"
	"github.com/mlagarrigue/sluice/example/internal/pgfake"
)

// A server that stops answering mid-batch holds the batch for the service's
// own timeout and no longer: the batch's context ends, the connection
// cancels, the cancellation is not confirmed, and the connection is closed
// under the batch, whose callers are answered. The call after it is
// answered too — the gateway is not left behind the silent server. Until
// 2026-10-28 the batch ran under context.Background(): nothing on this side
// was armed, the transaction's Timeout bounds the server's work only, and
// every batch behind waited for as long as the socket lived
// (RP-20261027-secapp-04).
func TestASilentServerHoldsABatchForTheServiceTimeoutOnly(t *testing.T) {
	release := make(chan struct{})
	backend := &pgfake.Backend{Exec: func(sql string, _ [][]byte, _ map[string]string) pgfake.Result {
		if sql == readLines {
			<-release // the server goes silent on the batch's query
		}
		return pgfake.Result{}
	}}
	dctx, dcancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dcancel()
	conn, err := postgres.Connect(dctx, backend.Dial(t), postgres.Config{User: "u", CancelTimeout: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	s := NewService(conn, 64, time.Millisecond)
	s.timeout = 200 * time.Millisecond
	t.Cleanup(func() { _ = s.Close() })
	// Registered last, so run first: the silent server is released before
	// Close waits on a pipeline it may hold, and before the backend's own
	// cleanup waits on it.
	t.Cleanup(func() { close(release) })

	for i, what := range []string{"the batch the server went silent on", "the call after it"} {
		start := time.Now()
		done := make(chan error, 1)
		go func() {
			_, err := s.Apply(context.Background(), "acme", Amendment{OrderID: 1, Index: 0, SaleQty: 2})
			done <- err
		}()
		select {
		case err := <-done:
			if err == nil {
				t.Errorf("%s: answered without an error by a server that never answers", what)
			}
			if took := time.Since(start); i == 0 && took > 2*time.Second {
				t.Errorf("%s: answered after %v, want the service's 200 ms and the 100 ms cancellation", what, took)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("%s: still waiting 10 s on a server that went silent", what)
		}
	}
}
