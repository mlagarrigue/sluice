package httpstream

import (
	"context"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/net/quic"
)

// The connection's end against the delivery of a packet's last requests,
// interleaved the way the read goroutine and the serving goroutine can once
// the peer's CONNECTION_CLOSE has closed Done in the same packet as a
// complete request: onFrames holds amu (as the OnStreamFrames callback
// does), end cancels the batches in flight (leaveAll) without it, and the
// batch is registered afterwards. A sentinel batch registered beforehand
// tells when leaveAll has run. Registered after it, the batch kept a context
// with the forced stop for its only parent: the handler waiting on ctx was
// never cancelled although the client had closed, and end — ServeH3Conn's
// return — waited for it until the server's forced stop. The witness runs
// the other order. Now a batch registered once leaveAll has run is born
// cancelled.
func TestH3BatchHandedOverAfterTheEndIsCancelled(t *testing.T) {
	for _, order := range []string{"witness: onFrames then end", "end's leaveAll during onFrames"} {
		t.Run(order, func(t *testing.T) {
			server, _ := connPair(t, quic.DefaultConfig())
			control, err := server.Stream(3)
			if err != nil {
				t.Fatal(err)
			}
			cfg := Config{BodyDelivery: BodyWhole}.mustResolve()
			forced, cancelForced := context.WithCancel(context.Background())
			defer cancelForced()
			started := make(chan struct{})
			cancelled := make(chan bool, 1)
			s := &h3Serving{
				c: server, cfg: cfg, forced: forced, control: control, a: newH3Assembler(cfg),
				handle: func(ctx context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
					close(started)
					select {
					case <-ctx.Done():
						cancelled <- true
					case <-time.After(5 * time.Second):
						cancelled <- false
						<-ctx.Done() // held until forced, as a handler waiting on ctx is
					}
					return sluice.Batch[Response]{Items: make([]Response, b.Len())}
				},
			}
			_ = server.Close() // the peer's CONNECTION_CLOSE: Done is closed
			<-server.Done()
			deliver := func() error {
				return s.onFrames([]quic.Frame{streamFrame(0, h3Request("GET", "/"), true)})
			}
			endDone := make(chan error, 1)
			if order == "witness: onFrames then end" {
				s.amu.Lock()
				err := deliver()
				s.amu.Unlock()
				if err != nil {
					t.Fatal(err)
				}
				<-started
				go func() { endDone <- s.end() }()
			} else {
				sentinel := s.cancels.register(s.forced, []uint64{1 << 40})
				s.amu.Lock() // onFrames is in progress on the read goroutine
				go func() { endDone <- s.end() }()
				select {
				case <-sentinel.Done(): // end's leaveAll has run
				case <-time.After(10 * time.Second):
					s.amu.Unlock()
					t.Fatal("end never reached leaveAll")
				}
				err := deliver()
				s.amu.Unlock()
				if err != nil {
					t.Fatal(err)
				}
				select {
				case <-started:
				case <-time.After(10 * time.Second):
					t.Fatal("the batch delivered after leaveAll was not launched (launch refused it)")
				}
			}
			if !<-cancelled {
				t.Error("the handler's context was not cancelled although the client closed the connection")
			}
			select {
			case err := <-endDone:
				t.Logf("end returned without forced: %v", err)
			case <-time.After(2 * time.Second):
				t.Error("end (ServeH3Conn's return) still waiting on the handler; only forced releases it")
				cancelForced()
				t.Logf("after forced: %v", <-endDone)
			}
		})
	}
}

// A request completed by its FIN and cancelled in the same delivery reaches
// the handler with a cancelled context, as it does when the cancellation
// comes one delivery later (the witness). The cancellations of a delivery
// used to be applied before its requests were assembled and their batch
// registered: leave found nothing, the FIN had already put the request in
// the batch and retired its stream, and the request ran with a context only
// the forced stop would cancel. Each variant is one delivery the transport
// can hand up: frames in packet order, STOP_SENDING alone, RESET_STREAM
// alone, or the cancellation ahead of the STREAM frame.
func TestH3CancelInTheDeliveryThatCompletesTheRequest(t *testing.T) {
	stop := quic.Frame{Type: quic.FrameStopSending, StreamID: 0}
	reset := quic.Frame{Type: quic.FrameResetStream, StreamID: 0}
	req := streamFrame(0, h3Request("GET", "/"), true)
	for _, tc := range []struct {
		name    string
		packets [][]quic.Frame
	}{
		{"witness next delivery", [][]quic.Frame{{req}, {stop}}},
		{"stop_sending alone after fin", [][]quic.Frame{{req, stop}}},
		{"reset_stream alone after fin", [][]quic.Frame{{req, reset}}},
		{"stop_sending ahead of the stream frame", [][]quic.Frame{{stop, req}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, _ := connPair(t, quic.DefaultConfig())
			control, err := server.Stream(3)
			if err != nil {
				t.Fatal(err)
			}
			cfg := Config{BodyDelivery: BodyWhole}.mustResolve()
			forced, cancelForced := context.WithCancel(context.Background())
			defer cancelForced()
			started, cancelled := make(chan struct{}), make(chan bool, 1)
			s := &h3Serving{
				c: server, cfg: cfg, forced: forced, control: control, a: newH3Assembler(cfg),
				handle: func(ctx context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
					close(started)
					select {
					case <-ctx.Done():
						cancelled <- true
					case <-time.After(3 * time.Second):
						cancelled <- false
					}
					return sluice.Batch[Response]{Items: make([]Response, b.Len())}
				},
			}
			for i, p := range tc.packets {
				if i > 0 {
					<-started
				}
				s.amu.Lock()
				err := s.onFrames(p)
				s.amu.Unlock()
				if err != nil {
					t.Fatal(err)
				}
			}
			select {
			case <-started:
			case <-time.After(10 * time.Second):
				t.Fatal("the request never reached the handler")
			}
			if !<-cancelled {
				t.Error("cancelled in the delivery that completed it, the request ran with a live context")
			}
			cancelForced()
			s.calls.Wait()
			s.writers.Wait()
		})
	}
}
