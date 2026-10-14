package interop

import (
	"context"
	"io"
	"sync"
	"testing"

	quicgo "github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/qlog"
	"github.com/quic-go/quic-go/qlogwriter"

	"github.com/mlagarrigue/sluice/net/quic"
)

// ecnTrace is a qlog trace that keeps, instead of writing, what quic-go's
// ECN validation concluded and how its peer's packets arrived marked.
type ecnTrace struct {
	mu       sync.Mutex
	states   []qlog.ECNState
	received map[qlog.ECN]int
}

func (t *ecnTrace) AddProducer() qlogwriter.Recorder { return &ecnRecorder{t: t} }
func (t *ecnTrace) SupportsSchemas(string) bool      { return true }

type ecnRecorder struct{ t *ecnTrace }

func (r *ecnRecorder) RecordEvent(ev qlogwriter.Event) {
	r.t.mu.Lock()
	defer r.t.mu.Unlock()
	switch ev := ev.(type) {
	case qlog.ECNStateUpdated:
		r.t.states = append(r.t.states, ev.State)
	case qlog.PacketReceived:
		if r.t.received == nil {
			r.t.received = make(map[qlog.ECN]int)
		}
		r.t.received[ev.ECN]++
	}
}

func (r *ecnRecorder) Close() error { return nil }

// Compile-time check that the recorder is what quic-go hands events to.
var _ qlogwriter.Event = qlog.ECNStateUpdated{}

// TestECNAgainstQuicGo: quic-go listens and marks its packets ECT(0) while
// it validates ECN (RFC 9000 §13.4.2); sluice dials, reads the marks off
// each datagram, reports them in its ACK frames (§13.4.1) and marks its own
// packets. Both sides must then see the other's marks: quic-go's state
// machine reaches "capable" — which it only does when sluice's ACKs carry
// counts that account for every marked packet — and keeps receiving marked
// packets past sluice's ten test packets, which is sluice's own validation
// having passed; sluice's Stats count the ECT(0) packets quic-go sent.
func TestECNAgainstQuicGo(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	trace := &ecnTrace{}
	ln, err := quicgo.Listen(listenUDP(t), serverTLS(t), &quicgo.Config{
		MaxIdleTimeout: timeout,
		Tracer: func(context.Context, bool, quicgo.ConnectionID) qlogwriter.Trace {
			return trace
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	serverDone := make(chan error, 1)
	go func() {
		serverDone <- func() error {
			c, err := ln.Accept(ctx)
			if err != nil {
				return err
			}
			st, err := c.AcceptStream(ctx)
			if err != nil {
				return err
			}
			if _, err := io.Copy(st, st); err != nil {
				return err
			}
			if err := st.Close(); err != nil {
				return err
			}
			<-c.Context().Done()
			return nil
		}()
	}()

	client, err := quic.DialContext(ctx, listenUDP(t), ln.Addr(), clientTLS(), quic.DefaultParameters())
	if err != nil {
		t.Fatalf("sluice could not complete a handshake with quic-go: %v", err)
	}
	st, err := client.OpenStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	want := payload()
	go func() {
		if _, err := st.Write(want); err == nil {
			_ = st.CloseWrite()
		}
	}()
	got, err := io.ReadAll(st)
	if err != nil {
		t.Fatalf("reading the echo: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("echo of %d bytes, want %d", len(got), len(want))
	}
	stats := client.Stats()
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatalf("quic-go server: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("the quic-go server never saw the connection end")
	}

	// sluice's side: quic-go marked, sluice read the marks.
	if stats.ECT0Received == 0 {
		t.Errorf("sluice read no ECT(0) mark off quic-go's datagrams: %+v", stats)
	}
	if stats.ECT1Received != 0 || stats.CEReceived != 0 {
		t.Errorf("unexpected ECT(1)/CE on a loopback: %+v", stats)
	}
	// quic-go's side: sluice's ACKs reported the marks (capable), and
	// sluice's own packets kept arriving marked after its test packets.
	trace.mu.Lock()
	defer trace.mu.Unlock()
	capable := false
	for _, s := range trace.states {
		if s == qlog.ECNStateCapable {
			capable = true
		}
		if s == qlog.ECNStateFailed {
			t.Errorf("quic-go's ECN validation failed; states %v", trace.states)
		}
	}
	if !capable {
		t.Errorf("quic-go never validated ECN against sluice's ACKs; states %v", trace.states)
	}
	if n := trace.received[qlog.ECT0]; n <= 10 {
		t.Errorf("quic-go received %d ECT(0)-marked packets from sluice (received by mark: %v); sluice stopped marking", n, trace.received)
	}
}
