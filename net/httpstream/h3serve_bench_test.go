package httpstream

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/internal/dgram"
	"github.com/mlagarrigue/sluice/net/quic"
)

// BenchmarkServeH3 is ServeH3Conn's whole path with no socket: a QUIC
// connection over an in-memory datagram pair, the client opening depth
// request streams at once and reading every response to its FIN. It is what
// the receive side — the transport's delivery, the per-datagram lock the
// assembler is taken under, the assembly — is read against: their share of
// a request, not their cost alone.
func BenchmarkServeH3(b *testing.B) {
	for _, depth := range []int{1, 8} {
		b.Run(fmt.Sprintf("depth=%d", depth), func(b *testing.B) {
			serverPC, clientPC := dgram.Pair()
			b.Cleanup(func() { _ = serverPC.Close(); _ = clientPC.Close() })
			ctx, cancel := context.WithCancel(context.Background())
			served := make(chan struct{})
			b.Cleanup(func() { cancel(); <-served })
			go func() {
				defer close(served)
				conn, err := acceptOne(b, serverPC, h3ServerTLS(b), quic.DefaultParameters())
				if err != nil {
					b.Error(err)
					return
				}
				_ = ServeH3Conn(ctx, conn, Config{}, func(_ context.Context, bt sluice.Batch[Request]) sluice.Batch[Response] {
					res := make([]Response, bt.Len())
					for i := range res {
						res[i] = Response{Status: 200, Body: []byte("ok")}
					}
					return sluice.Batch[Response]{Items: res}
				})
			}()
			conn, err := quic.Dial(context.Background(), clientPC, serverPC.LocalAddr(), h3ClientTLS(), quic.DefaultParameters())
			if err != nil {
				b.Fatalf("Dial: %v", err)
			}
			b.Cleanup(func() { _ = conn.Close() })

			req := h3Request("GET", "/orders/7")
			streams := make([]*quic.Stream, depth)
			errs := make(chan error, depth)
			b.ReportAllocs()
			for b.Loop() {
				for i := range streams {
					s, err := conn.OpenStream(ctx)
					if err != nil {
						b.Fatal(err)
					}
					if _, err := s.WriteFinal(req); err != nil {
						b.Fatal(err)
					}
					streams[i] = s
				}
				for _, s := range streams {
					go func() {
						_, err := io.Copy(io.Discard, s)
						errs <- err
					}()
				}
				for range streams {
					if err := <-errs; err != nil {
						b.Fatal(err)
					}
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*depth), "ns/request")
		})
	}
}
