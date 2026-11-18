package httpstream

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/net/quic"
)

// A handler that calls runtime.Goexit — a t.FailNow inside one, say — ends
// its goroutine without a result. Over HTTP/3 the batch is still answered,
// its streams retired and its registration released, as HTTP/2 already did
// (TestH2HandlerGoexitStillAnswers): answerH3 stopped with the goroutine,
// and the batch stayed counted in flight for the life of the connection.
func TestH3HandlerGoexitStillAnswers(t *testing.T) {
	exits := func(context.Context, sluice.Batch[Request]) sluice.Batch[Response] {
		runtime.Goexit()
		return sluice.Batch[Response]{}
	}
	panics := func(context.Context, sluice.Batch[Request]) sluice.Batch[Response] { panic("handler") }
	// The control is the path a panic already took: answered 500 the same
	// way, so the Goexit is held to what the panic gets.
	wantRetired, wantAnswered, wantLeft := h3AnswerOf(t, panics)
	if wantRetired != 2 || wantLeft != 0 {
		t.Fatalf("control: a panicking handler retired %d streams and left %d registered", wantRetired, wantLeft)
	}
	retired, answered, left := h3AnswerOf(t, exits)
	if retired != wantRetired || answered != wantAnswered || left != wantLeft {
		t.Errorf("after Goexit: retire %d, answered %d, streams registered %d; a panic gives %d, %d, %d",
			retired, answered, left, wantRetired, wantAnswered, wantLeft)
	}
}

// h3AnswerOf runs answerH3 over a batch of two GETs with handle, and counts
// the streams retired, the responses written whole and the registrations
// left.
func h3AnswerOf(t *testing.T, handle Handler) (retired, answered int32, left int) {
	t.Helper()
	c := serverConnWith(t, quic.DefaultConfig())
	ids := []quic.StreamID{400, 404}
	var calls streamCalls[quic.StreamID]
	b := h3Batch{reqs: make([]Request, 2), ids: ids, streams: make([]*quic.Stream, 2), after: make([]<-chan struct{}, 2), fieldSection: h3NoFieldSectionLimit}
	for i := range b.reqs {
		b.reqs[i].Method = []byte("GET")
	}
	b.ctx, b.calls = calls.register(t.Context(), ids), &calls
	var nRetired, nAnswered atomic.Int32
	b.retire = func() { nRetired.Add(1) }
	b.answered = func() { nAnswered.Add(1) }
	var writers, ran sync.WaitGroup
	ran.Go(func() {
		answerH3(t.Context(), c, nil, b, handle, writeBoundOf(Config{}.mustResolve()), &writers, &warmGoroutines{})
	})
	ran.Wait()
	writers.Wait()
	// A writer signals writers before it retires its stream and releases
	// the batch, both in the same deferred call: the release is waited
	// for, bounded, rather than read the moment writers lets go (a
	// Windows runner read 2 streams still registered).
	deadline := time.Now().Add(2 * time.Second)
	for {
		calls.mu.Lock()
		left = len(calls.m)
		calls.mu.Unlock()
		if left == 0 && nRetired.Load() == int32(len(ids)) || time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	return nRetired.Load(), nAnswered.Load(), left
}
