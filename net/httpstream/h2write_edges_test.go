package httpstream

import (
	"errors"
	"testing"
	"time"
)

// testWriter is a writer with no connection and no write goroutine: the
// tests call its methods as that goroutine would.
func testWriter(ids ...uint32) *h2Writer {
	w := newH2Writer(nil, Config{}.mustResolve())
	for _, id := range ids {
		w.windows[id] = w.initial
	}
	return w
}

// dataFrames lists the stream of each DATA frame in out, in order, and the
// flags of the last one.
func dataFrames(out []byte) (streams []uint32, last byte) {
	for len(out) >= frameHeaderSize {
		n := int(out[0])<<16 | int(out[1])<<8 | int(out[2])
		if out[3] == frameData {
			streams = append(streams, uint32(out[5]&0x7f)<<24|uint32(out[6])<<16|uint32(out[7])<<8|uint32(out[8]))
			last = out[4]
		}
		out = out[frameHeaderSize+n:]
	}
	return streams, last
}

// A job is runnable when its END_STREAM is all it has left, whatever the
// windows say, and not when its data waits for a connection window of zero.
func TestH2WriterRunnableAtTheWindowsEdges(t *testing.T) {
	w := testWriter(1)
	w.connWind = 0
	w.jobs = []*h2Job{{id: 1, headSent: true, data: []byte("ab"), off: 2, end: true}}
	if !w.runnable() {
		t.Error("a job owing only its END_STREAM is not runnable")
	}
	w.jobs = []*h2Job{{id: 1, headSent: true, data: []byte("ab")}}
	if w.runnable() {
		t.Error("a job is runnable with a connection window of zero")
	}
}

// A round spends its budget exactly and stops there, a frame per job per
// pass, and the next round starts from the next job.
func TestH2WriterRoundAtItsBudget(t *testing.T) {
	w := testWriter(1, 3)
	w.maxFrame, w.cfg.MaxWriteBufferBytes = 4, 8
	a := &h2Job{id: 1, headSent: true, data: make([]byte, 12)}
	b := &h2Job{id: 3, headSent: true, data: make([]byte, 12)}
	w.jobs = []*h2Job{a, b}
	w.fill()
	if a.off != 4 || b.off != 4 {
		t.Errorf("one round of 8 bytes wrote %d and %d bytes, want 4 and 4", a.off, b.off)
	}
	if w.turn != 1 {
		t.Errorf("the next round starts at job %d, want 1", w.turn)
	}

	// A budget spent mid-pass stops the pass there.
	w = testWriter(1, 3)
	w.maxFrame, w.cfg.MaxWriteBufferBytes = 4, 4
	a = &h2Job{id: 1, headSent: true, data: make([]byte, 12)}
	b = &h2Job{id: 3, headSent: true, data: make([]byte, 12)}
	w.jobs = []*h2Job{a, b}
	w.fill()
	if a.off != 4 || b.off != 0 {
		t.Errorf("one round of 4 bytes wrote %d and %d bytes, want 4 and 0", a.off, b.off)
	}

	w = testWriter(1, 3)
	w.maxFrame = 4
	w.jobs = []*h2Job{{id: 1, headSent: true, data: make([]byte, 12)}, {id: 3, headSent: true, data: make([]byte, 12)}}
	w.fill()
	streams, _ := dataFrames(w.out)
	want := []uint32{1, 3, 1, 3, 1, 3}
	if len(streams) != len(want) {
		t.Fatalf("frames on streams %v, want %v", streams, want)
	}
	for i := range want {
		if streams[i] != want[i] {
			t.Fatalf("frames on streams %v, want %v", streams, want)
		}
	}
}

// With no window, nothing is framed, not even an empty DATA frame; with
// one, the last DATA frame carries END_STREAM, and WriteProgressBytes
// written exactly restarts the write clock.
func TestH2WriterEmitAtItsEdges(t *testing.T) {
	w := testWriter(1)
	w.connWind = 0
	if n, err := w.emit(&h2Job{id: 1, headSent: true, data: []byte("ab")}); n != 0 || err != nil || len(w.out) != 0 {
		t.Errorf("no window: %d, %v, %d bytes framed", n, err, len(w.out))
	}

	w = testWriter(1)
	w.cfg.WriteProgressBytes = 3
	j := &h2Job{id: 1, headSent: true, data: []byte("abc"), end: true}
	if n, err := w.emit(j); n != 3 || err != nil {
		t.Fatalf("emit: %d, %v", n, err)
	}
	if _, flags := dataFrames(w.out); flags&flagEndStream == 0 {
		t.Error("the last DATA frame does not carry END_STREAM")
	}
	if j.written != 0 || j.deadline.IsZero() {
		t.Errorf("after WriteProgressBytes exactly: %d bytes counted, deadline %v", j.written, j.deadline)
	}
}

// blocks reports whether f is still running after a while, then releases
// it and waits for it.
func blocks(t *testing.T, w *h2Writer, f func() error, release func()) bool {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- f() }()
	select {
	case <-done:
		return false
	case <-time.After(50 * time.Millisecond):
	}
	w.mu.Lock()
	release()
	w.cond.Broadcast()
	w.mu.Unlock()
	if err := <-done; err != nil {
		t.Errorf("after the release: %v", err)
	}
	return true
}

// Waiting for a job waits for it, and so does finishing a stream; the
// control queue at its bound holds whoever adds to it.
func TestH2WriterWaitsWhereItMust(t *testing.T) {
	w := testWriter(1)
	var head h2HeadOutcome
	j := &h2Job{id: 1, queued: true, notify: &head}
	if !blocks(t, w, func() error { return w.awaitHead(&head) }, func() { w.settle(j, nil) }) {
		t.Error("awaitHead returned before the job left the writer")
	}
	j = &h2Job{id: 1, queued: true}
	if !blocks(t, w, func() error { return w.finish(1, j, nil, nil, false) }, func() { w.settle(j, nil) }) {
		t.Error("finish returned before the stream's last job left the writer")
	}
	w.ctrl = make([]byte, w.cfg.MaxPendingControlBytes)
	wait := func() error { w.mu.Lock(); defer w.mu.Unlock(); return w.waitCtrlLocked() }
	if !blocks(t, w, wait, func() { w.stopping = true }) {
		t.Error("the control queue at its bound let another frame in")
	}
}

func TestH2WriterSettleKeepsTheError(t *testing.T) {
	w := testWriter(1)
	j := &h2Job{id: 1, queued: true}
	failed := errors.New("test: failed")
	w.settle(j, failed)
	if !errors.Is(j.err, failed) || !j.done {
		t.Errorf("settled with %v, done %v; want the error kept", j.err, j.done)
	}
}

// An idle writer keeps two free batches, each no larger than an ordinary
// batch needs.
func TestH2WriterShedKeepsTwoOrdinaryBatches(t *testing.T) {
	w := testWriter()
	w.free = []*h2Sent{
		{jobs: make([]h2Job, 0, h2KeptJobs+1), out: make([]byte, 0, h2KeptOut+1)},
		{jobs: make([]h2Job, 0, h2KeptJobs), out: make([]byte, 0, h2KeptOut)},
		{},
		{},
	}
	w.shed()
	if len(w.free) != 2 {
		t.Fatalf("%d free batches kept, want 2", len(w.free))
	}
	if w.free[0].jobs != nil || w.free[0].out != nil {
		t.Error("a batch grown past the kept sizes was kept whole")
	}
	if cap(w.free[1].jobs) != h2KeptJobs || cap(w.free[1].out) != h2KeptOut {
		t.Error("a batch of the kept sizes was let go")
	}
}

// A table size update the peer is owed once is written once.
func TestH2WriterTableUpdateOfOneSize(t *testing.T) {
	w := testWriter()
	w.tableMin, w.tableFinal = 100, 100
	if got, want := w.appendTableUpdate(nil), appendHPACKInt(nil, 100, 5, 0x20); string(got) != string(want) {
		t.Errorf("appendTableUpdate = %x, want %x", got, want)
	}
}

// wakes reports whether f, which queues control frames, wakes a goroutine
// already waiting for them, as the write goroutine waits.
func wakes(t *testing.T, w *h2Writer, f func() error) bool {
	t.Helper()
	waiting, woke := make(chan struct{}), make(chan struct{})
	go func() {
		w.mu.Lock()
		close(waiting) // f needs the lock, which only Wait gives up
		for len(w.ctrl) == 0 {
			w.cond.Wait()
		}
		w.mu.Unlock()
		close(woke)
	}()
	<-waiting
	if err := f(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-woke:
		return true
	case <-time.After(time.Second):
		w.cond.Broadcast()
		<-woke
		return false
	}
}

// A refusal carries its line of body when the window holds it exactly; it
// and an interim head wake the write goroutine.
func TestH2WriterRefusalAtTheWindowsEdge(t *testing.T) {
	w := testWriter(1)
	n := int32(len(transportError(400).Body)) //nolint:gosec // G115: one line of text
	w.connWind, w.initial = n, n
	if !wakes(t, w, func() error { return w.refuse(1, 400, false, errNoError) }) {
		t.Error("the refusal did not wake the write goroutine")
	}
	if streams, _ := dataFrames(w.ctrl); len(streams) != 1 {
		t.Errorf("%d DATA frames with a window of exactly the body, want 1", len(streams))
	}
	w = testWriter(1)
	if !wakes(t, w, func() error { return w.interim(1, 100) }) {
		t.Error("the interim head did not wake the write goroutine")
	}
}

// A forgotten stream's jobs are answered and no other's, and its window is
// gone: what reset and streamGrant ask (TestH2NoResetOnAStreamForgottenLongAgo).
func TestH2WriterForget(t *testing.T) {
	w := testWriter(1, 3)
	a, b := &h2Job{id: 1, queued: true}, &h2Job{id: 3, queued: true}
	w.jobs = []*h2Job{a, b}
	w.forget(1)
	if !errors.Is(a.err, errStreamGone) || b.done || len(w.jobs) != 1 || w.jobs[0] != b {
		t.Errorf("forget(1): job 1 %v, job 3 done %v, %d jobs left", a.err, b.done, len(w.jobs))
	}
	if _, open := w.windows[1]; open {
		t.Error("forget(1) left stream 1's window")
	}
}

// SETTINGS_INITIAL_WINDOW_SIZE moves every open window by the difference.
func TestH2WriterInitialWindowMovesByTheDifference(t *testing.T) {
	w := testWriter(1)
	w.windows[1] = 100
	if err := w.setInitialWindow(w.initial + 10); err != nil {
		t.Fatal(err)
	}
	if w.windows[1] != 110 {
		t.Errorf("window %d, want 110", w.windows[1])
	}
}

// The drain's GOAWAY (2^31-1) and the reader's (its last stream) come from
// two goroutines, and the identifier must never rise on the wire (RFC 9113
// §6.8). With the floor lowered in one step and the frame queued in another,
// each could pass the floor and then be queued in the other order: the
// GOAWAYs in the control queue are read back here, in the order they would
// go out, and must not increase.
func TestH2GoAwayIdentifiersQueuedFromTwoGoroutinesNeverRise(t *testing.T) {
	for range 1000 {
		s := &h2Conn{w: testWriter()}
		start := make(chan struct{})
		done := make(chan struct{}, 2)
		for _, last := range []uint32{maxStreamID, 5} {
			go func() {
				<-start
				s.goAwayWith(last, errNoError)
				done <- struct{}{}
			}()
		}
		close(start)
		<-done
		<-done
		var ids []uint32
		for out := s.w.ctrl; len(out) >= frameHeaderSize; {
			n := int(out[0])<<16 | int(out[1])<<8 | int(out[2])
			if out[3] == frameGoAway {
				p := out[frameHeaderSize:]
				ids = append(ids, uint32(p[0]&0x7f)<<24|uint32(p[1])<<16|uint32(p[2])<<8|uint32(p[3]))
			}
			out = out[frameHeaderSize+n:]
		}
		if len(ids) != 2 || ids[1] > ids[0] {
			t.Fatalf("GOAWAYs queued with last stream %v; the identifier rose", ids)
		}
	}
}
