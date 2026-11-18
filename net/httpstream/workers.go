package httpstream

import (
	"slices"
	"sync"
	"time"
)

// warmGoroutines runs functions on goroutines kept from one call to the next.
//
// A new goroutine starts on a small stack, and an HTTP/3 request's two —
// the batch's handler call and the response's writer, down through the QUIC
// stack's packet building and sealing, a few kilobytes of frames — grew
// theirs once or twice each: runtime.newstack was 3.5% of the static-h3
// profile (2026-11-18), where an HTTP/1.1 connection serves request after
// request on the one goroutine. A goroutine that has finished one call and
// waits for the next keeps its grown stack, so the next call pays nothing;
// the garbage collector shrinks a stack an idle goroutine no longer uses,
// and an idle one exits after warmIdle.
//
// Go never waits: with no goroutine idle, it starts one. What the pool
// holds is bounded by warmMaxIdle; how many run at once is the callers'
// business, as it was with a go statement.
//
// A pool is a connection's, and stops with it: a goroutine belongs to the
// synctest bubble it was started in, and one shared between connections
// would carry calls from one bubble into another.
type warmGoroutines struct {
	mu      sync.Mutex
	idle    []*warmGoroutine
	stopped bool
}

// warmGoroutine is one goroutine of the pool, parked on work while idle.
type warmGoroutine struct {
	// work is buffered: Go sends after taking the goroutine off the idle
	// list, so the send never waits on a goroutine that timed out.
	work chan func()
}

const (
	// warmMaxIdle bounds the idle goroutines: past it, a goroutine that
	// finishes its call exits as a go statement's would.
	warmMaxIdle = 256
	// warmIdle is how long a goroutine waits for a next call before it
	// exits.
	warmIdle = time.Second
)

// Go runs f on an idle goroutine of the pool, or on a new one; on a
// goroutine of its own once the pool has stopped.
func (p *warmGoroutines) Go(f func()) {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		go f()
		return
	}
	if n := len(p.idle); n > 0 {
		g := p.idle[n-1]
		p.idle[n-1] = nil
		p.idle = p.idle[:n-1]
		p.mu.Unlock()
		g.work <- f
		return
	}
	p.mu.Unlock()
	go p.run(&warmGoroutine{work: make(chan func(), 1)}, f)
}

// stop ends the idle goroutines, and each running one once its call
// returns.
func (p *warmGoroutines) stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stopped = true
	for _, g := range p.idle {
		g.work <- nil
	}
	clear(p.idle)
	p.idle = nil
}

// run is a pool goroutine: f, then each call it is handed until it has
// waited warmIdle for one, finds the pool full or stopped, or is handed nil
// by stop. A call that panics ends
// the goroutine and the program, as it would on a goroutine of its own; one
// that calls runtime.Goexit ends the goroutine alone, which was running and
// so not on the idle list.
func (p *warmGoroutines) run(g *warmGoroutine, f func()) {
	var timer *time.Timer
	for {
		f()
		p.mu.Lock()
		if p.stopped || len(p.idle) >= warmMaxIdle {
			p.mu.Unlock()
			return
		}
		p.idle = append(p.idle, g)
		p.mu.Unlock()
		if timer == nil {
			timer = time.NewTimer(warmIdle)
		} else {
			timer.Reset(warmIdle)
		}
		select {
		case f = <-g.work:
			timer.Stop()
		case <-timer.C:
			p.mu.Lock()
			if i := slices.Index(p.idle, g); i >= 0 {
				p.idle = slices.Delete(p.idle, i, i+1)
				p.mu.Unlock()
				return
			}
			p.mu.Unlock()
			// Taken off the list before the timer fired: a call is on
			// its way, or already in work.
			f = <-g.work
		}
		if f == nil {
			return // stopped
		}
	}
}
