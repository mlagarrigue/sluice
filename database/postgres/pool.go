package postgres

import (
	"context"
	"errors"
	"sync"

	"github.com/mlagarrigue/sluice"
)

// ErrPoolClosed is what a Pool answers once Close has been called.
var ErrPoolClosed = errors.New("postgres: the pool is closed")

// A Pool is a fixed number of connections for calls that need several at
// once: independent queries that grouping cannot merge without running them
// one after the other on a single server process — a page of a range each,
// a write each. Keyed reads are [github.com/mlagarrigue/sluice/gateway]'s,
// which does with one connection what a pool does with sixty-four; a Pool is
// for what remains.
//
// It holds at most size connections, made by connect when a call first needs
// one, and lends one per call: [Pool.ExecParams] for the statement,
// [Pool.Query] for as long as its stream is read. A connection that broke
// ([Conn.Err] not nil, [ErrConnBroken] among others) is closed when it comes
// back, and the next call opens a new one; a connection the server closed
// while it sat idle — a restart, a failover, an idle-session timeout — is
// seen when it is lent, closed, and another lent in its place: the call does
// not pay for it. The look is at the socket, without reading or waiting, on
// Linux, macOS and the BSDs; elsewhere the call that finds it fails, and it
// is replaced after.
// A pool carries no transaction: a call that leaves one open — a BEGIN sent
// as a statement — or aborted has its connection closed when it comes back,
// which rolls the block back on the server, rather than lent to a caller
// whose writes would land in it. A transaction is [Conn.Begin]'s, on a
// connection of its own.
// When all are lent, a call waits for one, as long as its context allows.
//
// There is no idle timeout, lifetime or setting of any kind: they are a
// deployment's tuning, and a pool that needs them is a module of its own
// (ADR 0022). The look before a lend is not one: it has nothing to tune, and
// it spares every caller a retry of its own. The zero Pool is not usable; build one with [NewPool] and
// release it with [Pool.Close].
type Pool struct {
	connect func(context.Context) (*Conn, error)
	slots   chan struct{} // one token per connection lent or being opened

	mu     sync.Mutex
	idle   []*Conn
	closed bool
	lent   sync.WaitGroup
}

// NewPool returns a pool of at most size connections made by connect. It
// opens none: the first calls do. It panics on a size that is not positive
// or a nil connect.
func NewPool(size int, connect func(ctx context.Context) (*Conn, error)) *Pool {
	if size <= 0 {
		panic("postgres: NewPool needs a positive size")
	}
	if connect == nil {
		panic("postgres: NewPool needs a connect function")
	}
	return &Pool{connect: connect, slots: make(chan struct{}, size)}
}

// Query is [Conn.Query] on a connection of the pool, lent when the stream is
// first read and returned when it ends or its reader stops. A failure to get
// one — the context, the pool closed, connect — is the Source's error.
func (p *Pool) Query(ctx context.Context, sql string, params []Param, cfg QueryConfig) sluice.Source[Row] {
	var src sluice.Source[Row]
	var lendErr error
	stream := func(yield func(sluice.Batch[Row]) bool) {
		c, err := p.lend(ctx)
		if err != nil {
			lendErr = err
			return
		}
		defer p.give(c)
		src = c.Query(ctx, sql, params, cfg)
		src.Stream()(yield)
	}
	return sluice.NewSource(stream, func() error {
		if lendErr != nil {
			return lendErr
		}
		return src.Err()
	})
}

// ExecParams is [Conn.ExecParams] on a connection of the pool.
func (p *Pool) ExecParams(ctx context.Context, sql string, params []Param) (CommandTag, error) {
	c, err := p.lend(ctx)
	if err != nil {
		return "", err
	}
	defer p.give(c)
	return c.ExecParams(ctx, sql, params)
}

// Close refuses new calls, waits for the connections lent to come back,
// and closes every one.
func (p *Pool) Close() error {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	p.lent.Wait()
	p.mu.Lock()
	idle := p.idle
	p.idle = nil
	p.mu.Unlock()
	var errs []error
	for _, c := range idle {
		errs = append(errs, c.Close())
	}
	return errors.Join(errs...)
}

// lend takes a token, then an idle connection or a new one.
func (p *Pool) lend(ctx context.Context) (*Conn, error) {
	select {
	case p.slots <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		<-p.slots
		return nil, ErrPoolClosed
	}
	p.lent.Add(1)
	for {
		var c *Conn
		if n := len(p.idle); n > 0 {
			c, p.idle = p.idle[n-1], p.idle[:n-1]
		}
		p.mu.Unlock()
		if c == nil {
			break
		}
		if c.idleSound() {
			return c, nil
		}
		// The server ended it while it waited — a restart, a failover, an
		// idle-session timeout: closed here, and the next idle one or a new
		// one lent instead, so the caller does not pay for it.
		_ = c.Close()
		p.mu.Lock()
	}
	c, err := p.connect(ctx)
	if err != nil {
		p.lent.Done()
		<-p.slots
		return nil, err
	}
	return c, nil
}

// idleSound reports whether a connection that sat idle can be lent: nothing
// arrived while it waited, and a look at its socket ([peekIdle]) finds it
// quiet. Bytes that arrived — the FATAL 57P01 a terminated backend sends, a
// notice, a parameter the server re-announced — or the socket's end say it
// cannot: an idle connection's protocol position has nothing to read, so
// what came is either its end or something no caller asked for, and a new
// connection is cheaper to reason about than either. A transport the look
// cannot reach is lent as it is.
func (c *Conn) idleSound() bool {
	if c.broken != nil || c.r.Pending() {
		return false
	}
	quiet, looked := peekIdle(c.rw)
	if !looked || quiet {
		return true
	}
	c.broken = errIdleEnded
	return false
}

// errIdleEnded breaks a connection whose socket the server closed or wrote
// to while it sat idle, so that its Close sends no Terminate after it.
var errIdleEnded = errors.New("postgres: the server ended or wrote to an idle connection")

// give takes a connection back: kept if it is sound, out of any
// transaction block, and the pool open; closed otherwise. A block the call
// left open, or aborted, is the server's word in the last ReadyForQuery
// (inTx); lent again, the next caller's statements would run inside it and
// be lost with it, so the close rolls it back instead.
func (p *Pool) give(c *Conn) {
	p.mu.Lock()
	keep := !p.closed && c.Err() == nil && !c.inTx
	if keep {
		p.idle = append(p.idle, c)
	}
	p.mu.Unlock()
	if !keep {
		_ = c.Close()
	}
	p.lent.Done()
	<-p.slots
}
