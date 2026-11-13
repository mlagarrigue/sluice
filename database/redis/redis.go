// Package redis speaks RESP2, the protocol of Redis and Valkey, over one
// connection and the standard library alone, and sends a batch of commands
// in one write.
//
// A batch is the unit here as everywhere in the library: [Conn.Pipeline]
// writes every command of a batch at once and reads their replies in order,
// one round trip for the batch where one command at a time pays a round trip
// each — a cache looked up for a thousand keys in the time of one lookup.
// The server answers each command on its own: an error reply is the reply of
// its command, at its place, and the batch's other replies stand.
//
// In plain words: Redis is an in-memory key–value server. Each command —
// GET, SET, INCR — normally waits for its answer before the next is sent;
// pipelining sends them all and then reads the answers, so the network's
// delay is paid once per batch instead of once per command.
//
//	conn, err := redis.Connect(ctx, redis.DialTCP("127.0.0.1:6379"), redis.Config{})
//	replies, err := conn.Pipeline(ctx, [][]string{
//		{"GET", "user:1"},
//		{"GET", "user:2"},
//	})
//	for _, r := range replies {
//		// r.Kind is KindBulk with r.Str, or KindNil for a missing key
//	}
//
// One connection, no pool, as the PostgreSQL connector (ADR 0022): holding
// several is the caller's business, and gateway groups calls onto one. The
// transport is the caller's: [DialTCP], [DialTLS] with the caller's
// tls.Config, or a recipe of its own. Commands are arrays of binary-safe
// strings, never a command line assembled from text.
//
// The guide: docs/guide/redis.md.
package redis

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

// DialFunc opens the transport a [Conn] runs over.
type DialFunc func(ctx context.Context) (net.Conn, error)

// DialTCP is the recipe for a plain TCP connection to addr, "host:port".
// The bytes, the password included, cross in clear: the right recipe for a
// loopback or a network the deployment already encrypts.
func DialTCP(addr string) DialFunc {
	return func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", addr)
	}
}

// DialTLS is the recipe for a TLS connection to addr with the caller's
// configuration, which names the server to verify (ServerName) unless addr's
// host does.
func DialTLS(addr string, cfg *tls.Config) DialFunc {
	return func(ctx context.Context) (net.Conn, error) {
		d := tls.Dialer{Config: cfg}
		return d.DialContext(ctx, "tcp", addr)
	}
}

// DefaultMaxReplyBytes is the bound on one reply when
// [Config.MaxReplyBytes] is zero: 64 MiB.
const DefaultMaxReplyBytes = 64 << 20

// Config configures a connection. The zero value connects without
// authentication to database 0, with replies bounded at
// [DefaultMaxReplyBytes].
type Config struct {
	// User and Password authenticate the connection (AUTH). With Password
	// empty, no AUTH is sent; with User empty, the server's default user is
	// meant, as AUTH with one argument does.
	User     string
	Password string
	// Database is the logical database SELECT picks. Zero means database
	// 0, the server's default, and sends nothing; a negative value is
	// refused.
	Database int
	// MaxReplyBytes bounds one reply as it stands on the wire, every nested
	// element included. A reply past it breaks the connection with
	// [ErrReplyTooLarge]: the stream is left mid-reply. Zero means
	// [DefaultMaxReplyBytes]; a negative value is refused. The server's own
	// bound on a value is 512 MB by default (proto-max-bulk-len).
	MaxReplyBytes int
}

func (c Config) resolve() (Config, error) {
	var errs []error
	if c.Database < 0 {
		errs = append(errs, fmt.Errorf("redis: Database %d is negative; zero means database 0", c.Database))
	}
	if c.MaxReplyBytes < 0 {
		errs = append(errs, fmt.Errorf("redis: MaxReplyBytes %d is negative; zero means %d", c.MaxReplyBytes, DefaultMaxReplyBytes))
	}
	if c.MaxReplyBytes == 0 {
		c.MaxReplyBytes = DefaultMaxReplyBytes
	}
	return c, errors.Join(errs...)
}

// Conn is one connection to a server. Its methods may be called from several
// goroutines; they run one at a time, in the order they take the connection.
type Conn struct {
	mu     sync.Mutex
	nc     net.Conn
	bw     []byte
	r      reader
	max    int
	broken error
}

// Connect opens a connection with dial and readies it: AUTH when
// cfg.Password is set, SELECT when cfg.Database is not zero, and a PING, in
// one round trip. A server that refuses any of them — a wrong password, a
// protected server that answers -DENIED to every client — is reported here
// as its [*Error], and the connection closed.
func Connect(ctx context.Context, dial DialFunc, cfg Config) (*Conn, error) {
	cfg, err := cfg.resolve()
	if err != nil {
		return nil, err
	}
	nc, err := dial(ctx)
	if err != nil {
		return nil, err
	}
	c := &Conn{nc: nc, max: cfg.MaxReplyBytes}
	c.r.br = bufio.NewReaderSize(nc, 32<<10)
	var setup [][]string
	switch {
	case cfg.Password != "" && cfg.User != "":
		setup = append(setup, []string{"AUTH", cfg.User, cfg.Password})
	case cfg.Password != "":
		setup = append(setup, []string{"AUTH", cfg.Password})
	}
	if cfg.Database != 0 {
		setup = append(setup, []string{"SELECT", strconv.Itoa(cfg.Database)})
	}
	setup = append(setup, []string{"PING"})
	replies, err := c.Pipeline(ctx, setup)
	if err == nil {
		for _, r := range replies {
			if err = r.Err(); err != nil {
				break
			}
		}
	}
	if err != nil {
		_ = nc.Close()
		return nil, err
	}
	return c, nil
}

// Do sends one command and returns its reply. A server error is the reply's
// ([Reply.Err]), not Do's: Do's error is the connection's.
func (c *Conn) Do(ctx context.Context, args ...string) (Reply, error) {
	replies, err := c.Pipeline(ctx, [][]string{args})
	if err != nil {
		return Reply{}, err
	}
	return replies[0], nil
}

// Pipeline sends every command of cmds in one write and returns their
// replies, in order, one per command.
//
// A server error answers its command and no other: it is that reply's
// ([Reply.Err]), the replies after it are read as usual, and the connection
// goes on. The error Pipeline returns is the connection's — ctx ended, the
// write or a read failed, a reply could not be parsed or passed
// [Config.MaxReplyBytes] — and leaves the connection broken: the replies
// still owed are lost with their place in the stream, and every later call
// returns [ErrConnBroken].
//
// ctx bounds the whole round trip: its deadline is the connection's, and
// its end interrupts a write or a read in progress. The replies' bytes are
// the caller's to keep.
func (c *Conn) Pipeline(ctx context.Context, cmds [][]string) ([]Reply, error) {
	if len(cmds) == 0 {
		return nil, nil
	}
	for i, cmd := range cmds {
		if len(cmd) == 0 {
			return nil, fmt.Errorf("redis: command %d of the batch is empty", i)
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.broken != nil {
		return nil, fmt.Errorf("%w (%w)", ErrConnBroken, c.broken)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	dl, _ := ctx.Deadline()
	_ = c.nc.SetDeadline(dl)
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		_ = c.nc.SetDeadline(time.Unix(1, 0))
		close(done)
	})
	defer func() {
		if !stop() {
			// Started as the call ended: its deadline in the past must land
			// before the next call sets its own, not after, where it would
			// fail that call and break the connection.
			<-done
		}
	}()

	c.bw = c.bw[:0]
	for _, cmd := range cmds {
		c.bw = appendCommand(c.bw, cmd)
	}
	if _, err := c.nc.Write(c.bw); err != nil {
		return nil, c.fail(ctx, err)
	}
	c.r.arena = nil // the replies' bytes are the caller's from here
	replies := make([]Reply, len(cmds))
	for i := range replies {
		r, err := c.r.read(c.max)
		if err != nil {
			return nil, c.fail(ctx, err)
		}
		replies[i] = r
	}
	return replies, nil
}

// fail breaks the connection with err, reported as ctx's end when that is
// what interrupted the I/O.
func (c *Conn) fail(ctx context.Context, err error) error {
	// The connection's deadline is ctx's and can pass a moment before ctx
	// says it has: a timeout at or past ctx's deadline is that deadline.
	dl, has := ctx.Deadline()
	switch {
	case ctx.Err() != nil:
		err = fmt.Errorf("%w: %w", ctx.Err(), err)
	case has && !time.Now().Before(dl) && errors.Is(err, os.ErrDeadlineExceeded):
		err = fmt.Errorf("%w: %w", context.DeadlineExceeded, err)
	}
	c.broken = err
	_ = c.nc.Close()
	return fmt.Errorf("%w (%w)", ErrConnBroken, err)
}

// Close closes the connection. Calls after it return [ErrConnBroken].
func (c *Conn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.broken == nil {
		c.broken = net.ErrClosed
	}
	return c.nc.Close()
}
