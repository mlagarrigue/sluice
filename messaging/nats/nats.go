// Package nats speaks the NATS client protocol over one connection and the
// standard library alone, and reads a JetStream consumer a batch at a time,
// each message acknowledged by the code that processed it.
//
// NATS is a messaging server: publishers send messages on subjects, and
// JetStream, its persistence layer, keeps them in streams that consumers read
// at their own pace. A pull consumer asks for a batch — "the next 256" —
// and the server delivers up to that many; each stays unacknowledged until
// the consumer says it is done with it, and is delivered again if it never
// does. [Conn.Consume] gives that batch as a [sluice.Source]; [Consumer.Ack]
// is the acknowledgement, sent with the next request.
//
// In plain words: a queue that remembers. A message read is not a message
// gone; it goes once your code says it was handled, and comes back if your
// program stops first.
//
//	conn, err := nats.Connect(ctx, nats.DialTCP("127.0.0.1:4222"), nats.Config{})
//	c, err := conn.Consume(nats.ConsumerConfig{Stream: "ORDERS", Consumer: "billing"})
//	src := c.Messages(ctx)
//	for b := range src.Stream() {
//		for _, m := range b.Items {
//			// m.Subject, m.Data
//			c.Ack(m.Reply)
//		}
//	}
//
// One connection, no pool, synchronous as the Redis connector is: the
// connection is read only while a call waits for the server, which answers
// its PINGs there. The stream and the durable consumer are the caller's to
// create, as the Redis group and the PostgreSQL slot are ([Conn.Request]
// sends the JetStream API's requests). TLS is not offered yet: a NATS server
// sends its INFO in clear before the upgrade, which a dial recipe cannot do.
//
// The guide: docs/guide/nats.md.
package nats

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// DialFunc opens the transport a [Conn] runs over.
type DialFunc func(ctx context.Context) (net.Conn, error)

// DialTCP is the recipe for a plain TCP connection to addr, "host:port".
func DialTCP(addr string) DialFunc {
	return func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", addr)
	}
}

// DefaultMaxPayloadBytes is the bound on one received message when
// [Config.MaxPayloadBytes] is zero: 8 MiB, past the server's own default
// max_payload of 1 MiB.
const DefaultMaxPayloadBytes = 8 << 20

// Config configures a connection. The zero value connects without
// credentials, under no name, with received messages bounded at
// [DefaultMaxPayloadBytes].
type Config struct {
	// User and Password, when set, go in CONNECT.
	User     string
	Password string
	// Name is the client name the server shows in its monitoring.
	Name string
	// MaxPayloadBytes bounds one received message, headers and payload.
	// A message past it breaks the connection with [ErrTooLarge]. Zero means
	// [DefaultMaxPayloadBytes]; a negative value is refused.
	MaxPayloadBytes int
}

// Conn is one connection to a NATS server. Its methods may be called from
// several goroutines; they run one at a time.
type Conn struct {
	mu     sync.Mutex
	nc     net.Conn
	r      reader
	wb     []byte
	broken error
	inbox  string
	sid    int
}

type serverInfo struct {
	Headers    bool `json:"headers"`
	MaxPayload int  `json:"max_payload"`
}

// Connect opens a connection with dial: it reads the server's INFO, sends
// CONNECT and a PING, and returns once the PONG comes back — or the
// server's -ERR, as an [*Error], for credentials it refuses.
func Connect(ctx context.Context, dial DialFunc, cfg Config) (*Conn, error) {
	if cfg.MaxPayloadBytes < 0 {
		return nil, fmt.Errorf("nats: MaxPayloadBytes %d is negative; zero means %d", cfg.MaxPayloadBytes, DefaultMaxPayloadBytes)
	}
	if cfg.MaxPayloadBytes == 0 {
		cfg.MaxPayloadBytes = DefaultMaxPayloadBytes
	}
	nc, err := dial(ctx)
	if err != nil {
		return nil, err
	}
	c := &Conn{nc: nc, inbox: "_INBOX." + rand.Text()}
	c.r = reader{br: bufio.NewReaderSize(nc, 32<<10), maxPayload: cfg.MaxPayloadBytes}
	err = c.handshake(ctx, cfg)
	if err != nil {
		_ = nc.Close()
		return nil, err
	}
	return c, nil
}

func (c *Conn) handshake(ctx context.Context, cfg Config) error {
	stop := c.bound(ctx)
	defer stop()
	o, err := c.r.read()
	if err != nil {
		return err
	}
	if o.kind != opInfo {
		return fmt.Errorf("%w: the server did not open with INFO", ErrProtocol)
	}
	var info serverInfo
	if err := json.Unmarshal(o.arg, &info); err != nil {
		return fmt.Errorf("%w: INFO: %w", ErrProtocol, err)
	}
	if !info.Headers {
		return errors.New("nats: the server does not support headers, which JetStream's replies need")
	}
	connect, err := json.Marshal(map[string]any{
		"verbose": false, "pedantic": false, "headers": true, "no_responders": true,
		"protocol": 1, "lang": "go", "version": "sluice", "name": cfg.Name,
		"user": cfg.User, "pass": cfg.Password,
	})
	if err != nil {
		return err
	}
	c.wb = append(append(append(c.wb[:0], "CONNECT "...), connect...), "\r\nPING\r\n"...)
	if _, err := c.nc.Write(c.wb); err != nil {
		return err
	}
	for {
		o, err := c.next()
		if err != nil {
			return err
		}
		if o.kind == opPong {
			return nil
		}
	}
}

// next reads the next operation that is not the connection's own business:
// a PING is answered, an INFO or a +OK skipped, an -ERR returned as an
// [*Error].
func (c *Conn) next() (op, error) {
	for {
		o, err := c.r.read()
		if err != nil {
			return op{}, err
		}
		switch o.kind {
		case opPing:
			if _, err := c.nc.Write([]byte("PONG\r\n")); err != nil {
				return op{}, err
			}
		case opInfo, opOK:
		case opErr:
			return op{}, &Error{Message: string(o.arg)}
		default:
			return o, nil
		}
	}
}

// bound sets the connection's deadline to ctx's, and to now once ctx ends,
// until the returned stop.
func (c *Conn) bound(ctx context.Context) func() bool {
	dl, _ := ctx.Deadline()
	_ = c.nc.SetDeadline(dl)
	return context.AfterFunc(ctx, func() { _ = c.nc.SetDeadline(time.Unix(1, 0)) })
}

// begin takes the connection for one call.
func (c *Conn) begin(ctx context.Context) error {
	c.mu.Lock()
	if c.broken != nil {
		c.mu.Unlock()
		return fmt.Errorf("%w (%w)", ErrConnBroken, c.broken)
	}
	if err := ctx.Err(); err != nil {
		c.mu.Unlock()
		return err
	}
	return nil
}

// fail breaks the connection with err, unless err is a server -ERR the
// server survives; it returns what the call reports.
func (c *Conn) fail(ctx context.Context, err error) error {
	var se *Error
	if errors.As(err, &se) && recoverable(se.Message) {
		return err
	}
	if ctx.Err() != nil {
		err = fmt.Errorf("%w: %w", ctx.Err(), err)
	} else if dl, ok := ctx.Deadline(); ok && !time.Now().Before(dl) {
		err = fmt.Errorf("%w: %w", context.DeadlineExceeded, err)
	}
	c.broken = err
	_ = c.nc.Close()
	return fmt.Errorf("%w (%w)", ErrConnBroken, err)
}

// recoverable says whether the server keeps the connection after this
// -ERR: an invalid subject and the permissions violations, by the protocol's
// table; every other one it closes.
func recoverable(msg string) bool {
	m := strings.ToLower(msg)
	return strings.HasPrefix(m, "invalid subject") || strings.HasPrefix(m, "permissions violation")
}

// Message is one message to publish.
type Message struct {
	Subject string
	Data    []byte
}

// Publish sends msgs in one write, core NATS: the server takes them in
// order and confirms nothing. A subject a JetStream stream listens on is
// stored, without the stream's acknowledgement — enough for a test or a
// feed whose loss the caller accepts; acknowledged publishing is not offered
// yet. A subject that is empty or holds white space is refused before
// anything is sent.
func (c *Conn) Publish(ctx context.Context, msgs []Message) error {
	for i, m := range msgs {
		if !validSubject(m.Subject) {
			return fmt.Errorf("nats: message %d has the subject %q", i, m.Subject)
		}
	}
	if err := c.begin(ctx); err != nil {
		return err
	}
	defer c.mu.Unlock()
	stop := c.bound(ctx)
	defer stop()
	c.wb = c.wb[:0]
	for _, m := range msgs {
		c.wb = appendPub(c.wb, m.Subject, "", m.Data)
	}
	if _, err := c.nc.Write(c.wb); err != nil {
		return c.fail(ctx, err)
	}
	return nil
}

// Request publishes data on subject and returns the first reply — the
// JetStream API's way of being called ($JS.API.STREAM.CREATE.<name> and
// the others). A subject no one answers on returns a [*StatusError] of
// code 503 (no responders).
func (c *Conn) Request(ctx context.Context, subject string, data []byte) (Msg, error) {
	if !validSubject(subject) {
		return Msg{}, fmt.Errorf("nats: the subject %q", subject)
	}
	if err := c.begin(ctx); err != nil {
		return Msg{}, err
	}
	defer c.mu.Unlock()
	stop := c.bound(ctx)
	defer stop()
	c.sid++
	sid := strconv.Itoa(c.sid)
	reply := c.inbox + ".r" + sid
	c.wb = append(c.wb[:0], "SUB "+reply+" "+sid+"\r\nUNSUB "+sid+" 1\r\n"...)
	c.wb = appendPub(c.wb, subject, reply, data)
	if _, err := c.nc.Write(c.wb); err != nil {
		return Msg{}, c.fail(ctx, err)
	}
	for {
		o, err := c.next()
		if err != nil {
			return Msg{}, c.fail(ctx, err)
		}
		if o.kind != opMsg || o.sid != sid {
			continue // what a stopped consumer left on its inbox
		}
		if code, desc := status(o.header); code != 0 && len(o.data) == 0 {
			return Msg{}, &StatusError{Code: code, Description: desc}
		}
		return Msg{Subject: o.subject, Reply: o.reply, Header: o.header, Data: o.data}, nil
	}
}

// StatusError is a status the server sent as a message with headers and no
// payload: 503 for a request no one answers, 404 and 408 that end a pull,
// 409 for a consumer that cannot serve it.
type StatusError struct {
	Code        int
	Description string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("nats: status %d %s", e.Code, e.Description)
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

func appendPub(dst []byte, subject, reply string, data []byte) []byte {
	dst = append(dst, "PUB "...)
	dst = append(dst, subject...)
	if reply != "" {
		dst = append(dst, ' ')
		dst = append(dst, reply...)
	}
	dst = append(dst, ' ')
	dst = strconv.AppendInt(dst, int64(len(data)), 10)
	dst = append(dst, '\r', '\n')
	dst = append(dst, data...)
	return append(dst, '\r', '\n')
}

// validSubject refuses what would break the control line: an empty
// subject, or one holding a space, a tab or a line end, which would let a
// value write a second operation.
func validSubject(s string) bool {
	return s != "" && !strings.ContainsAny(s, " \t\r\n")
}
