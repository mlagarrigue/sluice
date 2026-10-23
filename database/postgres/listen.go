package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/internal/pgwire"
)

// ListenConfig says which channels [Listen] subscribes to.
type ListenConfig struct {
	// Channels are the channels to LISTEN on. At least one is required. A
	// name is taken as written, case included.
	Channels []string

	// BatchNotifications is the most notifications one batch holds. Zero
	// means [sluice.DefaultBatchSize]; a negative value is refused. A batch
	// also ends as soon as the next message is not already received.
	BatchNotifications int
}

// Notification is one NOTIFY, as a listening session receives it.
type Notification struct {
	Channel string
	Payload string // empty when NOTIFY gave none

	// PID is the server process of the session that sent it.
	PID uint32
}

// Listener is a connection subscribed to channels with LISTEN. [Listen] opens
// it, [Listener.Notifications] reads it.
type Listener struct {
	c       *Conn
	batch   int
	started atomic.Bool
	err     error

	// pending holds what arrived while the subscriptions were being made.
	pending []Notification
	buf     []Notification
}

// Listen opens a connection, subscribes it to the channels, and returns it
// ready to stream their notifications.
//
// The connection does nothing else: it stays outside any transaction, so
// notifications reach it as soon as the transactions that sent them commit,
// and it never holds back the server's cleanup of its notification queue. A
// notification that arrives while the subscriptions are being made is kept
// and delivered first.
//
// Delivery is the server's: a notification sent while the listener is closed
// is not kept for it. A consumer that must see every event reads a table, and
// uses notifications as the signal to look.
func Listen(ctx context.Context, dial DialFunc, cfg Config, lc ListenConfig) (*Listener, error) {
	sql, err := listenSQL(lc)
	if err != nil {
		return nil, err
	}
	batch := lc.BatchNotifications
	if batch == 0 {
		batch = sluice.DefaultBatchSize
	}
	c, err := Connect(ctx, dial, cfg)
	if err != nil {
		return nil, err
	}
	l := &Listener{c: c, batch: batch}
	stop := context.AfterFunc(ctx, func() { _ = c.rw.Close() })
	err = l.subscribe(sql)
	if !stop() {
		err = fmt.Errorf("postgres: subscribing: %w", context.Cause(ctx))
	}
	if err != nil {
		_ = c.Close()
		return nil, err
	}
	return l, nil
}

// listenSQL builds the LISTEN commands, one statement per channel in one
// query string.
func listenSQL(lc ListenConfig) (string, error) {
	if len(lc.Channels) == 0 {
		return "", errors.New("postgres: ListenConfig.Channels is empty — LISTEN needs a channel")
	}
	if lc.BatchNotifications < 0 {
		return "", fmt.Errorf("postgres: ListenConfig.BatchNotifications %d is negative — zero means %d", lc.BatchNotifications, sluice.DefaultBatchSize)
	}
	var b strings.Builder
	for _, ch := range lc.Channels {
		if ch == "" {
			return "", errors.New("postgres: ListenConfig.Channels holds an empty name")
		}
		b.WriteString("LISTEN " + quoteIdent(ch) + ";")
	}
	sql := b.String()
	if err := checkSQLText(sql); err != nil {
		return "", err
	}
	return sql, nil
}

// subscribe runs the LISTEN query and reads its answer itself: Exec discards
// a notification that lands before ReadyForQuery, and here that would be the
// first one the caller subscribed for.
func (l *Listener) subscribe(sql string) error {
	c := l.c
	c.w.Query(sql)
	if err := c.w.Flush(); err != nil {
		return c.breakConn(err)
	}
	var failed error
	for {
		m, err := c.r.Next()
		if err != nil {
			return c.breakConn(err)
		}
		switch m.Type {
		case pgwire.BackendReadyForQuery:
			return failed
		case pgwire.BackendNotificationResponse:
			n, err := parseNotification(m.Body)
			if err != nil {
				return c.breakConn(err)
			}
			l.pending = append(l.pending, n)
		case pgwire.BackendErrorResponse:
			e, perr := parseError(m.Body)
			if perr != nil {
				return c.breakConn(perr)
			}
			failed = e
		case pgwire.BackendCommandComplete, pgwire.BackendNoticeResponse, pgwire.BackendParameterStatus:
		default:
			return c.breakConn(fmt.Errorf("%w: message %q answering LISTEN", ErrProtocol, m.Type))
		}
	}
}

// Notifications streams what the channels receive, one batch at a time,
// until the consumer stops or ctx ends. It may be called once.
//
// The stream is pulled, and the server keeps what is not read yet in a queue
// shared by the whole instance (8 GB in a standard installation). When it
// fills, the transactions that send NOTIFY fail at commit: a listener that
// falls behind weighs on every sender, not on itself.
//
// When ctx ends, the connection is closed and Err reports why.
func (l *Listener) Notifications(ctx context.Context) sluice.Source[Notification] {
	if !l.started.CompareAndSwap(false, true) {
		return sluice.NewSource[Notification](nil, func() error {
			return errors.New("postgres: Listener.Notifications called twice — a listener is read once")
		})
	}
	stream := func(yield func(sluice.Batch[Notification]) bool) {
		stop := context.AfterFunc(ctx, func() { _ = l.c.rw.Close() })
		defer stop()
		for {
			b, err := l.readBatch()
			if err != nil {
				l.err = err
				if ctx.Err() != nil {
					l.err = fmt.Errorf("postgres: listening stopped: %w", context.Cause(ctx))
				}
				return
			}
			if !yield(b) {
				return
			}
		}
	}
	return sluice.NewSource(sluice.Stream[Notification](stream), func() error { return l.err })
}

// Close closes the connection, which ends its subscriptions.
func (l *Listener) Close() error { return l.c.Close() }

// readBatch returns what the subscription kept, or reads until a batch is
// full, or holds something and the next message is not already received.
func (l *Listener) readBatch() (sluice.Batch[Notification], error) {
	if len(l.pending) > 0 {
		b := sluice.Batch[Notification]{Items: l.pending}
		l.pending = nil
		return b, nil
	}
	l.buf = l.buf[:0]
	for len(l.buf) < l.batch && (len(l.buf) == 0 || l.c.r.Buffered()) {
		m, err := l.c.r.Next()
		if err != nil {
			return sluice.Batch[Notification]{}, l.c.breakConn(err)
		}
		switch m.Type {
		case pgwire.BackendNotificationResponse:
			n, err := parseNotification(m.Body)
			if err != nil {
				return sluice.Batch[Notification]{}, l.c.breakConn(err)
			}
			l.buf = append(l.buf, n)
		case pgwire.BackendErrorResponse:
			// An idle session is sent an error only on its way out: an
			// administrator's termination, a server shutdown.
			e, perr := parseError(m.Body)
			if perr != nil {
				return sluice.Batch[Notification]{}, l.c.breakConn(perr)
			}
			return sluice.Batch[Notification]{}, l.c.breakConn(e)
		case pgwire.BackendNoticeResponse, pgwire.BackendParameterStatus:
		default:
			return sluice.Batch[Notification]{}, l.c.breakConn(fmt.Errorf("%w: message %q while listening", ErrProtocol, m.Type))
		}
	}
	return sluice.Batch[Notification]{Items: l.buf}, nil
}
