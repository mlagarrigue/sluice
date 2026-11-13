package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mlagarrigue/sluice"
)

// Msg is one message the server delivered.
type Msg struct {
	// Subject is the subject the message was published on.
	Subject string
	// Reply is where an answer goes; for a JetStream message, the subject
	// its acknowledgement goes to ($JS.ACK…), which [Consumer.Ack] takes.
	Reply string
	// Header holds the message's headers as they came, "NATS/1.0\r\n" and
	// "name: value" lines; empty for a message without.
	Header []byte
	// Data is the payload.
	Data []byte
}

// DefaultBatchMessages is how many messages a pull asks for when
// [ConsumerConfig.BatchMessages] is zero: 256, the largest batch the
// JetStream API's documentation names for a pull request.
const DefaultBatchMessages = 256

// ConsumerConfig names the stream and the durable pull consumer a
// [Consumer] reads. Both are the caller's to create, with acknowledgement
// policy explicit: a durable object of the server, whose life is the
// deployment's.
type ConsumerConfig struct {
	// Stream and Consumer are required. A JetStream name holds no space,
	// '.', '*' or '>'.
	Stream   string
	Consumer string
	// BatchMessages is how many messages one pull asks for, and so the most
	// a batch holds. Zero means [DefaultBatchMessages]; a negative value is
	// refused, and a server whose consumer caps the batch lower answers
	// with a 409 that ends the stream.
	BatchMessages int
	// Expires is how long the server holds a pull open waiting for
	// messages, which is also how long the stream takes to notice that its
	// context ended. Zero means one second; a negative value is refused.
	Expires time.Duration
}

func (cfg ConsumerConfig) resolve() (ConsumerConfig, error) {
	var errs []error
	for _, f := range [...]struct{ name, v string }{{"Stream", cfg.Stream}, {"Consumer", cfg.Consumer}} {
		if f.v == "" || strings.ContainsAny(f.v, " \t\r\n.*>") {
			errs = append(errs, fmt.Errorf("nats: ConsumerConfig.%s %q is not a JetStream name", f.name, f.v))
		}
	}
	if cfg.BatchMessages < 0 {
		errs = append(errs, fmt.Errorf("nats: BatchMessages %d is negative; zero means %d", cfg.BatchMessages, DefaultBatchMessages))
	}
	if cfg.Expires < 0 {
		errs = append(errs, fmt.Errorf("nats: Expires %v is negative; zero means one second", cfg.Expires))
	}
	if cfg.BatchMessages == 0 {
		cfg.BatchMessages = DefaultBatchMessages
	}
	if cfg.Expires == 0 {
		cfg.Expires = time.Second
	}
	return cfg, errors.Join(errs...)
}

// Consumer reads a JetStream pull consumer in batches, and acknowledges
// what its caller says it has processed.
//
// It is ADR 0023's shape, as Redis's consumer group is: the caller
// acknowledges, never the connector as it reads. A message stays
// unacknowledged until [Consumer.Ack] names it, and the server delivers it
// again once the consumer's AckWait passes (30 s by default). Delivery is at
// least once.
type Consumer struct {
	c   *Conn
	cfg ConsumerConfig

	mu   sync.Mutex
	acks []string
}

// Consume readies a [Consumer] on c. Nothing is sent before
// [Consumer.Messages].
func (c *Conn) Consume(cfg ConsumerConfig) (*Consumer, error) {
	cfg, err := cfg.resolve()
	if err != nil {
		return nil, err
	}
	return &Consumer{c: c, cfg: cfg}, nil
}

// Ack declares the messages whose [Msg.Reply] subjects are given processed.
// The acknowledgements leave with the next pull — one write carries them and
// the request — and when the stream stops. Ack is safe to call from any
// goroutine; a subject with white space in it is dropped, as no
// acknowledgement subject holds one.
func (k *Consumer) Ack(replies ...string) {
	k.mu.Lock()
	for _, r := range replies {
		if validSubject(r) {
			k.acks = append(k.acks, r)
		}
	}
	k.mu.Unlock()
}

// Messages returns the consumer's messages as a stream of batches, at most
// [ConsumerConfig.BatchMessages] each: one pull per batch, which the server
// answers with as many messages as it has, up to the batch, within
// [ConsumerConfig.Expires].
//
// The stream runs until ctx ends, which it notices between two pulls, at
// most Expires later; it then sends the acknowledgements still owed and
// ends, the connection left usable, and [sluice.Source.Err] reports nil.
// Any other end is an error Err reports: a [*StatusError] for a consumer the
// server cannot serve (409: deleted, push-based; 503: no JetStream, or no
// such consumer), or the connection's, which leaves it broken. A caller that
// stops early ends the stream the same way.
func (k *Consumer) Messages(ctx context.Context) sluice.Source[Msg] {
	var srcErr error
	stream := func(yield func(sluice.Batch[Msg]) bool) {
		sid, inbox, err := k.subscribe(ctx)
		if err != nil {
			srcErr = err
			return
		}
		defer func() {
			if err := k.round(ctx, sid, inbox, false, nil); err != nil && srcErr == nil {
				srcErr = err
			}
		}()
		for ctx.Err() == nil {
			var batch []Msg
			if err := k.round(ctx, sid, inbox, true, &batch); err != nil {
				srcErr = err
				return
			}
			if len(batch) > 0 && !yield(sluice.Batch[Msg]{Items: batch}) {
				return
			}
		}
	}
	return sluice.NewSource(stream, func() error { return srcErr })
}

// subscribe opens the inbox the pulls are answered on.
func (k *Consumer) subscribe(ctx context.Context) (sid, inbox string, err error) {
	c := k.c
	if berr := c.begin(ctx); berr != nil {
		return "", "", berr
	}
	defer c.mu.Unlock()
	// Its own context's deadline, as every call sets it: the one an earlier
	// call left — Connect's, under a context with a deadline — would
	// otherwise fail the SUB once passed, and break the connection.
	stop := c.bound(ctx)
	defer stop()
	c.sid++
	sid = strconv.Itoa(c.sid)
	inbox = c.inbox + ".p" + sid
	if _, werr := c.nc.Write([]byte("SUB " + inbox + " " + sid + "\r\n")); werr != nil {
		return "", "", c.fail(ctx, werr)
	}
	return sid, inbox, nil
}

// round sends the acknowledgements owed and, when pull is set, a pull
// request, then reads its answer into batch. The last round, at the stream's
// end, sends the acknowledgements and closes the inbox. Its I/O does not end
// with ctx — ending the stream is noticed between pulls, and interrupting a
// read would break the connection — but within Expires plus a margin, so a
// server that stops answering still ends it.
func (k *Consumer) round(ctx context.Context, sid, inbox string, pull bool, batch *[]Msg) error {
	k.mu.Lock()
	acks := k.acks
	k.acks = nil
	k.mu.Unlock()
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), k.cfg.Expires+30*time.Second)
	defer cancel()
	c := k.c
	if err := c.begin(rctx); err != nil {
		return err
	}
	defer c.mu.Unlock()
	stop := c.bound(rctx)
	defer stop()
	c.wb = c.wb[:0]
	for _, a := range acks {
		c.wb = appendPub(c.wb, a, "", []byte("+ACK"))
	}
	if !pull {
		c.wb = append(c.wb, "UNSUB "+sid+"\r\n"...)
		if _, err := c.nc.Write(c.wb); err != nil {
			return c.fail(rctx, err)
		}
		return nil
	}
	req, err := json.Marshal(struct {
		Batch   int   `json:"batch"`
		Expires int64 `json:"expires"`
	}{k.cfg.BatchMessages, k.cfg.Expires.Nanoseconds()})
	if err != nil {
		return err
	}
	c.wb = appendPub(c.wb, "$JS.API.CONSUMER.MSG.NEXT."+k.cfg.Stream+"."+k.cfg.Consumer, inbox, req)
	if _, err := c.nc.Write(c.wb); err != nil {
		return c.fail(rctx, err)
	}
	for len(*batch) < k.cfg.BatchMessages {
		o, err := c.next()
		if err != nil {
			return c.fail(rctx, err)
		}
		if o.kind != opMsg || o.sid != sid {
			continue
		}
		if code, desc := status(o.header); code != 0 && len(o.data) == 0 {
			switch code {
			case 100: // an idle heartbeat
				continue
			case 404, 408: // no messages, or the pull expired: it is over
				return nil
			case 409:
				// A consumer gone or push based ends the stream, and so does
				// a pull the server refuses on its own terms — "Exceeded
				// MaxRequestBatch of N", MaxRequestExpires, MaxWaiting,
				// "Message Size Exceeds MaxBytes" — which every next pull
				// would draw again at once, delivering nothing.
				if d := strings.ToLower(desc); strings.Contains(d, "consumer deleted") || strings.Contains(d, "push based") || strings.Contains(d, "exceed") {
					return &StatusError{Code: code, Description: desc}
				}
				return nil // the pull ended for a reason of its own; the next one asks again
			}
			return &StatusError{Code: code, Description: desc}
		}
		*batch = append(*batch, Msg{Subject: o.subject, Reply: o.reply, Header: o.header, Data: o.data})
	}
	return nil
}
