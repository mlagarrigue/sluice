package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// PubAck is a JetStream stream's answer to one published message: where it
// was stored, or why it was not.
type PubAck struct {
	// Stream is the stream that stored the message, and Sequence its place
	// there, from 1.
	Stream   string
	Sequence uint64
	// Err is why the message was not stored: a [*StatusError] of code 503
	// when no stream listens on its subject, an [*APIError] when the stream
	// refused it (a limit reached with discard new, a wrong expected
	// sequence). Stream and Sequence are then empty.
	Err error
}

// APIError is an error the JetStream API reported in its reply.
type APIError struct {
	// Code is the reply's status, HTTP-like: 400, 503.
	Code int
	// ErrCode is JetStream's own code for the error, 10077 for a stream's
	// message limit reached, as the server's errors.json lists them.
	ErrCode     int
	Description string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("nats: JetStream error %d (%d): %s", e.Code, e.ErrCode, e.Description)
}

// PublishAcked publishes msgs in one write, each with a reply subject, and
// returns the stream's acknowledgement of each, in msgs' order, read once
// the write is done: one round trip for the batch, not one per message.
//
// The error is non-nil when any message was not stored. With the slice
// nil, nothing is known — the connection broke, or ctx ended before every
// acknowledgement came, which also breaks it, as replies would still be on
// their way. With the slice, each [PubAck.Err] says which messages were not
// stored, and the error names the first; the others were.
//
// A subject that is empty or holds white space, and a payload past the
// server's max_payload, are refused before anything is sent. The server
// holds the acknowledgements, about a hundred bytes each, until the write
// ends: a batch of a few thousand messages is the measure, not millions.
func (c *Conn) PublishAcked(ctx context.Context, msgs []Message) ([]PubAck, error) {
	if err := c.check(msgs); err != nil {
		return nil, err
	}
	if len(msgs) == 0 {
		return nil, nil
	}
	if err := c.begin(ctx); err != nil {
		return nil, err
	}
	defer c.mu.Unlock()
	stop := c.bound(ctx)
	defer stop()
	c.sid++
	sid := strconv.Itoa(c.sid)
	inbox := c.inbox + ".a" + sid + "."
	// The subscription ends itself once every message has its reply: one
	// per message, the acknowledgement or the 503 of no responders.
	c.wb = append(c.wb[:0], "SUB "+inbox+"* "+sid+"\r\nUNSUB "+sid+" "+strconv.Itoa(len(msgs))+"\r\n"...)
	var idx []byte
	for i, m := range msgs {
		idx = strconv.AppendInt(append(idx[:0], inbox...), int64(i), 10)
		c.wb = appendPub(c.wb, m.Subject, string(idx), m.Data)
	}
	if _, err := c.nc.Write(c.wb); err != nil {
		return nil, c.fail(ctx, err)
	}
	acks := make([]PubAck, len(msgs))
	seen := make([]bool, len(msgs))
	for got := 0; got < len(msgs); {
		o, err := c.next()
		if err != nil {
			return nil, c.fail(ctx, err)
		}
		if o.kind != opMsg || o.sid != sid {
			continue // what a stopped consumer left on its inbox
		}
		i, err := strconv.Atoi(strings.TrimPrefix(o.subject, inbox))
		if err != nil || !strings.HasPrefix(o.subject, inbox) || i < 0 || i >= len(msgs) || seen[i] {
			return nil, c.fail(ctx, fmt.Errorf("%w: a publish reply on %q", ErrProtocol, o.subject))
		}
		seen[i] = true
		got++
		acks[i] = pubAck(o)
	}
	failed, first := 0, -1
	for i := range acks {
		if acks[i].Err == nil {
			continue
		}
		failed++
		if first < 0 {
			first = i
		}
	}
	if failed > 0 {
		return acks, fmt.Errorf("nats: %d of %d messages not stored; message %d: %w", failed, len(msgs), first, acks[first].Err)
	}
	return acks, nil
}

// pubAck reads one reply to a published message.
func pubAck(o op) PubAck {
	if code, desc := status(o.header); code != 0 && len(o.data) == 0 {
		return PubAck{Err: &StatusError{Code: code, Description: desc}}
	}
	var r struct {
		Stream   string `json:"stream"`
		Sequence uint64 `json:"seq"`
		Error    *struct {
			Code        int    `json:"code"`
			ErrCode     int    `json:"err_code"`
			Description string `json:"description"`
		} `json:"error"`
	}
	if err := json.Unmarshal(o.data, &r); err != nil {
		return PubAck{Err: fmt.Errorf("%w: a publish acknowledgement: %w", ErrProtocol, err)}
	}
	if r.Error != nil {
		return PubAck{Err: &APIError{Code: r.Error.Code, ErrCode: r.Error.ErrCode, Description: r.Error.Description}}
	}
	if r.Stream == "" {
		return PubAck{Err: fmt.Errorf("%w: a publish acknowledgement without a stream: %q", ErrProtocol, o.data)}
	}
	return PubAck{Stream: r.Stream, Sequence: r.Sequence}
}
