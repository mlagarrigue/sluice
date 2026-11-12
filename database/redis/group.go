package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/mlagarrigue/sluice"
)

// Entry is one entry of a Redis stream, as XREADGROUP delivers it.
type Entry struct {
	// ID is the entry's identifier in the stream, "1700000000000-0".
	ID string
	// Fields are the entry's field–value pairs, in the order XADD wrote
	// them.
	Fields []Field
	// Deleted says the entry was removed from the stream (XDEL, a trim)
	// while it waited in this consumer's pending list: its ID is all that is
	// left, Fields is empty. Acknowledge it like any other, or it stays
	// pending for good.
	Deleted bool
}

// Field is one field of an [Entry] and its value.
type Field struct {
	Name, Value []byte
}

// GroupConfig names the stream, the consumer group and the consumer a
// [Group] reads as. The group is the caller's to create — XGROUP CREATE,
// with MKSTREAM for a stream that may not exist yet —, as the PostgreSQL
// connector leaves the replication slot to the caller: it is a durable
// object of the server, whose life is the deployment's, not a connection's.
type GroupConfig struct {
	// Stream, Group and Consumer are required.
	Stream   string
	Group    string
	Consumer string
	// BatchEntries is how many entries one read asks for (COUNT), and so
	// the most a batch holds. Zero means [sluice.DefaultBatchSize]; a
	// negative value is refused.
	BatchEntries int
	// Block is how long a read waits for new entries before it reads
	// again (BLOCK), which is also how long the stream takes to notice that
	// its context ended. Zero means one second; a negative value is
	// refused.
	Block time.Duration
}

func (g GroupConfig) resolve() (GroupConfig, error) {
	var errs []error
	for _, f := range [...]struct{ name, v string }{{"Stream", g.Stream}, {"Group", g.Group}, {"Consumer", g.Consumer}} {
		if f.v == "" {
			errs = append(errs, fmt.Errorf("redis: GroupConfig.%s is required", f.name))
		}
	}
	if g.BatchEntries < 0 {
		errs = append(errs, fmt.Errorf("redis: BatchEntries %d is negative; zero means %d", g.BatchEntries, sluice.DefaultBatchSize))
	}
	if g.Block < 0 {
		errs = append(errs, fmt.Errorf("redis: Block %v is negative; zero means one second", g.Block))
	}
	if g.BatchEntries == 0 {
		g.BatchEntries = sluice.DefaultBatchSize
	}
	if g.Block == 0 {
		g.Block = time.Second
	}
	return g, errors.Join(errs...)
}

// Group reads a stream as one consumer of a consumer group, in batches, and
// acknowledges what its consumer says it has processed.
//
// It is ADR 0023's shape, the one PostgreSQL's logical replication takes: the
// consumer acknowledges, never the connector as it reads. An entry the
// server delivered stays in the consumer's pending list until [Group.Ack]
// names it; a batch handed on is not a batch processed — a parallel.Async
// downstream has only received it —, so the stream cannot acknowledge on the
// consumer's behalf. Delivery is at least once: a consumer that stops or
// crashes before acknowledging gets its pending entries again, first, the
// next time it reads.
type Group struct {
	c   *Conn
	cfg GroupConfig

	mu   sync.Mutex
	acks []string
}

// ReadGroup readies a [Group] on c. Nothing is sent before
// [Group.Entries]. The connection is the Group's while a stream of it runs:
// other calls on c wait their turn between two reads.
func (c *Conn) ReadGroup(cfg GroupConfig) (*Group, error) {
	cfg, err := cfg.resolve()
	if err != nil {
		return nil, err
	}
	return &Group{c: c, cfg: cfg}, nil
}

// Ack declares the entries ids processed. They leave the consumer's pending
// list with the next read the stream makes — one round trip carries the
// XACK and the XREADGROUP — and when the stream stops. Ack is safe to call
// from any goroutine; an ID acknowledged twice, or one the group does not
// hold, costs the server nothing.
func (g *Group) Ack(ids ...string) {
	g.mu.Lock()
	g.acks = append(g.acks, ids...)
	g.mu.Unlock()
}

// Entries returns the group's entries as a stream of batches, at most
// [GroupConfig.BatchEntries] each.
//
// The consumer's own pending entries come first — those delivered to it
// before and never acknowledged, after a crash or a stop —, then the
// entries no consumer of the group has seen (">"). The stream runs until
// ctx ends, which it notices between two reads, at most
// [GroupConfig.Block] later; it then sends the acknowledgements still owed
// and ends, the connection left usable, and [sluice.Source.Err] reports
// nil. Any other end is an error Err reports: a server error (NOGROUP for a
// group that does not exist), or the connection's, which leaves it broken.
// A consumer that stops early ends the stream the same way: what it
// acknowledged is sent, the rest stays pending.
func (g *Group) Entries(ctx context.Context) sluice.Source[Entry] {
	var srcErr error
	stream := func(yield func(sluice.Batch[Entry]) bool) {
		defer func() {
			if err := g.flush(ctx); err != nil && srcErr == nil {
				srcErr = err
			}
		}()
		cursor := "0" // the consumer's pending entries, then ">"
		count := strconv.Itoa(g.cfg.BatchEntries)
		block := strconv.FormatInt(max(g.cfg.Block.Milliseconds(), 1), 10)
		for ctx.Err() == nil {
			read := []string{"XREADGROUP", "GROUP", g.cfg.Group, g.cfg.Consumer, "COUNT", count}
			if cursor == ">" {
				read = append(read, "BLOCK", block)
			}
			read = append(read, "STREAMS", g.cfg.Stream, cursor)
			replies, err := g.roundTrip(ctx, read)
			if err != nil {
				srcErr = err
				return
			}
			entries, err := parseEntries(replies[len(replies)-1])
			if err != nil {
				srcErr = err
				return
			}
			if cursor != ">" {
				if len(entries) == 0 {
					cursor = ">" // the pending list is read through
					continue
				}
				cursor = entries[len(entries)-1].ID
			}
			if len(entries) > 0 && !yield(sluice.Batch[Entry]{Items: entries}) {
				return
			}
		}
	}
	return sluice.NewSource(stream, func() error { return srcErr })
}

// roundTrip sends the acknowledgements owed and then cmd, in one pipeline,
// and returns the replies. Its context does not end with ctx: ending the
// stream is noticed between reads, and interrupting a read would leave the
// connection broken. It ends Block after the read could have answered, so a
// server that stops answering still breaks the stream.
func (g *Group) roundTrip(ctx context.Context, cmd []string) ([]Reply, error) {
	g.mu.Lock()
	acks := g.acks
	g.acks = nil
	g.mu.Unlock()
	var cmds [][]string
	if len(acks) > 0 {
		cmds = append(cmds, append([]string{"XACK", g.cfg.Stream, g.cfg.Group}, acks...))
	}
	if cmd != nil {
		cmds = append(cmds, cmd)
	}
	if len(cmds) == 0 {
		return nil, nil
	}
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), g.cfg.Block+30*time.Second)
	defer cancel()
	replies, err := g.c.Pipeline(rctx, cmds)
	if err != nil {
		return nil, err
	}
	for _, r := range replies {
		if err := r.Err(); err != nil {
			return nil, err
		}
	}
	return replies, nil
}

// flush sends the acknowledgements still owed.
func (g *Group) flush(ctx context.Context) error {
	_, err := g.roundTrip(ctx, nil)
	return err
}

// parseEntries reads XREADGROUP's RESP2 reply for one stream: nil when a
// BLOCK timed out, else [[key, [[id, [field, value, …]], …]]], the fields nil
// for an entry deleted while pending.
func parseEntries(r Reply) ([]Entry, error) {
	if r.Kind == KindNil {
		return nil, nil
	}
	bad := func(what string) ([]Entry, error) {
		return nil, fmt.Errorf("%w: XREADGROUP replied with %s", ErrProtocol, what)
	}
	if r.Kind != KindArray || len(r.Array) != 1 {
		return bad("not one stream")
	}
	s := r.Array[0]
	if s.Kind != KindArray || len(s.Array) != 2 || s.Array[1].Kind != KindArray {
		return bad("a stream that is not [key, entries]")
	}
	raw := s.Array[1].Array
	entries := make([]Entry, len(raw))
	for i, e := range raw {
		if e.Kind != KindArray || len(e.Array) != 2 || e.Array[0].Kind != KindBulk {
			return bad("an entry that is not [id, fields]")
		}
		entries[i].ID = string(e.Array[0].Str)
		f := e.Array[1]
		switch {
		case f.Kind == KindNil:
			entries[i].Deleted = true
		case f.Kind == KindArray && len(f.Array)%2 == 0:
			fields := make([]Field, len(f.Array)/2)
			for j := range fields {
				fields[j] = Field{Name: f.Array[2*j].Str, Value: f.Array[2*j+1].Str}
			}
			entries[i].Fields = fields
		default:
			return bad("fields that are not name–value pairs")
		}
	}
	return entries, nil
}
