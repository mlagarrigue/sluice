# NATS JetStream

A word you do not know is likely in the [glossary](../glossary.md).

## What this is about

NATS is a messaging server: publishers send messages on *subjects*, and
JetStream, its persistence layer, keeps them in *streams* that consumers
read at their own pace. A *pull consumer* asks for a batch — "the next
256" — and the server delivers up to that many. Each message stays
unacknowledged until your code says it handled it, and is delivered again
once the consumer's `AckWait` passes (30 seconds by default) if it never
does. `messaging/nats` gives those batches as a `sluice.Source`.

> **In plain words: a queue that remembers.** A message read is not a
> message gone. It goes once your code says it was handled, and comes back
> if your program stops first.

The package speaks the NATS client protocol over the standard library
alone.

## Connecting

`nats.Connect` takes a recipe for the transport, `nats.DialTCP`, and gives
one connection, synchronous and without a pool, as the Redis connector's.
TLS is not offered yet: a NATS server sends its first line in clear before
the upgrade, which a dial recipe does not do.

```go
conn, err := nats.Connect(ctx, nats.DialTCP("127.0.0.1:4222"), nats.Config{Name: "billing"})
if err != nil {
	return err
}
defer conn.Close()
```

## Reading a consumer

The stream and the durable consumer — with explicit acknowledgement — are
created once, by your deployment: with the `nats` command-line tool, or
with `Conn.Request` on the JetStream API
(`$JS.API.STREAM.CREATE.<name>`, `$JS.API.CONSUMER.CREATE.<stream>.<name>`).
Then:

```go
k, err := conn.Consume(nats.ConsumerConfig{Stream: "ORDERS", Consumer: "billing"})
if err != nil {
	return err
}
src := k.Messages(ctx)
for b := range src.Stream() {
	for _, m := range b.Items {
		// m.Subject, m.Data
		k.Ack(m.Reply) // handled: the server may forget it
	}
}
if err := src.Err(); err != nil {
	return err
}
```

`Consumer.Ack` takes the message's `Reply` subject, where JetStream listens
for its acknowledgement. The acknowledgements leave with the next pull, in
the same write, and when the stream stops. The stream runs until `ctx`
ends, which it notices between two pulls — at most `Expires` later — and
then ends cleanly: the connection stays usable and `Err` is nil. A consumer
that does not exist ends it with a `*nats.StatusError` (503, no
responders).

## Publishing

`Conn.Publish` sends a batch of messages in one write. It is core NATS: the
server confirms nothing, so a message a stream should have stored is not
known to have been. Acknowledged publishing is not offered yet.

## Settings

`nats.Config`:

| Field | Zero | Bounds | What it sets |
|---|---|---|---|
| `User` | no user | | the user CONNECT names |
| `Password` | no password | | the password CONNECT sends |
| `Name` | no name | | the client name the server's monitoring shows |
| `MaxPayloadBytes` | 8 MiB | ≥ 0 | bytes one received message may take, headers included |

`nats.ConsumerConfig`, the names, both required:

| Field | Zero | Bounds | What it sets |
|---|---|---|---|
| `Stream` | — (required) | no space, `.`, `*`, `>` | the stream |
| `Consumer` | — (required) | no space, `.`, `*`, `>` | the durable consumer |

and the batch and the wait:

| Field | Zero | Bounds | What it sets |
|---|---|---|---|
| `BatchMessages` | 256 | ≥ 0 | messages one pull asks for: the most a batch holds |
| `Expires` | one second | ≥ 0 | how long the server holds a pull open, and so how long a stop takes to be noticed |
