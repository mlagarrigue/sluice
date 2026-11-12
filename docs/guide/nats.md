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

```go
conn, err := nats.Connect(ctx, nats.DialTCP("127.0.0.1:4222"), nats.Config{Name: "billing"})
if err != nil {
	return err
}
defer conn.Close()
```

### Over TLS

`Config.TLS` takes a `*tls.Config`. A NATS server sends its first line, the
INFO, in clear, and the client then turns the same connection into a TLS
one: that is why `Connect` does the upgrade and not the dial recipe.
`ServerName` must name the server — the address given to `DialTCP` does not
reach the handshake.

```go
conn, err := nats.Connect(ctx, nats.DialTCP("nats.example.com:4222"), nats.Config{
	TLS: &tls.Config{ServerName: "nats.example.com", MinVersion: tls.VersionTLS12},
})
```

> **In plain words: TLS hides the traffic and proves who answers.** The
> connection is encrypted, and the server shows a certificate signed by a
> root you trust — the system's, or the ones you put in `RootCAs`.

`Connect` refuses a server that requires TLS when `Config.TLS` is nil, and
refuses a server that does not offer TLS when `Config.TLS` is set, rather
than carrying on in clear. A server set to `handshake_first`, which waits
for TLS before its INFO, is not supported.

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
known to have been.

`Conn.PublishAcked` sends the batch in one write too, each message with a
reply subject, and then reads the stream's acknowledgement of each: one
round trip for the batch, not one per message.

```go
acks, err := conn.PublishAcked(ctx, msgs)
if err != nil && acks == nil {
	return err // the connection broke: nothing is known
}
for i, a := range acks {
	if a.Err != nil {
		log.Printf("message %d not stored: %v", i, a.Err) // publish it again, or report it
		continue
	}
	// a.Stream and a.Sequence say where it was stored
}
```

It returns one `PubAck` per message, in the batch's order: `Stream` and
`Sequence`, its place in the stream, or `Err` — a `*nats.StatusError` of
code 503 when no stream listens on the subject, a `*nats.APIError` when the
stream refused it (its `max_msgs` reached with `discard: new`, for one).
The error returned is not nil as soon as one message was not stored, and
names the first. A batch of a few thousand messages is the measure: the
server holds the acknowledgements until the write ends.

Both refuse, before anything is sent, a subject that is empty or holds white
space, and a payload past the server's `max_payload` (`nats.ErrTooLarge`,
the connection kept): the server would close the connection on it.

## Settings

`nats.Config`, who connects:

| Field | Zero | Bounds | What it sets |
|---|---|---|---|
| `User` | no user | | the user CONNECT names |
| `Password` | no password | | the password CONNECT sends |
| `Name` | no name | | the client name the server's monitoring shows |

and how:

| Field | Zero | Bounds | What it sets |
|---|---|---|---|
| `MaxPayloadBytes` | 8 MiB | ≥ 0 | bytes one received message may take, headers included |
| `TLS` | in clear | `ServerName` set | the TLS configuration of the upgrade after the server's INFO |

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
