# Redis and Valkey

A word you do not know is likely in the [glossary](../glossary.md).

## What this is about

Redis — and Valkey, which speaks the same protocol — is an in-memory
key–value server, often a cache in front of a database. Each command
normally waits for its answer before the next one leaves: a thousand keys,
a thousand round trips. `database/redis` sends a **batch** of commands in
one write and reads the answers in order — *pipelining* — so a batch costs
one round trip.

> **In plain words: pipelining.** You post a thousand letters at once
> instead of waiting for each reply before writing the next; the postman
> makes one journey. The server still answers every letter, in order.

The package speaks RESP2, the protocol every Redis and Valkey server
accepts on a new connection, over the standard library alone.

## Connecting

`redis.Connect` takes a recipe for the transport — `redis.DialTCP`, or
`redis.DialTLS` with your own `tls.Config` — and gives one connection; there
is no pool, as with PostgreSQL. AUTH, SELECT and a PING leave in one round
trip, and a refusal comes back from `redis.Connect` itself: a wrong
password, or a server in protected mode answering `-DENIED`.

```go
conn, err := redis.Connect(ctx, redis.DialTCP("127.0.0.1:6379"), redis.Config{
	Password: os.Getenv("REDIS_PASSWORD"),
})
if err != nil {
	return err
}
defer conn.Close()
```

## A batch of commands

`Conn.Pipeline` takes the batch as a list of commands, each a list of
arguments, and returns one reply per command:

```go
replies, err := conn.Pipeline(ctx, [][]string{
	{"GET", "user:1"},
	{"GET", "user:2"},
	{"INCR", "visits"},
})
if err != nil {
	return err // the connection's: it is broken, open another
}
for _, r := range replies {
	if err := r.Err(); err != nil {
		// this command's error, at its place; the others stand
		continue
	}
	// r.Kind: KindBulk (r.Str), KindNil (no such key), KindInt (r.Int)…
}
```

A server error answers its own command and no other: it is that reply's
(`Reply.Err`, an `*redis.Error` whose `Prefix` is the server's kind, such as
`WRONGTYPE`), and the connection goes on. The error `Conn.Pipeline` returns
is the connection's — the context ended, the network failed, a reply could
not be read or passed `MaxReplyBytes` — and leaves it broken
(`redis.ErrConnBroken`): the replies still owed are lost with their place in
the stream. `Conn.Do` is a batch of one.

Arguments are strings, binary-safe, sent as RESP bulk strings: a command is
never a line of text assembled from your values, so a value cannot become a
second command.

## Settings

`redis.Config`:

| Field | Zero | Bounds | What it sets |
|---|---|---|---|
| `User` | the default user | | the user AUTH names |
| `Password` | no AUTH | | the password AUTH sends |
| `Database` | database 0 | ≥ 0 | the logical database SELECT picks |
| `MaxReplyBytes` | 64 MiB | ≥ 0 | bytes one reply may take on the wire, nested elements included |

## What a round trip costs

A thousand GETs against a Valkey in a container on the same machine
(`BenchmarkGetPipelined` and `BenchmarkGetOneByOne`, `database/redis`):
about half a microsecond per key in one pipeline, about 148 microseconds per
key one by one — each of those a round trip through the container's
network. The figures are in
[Benchmarks](../benchmarks.md#redis-a-batch-against-one-command-at-a-time).
