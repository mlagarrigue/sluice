# Exit conditions

What each experimental package must prove to leave the label, and where it
stands. [Limits](../guide/limits.md) says what is missing; this page says
what would make the label go.

## Experimental: the exit conditions

These packages leave the label only by meeting these conditions; they stay
in the module ([ADR 0021](../design/adr/0021-net-quic-stays.md)). One
bullet per package, each condition in it checkable:

- **`net/quic/udp`**: it leaves with `net/quic`, not before. Its options
  are the socket half of the transport and are judged by the same passes;
  the `test-udp` job already runs it on Windows, macOS and Linux ARM.
  Checked by: `net/quic`'s conditions met, and `test-udp` green on the
  commit that lifts the label.
- **`net/quic`**: three conditions, each checked on its own.
  - *Interop*: the *QUIC Interop Runner*'s weekly workflow green, not a
    pass started by hand, with `interop/expected-results.json` unchanged:
    every case against quic-go and ngtcp2, each direction, passing or
    failing for its written cause. Not yet: the weekly pass runs every
    Sunday, and none has finished green so far
    ([in detail](protocol-positions.md#the-interop-runner-in-detail)).
  - *Review*: an external review of the handshake and key handling,
    linked from `SECURITY.md`. Not done.
  - *Measurement*: throughput and latency against a real third-party
    client, over a network between two machines, in
    [Benchmarks](../benchmarks.md#http2-and-http3-from-a-real-client)
    with date and machines. Missing: see "Measurements: what is missing".

What `net/quic` does today, and within which bounds — the limits it keeps
whichever way the conditions go — is in the QUIC reference,
[Limits it keeps](quic.md#limits-it-keeps).
