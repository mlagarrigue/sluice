# HTTP reference

The settings of `net/httpstream.Config`, each with what its zero value means
and on which protocol it acts. The [HTTP guide](../guide/httpstream.md) says
when to reach for them.

## `net/httpstream` settings

Each field left at zero takes the value of the "Zero" column; a negative
value is refused, with the field's name, by `Config.Validate`, `Serve`, `Server`,
`ServeH2Conn`, `ServeH3Conn` and `Config.QUICConfig`, before
anything is read. The "Protocols" column says where the field acts.

| Field | Zero | Unit, bound | Protocols | What it bounds |
|---|---|---|---|---|
| `MaxRequestLineBytes` | 8 KiB | bytes | 1.1 | method, target and version together |
| `MaxHeaderBytes` | 32 KiB | bytes | all | all the headers of one request; announced on 2 and 3 |
| `MaxHeaders` | 64 | fields | all | how many headers one request carries |
| `MaxBodyBytes` | 1 MiB | bytes | all | a body delivered whole; four times that per HTTP/2 or HTTP/3 connection |
| `BodyDelivery` | `BodyStreamed` | form | all | whether a body streams or is held whole (`BodyWhole`) |
| `MaxStreamedBodyBytes` | `MaxBodyBytes` | bytes | all | a streamed body; the bound alone, never the switch |
| `MaxRequestsPerBatch` | 64 | requests | 1.1 | the pipelined requests of one batch |
| `MaxConcurrentStreams` | 100 | streams; at most 4096 on 3 | 2, 3 | the requests open at once on one connection |
| `MaxRequestsPerConn` | 1024 | requests | 1.1 | the requests of one connection before it closes |
| `IdleTimeout` | 2 min | duration | all | the silence between two requests; on 3, the transport's max_idle_timeout |
| `ReadTimeout` | 10 s | duration | all | the arrival of a whole request, and of the first byte; each `ReadProgressBytes` of a streamed body |
| `WriteTimeout` | 10 s | duration | all | a response write without progress |
| `WriteProgressBytes` | 4 KiB | bytes | all | the progress that restarts `WriteTimeout` |
| `ReadProgressBytes` | 4 KiB | bytes | all | the progress of a streamed body that restarts `ReadTimeout` |
| `MaxConns` | **no bound** | connections | 1.1, 2 | the connections open at once; also the memory bound |
| `MaxConnsPerIP` | **no bound** | connections | 1.1, 2 | the same bound per remote address |
| `IPv4PrefixBits` | 32 | bits; 1 to 32 | 1.1, 2 | the prefix `MaxConnsPerIP` counts IPv4 by |
| `IPv6PrefixBits` | 64 | bits; 1 to 128 | 1.1, 2 | the same for IPv6 |
| `ShutdownGrace` | 5 s | duration | all | the wait for requests in flight at a stop; `Server` ignores it |
| `KeepAlive` | nothing sent | duration | all | the silence before an HTTP/3 PING or a TCP probe |
| `Logger` | `slog.Default()` | — | all | where warnings go |
| `LingerTimeout` | 1 s, or `ReadTimeout` if shorter | duration | 1.1 | the read of a closing connection's tail |
| `MaxFrameSize` | 16384 | bytes; 16384 to 2^24−1 | 2 | one frame received or sent |
| `MaxFramesPerBatch` | 64 | frames | 2 | the frames of one batch |
| `MaxContinuationFrames` | 16 | frames | 2 | the CONTINUATION frames of one header block |
| `InitialWindowSize` | 65535 | bytes; at most 2^31−1 | 2 | the window of one request stream |
| `MaxPendingControlBytes` | 16 KiB | bytes | 2 | the control frames waiting to be written |
| `MaxWriteBufferBytes` | 64 KiB | bytes | 2 | what the writer gathers before one write |
| `QPACKStatic` | RFC 9204's table | — | 3 | for tests and interop only ([ADR 0013](../design/adr/0013-qpack-static-table.md)) |
| `EarlyData` | no request | predicate | all | the requests that may be acted on although they could be a replay ([ADR 0019](../design/adr/0019-quic-early-data.md)) |
