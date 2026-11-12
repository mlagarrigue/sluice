# PostgreSQL reference

The settings of `database/postgres`, each with what its zero value means and
what is refused, and the types its codec reads and writes. The
[PostgreSQL guide](../guide/postgres.md) says when to reach for them.

## Contents

- [Connection settings](#connection-settings)
- [Query settings](#query-settings)
- [Logical replication settings](#logical-replication-settings)
- [The codecs](#the-codecs)
- [Ranges and multiranges](#ranges-and-multiranges)

## Connection settings

Each field left at zero takes the value of the "Zero" column; what
`Connect` refuses, it refuses before anything is sent, naming the field.

| Field | Zero | Bounds | What it sets |
|---|---|---|---|
| `User` | — (required) | | the role |
| `Database` | the database named like `User` | | the database |
| `Password` | none | | sent only if the server asks for one |
| `AllowCleartextPassword` | refused | | answering a cleartext password request; only over verified TLS or a local socket |
| `RequireAuth` | not required | | refusing a server that asked for no credential |
| `ChannelBinding` | `ChannelBindingPrefer` | Prefer, Require, Disable | binding SCRAM to the server's certificate |
| `MinSCRAMIterations` | 4096 | ≥ 0, ≤ Max | PBKDF2 iterations below which the server is refused |
| `MaxSCRAMIterations` | 1,048,576 | ≥ Min | above which it is refused |
| `MinSCRAMSaltLen` | 16 bytes | ≥ 0 | a shorter salt is refused |
| `Params` | none | not `user`, `database`, `replication` | runtime parameters sent at startup |
| `MaxMessageBytes` | 64 MiB | 64 KiB to 1 GiB | the largest message accepted from the server |
| `MaxProtocol` | `Protocol32` | versions this package speaks | the version asked for |
| `MinProtocol` | any known version | ≤ `MaxProtocol` | the version below which the connection is refused |
| `StatementCacheSize` | 512 | negative: no cache | statements kept prepared on the server |
| `CancelTimeout` | 5 s | ≥ 0 | how long a cancellation the connection sends may take |
| `Logger` | `slog.Default` | | warnings with no error to return through |

## Query settings

`QueryConfig`, the last argument of `Query`:

| Field | Zero | Bounds | What it sets |
|---|---|---|---|
| `BatchRows` | `sluice.DefaultBatchSize` | ≥ 0 | rows per batch, and rows asked of the server at a time |
| `BatchBytes` | 64 MiB | 0 to 1 GiB | bytes of values per batch; a wider row travels alone |
| `Fetch` | `FetchAuto` | | everything at once when the statement fits one batch, in batches otherwise |
| `Demand` | none | exclusive of `FetchAll` | what the consumer no longer needs (a limit, a lower bound) |
| `KeyParam` | the bound is not pushed down | 1-based | the parameter the demand's lower bound replaces |
| `Tag` | nothing copied | | receives the command tag, read once the source is consumed |

## Logical replication settings

`ReplicationConfig`, the argument of `Replicate`:

| Field | Zero | Bounds | What it sets |
|---|---|---|---|
| `Slot` | — (required) | ≤ 63 bytes | the logical slot, created with pgoutput |
| `Publications` | — (required) | ≤ 63 bytes each | the publications whose tables are streamed |
| `Start` | the slot's `confirmed_flush_lsn` | | the position to stream from |
| `BatchChanges` | `sluice.DefaultBatchSize` | ≥ 0 | changes per batch |
| `BatchBytes` | 64 MiB | 0 to 1 GiB | bytes of messages per batch |
| `StatusInterval` | 10 s | ≥ 0 | how often, at most, the acknowledged position is reported |

## The codecs

Every type below has the same pair in `pgtype`, `AppendX`/`DecodeX`, named
after the PostgreSQL type, and its codec, `XCodec()`, which opens the
containers (arrays of any shape, ranges, composites:
[in the guide](../guide/postgres.md#containers-arrays-ranges-composites)). The first thirteen also
have a named one-dimensional array (`AppendXArray`/`DecodeXArray`), the
same array with NULL elements
(`AppendNullableXArray`/`DecodeNullableXArray`, over `[]Null[T]`), and a
column scanner (`ScanX`). What is missing is listed in
[Limits](../guide/limits.md), not discovered at use.

The types fall in three groups: the thirteen with arrays and a scanner,
the other everyday types, and the system catalogue's own.

**With arrays and a column scanner.**

| PostgreSQL | Go | X | Array | Array with NULLs | Scan |
|---|---|---|---|---|---|
| `bool` | `bool` | `Bool` | yes | yes | yes |
| `int2` | `int16` | `Int2` | yes | yes | yes |
| `int4` | `int32` | `Int4` | yes | yes | yes |
| `int8` | `int64` | `Int8` | yes | yes | yes |
| `float4` | `float32` | `Float4` | yes | yes | yes |
| `float8` | `float64` | `Float8` | yes | yes | yes |
| `text`, `varchar`, `char(n)`, `name` | `string` | `Text` | yes | yes | yes |
| `bytea` | `[]byte` | `Bytea` | yes | yes | yes |
| `uuid` | `[16]byte` | `UUID` | yes | yes | yes |
| `timestamptz` | `time.Time` (UTC) | `TimestampTZ` | yes | yes | yes |
| `timestamp` | `time.Time` (no zone) | `Timestamp` | yes | yes | yes |
| `date` | `time.Time` (midnight UTC) | `Date` | yes | yes | yes |
| `numeric` | `Numeric` (exact) | `Numeric` | yes | yes | yes |

**Other everyday types: the pair and the codec.**

| PostgreSQL | Go | X | Array | Array with NULLs | Scan |
|---|---|---|---|---|---|
| `json` | `[]byte` | `JSON` | — | — | as text |
| `jsonb` | `[]byte` | `JSONB` | — | — | — |
| `inet`, `cidr` | `netip.Prefix` | `Inet`, `CIDR` (one decoder) | — | — | — |
| `interval` | `Interval` | `Interval` | — | — | — |
| `time` | `TimeOfDay` | `Time` | — | — | — |
| `timetz` | `TimeTZ` | `TimeTZ` | — | — | — |
| `macaddr`, `macaddr8` | `[6]byte`, `[8]byte` | `Macaddr`, `Macaddr8` | — | — | — |
| `bit`, `varbit` | `Bits` | `Bits` | — | — | — |
| `point`, `line`, `lseg`, `box`, `circle` | `Point`, `Line`, `LSeg`, `Box`, `Circle` | `Point`, `Line`, `LSeg`, `Box`, `Circle` | — | — | — |
| `path`, `polygon` | `Path`, `[]Point` | `Path`, `Polygon` | — | — | — |
| `money` | `int64` (units) | `Money` | — | — | — |
| `xml`, `refcursor` | `string` | `Text` | — | — | as text |
| `jsonpath` | `string` | `JSONPath` | — | — | — |
| `tsvector` | `[]Lexeme` | `TSVector` | — | — | — |
| `tsquery` | `[]TSQueryItem` | `TSQuery` | — | — | — |

**The system catalogue's types: the pair and the codec.**

| PostgreSQL | Go | X | Array | Array with NULLs | Scan |
|---|---|---|---|---|---|
| `oid` and the `reg*` aliases (`regclass`, `regtype`…, `regdatabase` in 19) | `uint32` | `OID` | — | — | — |
| `oid8` (19), `xid8` | `uint64` | `OID8`, `XID8` | — | — | — |
| `xid`, `cid` | `uint32` | `XID`, `CID` | — | — | — |
| `tid` | `TID` | `TID` | — | — | — |
| `pg_lsn` | `uint64` (`postgres.LSN(v)`) | `PGLSN` | — | — | — |
| `pg_snapshot`, `txid_snapshot` | `Snapshot` | `Snapshot` | — | — | — |
| `int2vector`, `oidvector` | `[]int16`, `[]uint32` | `Int2Vector`, `OIDVector` | — | — | — |
| `char`, the quoted `"char"` (not `char(n)`) | `byte` | `Char` | — | — | — |

## Ranges and multiranges

The six built-in ranges and their multiranges have their codecs and
OIDs, arrays included (`OIDInt4RangeArray`…):

| PostgreSQL | Go | Codec | OID |
|---|---|---|---|
| `int4range`, `int4multirange` | `Range[int32]`, `Multirange[int32]` | `Int4RangeCodec()`, `Int4MultirangeCodec()` | `OIDInt4Range`, `OIDInt4Multirange` |
| `int8range`, `int8multirange` | `Range[int64]`, `Multirange[int64]` | `Int8RangeCodec()`, `Int8MultirangeCodec()` | `OIDInt8Range`, `OIDInt8Multirange` |
| `numrange`, `nummultirange` | `Range[Numeric]`, `Multirange[Numeric]` | `NumRangeCodec()`, `NumMultirangeCodec()` | `OIDNumRange`, `OIDNumMultirange` |
| `tsrange`, `tsmultirange` | `Range[time.Time]`, `Multirange[time.Time]` | `TSRangeCodec()`, `TSMultirangeCodec()` | `OIDTSRange`, `OIDTSMultirange` |
| `tstzrange`, `tstzmultirange` | `Range[time.Time]`, `Multirange[time.Time]` | `TSTZRangeCodec()`, `TSTZMultirangeCodec()` | `OIDTSTZRange`, `OIDTSTZMultirange` |
| `daterange`, `datemultirange` | `Range[time.Time]`, `Multirange[time.Time]` | `DateRangeCodec()`, `DateMultirangeCodec()` | `OIDDateRange`, `OIDDateMultirange` |

