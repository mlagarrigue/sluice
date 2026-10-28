# ADR 0013 — QPACK's static table ships, checked

- **Created:** 2026-08-22
- **Revised:** 2026-10-07 (renumbered), 2026-10-28 (the foreign interop run named)
- **Theme:** httpstream

## Problem

The QPACK decoder (HTTP/3's header compression) needs RFC 9204 Appendix
A's ninety-nine static entries, and unlike the HPACK Huffman code, which a
Kraft equality check verifies, a lookup table has no property a wrong entry
would break — entry forty-two being wrong decodes into a plausible header,
silently. Making the table a required parameter would refuse every real
client, all of which index the static table, unless the caller transcribed
the appendix themselves.

## Decision

The table ships, with the checking a transcription needs: diffed cell
by cell against the RFC's HTML rendering when written; shape tests (count,
token syntax, lowercase names, control-free values); the RFC's own encoded
example from Appendix B.1, byte for byte; and a hand-encoded client
fixture resolving entries across the table's range. `Config.QPACKStatic`
remains as the override: nil means the shipped table, an empty non-nil
table refuses indexed field lines outright.

## Rationale

The risk is real and a required parameter is the wrong remedy: pushing the
transcription to every caller gives it less checking than one shared copy,
against the
project's usage-simple rule, and prices the experimental path's headline
(three protocols, one handler) out of reach of any real client. The
residual risk is stated: a transcription error shared by this copy and its
checks would survive, and only an interop run against a foreign peer
retires it. The QUIC Interop Runner's `http3` case is that run, against
every implementation that plays it (`interop/expected-results.json`); it
retires the entries a request and its response use, not all ninety-nine.
