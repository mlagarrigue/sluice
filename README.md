# QUIC Interop Runner: sluice against every implementation

Pass of 2026-10-30, sluice at 9262dcf, runner at ab039a4 ([run](https://github.com/mlagarrigue/sluice/actions/runs/41025122707)).
Each cell: sluice as server / sluice as client; s succeeded, f failed, u unsupported, - not run; f>s failed, then succeeded when run once more.

| peer | amplificationlimit | blackhole | chacha20 | connectionmigration | ecn | handshake | handshakecorruption | handshakeloss | http3 | ipv6 | keyupdate | longrtt | multiplexing | rebind-addr | rebind-port | resumption | retry | transfer | transfercorruption | transferloss | v2 | zerortt |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| aioquic | s/s | s/s | s/u | s/u | u/u | s/s | f>s/s | s/s | s/u | s/s | s/u | s/s | s/s | s/s | s/s | s/s | s/s | s/s | s/s | s/s | u/u | s/s |
| chrome | u/- | u/- | u/- | u/- | u/- | u/- | u/- | u/- | s/- | u/- | u/- | u/- | u/- | u/- | u/- | u/- | u/- | u/- | u/- | u/- | u/- | u/- |
| go-x-net | s/s | s/s | u/u | f>f/u | u/u | s/s | u/u | u/u | u/u | s/s | s/u | s/s | s/s | s/f>f | f>s/f>f | u/u | u/u | s/s | s/s | s/s | u/u | u/u |
| haproxy | -/s | -/s | -/u | -/u | -/u | -/s | -/s | -/s | -/u | -/s | -/u | -/s | -/s | -/f>f | -/f>f | -/s | -/s | -/s | -/s | -/s | -/u | -/s |
| kwik | s/s | s/s | s/u | f>f/u | u/u | s/s | s/s | s/s | s/u | s/s | s/u | s/s | s/s | s/s | s/s | s/s | s/s | s/s | s/s | s/s | u/u | s/s |
| lsquic | s/s | s/s | s/u | s/s | s/s | s/s | s/f>f | s/f>f | s/u | s/s | u/u | s/s | s/s | s/s | s/s | s/s | s/s | s/s | s/s | s/s | u/u | s/s |
| msquic | s/f>f | s/s | s/u | f>f/u | u/u | s/s | s/f>f | s/f>f | u/u | s/s | s/u | s/s | s/s | s/s | s/s | s/s | s/f>f | s/s | s/s | s/s | u/u | s/s |
| neqo | s/s | s/s | s/u | s/s | s/s | s/s | f>s/s | s/s | s/u | s/s | s/u | s/s | s/s | s/s | s/s | s/s | s/s | s/s | s/s | s/s | u/u | s/s |
| nginx | -/s | -/s | -/u | -/u | -/u | -/s | -/s | -/s | -/u | -/s | -/u | -/s | -/s | -/f>f | -/f>f | -/s | -/s | -/s | -/s | -/s | -/u | -/s |
| ngtcp2 | s/s | s/s | s/u | s/f>f | s/s | s/s | s/s | f>s/s | s/u | s/s | s/u | s/s | s/s | s/s | s/s | s/s | s/s | s/s | s/s | s/s | u/u | s/s |
| picoquic | s/f>f | s/s | s/u | s/s | s/s | s/s | s/s | s/f>s | s/u | s/s | s/u | s/s | s/s | s/s | s/s | s/s | s/s | s/s | s/s | s/s | u/u | s/s |
| quic-go | s/s | s/s | s/u | f>f/u | u/u | s/s | s/s | s/s | s/u | s/s | s/u | s/s | s/s | s/s | s/s | s/s | s/s | s/s | s/s | s/s | u/u | s/s |
| quiche | s/s | s/s | u/u | f>f/u | u/u | s/s | s/s | s/s | s/u | s/s | u/u | s/s | s/s | s/f>f | f>f/f>f | s/s | s/s | s/s | s/s | s/s | u/u | s/s |
| quinn | s/s | s/s | s/u | f>f/u | s/s | s/s | f>f/s | f>f/s | s/u | s/s | s/u | s/s | s/s | s/s | s/s | s/s | s/s | s/s | s/s | s/s | u/u | s/s |
| s2n-quic | s/s | s/s | u/u | f>f/f>f | s/s | s/s | s/s | s/s | s/u | s/s | u/u | s/s | s/s | s/s | s/s | s/s | s/s | s/s | s/s | s/s | u/u | u/u |
| xquic | s/f>f | s/s | s/u | f>f/u | u/u | s/s | s/s | s/f>s | s/u | s/s | s/u | s/s | s/s | s/s | s/s | f>f/s | s/f>f | s/s | s/s | s/s | u/u | s/s |
