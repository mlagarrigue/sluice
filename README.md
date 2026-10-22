# QUIC Interop Runner: sluice against every implementation

Pass of 2026-10-22, sluice at 37bc440, runner at ab039a4 ([run](https://github.com/mlagarrigue/sluice/actions/runs/39838039404)).
Each cell: sluice as server / sluice as client; s succeeded, f failed, u unsupported, - not run; f>s failed, then succeeded when run once more.

| peer | amplificationlimit | blackhole | chacha20 | connectionmigration | ecn | handshake | handshakecorruption | handshakeloss | http3 | ipv6 | keyupdate | longrtt | multiplexing | rebind-addr | rebind-port | resumption | retry | transfer | transfercorruption | transferloss | v2 | zerortt |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| aioquic | s/s | s/s | s/u | s/u | u/u | s/s | s/s | s/s | s/u | s/s | s/u | s/s | s/s | s/s | s/s | u/u | s/s | s/s | s/s | s/s | u/u | u/u |
| chrome | u/- | u/- | u/- | u/- | u/- | u/- | u/- | u/- | s/- | u/- | u/- | u/- | u/- | u/- | u/- | u/- | u/- | u/- | u/- | u/- | u/- | u/- |
| go-x-net | s/s | s/s | u/u | f>f/u | u/u | s/s | u/u | u/u | u/u | s/s | s/u | s/s | s/s | s/f>f | s/f>f | u/u | u/u | s/s | s/s | s/s | u/u | u/u |
| haproxy | -/s | -/s | -/u | -/u | -/u | -/s | -/s | -/s | -/u | -/s | -/u | -/s | -/s | -/f>f | -/f>f | -/u | -/s | -/s | -/s | -/s | -/u | -/u |
| kwik | s/s | s/s | s/u | f>f/u | u/u | s/s | s/f>f | s/f>s | s/u | s/s | s/u | s/s | s/s | s/s | s/s | u/u | s/s | s/s | s/s | s/s | u/u | u/u |
| lsquic | s/s | s/s | s/u | s/s | s/s | s/s | s/f>f | f>s/f>f | s/u | s/s | u/u | s/s | s/s | s/f>f | s/f>f | u/u | s/s | s/s | s/s | s/s | u/u | u/u |
| msquic | s/f>f | s/s | s/u | f>f/u | u/u | s/s | s/f>f | s/f>f | u/u | s/s | s/u | s/s | s/s | s/s | s/s | u/u | s/f>f | s/s | s/s | s/s | u/u | u/u |
| mvfst | s/f>f | s/s | u/u | f>f/u | u/u | s/s | f>f/s | f>f/s | s/u | s/s | u/u | s/s | s/s | s/s | s/s | u/u | u/u | s/s | s/s | s/s | u/u | u/u |
| neqo | s/s | s/s | s/u | s/s | s/s | s/s | s/s | s/s | s/u | s/s | s/u | s/s | s/s | s/s | s/s | u/u | s/s | s/s | s/s | s/s | u/u | u/u |
| nginx | -/s | -/s | -/u | -/u | -/u | -/s | -/s | -/s | -/u | -/s | -/u | -/s | -/s | -/s | -/s | -/u | -/s | -/s | -/s | -/s | -/u | -/u |
| ngtcp2 | s/s | s/s | s/u | s/f>f | s/s | s/s | s/s | f>s/s | s/u | s/s | s/u | s/s | s/s | s/s | s/s | u/u | s/s | s/s | s/s | s/s | u/u | u/u |
| picoquic | s/f>f | s/s | s/u | s/s | s/s | s/s | s/s | s/s | s/u | s/s | s/u | s/s | s/s | s/s | s/f>f | u/u | s/s | s/s | s/s | s/s | u/u | u/u |
| quic-go | s/s | s/s | s/u | f>f/u | u/u | s/s | s/s | s/s | s/u | s/s | s/u | s/s | s/s | s/s | s/s | u/u | s/s | s/s | s/s | s/s | u/u | u/u |
| quiche | s/s | s/s | u/u | f>f/u | u/u | s/s | f>s/s | s/s | s/u | s/s | u/u | s/s | s/s | s/f>f | s/f>f | u/u | s/s | s/s | s/s | s/s | u/u | u/u |
| quinn | s/s | s/s | s/u | f>f/u | s/s | s/s | f>f/s | f>s/s | s/u | s/s | s/u | s/s | s/s | s/s | s/s | u/u | s/s | s/s | s/s | s/s | u/u | u/u |
| s2n-quic | s/s | s/s | u/u | f>f/f>f | s/s | s/s | s/s | s/s | s/u | s/s | u/u | s/s | s/s | s/s | s/s | u/u | s/s | s/s | s/s | s/s | u/u | u/u |
| xquic | s/f>f | s/s | s/u | f>f/u | u/u | s/s | s/s | s/f>f | s/u | s/s | s/u | s/s | s/s | s/s | s/s | u/u | s/f>f | s/s | s/s | s/s | u/u | u/u |
