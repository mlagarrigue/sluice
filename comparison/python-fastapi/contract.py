"""What the Python contender's versions share and none is measured on.

The contract's environment (comparison/README.md, "The contract every
contender honours"), the static document, the write table and the metrics
point. The contender's own code -- routes, handlers, the driver -- lives in
standard/, optimised/ and dataloader-optimised/, where the study counts its
lines (section 1.1).
"""

import gc
import os
import ssl
import time
from dataclasses import dataclass

LIST_MAX = 100_000

WRITE_TABLE = [
    """CREATE TABLE IF NOT EXISTS comparison_writes (
        id bigserial PRIMARY KEY, key bigint NOT NULL, total bigint NOT NULL,
        created timestamptz NOT NULL DEFAULT now())""",
    "ALTER TABLE comparison_writes SET (autovacuum_enabled = off)",
]

STATIC = {
    "status": "ok",
    "service": "sluice-study",
    "version": "r",
    "padding": [f"line-{i:02d}-abcdefghijklmnop" for i in range(40)],
}


@dataclass
class Config:
    host: str
    port: int
    protocol: str
    tls: bool
    cert: str
    key: str
    resume: bool
    compression: str
    pool: int

    def offers(self, protocols, codings):
        """Refuses what a version does not implement, before it listens: a
        configuration it cannot serve is published "not supported" (2.4)."""
        if self.protocol not in protocols:
            raise SystemExit(f"STUDY_PROTOCOL={self.protocol}: not supported by this contender")
        if self.compression != "off" and self.compression not in codings:
            raise SystemExit(f"STUDY_COMPRESSION={self.compression}: not offered by this contender")

    def alpn(self):
        return {"h1": "http/1.1", "h2": "h2", "h3": "h3"}[self.protocol]

    def ssl_context(self):
        """The TLS of every contender: TLS 1.3 only, the campaign's
        certificate, ALPN naming the run's one protocol, no session tickets
        unless asked (num_tickets = 0: the server issues none, and TLS 1.3
        resumes through nothing else)."""
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.minimum_version = ssl.TLSVersion.TLSv1_3
        ctx.load_cert_chain(self.cert, self.key)
        ctx.set_alpn_protocols([self.alpn()])
        if not self.resume:
            ctx.num_tickets = 0
        return ctx

    def summary(self):
        if not self.tls:
            return "clear text, HTTP/1.1"
        return (f"TLS 1.3+, certificate {self.cert}, ALPN {self.alpn()}, "
                f"resumption {'on' if self.resume else 'off'}")


def read_config():
    """Reads STUDY_* with the study server's defaults. The database is not
    here: PGHOST and its siblings are libpq's, which psycopg and the URL
    built from them read."""
    env = os.environ.get

    def on_off(name, default):
        v = env(name, "")
        if v == "":
            return default
        if v in ("on", "off"):
            return v == "on"
        raise SystemExit(f"{name}={v!r}: on or off")

    listen = env("STUDY_LISTEN") or "127.0.0.1:8443"
    host, _, port = listen.rpartition(":")
    c = Config(
        host=host.strip("[]"),
        port=int(port),
        protocol=env("STUDY_PROTOCOL") or "h1",
        tls=on_off("STUDY_TLS", True),
        cert=env("STUDY_TLS_CERT", ""),
        key=env("STUDY_TLS_KEY", ""),
        resume=on_off("STUDY_TLS_RESUME", False),
        compression=env("STUDY_COMPRESSION") or "off",
        pool=int(env("STUDY_DB_POOL") or 8),
    )
    if c.pool <= 0:
        raise SystemExit(f"STUDY_DB_POOL={c.pool}: a positive count")
    if c.protocol not in ("h1", "h2", "h3"):
        raise SystemExit(f"STUDY_PROTOCOL={c.protocol!r}: h1, h2 or h3")
    if c.compression not in ("off", "gzip", "deflate", "br", "zstd"):
        raise SystemExit(f"STUDY_COMPRESSION={c.compression!r}: off, gzip, deflate, br or zstd")
    if not c.tls and c.protocol != "h1":
        raise SystemExit(f"STUDY_TLS=off with STUDY_PROTOCOL={c.protocol}: in clear the study runs HTTP/1.1 only")
    if c.tls and not (c.cert and c.key):
        # The images carry a certificate for a local run, named by default.
        raise SystemExit("STUDY_TLS_CERT and STUDY_TLS_KEY name the certificate and its key")
    return c


# The metrics point. CPython's collector reports each collection to
# gc.callbacks, at its start and its stop: the time between is a pause of
# the thread that collects, and it is also the collector's CPU, which no
# other thread does. Buckets double from 10 us. cpu_seconds is the process's
# CPU (time.process_time, every thread).
_gc = {"count": 0, "seconds": 0.0, "buckets": {}, "start": 0.0}


def _on_gc(phase, info):
    if phase == "start":
        _gc["start"] = time.perf_counter()
        return
    s = time.perf_counter() - _gc["start"]
    _gc["count"] += 1
    _gc["seconds"] += s
    le = 1e-5
    while le < s:
        le *= 2
    _gc["buckets"][le] = _gc["buckets"].get(le, 0) + 1


gc.callbacks.append(_on_gc)


# The DataLoader variant's batches: one per call of its batch function, by
# size in powers of two (1, 2, 4...), as the study's server counts its own.
_batches = {"count": 0, "keys": 0, "sizes": {}}


def record_batch(n):
    """Counts one batch of n keys."""
    _batches["count"] += 1
    _batches["keys"] += n
    le = 1
    while le < n:
        le *= 2
    _batches["sizes"][le] = _batches["sizes"].get(le, 0) + 1


def metrics_part():
    """This process's share: cumulative since it started."""
    return {
        "cpu": time.process_time(),
        "gc": {"count": _gc["count"], "seconds": _gc["seconds"], "buckets": dict(_gc["buckets"])},
        "batches": {"count": _batches["count"], "keys": _batches["keys"], "sizes": dict(_batches["sizes"])},
    }


def metrics_doc(parts, grouped=False):
    """The contract's document, summed over the processes (one, or the
    workers of a server that forks); batches is null unless the version
    groups."""
    cpu = count = seconds = 0
    buckets = {}
    for p in parts:
        cpu += p["cpu"]
        count += p["gc"]["count"]
        seconds += p["gc"]["seconds"]
        for le, n in p["gc"]["buckets"].items():
            buckets[float(le)] = buckets.get(float(le), 0) + n
    return {
        "runtime": "python " + ".".join(map(str, __import__("sys").version_info[:3])),
        "cpu_seconds": cpu,
        "gc": {
            "cpu_seconds": seconds,
            "pauses": {
                "count": count,
                "seconds": seconds,
                "buckets": [{"le": le, "count": n} for le, n in sorted(buckets.items())],
            },
        },
        "batches": _sum_batches(parts) if grouped else None,
    }


def _sum_batches(parts):
    count = keys = 0
    sizes = {}
    for p in parts:
        b = p["batches"]
        count += b["count"]
        keys += b["keys"]
        for le, n in b["sizes"].items():
            sizes[int(le)] = sizes.get(int(le), 0) + n
    return {
        "count": count,
        "keys": keys,
        "sizes": [{"le": le, "count": n} for le, n in sorted(sizes.items())],
    }
