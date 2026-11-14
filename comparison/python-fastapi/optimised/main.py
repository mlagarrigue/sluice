"""The study's Python contender after its optimisation budget.

Study 2.4, "Optimised": the routes, documents and queries of the standard
version, with each change listed, and its source, in comparison/README.md:
  - async path operations on asyncpg instead of SQLModel on synchronous
    psycopg (FastAPI, "Concurrency and async / await"; study 2.6);
  - Granian as the server, which speaks HTTP/2 (study 2.6, the optimised
    column), one worker per usable CPU and at most one per pooled
    connection, the contract's pool split between them (2.6);
  - answers returned as a JSONResponse directly, without a response model
    to validate them against (FastAPI, "Return a Response Directly").
GZipMiddleware as in the standard version.

Granian's workers are processes with no channel between them: each writes
its share of the metrics point to a file every half second, and the worker
that answers adds them up -- a document at most half a second old.
"""

import asyncio
import json
import os
import sys
from contextlib import asynccontextmanager

import asyncpg
from fastapi import FastAPI, Query, Request
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import contract  # noqa: E402

cfg = contract.read_config()
cfg.offers(["h1", "h2"], ["gzip"])
WORKERS = max(1, min(os.process_cpu_count() or 1, cfg.pool))
METRICS_DIR = os.environ.get("STUDY_METRICS_DIR", "/tmp/study-metrics")


def _write_part():
    path = os.path.join(METRICS_DIR, f"{os.getpid()}.json")
    with open(path + ".tmp", "w") as f:
        json.dump(contract.metrics_part(), f)
    os.replace(path + ".tmp", path)


async def _publish():
    while True:
        _write_part()
        await asyncio.sleep(0.5)


@asynccontextmanager
async def lifespan(app):
    # An empty DSN: asyncpg reads PGHOST, PGUSER and the rest, as libpq does.
    app.state.pool = await asyncpg.create_pool(min_size=1, max_size=max(1, cfg.pool // WORKERS))
    async with app.state.pool.acquire() as conn:
        for ddl in contract.WRITE_TABLE:
            await conn.execute(ddl)
    os.makedirs(METRICS_DIR, exist_ok=True)
    task = asyncio.create_task(_publish())
    yield
    task.cancel()
    await app.state.pool.close()


app = FastAPI(lifespan=lifespan)
if cfg.compression == "gzip":
    app.add_middleware(GZipMiddleware, minimum_size=1000, compresslevel=1)


def fail(status, error, key=None):
    body = {"error": error} if key is None else {"error": error, "key": key}
    return JSONResponse(status_code=status, content=body)


@app.get("/static")
async def static():
    return JSONResponse(contract.STATIC)


@app.get("/orders/{id}")
async def unit(id: str, request: Request):
    key = contract.parse_int64(id)
    if key is None:
        return fail(400, "bad key")
    try:
        row = await request.app.state.pool.fetchrow(
            "SELECT id, total, label FROM bench_orders WHERE id = $1", key)
    except Exception as e:
        print(f"unit {key}: {e}", file=sys.stderr)
        return fail(503, "unavailable", key)
    if row is None:
        return fail(404, "no such key", key)
    return JSONResponse({"id": row[0], "total": row[1], "label": row[2]})


@app.get("/orders")
async def list_orders(request: Request, from_: str = Query("", alias="from"), limit: str = ""):
    start = contract.parse_int64(from_)
    if start is None:
        return fail(400, "bad from")
    n = contract.parse_int64(limit) or 0
    if n <= 0 or n > contract.LIST_MAX:
        return fail(400, "bad limit", start)
    try:
        rows = await request.app.state.pool.fetch(
            "SELECT id, total, label FROM bench_orders WHERE id > $1 ORDER BY id LIMIT $2", start, n)
    except Exception as e:
        print(f"list {start}: {e}", file=sys.stderr)
        return fail(503, "unavailable", start)
    return JSONResponse({"from": start, "limit": n,
                         "items": [{"id": r[0], "total": r[1], "label": r[2]} for r in rows]})


@app.post("/orders")
async def write(request: Request):
    try:
        body = await request.json()
        key, total = body["key"], body["total"]
        if not contract.in_int64(key) or not contract.in_int64(total) or key == 0:
            raise ValueError
    except Exception:
        return fail(400, "bad body")
    try:
        await request.app.state.pool.execute(
            "INSERT INTO comparison_writes (key, total) VALUES ($1, $2)", key, total)
    except Exception as e:
        print(f"write {key}: {e}", file=sys.stderr)
        return fail(503, "unavailable", key)
    return JSONResponse({"key": key})


@app.get("/study/metrics")
async def metrics():
    _write_part()
    parts = []
    for name in os.listdir(METRICS_DIR):
        if name.endswith(".json"):
            try:
                with open(os.path.join(METRICS_DIR, name)) as f:
                    parts.append(json.load(f))
            except (OSError, ValueError):
                pass
    return JSONResponse(contract.metrics_doc(parts))


if __name__ == "__main__":
    from pathlib import Path

    from granian import Granian
    from granian.constants import HTTPModes, Interfaces

    print(f"python-fastapi-optimised: {cfg.protocol} on {cfg.host}:{cfg.port} ({cfg.summary()}), "
          f"{WORKERS} workers", file=sys.stderr)
    Granian(
        "optimised.main:app",
        address=cfg.host,
        port=cfg.port,
        interface=Interfaces.ASGI,
        workers=WORKERS,
        http=HTTPModes.http2 if cfg.protocol == "h2" else HTTPModes.http1,
        ssl_cert=Path(cfg.cert) if cfg.tls else None,
        ssl_key=Path(cfg.key) if cfg.tls else None,
    ).serve()
