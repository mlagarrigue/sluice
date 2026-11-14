"""The study's Python contender as FastAPI's documentation writes it.

Study 2.4, "Standard"; level 2, 2.5 and 2.6: FastAPI with SQLModel on
psycopg, synchronous path operations (FastAPI runs them in its thread pool),
a session per request through a dependency, one query per request, no
grouping, one process. Compression is FastAPI's GZipMiddleware, at the
contract's level 1.

Served by uvicorn, the server `fastapi run` starts, but started from code
(uvicorn.Server) rather than by `fastapi run`: the contract's TLS -- TLS 1.3
only, ALPN naming the run's one protocol, no tickets -- needs an SSLContext
that neither command line takes. HTTP/1.1 only: uvicorn speaks no HTTP/2;
h2 and h3 are refused and published "not supported".
"""

import os
import sys
from typing import Annotated

import uvicorn
from fastapi import Depends, FastAPI, Query
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlmodel import Field, Session, SQLModel, create_engine, select

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import contract  # noqa: E402

cfg = contract.read_config()
cfg.offers(["h1"], ["gzip"])


class Order(SQLModel, table=True):
    __tablename__ = "bench_orders"
    id: int = Field(primary_key=True)
    total: int
    label: str


class Write(SQLModel, table=True):
    __tablename__ = "comparison_writes"
    id: int | None = Field(default=None, primary_key=True)
    key: int
    total: int


class WriteReq(SQLModel):
    key: int
    total: int


# An empty URL: psycopg reads PGHOST, PGUSER and the rest, as libpq does.
engine = create_engine("postgresql+psycopg://", pool_size=cfg.pool, max_overflow=0)
with engine.begin() as conn:
    for ddl in contract.WRITE_TABLE:
        conn.execute(text(ddl))


def get_session():
    with Session(engine) as session:
        yield session


SessionDep = Annotated[Session, Depends(get_session)]

app = FastAPI()
if cfg.compression == "gzip":
    app.add_middleware(GZipMiddleware, minimum_size=1000, compresslevel=1)


def fail(status, error, key=None):
    body = {"error": error} if key is None else {"error": error, "key": key}
    return JSONResponse(status_code=status, content=body)


@app.get("/static")
def static():
    return contract.STATIC


@app.get("/orders/{id}")
def unit(id: str, session: SessionDep):
    key = contract.parse_int64(id)
    if key is None:
        return fail(400, "bad key")
    try:
        order = session.get(Order, key)
    except Exception as e:
        print(f"unit {key}: {e}", file=sys.stderr)
        return fail(503, "unavailable", key)
    if order is None:
        return fail(404, "no such key", key)
    return order


@app.get("/orders")
def list_orders(session: SessionDep, from_: Annotated[str, Query(alias="from")] = "", limit: str = ""):
    start = contract.parse_int64(from_)
    if start is None:
        return fail(400, "bad from")
    limit = contract.parse_int64(limit) or 0
    if limit <= 0 or limit > contract.LIST_MAX:
        return fail(400, "bad limit", start)
    try:
        items = session.exec(select(Order).where(Order.id > start).order_by(Order.id).limit(limit)).all()
    except Exception as e:
        print(f"list {start}: {e}", file=sys.stderr)
        return fail(503, "unavailable", start)
    return {"from": start, "limit": limit, "items": items}


@app.post("/orders")
def write(req: WriteReq, session: SessionDep):
    if req.key == 0 or not contract.in_int64(req.key) or not contract.in_int64(req.total):
        return fail(400, "bad body")
    try:
        session.add(Write(key=req.key, total=req.total))
        session.commit()
    except Exception as e:
        print(f"write {req.key}: {e}", file=sys.stderr)
        return fail(503, "unavailable", req.key)
    return {"key": req.key}


@app.get("/study/metrics")
def metrics():
    return contract.metrics_doc([contract.metrics_part()])


if __name__ == "__main__":
    print(f"python-fastapi-standard: {cfg.protocol} on {cfg.host}:{cfg.port} ({cfg.summary()})", file=sys.stderr)
    config = uvicorn.Config(app, host=cfg.host, port=cfg.port)
    config.load()
    if cfg.tls:
        config.ssl = cfg.ssl_context()
    uvicorn.Server(config).run()
