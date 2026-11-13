//! The study's Rust contender as the examples of axum and sqlx write it
//! (study §2.4, "Standard"): a Router of handlers returning Json, a PgPool
//! with one query per request (query_as into a FromRow struct), no grouping;
//! TLS by axum-server and rustls, HTTP/1.1 or HTTP/2 by ALPN; compression by
//! tower-http's CompressionLayer, gzip only, at the contract's level 1. The
//! optimised version is this code built with the `optimised` profile
//! (Cargo.toml), the one change listed in comparison/README.md.
//!
//! HTTP/3 is refused and published "not supported": axum-server serves none.

use std::collections::HashMap;
use std::net::SocketAddr;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::PgPool;
use tower_http::compression::{CompressionLayer, CompressionLevel};

use study_rust_axum::{Config, LIST_MAX, WRITE_TABLE};

#[derive(Serialize, sqlx::FromRow)]
struct Order {
    id: i64,
    total: i64,
    label: String,
}

#[derive(Serialize)]
struct ListDoc {
    from: i64,
    limit: i64,
    items: Vec<Order>,
}

#[derive(Deserialize)]
struct WriteReq {
    key: i64,
    total: i64,
}

#[derive(Serialize)]
struct WriteDoc {
    key: i64,
}

#[derive(Serialize)]
struct ErrDoc {
    error: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    key: Option<i64>,
}

fn fail(status: StatusCode, error: &'static str, key: Option<i64>) -> Response {
    (status, Json(ErrDoc { error, key })).into_response()
}

async fn unit(State(db): State<PgPool>, Path(id): Path<String>) -> Response {
    let Ok(id) = id.parse::<i64>() else {
        return fail(StatusCode::BAD_REQUEST, "bad key", None);
    };
    match sqlx::query_as::<_, Order>("SELECT id, total, label FROM bench_orders WHERE id = $1")
        .bind(id)
        .fetch_optional(&db)
        .await
    {
        Ok(Some(o)) => Json(o).into_response(),
        Ok(None) => fail(StatusCode::NOT_FOUND, "no such key", Some(id)),
        Err(e) => {
            eprintln!("unit {id}: {e}");
            fail(StatusCode::SERVICE_UNAVAILABLE, "unavailable", Some(id))
        }
    }
}

async fn list(State(db): State<PgPool>, Query(q): Query<HashMap<String, String>>) -> Response {
    let Some(from) = q.get("from").and_then(|v| v.parse::<i64>().ok()) else {
        return fail(StatusCode::BAD_REQUEST, "bad from", None);
    };
    let limit = match q.get("limit").and_then(|v| v.parse::<i64>().ok()) {
        Some(n) if n > 0 && n <= LIST_MAX => n,
        _ => return fail(StatusCode::BAD_REQUEST, "bad limit", Some(from)),
    };
    match sqlx::query_as::<_, Order>("SELECT id, total, label FROM bench_orders WHERE id > $1 ORDER BY id LIMIT $2")
        .bind(from)
        .bind(limit)
        .fetch_all(&db)
        .await
    {
        Ok(items) => Json(ListDoc { from, limit, items }).into_response(),
        Err(e) => {
            eprintln!("list {from}: {e}");
            fail(StatusCode::SERVICE_UNAVAILABLE, "unavailable", Some(from))
        }
    }
}

async fn write(State(db): State<PgPool>, Json(req): Json<WriteReq>) -> Response {
    if req.key == 0 {
        return fail(StatusCode::BAD_REQUEST, "bad body", None);
    }
    match sqlx::query("INSERT INTO comparison_writes (key, total) VALUES ($1, $2)")
        .bind(req.key)
        .bind(req.total)
        .execute(&db)
        .await
    {
        Ok(_) => Json(WriteDoc { key: req.key }).into_response(),
        Err(e) => {
            eprintln!("write {}: {e}", req.key);
            fail(StatusCode::SERVICE_UNAVAILABLE, "unavailable", Some(req.key))
        }
    }
}

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("rust-axum: {e}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let cfg = Config::from_env()?;
    cfg.offers(&["h1", "h2"], &["gzip"])?;
    let db = PgPoolOptions::new()
        .max_connections(cfg.pool)
        .connect_with(PgConnectOptions::new())
        .await
        .map_err(|e| e.to_string())?;
    for ddl in WRITE_TABLE {
        sqlx::query(ddl).execute(&db).await.map_err(|e| e.to_string())?;
    }

    let mut app = Router::new()
        .route("/static", get(|| async { Json(study_rust_axum::static_doc()) }))
        .route("/orders/{id}", get(unit))
        .route("/orders", get(list).post(write))
        .route("/study/metrics", get(|| async { Json(study_rust_axum::metrics()) }))
        .with_state(db);
    if cfg.compression == "gzip" {
        app = app.layer(CompressionLayer::new().no_br().no_deflate().no_zstd().quality(CompressionLevel::Fastest));
    }

    let addr: SocketAddr = cfg.listen.parse().map_err(|e| format!("STUDY_LISTEN={}: {e}", cfg.listen))?;
    eprintln!("rust-axum: {} on {} ({})", cfg.protocol, cfg.listen, cfg.summary());
    if !cfg.tls {
        let ln = tokio::net::TcpListener::bind(addr).await.map_err(|e| e.to_string())?;
        return axum::serve(ln, app).await.map_err(|e| e.to_string());
    }
    let tls = axum_server::tls_rustls::RustlsConfig::from_config(cfg.rustls()?);
    axum_server::bind_rustls(addr, tls)
        .serve(app.into_make_service())
        .await
        .map_err(|e| e.to_string())
}
