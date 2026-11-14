//! The Rust contender with grouping (study §2.1, "with"): src/main.rs with
//! its unit reads through async-graphql's DataLoader, used on its own,
//! without GraphQL (§2.5: it takes a batch function and a Tokio spawner).
//! The loader is shared by the process and has no cache (`DataLoader::new`,
//! the constructor without one); its settings are its defaults — a batch
//! leaves 1 ms after its first key, at most 1 000 keys. One `= ANY($1)`
//! query per batch, each counted in the metrics point's "batches".
//!
//! One deviation from the documentation, declared (§2.1): async-graphql
//! builds its loaders per GraphQL request, as data for the schema's context;
//! this one is shared by the process, so that keys of different HTTP
//! requests can meet in one batch.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use async_graphql::dataloader::{DataLoader, Loader};
use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use tower_http::compression::{CompressionLayer, CompressionLevel};

use study_rust_axum::{Config, LIST_MAX, WRITE_TABLE};

#[derive(Clone, Serialize, sqlx::FromRow)]
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

/// The batch function: the rows of a batch's keys; a key without a row is
/// absent from the map, which the loader answers as None.
struct Orders(PgPool);

impl Loader<i64> for Orders {
    type Value = Order;
    type Error = Arc<sqlx::Error>;

    async fn load(&self, keys: &[i64]) -> Result<HashMap<i64, Order>, Self::Error> {
        study_rust_axum::record_batch(keys.len());
        let rows = sqlx::query_as::<_, Order>("SELECT id, total, label FROM bench_orders WHERE id = ANY($1)")
            .bind(keys)
            .fetch_all(&self.0)
            .await
            .map_err(Arc::new)?;
        Ok(rows.into_iter().map(|o| (o.id, o)).collect())
    }
}

#[derive(Clone)]
struct App {
    db: PgPool,
    orders: Arc<DataLoader<Orders>>,
}

async fn unit(State(app): State<App>, Path(id): Path<String>) -> Response {
    let Ok(id) = id.parse::<i64>() else {
        return fail(StatusCode::BAD_REQUEST, "bad key", None);
    };
    match app.orders.load_one(id).await {
        Ok(Some(o)) => Json(o).into_response(),
        Ok(None) => fail(StatusCode::NOT_FOUND, "no such key", Some(id)),
        Err(e) => {
            eprintln!("unit {id}: {e}");
            fail(StatusCode::SERVICE_UNAVAILABLE, "unavailable", Some(id))
        }
    }
}

async fn list(State(app): State<App>, Query(q): Query<HashMap<String, String>>) -> Response {
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
        .fetch_all(&app.db)
        .await
    {
        Ok(items) => Json(ListDoc { from, limit, items }).into_response(),
        Err(e) => {
            eprintln!("list {from}: {e}");
            fail(StatusCode::SERVICE_UNAVAILABLE, "unavailable", Some(from))
        }
    }
}

async fn write(State(app): State<App>, body: Result<Json<WriteReq>, JsonRejection>) -> Response {
    // axum answers 422 to a body it cannot deserialize, a number past i64
    // among them; R answers 400 with its document.
    let Ok(Json(req)) = body else {
        return fail(StatusCode::BAD_REQUEST, "bad body", None);
    };
    if req.key == 0 {
        return fail(StatusCode::BAD_REQUEST, "bad body", None);
    }
    match sqlx::query("INSERT INTO comparison_writes (key, total) VALUES ($1, $2)")
        .bind(req.key)
        .bind(req.total)
        .execute(&app.db)
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
        eprintln!("rust-axum-dataloader: {e}");
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

    let orders = Arc::new(DataLoader::new(Orders(db.clone()), tokio::spawn));
    let mut app = Router::new()
        .route("/static", get(|| async { Json(study_rust_axum::static_doc()) }))
        .route("/orders/{id}", get(unit))
        .route("/orders", get(list).post(write))
        .route("/study/metrics", get(|| async { Json(study_rust_axum::metrics_grouped()) }))
        .with_state(App { db, orders });
    if cfg.compression == "gzip" {
        app = app.layer(CompressionLayer::new().no_br().no_deflate().no_zstd().quality(CompressionLevel::Fastest));
    }

    let addr: SocketAddr = cfg.listen.parse().map_err(|e| format!("STUDY_LISTEN={}: {e}", cfg.listen))?;
    eprintln!("rust-axum-dataloader: {} on {} ({})", cfg.protocol, cfg.listen, cfg.summary());
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
