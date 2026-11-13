//! What the Rust contender's server shares with every contender and is not
//! measured on: the contract's environment (comparison/README.md, "The
//! contract every contender honours"), the TLS every contender runs under,
//! the static document, the write table and the metrics point. The
//! contender's own code — routes, handlers, the driver — is src/main.rs,
//! where the study counts its lines (§1.1).

use std::sync::Arc;

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use serde::Serialize;

/// The contract's environment, read once at startup. The database is not
/// here: PGHOST and its siblings are libpq's, and sqlx reads them itself.
#[derive(Debug, Clone)]
pub struct Config {
    pub listen: String,
    pub protocol: String,
    pub tls: bool,
    pub cert: String,
    pub key: String,
    pub resume: bool,
    pub compression: String,
    pub pool: u32,
}

impl Config {
    /// Reads STUDY_* from the environment, with the study server's defaults.
    pub fn from_env() -> Result<Self, String> {
        let var = |name: &str| std::env::var(name).unwrap_or_default();
        let on_off = |name: &str, def: bool| match var(name).as_str() {
            "" => Ok(def),
            "on" => Ok(true),
            "off" => Ok(false),
            v => Err(format!("{name}={v:?}: on or off")),
        };
        let or = |v: String, def: &str| if v.is_empty() { def.to_string() } else { v };
        let c = Config {
            listen: or(var("STUDY_LISTEN"), "127.0.0.1:8443"),
            protocol: or(var("STUDY_PROTOCOL"), "h1"),
            tls: on_off("STUDY_TLS", true)?,
            cert: var("STUDY_TLS_CERT"),
            key: var("STUDY_TLS_KEY"),
            resume: on_off("STUDY_TLS_RESUME", false)?,
            compression: or(var("STUDY_COMPRESSION"), "off"),
            pool: match var("STUDY_DB_POOL").as_str() {
                "" => 8,
                v => v
                    .parse()
                    .ok()
                    .filter(|n| *n > 0)
                    .ok_or(format!("STUDY_DB_POOL={v:?}: a positive count"))?,
            },
        };
        if !["h1", "h2", "h3"].contains(&c.protocol.as_str()) {
            return Err(format!("STUDY_PROTOCOL={:?}: h1, h2 or h3", c.protocol));
        }
        if !["off", "gzip", "deflate", "br", "zstd"].contains(&c.compression.as_str()) {
            return Err(format!("STUDY_COMPRESSION={:?}: off, gzip, deflate, br or zstd", c.compression));
        }
        if !c.tls && c.protocol != "h1" {
            return Err(format!("STUDY_TLS=off with STUDY_PROTOCOL={}: in clear the study runs HTTP/1.1 only", c.protocol));
        }
        if c.tls && (c.cert.is_empty() || c.key.is_empty()) {
            // The images carry a certificate for a local run, made when they
            // are built and named by default (Dockerfile).
            return Err("STUDY_TLS_CERT and STUDY_TLS_KEY name the certificate and its key".into());
        }
        Ok(c)
    }

    /// Refuses what the contender does not implement, before it listens: a
    /// configuration it cannot serve is published "not supported" (§2.4).
    pub fn offers(&self, protocols: &[&str], codings: &[&str]) -> Result<(), String> {
        if !protocols.contains(&self.protocol.as_str()) {
            return Err(format!("STUDY_PROTOCOL={}: not supported by this contender", self.protocol));
        }
        if self.compression != "off" && !codings.contains(&self.compression.as_str()) {
            return Err(format!("STUDY_COMPRESSION={}: not offered by this contender", self.compression));
        }
        Ok(())
    }

    /// The run's one protocol identifier.
    pub fn alpn(&self) -> &'static str {
        match self.protocol.as_str() {
            "h2" => "h2",
            "h3" => "h3",
            _ => "http/1.1",
        }
    }

    /// The TLS of every contender: TLS 1.3 only, the campaign's certificate,
    /// ALPN naming the run's one protocol, no session tickets unless asked
    /// (rustls resumes TLS 1.3 only through the tickets it sends).
    pub fn rustls(&self) -> Result<Arc<rustls::ServerConfig>, String> {
        let certs = CertificateDer::pem_file_iter(&self.cert)
            .and_then(|it| it.collect::<Result<Vec<_>, _>>())
            .map_err(|e| format!("{}: {e}", self.cert))?;
        let key = PrivateKeyDer::from_pem_file(&self.key).map_err(|e| format!("{}: {e}", self.key))?;
        let mut cfg = rustls::ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| e.to_string())?;
        cfg.alpn_protocols = vec![self.alpn().as_bytes().to_vec()];
        if !self.resume {
            cfg.send_tls13_tickets = 0;
        }
        Ok(Arc::new(cfg))
    }

    /// The startup line's account of the transport.
    pub fn summary(&self) -> String {
        if !self.tls {
            return "clear text, HTTP/1.1".into();
        }
        format!(
            "TLS 1.3+, certificate {}, ALPN {}, resumption {}",
            self.cert,
            self.alpn(),
            if self.resume { "on" } else { "off" }
        )
    }
}

/// The write table, as the study's server creates it at startup: no read
/// touches it, and autovacuum is off so that nothing fires mid-step.
pub const WRITE_TABLE: [&str; 2] = [
    "CREATE TABLE IF NOT EXISTS comparison_writes (
        id bigserial PRIMARY KEY, key bigint NOT NULL, total bigint NOT NULL,
        created timestamptz NOT NULL DEFAULT now())",
    "ALTER TABLE comparison_writes SET (autovacuum_enabled = off)",
];

/// The largest limit a list request may ask for, R's -list-max.
pub const LIST_MAX: i64 = 100_000;

/// R's static document, value for value.
#[derive(Serialize)]
pub struct Static {
    status: &'static str,
    service: &'static str,
    version: &'static str,
    padding: Vec<String>,
}

pub fn static_doc() -> Static {
    Static {
        status: "ok",
        service: "sluice-study",
        version: "r",
        padding: (0..40).map(|i| format!("line-{i:02}-abcdefghijklmnop")).collect(),
    }
}

/// The metrics point. Rust has no collector: gc is null, and batches is
/// null as this contender does not group. cpu_seconds is the process's
/// user and system time from /proc/self/stat, in clock ticks of 1/100 s
/// (USER_HZ, 100 on every Linux the study runs).
#[derive(Serialize)]
pub struct Metrics {
    runtime: &'static str,
    cpu_seconds: f64,
    gc: Option<()>,
    batches: Option<()>,
}

pub fn metrics() -> Metrics {
    let cpu = std::fs::read_to_string("/proc/self/stat")
        .ok()
        .and_then(|s| {
            // The command name, field 2, may hold spaces: count from the
            // closing parenthesis. utime and stime are fields 14 and 15.
            let rest = &s[s.rfind(')')? + 2..];
            let f: Vec<&str> = rest.split(' ').collect();
            Some((f.get(11)?.parse::<f64>().ok()? + f.get(12)?.parse::<f64>().ok()?) / 100.0)
        })
        .unwrap_or(0.0);
    Metrics { runtime: "rust", cpu_seconds: cpu, gc: None, batches: None }
}
