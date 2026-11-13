// What the Node contender's two versions share and what neither is measured
// on: the contract's environment (comparison/README.md, "The contract every
// contender honours"), the TLS every contender runs under, the static
// document, the write table and the metrics point. The contender's own
// code — routes, handlers, the driver — lives in standard/ and optimised/,
// where the study counts its lines (§1.1).

import { readFileSync } from 'node:fs';
import { constants } from 'node:crypto';
import { PerformanceObserver } from 'node:perf_hooks';

// readConfig reads the contract's variables. The database is not here:
// PGHOST and its siblings are libpq's, and node-postgres reads them itself.
export function readConfig(env = process.env) {
  const onOff = (name, def) => {
    const v = env[name] ?? '';
    if (v === '') return def;
    if (v === 'on') return true;
    if (v === 'off') return false;
    throw new Error(`${name}=${JSON.stringify(v)}: on or off`);
  };
  const c = {
    listen: env.STUDY_LISTEN || '127.0.0.1:8443',
    protocol: env.STUDY_PROTOCOL || 'h1',
    tls: onOff('STUDY_TLS', true),
    cert: env.STUDY_TLS_CERT || '',
    key: env.STUDY_TLS_KEY || '',
    resume: onOff('STUDY_TLS_RESUME', false),
    compression: env.STUDY_COMPRESSION || 'off',
    pool: 8,
  };
  if (env.STUDY_DB_POOL) {
    c.pool = Number(env.STUDY_DB_POOL);
    if (!Number.isInteger(c.pool) || c.pool <= 0) {
      throw new Error(`STUDY_DB_POOL=${JSON.stringify(env.STUDY_DB_POOL)}: a positive count`);
    }
  }
  if (!['h1', 'h2', 'h3'].includes(c.protocol)) {
    throw new Error(`STUDY_PROTOCOL=${JSON.stringify(c.protocol)}: h1, h2 or h3`);
  }
  if (!['off', 'gzip', 'deflate', 'br', 'zstd'].includes(c.compression)) {
    throw new Error(`STUDY_COMPRESSION=${JSON.stringify(c.compression)}: off, gzip, deflate, br or zstd`);
  }
  if (c.tls && (!c.cert || !c.key)) {
    // Node has no API to make a certificate at startup; the images carry a
    // local one made when they are built (Dockerfile), named by default.
    throw new Error('STUDY_TLS_CERT and STUDY_TLS_KEY name the certificate and its key');
  }
  const i = c.listen.lastIndexOf(':');
  c.host = c.listen.slice(0, i).replace(/^\[|\]$/g, '');
  c.port = Number(c.listen.slice(i + 1));
  return c;
}

// offers refuses what a version does not implement, before it listens: a
// configuration it cannot serve is published "not supported", never served
// as something else (study §2.4).
export function offers(c, protocols, codings) {
  if (!protocols.includes(c.protocol)) {
    throw new Error(`STUDY_PROTOCOL=${c.protocol}: not supported by this contender`);
  }
  if (c.compression !== 'off' && !codings.includes(c.compression)) {
    throw new Error(`STUDY_COMPRESSION=${c.compression}: not offered by this contender`);
  }
}

// tlsOptions is the TLS of every contender: TLS 1.3 only, the campaign's
// certificate, ALPN naming the run's one protocol, no session tickets unless
// asked. Without tickets a TLS 1.3 server could still resume through a
// session cache, which node:tls keeps only for a server that handles its
// newSession and resumeSession events; this one does not.
export function tlsOptions(c) {
  return {
    cert: readFileSync(c.cert),
    key: readFileSync(c.key),
    minVersion: 'TLSv1.3',
    ALPNProtocols: ['http/1.1'],
    secureOptions: c.resume ? 0 : constants.SSL_OP_NO_TICKET,
  };
}

// summary is the startup line's account of the transport, worded as the
// study's server words it.
export function summary(c) {
  if (!c.tls) return 'clear text, HTTP/1.1';
  return `TLS 1.3+, certificate ${c.cert}, ALPN http/1.1, resumption ${c.resume ? 'on' : 'off'}`;
}

// The write table, as the study's server creates it at startup: no read
// touches it, and autovacuum is off so that nothing fires mid-step.
export const writeTable = [
  `CREATE TABLE IF NOT EXISTS comparison_writes (
    id bigserial PRIMARY KEY, key bigint NOT NULL, total bigint NOT NULL,
    created timestamptz NOT NULL DEFAULT now())`,
  'ALTER TABLE comparison_writes SET (autovacuum_enabled = off)',
];

// listMax is the largest limit a list request may ask for, R's -list-max.
export const listMax = 100000;

// staticDoc is R's static document, value for value.
export const staticDoc = {
  status: 'ok',
  service: 'sluice-study',
  version: 'r',
  padding: Array.from({ length: 40 }, (_, i) => `line-${String(i).padStart(2, '0')}-abcdefghijklmnop`),
};

// The metrics point. V8 reports each collection to perf_hooks with its
// duration on the main thread: that is what a request waits for, so it
// stands for both the pauses and the collector's CPU, and the work V8's
// helper threads do concurrently is not counted — declared with the
// figures (§1.3). Buckets double from 10 µs.
const gc = { count: 0, seconds: 0, buckets: new Map() };
new PerformanceObserver((list) => {
  for (const e of list.getEntries()) {
    const s = e.duration / 1000;
    gc.count++;
    gc.seconds += s;
    let le = 1e-5;
    while (le < s) le *= 2;
    gc.buckets.set(le, (gc.buckets.get(le) || 0) + 1);
  }
}).observe({ entryTypes: ['gc'] });

// The DataLoader variant's batches: one per call of its batch function, by
// size in powers of two (1, 2, 4…), as the study's server counts its own.
const batches = { count: 0, keys: 0, sizes: new Map() };

// recordBatch counts one batch of n keys.
export function recordBatch(n) {
  batches.count++;
  batches.keys += n;
  let le = 1;
  while (le < n) le *= 2;
  batches.sizes.set(le, (batches.sizes.get(le) || 0) + 1);
}

// metricsPart is this process's share of the document: cumulative since it
// started.
export function metricsPart() {
  const u = process.cpuUsage();
  return {
    cpu: (u.user + u.system) / 1e6,
    gc: { count: gc.count, seconds: gc.seconds, buckets: [...gc.buckets] },
    batches: { count: batches.count, keys: batches.keys, sizes: [...batches.sizes] },
  };
}

// metricsDoc sums the parts of every process (one, or a cluster's workers)
// into the contract's document; batches is null unless the version groups.
export function metricsDoc(parts, grouped = false) {
  let cpu = 0;
  let count = 0;
  let seconds = 0;
  const buckets = new Map();
  for (const p of parts) {
    cpu += p.cpu;
    count += p.gc.count;
    seconds += p.gc.seconds;
    for (const [le, n] of p.gc.buckets) buckets.set(le, (buckets.get(le) || 0) + n);
  }
  return {
    runtime: `node ${process.version}`,
    cpu_seconds: cpu,
    gc: {
      cpu_seconds: seconds,
      pauses: {
        count,
        seconds,
        buckets: [...buckets].sort((a, b) => a[0] - b[0]).map(([le, n]) => ({ le, count: n })),
      },
    },
    batches: grouped ? sumBatches(parts) : null,
  };
}

function sumBatches(parts) {
  let count = 0;
  let keys = 0;
  const sizes = new Map();
  for (const p of parts) {
    count += p.batches.count;
    keys += p.batches.keys;
    for (const [le, n] of p.batches.sizes) sizes.set(le, (sizes.get(le) || 0) + n);
  }
  return { count, keys, sizes: [...sizes].sort((a, b) => a[0] - b[0]).map(([le, n]) => ({ le, count: n })) };
}
