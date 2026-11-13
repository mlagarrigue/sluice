// The standard version with grouping (study §2.1, "with"): the unit reads go
// through a DataLoader (the `dataloader` package), which gathers the keys
// loaded in one tick of the event loop and fetches them in one `= ANY($1)`
// query. Everything else is the standard version, line for line.
//
// One deviation from DataLoader's documentation, declared (§2.1): its
// README builds a loader per request, which with one key per HTTP request
// groups nothing; this one is shared by the process, and its memoisation
// cache is off (`cache: false`) — every key differs, so the cache would never
// serve and would only grow. The loader's settings are its defaults: a batch
// is dispatched on process.nextTick, which on HTTP/1.1 — one socket callback
// per request — leaves batches of about one key. That is the default's
// result, published with the batch size observed, not something to correct
// here. Every batch is counted in the metrics point's "batches".

import express from 'express';
import compression from 'compression';
import DataLoader from 'dataloader';
import https from 'node:https';
import http from 'node:http';
import pg from 'pg';
import * as contract from '../contract.js';

const cfg = contract.readConfig();
contract.offers(cfg, ['h1'], ['gzip']);

const pool = new pg.Pool({ max: cfg.pool });
for (const ddl of contract.writeTable) await pool.query(ddl);

// The batch function: the rows of a batch's keys, one per key and in their
// order, null for a key without a row.
const orders = new DataLoader(async (ids) => {
  contract.recordBatch(ids.length);
  const { rows } = await pool.query('SELECT id, total, label FROM bench_orders WHERE id = ANY($1)', [ids]);
  const byId = new Map(rows.map((r) => [Number(r.id), order(r)]));
  return ids.map((id) => byId.get(id) ?? null);
}, { cache: false });

const app = express();
if (cfg.compression === 'gzip') app.use(compression({ level: 1 }));
app.use(express.json());

app.get('/static', (req, res) => {
  res.json(contract.staticDoc);
});

app.get('/orders/:id', async (req, res) => {
  if (!/^-?\d+$/.test(req.params.id)) {
    return res.status(400).json({ error: 'bad key' });
  }
  const id = Number(req.params.id);
  try {
    const o = await orders.load(id);
    if (o === null) return res.status(404).json({ error: 'no such key', key: id });
    res.json(o);
  } catch (err) {
    console.error(`unit ${id}: ${err.message}`);
    res.status(503).json({ error: 'unavailable', key: id });
  }
});

app.get('/orders', async (req, res) => {
  if (!/^-?\d+$/.test(req.query.from ?? '')) {
    return res.status(400).json({ error: 'bad from' });
  }
  const from = Number(req.query.from);
  const limit = Number(req.query.limit);
  if (!Number.isInteger(limit) || limit <= 0 || limit > contract.listMax) {
    return res.status(400).json({ error: 'bad limit', key: from });
  }
  try {
    const { rows } = await pool.query(
      'SELECT id, total, label FROM bench_orders WHERE id > $1 ORDER BY id LIMIT $2', [from, limit]);
    res.json({ from, limit, items: rows.map(order) });
  } catch (err) {
    console.error(`list ${from}: ${err.message}`);
    res.status(503).json({ error: 'unavailable', key: from });
  }
});

app.post('/orders', async (req, res) => {
  const key = req.body?.key;
  const total = req.body?.total;
  if (!Number.isInteger(key) || key === 0 || !Number.isInteger(total)) {
    return res.status(400).json({ error: 'bad body' });
  }
  try {
    await pool.query('INSERT INTO comparison_writes (key, total) VALUES ($1, $2)', [key, total]);
    res.json({ key });
  } catch (err) {
    console.error(`write ${key}: ${err.message}`);
    res.status(503).json({ error: 'unavailable', key });
  }
});

app.get('/study/metrics', (req, res) => {
  res.json(contract.metricsDoc([contract.metricsPart()], true));
});

function order(r) {
  return { id: Number(r.id), total: Number(r.total), label: r.label };
}

const server = cfg.tls ? https.createServer(contract.tlsOptions(cfg), app) : http.createServer(app);
server.listen(cfg.port, cfg.host, () => {
  console.error(`node-express-dataloader: ${cfg.protocol} on ${cfg.listen} (${contract.summary(cfg)})`);
});
