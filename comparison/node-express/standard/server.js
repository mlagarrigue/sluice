// The study's Node contender as the documentation of Express 5 and
// node-postgres teaches it (study §2.4, "Standard"): express(),
// express.json(), res.json, a pg.Pool with one query per request, no
// grouping, one process. Compression is Express's documented middleware,
// `compression`, at gzip level 1 when the contract asks for gzip.
//
// HTTP/1.1 only: Express does not run on node:http2, and Node serves no
// HTTP/3; h2 and h3 are refused and published "not supported".

import express from 'express';
import compression from 'compression';
import https from 'node:https';
import http from 'node:http';
import pg from 'pg';
import * as contract from '../contract.js';

const cfg = contract.readConfig();
contract.offers(cfg, ['h1'], ['gzip']);

const pool = new pg.Pool({ max: cfg.pool });
for (const ddl of contract.writeTable) await pool.query(ddl);

const app = express();
if (cfg.compression === 'gzip') app.use(compression({ level: 1 }));
app.use(express.json());

app.get('/static', (req, res) => {
  res.json(contract.staticDoc);
});

app.get('/orders/:id', async (req, res) => {
  const id = contract.parseInt64(req.params.id);
  if (id === null) {
    return res.status(400).json({ error: 'bad key' });
  }
  try {
    const { rows } = await pool.query('SELECT id, total, label FROM bench_orders WHERE id = $1', [id]);
    if (rows.length === 0) return res.status(404).json({ error: 'no such key', key: id });
    res.json(order(rows[0]));
  } catch (err) {
    console.error(`unit ${id}: ${err.message}`);
    res.status(503).json({ error: 'unavailable', key: id });
  }
});

app.get('/orders', async (req, res) => {
  const from = contract.parseInt64(req.query.from);
  if (from === null) {
    return res.status(400).json({ error: 'bad from' });
  }
  const limit = contract.parseInt64(req.query.limit) ?? 0;
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
  if (!contract.inInt64(key) || key === 0 || !contract.inInt64(total)) {
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
  res.json(contract.metricsDoc([contract.metricsPart()]));
});

// node-postgres returns a bigint column as a string, which JSON would write
// quoted: the contract's numbers are numbers, and every id and total of the
// study fits a double exactly.
function order(r) {
  return { id: Number(r.id), total: Number(r.total), label: r.label };
}

const server = cfg.tls ? https.createServer(contract.tlsOptions(cfg), app) : http.createServer(app);
server.listen(cfg.port, cfg.host, () => {
  console.error(`node-express-standard: ${cfg.protocol} on ${cfg.listen} (${contract.summary(cfg)})`);
});
