// The study's Node contender after its optimisation budget (study §2.4,
// "Optimised"): the routes, documents and queries of the standard version,
// with each change listed, and its source, in comparison/README.md:
//   - a cluster of processes, one per CPU the process may use and at most
//     one per pooled connection (Express, "Production best practices:
//     performance and reliability", "Run your app in a cluster"); the pool's
//     total is the contract's, split between the workers (§2.6);
//   - NODE_ENV=production (the same page, "Set NODE_ENV to production");
//   - named queries, which node-postgres prepares once per connection
//     (node-postgres, "Queries", "Prepared Statements").
// The metrics point answers for the whole cluster: the worker that gets the
// request asks the primary, which collects every worker's share.

import cluster from 'node:cluster';
import os from 'node:os';
import * as contract from '../contract.js';

const cfg = contract.readConfig();
contract.offers(cfg, ['h1'], ['gzip']);
process.env.NODE_ENV = 'production';

if (cluster.isPrimary) {
  const workers = Math.max(1, Math.min(os.availableParallelism(), cfg.pool));
  const pending = new Map();
  for (let i = 0; i < workers; i++) {
    // The contract's total, split: the first workers take the remainder.
    const pool = Math.floor(cfg.pool / workers) + (i < cfg.pool % workers ? 1 : 0);
    cluster.fork({ STUDY_WORKER_POOL: String(pool) });
  }
  cluster.on('message', (worker, m) => {
    if (m.t === 'ask') {
      const ids = Object.values(cluster.workers);
      pending.set(m.id, { from: worker, parts: [contract.metricsPart()], left: ids.length });
      for (const w of ids) w.send({ t: 'collect', id: m.id });
    } else if (m.t === 'part') {
      const p = pending.get(m.id);
      if (!p) return;
      p.parts.push(m.part);
      if (--p.left === 0) {
        pending.delete(m.id);
        p.from.send({ t: 'answer', id: m.id, doc: contract.metricsDoc(p.parts) });
      }
    }
  });
  cluster.on('exit', (w, code) => {
    console.error(`node-express-optimised: worker ${w.process.pid} exited (${code})`);
    process.exit(1);
  });
  console.error(`node-express-optimised: ${cfg.protocol} on ${cfg.listen} (${contract.summary(cfg)}), ${workers} workers`);
} else {
  await serve(Number(process.env.STUDY_WORKER_POOL));
}

async function serve(poolSize) {
  const { default: express } = await import('express');
  const { default: compression } = await import('compression');
  const { default: pg } = await import('pg');
  const https = await import('node:https');
  const http = await import('node:http');

  const pool = new pg.Pool({ max: poolSize });
  if (cluster.worker.id === 1) {
    for (const ddl of contract.writeTable) await pool.query(ddl);
  }

  const asked = new Map();
  let next = 0;
  process.on('message', (m) => {
    if (m.t === 'collect') {
      process.send({ t: 'part', id: m.id, part: contract.metricsPart() });
    } else if (m.t === 'answer') {
      asked.get(m.id)?.(m.doc);
      asked.delete(m.id);
    }
  });

  const unitQuery = { name: 'unit', text: 'SELECT id, total, label FROM bench_orders WHERE id = $1' };
  const listQuery = { name: 'list', text: 'SELECT id, total, label FROM bench_orders WHERE id > $1 ORDER BY id LIMIT $2' };
  const writeQuery = { name: 'write', text: 'INSERT INTO comparison_writes (key, total) VALUES ($1, $2)' };

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
      const { rows } = await pool.query({ ...unitQuery, values: [id] });
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
      const { rows } = await pool.query({ ...listQuery, values: [from, limit] });
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
      await pool.query({ ...writeQuery, values: [key, total] });
      res.json({ key });
    } catch (err) {
      console.error(`write ${key}: ${err.message}`);
      res.status(503).json({ error: 'unavailable', key });
    }
  });

  app.get('/study/metrics', (req, res) => {
    const id = `${process.pid}-${next++}`;
    asked.set(id, (doc) => res.json(doc));
    process.send({ t: 'ask', id });
  });

  function order(r) {
    return { id: Number(r.id), total: Number(r.total), label: r.label };
  }

  const server = cfg.tls ? https.createServer(contract.tlsOptions(cfg), app) : http.createServer(app);
  server.listen(cfg.port, cfg.host);
}
