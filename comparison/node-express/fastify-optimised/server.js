// The study's Fastify contender after its optimisation budget (study §2.4,
// "Optimised"): the routes, documents, queries and protocols of
// fastify-standard, with each change listed, and its source, in
// comparison/README.md:
//   - response schemas, which Fastify compiles into a serializer
//     (Getting Started, "Serialize your data"; Recommendations, "Common
//     Causes Of Performance Degradation");
//   - no logger: Fastify's default, "for performance reasons" (Logging);
//     errors still go to stderr;
//   - several instances in the container, one per two usable CPUs, and at
//     most one per pooled connection (Recommendations, "Capacity Planning
//     For Production": 2 vCPU per instance for the lowest latency), by
//     node:cluster; the pool's total is the contract's, split (§2.6);
//   - named queries, which node-postgres prepares once per connection
//     (node-postgres, "Queries", "Prepared Statements").
// The metrics point answers for the whole cluster, as in optimised/.

import cluster from 'node:cluster';
import os from 'node:os';
import * as contract from '../contract.js';

const cfg = contract.readConfig();
contract.offers(cfg, ['h1', 'h2'], ['gzip']);

if (cluster.isPrimary) {
  const workers = Math.max(1, Math.min(Math.floor(os.availableParallelism() / 2), cfg.pool));
  const pending = new Map();
  for (let i = 0; i < workers; i++) {
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
    console.error(`node-fastify-optimised: worker ${w.process.pid} exited (${code})`);
    process.exit(1);
  });
  console.error(`node-fastify-optimised: ${cfg.protocol} on ${cfg.listen} (${contract.summary(cfg)}), ${workers} workers`);
} else {
  await serve(Number(process.env.STUDY_WORKER_POOL));
}

async function serve(poolSize) {
  const { default: Fastify } = await import('fastify');
  const { default: compress } = await import('@fastify/compress');
  const { constants: zlib } = await import('node:zlib');
  const { default: pg } = await import('pg');

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

  // The documents' schemas: what the serializer is compiled from.
  const err = {
    type: 'object',
    properties: { error: { type: 'string' }, key: { type: 'integer' } },
  };
  const orderSchema = {
    type: 'object',
    properties: { id: { type: 'integer' }, total: { type: 'integer' }, label: { type: 'string' } },
  };
  const schemas = {
    static: {
      response: {
        200: {
          type: 'object',
          properties: {
            status: { type: 'string' },
            service: { type: 'string' },
            version: { type: 'string' },
            padding: { type: 'array', items: { type: 'string' } },
          },
        },
      },
    },
    unit: { response: { 200: orderSchema, '4xx': err, '5xx': err } },
    list: {
      response: {
        200: {
          type: 'object',
          properties: {
            from: { type: 'integer' },
            limit: { type: 'integer' },
            items: { type: 'array', items: orderSchema },
          },
        },
        '4xx': err,
        '5xx': err,
      },
    },
    write: {
      response: { 200: { type: 'object', properties: { key: { type: 'integer' } } }, '4xx': err, '5xx': err },
    },
  };

  const fastify = Fastify({
    ...(cfg.tls ? { https: contract.tlsOptions(cfg) } : {}),
    ...(cfg.protocol === 'h2' ? { http2: true } : {}),
  });
  if (cfg.compression === 'gzip') {
    await fastify.register(compress, { encodings: ['gzip'], zlibOptions: { level: zlib.Z_BEST_SPEED } });
  }

  fastify.get('/static', { schema: schemas.static }, async () => contract.staticDoc);

  fastify.get('/orders/:id', { schema: schemas.unit }, async (request, reply) => {
    if (!/^-?\d+$/.test(request.params.id)) {
      return reply.code(400).send({ error: 'bad key' });
    }
    const id = Number(request.params.id);
    try {
      const { rows } = await pool.query({ ...unitQuery, values: [id] });
      if (rows.length === 0) return reply.code(404).send({ error: 'no such key', key: id });
      return order(rows[0]);
    } catch (e) {
      console.error(`unit ${id}: ${e.message}`);
      return reply.code(503).send({ error: 'unavailable', key: id });
    }
  });

  fastify.get('/orders', { schema: schemas.list }, async (request, reply) => {
    if (!/^-?\d+$/.test(request.query.from ?? '')) {
      return reply.code(400).send({ error: 'bad from' });
    }
    const from = Number(request.query.from);
    const limit = Number(request.query.limit);
    if (!Number.isInteger(limit) || limit <= 0 || limit > contract.listMax) {
      return reply.code(400).send({ error: 'bad limit', key: from });
    }
    try {
      const { rows } = await pool.query({ ...listQuery, values: [from, limit] });
      return { from, limit, items: rows.map(order) };
    } catch (e) {
      console.error(`list ${from}: ${e.message}`);
      return reply.code(503).send({ error: 'unavailable', key: from });
    }
  });

  fastify.post('/orders', { schema: schemas.write }, async (request, reply) => {
    const key = request.body?.key;
    const total = request.body?.total;
    if (!Number.isInteger(key) || key === 0 || !Number.isInteger(total)) {
      return reply.code(400).send({ error: 'bad body' });
    }
    try {
      await pool.query({ ...writeQuery, values: [key, total] });
      return { key };
    } catch (e) {
      console.error(`write ${key}: ${e.message}`);
      return reply.code(503).send({ error: 'unavailable', key });
    }
  });

  fastify.get('/study/metrics', (request, reply) => {
    const id = `${process.pid}-${next++}`;
    asked.set(id, (doc) => reply.send(doc));
    process.send({ t: 'ask', id });
  });

  function order(r) {
    return { id: Number(r.id), total: Number(r.total), label: r.label };
  }

  await fastify.listen({ port: cfg.port, host: cfg.host });
}
