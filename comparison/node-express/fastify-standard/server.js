// The study's Fastify contender as Fastify's "Getting Started" writes it
// (study §2.4, "Standard"; level 2, §2.5): fastify({ logger: true }), routes
// returning their documents, a pg.Pool with one query per request, no
// grouping, one process. With logger: true Fastify writes two JSON lines per
// request to stdout (received, completed): the guide's access log, published
// with the figures (§4, declared logs). Compression is @fastify/compress,
// gzip only, at the contract's level 1.
//
// HTTP/1.1, and HTTP/2 over TLS as Fastify's "HTTP2" page writes it
// (http2: true, https); HTTP/3 is refused and published "not supported".

import Fastify from 'fastify';
import compress from '@fastify/compress';
import { constants as zlib } from 'node:zlib';
import pg from 'pg';
import * as contract from '../contract.js';

const cfg = contract.readConfig();
contract.offers(cfg, ['h1', 'h2'], ['gzip']);

const pool = new pg.Pool({ max: cfg.pool });
for (const ddl of contract.writeTable) await pool.query(ddl);

const fastify = Fastify({
  logger: true,
  ...(cfg.tls ? { https: contract.tlsOptions(cfg) } : {}),
  ...(cfg.protocol === 'h2' ? { http2: true } : {}),
});
if (cfg.compression === 'gzip') {
  await fastify.register(compress, { encodings: ['gzip'], zlibOptions: { level: zlib.Z_BEST_SPEED } });
}

fastify.get('/static', async () => contract.staticDoc);

fastify.get('/orders/:id', async (request, reply) => {
  const id = contract.parseInt64(request.params.id);
  if (id === null) {
    return reply.code(400).send({ error: 'bad key' });
  }
  try {
    const { rows } = await pool.query('SELECT id, total, label FROM bench_orders WHERE id = $1', [id]);
    if (rows.length === 0) return reply.code(404).send({ error: 'no such key', key: id });
    return order(rows[0]);
  } catch (err) {
    request.log.error(`unit ${id}: ${err.message}`);
    return reply.code(503).send({ error: 'unavailable', key: id });
  }
});

fastify.get('/orders', async (request, reply) => {
  const from = contract.parseInt64(request.query.from);
  if (from === null) {
    return reply.code(400).send({ error: 'bad from' });
  }
  const limit = contract.parseInt64(request.query.limit) ?? 0;
  if (!Number.isInteger(limit) || limit <= 0 || limit > contract.listMax) {
    return reply.code(400).send({ error: 'bad limit', key: from });
  }
  try {
    const { rows } = await pool.query(
      'SELECT id, total, label FROM bench_orders WHERE id > $1 ORDER BY id LIMIT $2', [from, limit]);
    return { from, limit, items: rows.map(order) };
  } catch (err) {
    request.log.error(`list ${from}: ${err.message}`);
    return reply.code(503).send({ error: 'unavailable', key: from });
  }
});

fastify.post('/orders', async (request, reply) => {
  const key = request.body?.key;
  const total = request.body?.total;
  if (!contract.inInt64(key) || key === 0 || !contract.inInt64(total)) {
    return reply.code(400).send({ error: 'bad body' });
  }
  try {
    await pool.query('INSERT INTO comparison_writes (key, total) VALUES ($1, $2)', [key, total]);
    return { key };
  } catch (err) {
    request.log.error(`write ${key}: ${err.message}`);
    return reply.code(503).send({ error: 'unavailable', key });
  }
});

fastify.get('/study/metrics', async () => contract.metricsDoc([contract.metricsPart()]));

// node-postgres returns a bigint column as a string; the contract's numbers
// are numbers, and every id and total of the study fits a double exactly.
function order(r) {
  return { id: Number(r.id), total: Number(r.total), label: r.label };
}

await fastify.listen({ port: cfg.port, host: cfg.host });
console.error(`node-fastify-standard: ${cfg.protocol} on ${cfg.listen} (${contract.summary(cfg)})`);
