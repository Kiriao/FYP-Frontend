// probe_recs.js
// Usage: node probe_recs.js "your query" [limit]
// Env: DATABASE_URL, APP_API_BASE (or APP_ORIGIN), EMBED_URL
// Optional: ALLOW_INSECURE_TLS=1 (only for testing self-signed proxies)

const { Client } = require('pg');
const { fetch } = require('undici');

async function getEmbedding(text) {
  const EMBED_URL =
    process.env.EMBED_URL ||
    `${(process.env.APP_API_BASE || process.env.APP_ORIGIN || '').replace(/\/+$/, '')}/embed`;

  const r = await fetch(EMBED_URL, {
    method: 'POST',
    headers: { 'content-type': 'application/json', accept: 'application/json' },
    body: JSON.stringify({ text }),
  });
  if (!r.ok) {
    const body = await r.text().catch(() => '');
    throw new Error(`Embed HTTP ${r.status}: ${body || r.statusText}`);
  }
  const data = await r.json();
  if (!data || !Array.isArray(data.vector)) {
    throw new Error('Embed response missing "vector" array');
  }
  // Ensure numbers and build pgvector literal: [v1,v2,...]
  const nums = data.vector.map(Number);
  if (nums.some((n) => Number.isNaN(n))) {
    throw new Error('Embedding contains non-numeric values');
  }
  if (nums.length !== 1536) {
    console.warn(`⚠️ Embedding length is ${nums.length}, expected 1536. Continuing anyway.`);
  }
  return `[${nums.join(',')}]`;
}

async function main() {
  const query = process.argv[2] || 'picture books for toddlers';
  const limit = Math.max(1, Math.min(50, Number(process.argv[3]) || 10));

  if (!process.env.DATABASE_URL) {
    throw new Error('Missing DATABASE_URL env');
  }

  // (Optional) allow insecure TLS for corp proxies (testing only)
  if (process.env.ALLOW_INSECURE_TLS) {
    process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';
  }

  console.log(`Query: ${query}`);
  const vecLiteral = await getEmbedding(query);

  const client = new Client({ connectionString: process.env.DATABASE_URL });
  await client.connect();

  const sql = `
    SELECT id, title, type, tags, link, thumb,
           (embedding <=> $1)::float AS dist
    FROM items
    ORDER BY embedding <=> $1
    LIMIT $2
  `;
  const { rows } = await client.query(sql, [vecLiteral, limit]);

  await client.end();

  // Pretty print
  const out = rows.map((r, i) => ({
    '#': i + 1,
    id: r.id,
    type: r.type,
    dist: Number(r.dist.toFixed(6)),
    title: r.title?.slice(0, 80) || '',
  }));
  console.table(out);

  console.log('\nTip: set LIMIT with a 2nd arg, e.g.:\n  node probe_recs.js "space adventures for 8 year olds" 15');
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
