import { getPool } from "./lib/db";

type ItemUpsert = {
  id: string;
  type: "book" | "video";
  title: string;
  description?: string;
  authors?: string[];
  tags?: string[];
  age_min?: number | null;
  age_max?: number | null;
  thumb?: string | null;   // NEW (optional)
  link?: string | null;    // NEW (optional)
};

const OPENAI = process.env.OPENAI_API_KEY!;
const MODEL = process.env.EMBEDDING_MODEL || "text-embedding-3-small";

// Embedding with retries + error detail
async function embed(texts: string[]): Promise<number[][]> {
  const body = JSON.stringify({ model: MODEL, input: texts });
  let lastDetail = "";
  for (let attempt = 0; attempt < 5; attempt++) {
    const r = await fetch("https://api.openai.com/v1/embeddings", {
      method: "POST",
      headers: { "content-type": "application/json", authorization: `Bearer ${OPENAI}` },
      body,
    });

    if (r.ok) {
      const j = (await r.json()) as { data: Array<{ embedding: number[] }> };
      return j.data.map((d) => d.embedding);
    }

    try {
      const e = await r.json();
      lastDetail = e?.error?.message || JSON.stringify(e) || lastDetail;
    } catch {}

    if (r.status === 429) {
      const backoff = Math.min(2000, 200 * Math.pow(2, attempt)) + Math.floor(Math.random() * 200);
      console.warn(`OpenAI 429, retrying in ${backoff}ms… last: ${lastDetail}`);
      await new Promise((res) => setTimeout(res, backoff));
      continue;
    }
    throw new Error(`OpenAI HTTP ${r.status}${lastDetail ? `: ${lastDetail}` : ""}`);
  }
  throw new Error(`OpenAI 429: exhausted retries${lastDetail ? `; last: ${lastDetail}` : ""}`);
}

export async function upsertItems(items: ItemUpsert[]) {
  if (!items?.length) return;

  const pool = await getPool();

  const texts = items.map(
    (i) =>
      `${i.title}. ${(i.authors || []).join(", ")}. ${i.description || ""}. ` +
      `tags:${(i.tags || []).join(",")}. type:${i.type}`
  );

  const CHUNK = 50;
  for (let start = 0; start < texts.length; start += CHUNK) {
    const sliceItems: ItemUpsert[] = items.slice(start, start + CHUNK);
    const sliceTexts = texts.slice(start, start + CHUNK);

    const vectors = await embed(sliceTexts);

    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      for (let i = 0; i < sliceItems.length; i++) {
        const it: ItemUpsert = sliceItems[i];       // <-- typed (fixes TS7006)
        const v: number[] = vectors[i];

        await client.query(
          `INSERT INTO items (id,type,title,description,authors,tags,age_min,age_max,thumb,link,embedding)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::float8[]::vector)
           ON CONFLICT (id) DO UPDATE
           SET title=EXCLUDED.title,
               description=EXCLUDED.description,
               authors=EXCLUDED.authors,
               tags=EXCLUDED.tags,
               age_min=EXCLUDED.age_min,
               age_max=EXCLUDED.age_max,
               thumb=EXCLUDED.thumb,
               link=EXCLUDED.link,
               embedding=EXCLUDED.embedding`,
          [
            it.id,
            it.type,
            it.title,
            it.description ?? "",
            it.authors ?? null,
            it.tags ?? null,
            it.age_min ?? null,
            it.age_max ?? null,
            it.thumb ?? null,               // NEW
            it.link ?? null,                // NEW
            v,
          ]
        );
      }
      await client.query("COMMIT");
    } catch (e) {
      await client.query("ROLLBACK");
      throw e;
    } finally {
      client.release();
    }
  }
}

