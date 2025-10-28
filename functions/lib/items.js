"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.upsertItems = upsertItems;
const db_1 = require("./lib/db");
const OPENAI = process.env.OPENAI_API_KEY;
const MODEL = process.env.EMBEDDING_MODEL || "text-embedding-3-small";
// Embedding with retries + error detail
async function embed(texts) {
    const body = JSON.stringify({ model: MODEL, input: texts });
    let lastDetail = "";
    for (let attempt = 0; attempt < 5; attempt++) {
        const r = await fetch("https://api.openai.com/v1/embeddings", {
            method: "POST",
            headers: {
                "content-type": "application/json",
                authorization: `Bearer ${OPENAI}`,
            },
            body,
        });
        if (r.ok) {
            const j = (await r.json());
            return j.data.map(d => d.embedding);
        }
        // capture error payload for debugging
        try {
            const e = await r.json();
            lastDetail = e?.error?.message || JSON.stringify(e) || lastDetail;
        }
        catch {
            /* ignore */
        }
        // 429 backoff
        if (r.status === 429) {
            const backoff = Math.min(2000, 200 * Math.pow(2, attempt)) + Math.floor(Math.random() * 200);
            console.warn(`OpenAI 429, retrying in ${backoff}ms… last: ${lastDetail}`);
            await new Promise(res => setTimeout(res, backoff));
            continue;
        }
        throw new Error(`OpenAI HTTP ${r.status}${lastDetail ? `: ${lastDetail}` : ""}`);
    }
    throw new Error(`OpenAI 429: exhausted retries${lastDetail ? `; last: ${lastDetail}` : ""}`);
}
async function upsertItems(items) {
    if (!items?.length)
        return;
    const pool = await (0, db_1.getPool)();
    // 1) Build all texts to embed in ONE call
    const texts = items.map(i => `${i.title}. ${(i.authors || []).join(", ")}. ${i.description || ""}. ` +
        `tags:${(i.tags || []).join(",")}. type:${i.type}`);
    // If you ever post >100 items, chunk to stay safe
    const CHUNK = 50;
    for (let start = 0; start < texts.length; start += CHUNK) {
        const sliceItems = items.slice(start, start + CHUNK);
        const sliceTexts = texts.slice(start, start + CHUNK);
        const vectors = await embed(sliceTexts); // number[][] same length as sliceItems
        // 2) Upsert each row (wrap this slice in a transaction)
        const client = await pool.connect();
        try {
            await client.query("BEGIN");
            for (let i = 0; i < sliceItems.length; i++) {
                const it = sliceItems[i];
                const v = vectors[i];
                await client.query(`INSERT INTO items (id,type,title,description,authors,tags,age_min,age_max,embedding)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::float8[]::vector)
           ON CONFLICT (id) DO UPDATE
           SET title=EXCLUDED.title, description=EXCLUDED.description, authors=EXCLUDED.authors,
               tags=EXCLUDED.tags, age_min=EXCLUDED.age_min, age_max=EXCLUDED.age_max, embedding=EXCLUDED.embedding`, [
                    it.id,
                    it.type,
                    it.title,
                    it.description || "",
                    it.authors || null,
                    it.tags || null,
                    it.age_min ?? null,
                    it.age_max ?? null,
                    v,
                ]);
            }
            await client.query("COMMIT");
        }
        catch (e) {
            await client.query("ROLLBACK");
            throw e;
        }
        finally {
            client.release();
        }
    }
}
//# sourceMappingURL=items.js.map