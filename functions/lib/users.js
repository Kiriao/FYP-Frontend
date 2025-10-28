"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.rebuildUserProfile = rebuildUserProfile;
const app_1 = require("firebase-admin/app");
const firestore_1 = require("firebase-admin/firestore");
const db_1 = require("./lib/db");
if (!(0, app_1.getApps)().length)
    (0, app_1.initializeApp)();
const db = (0, firestore_1.getFirestore)();
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
        try {
            const e = await r.json();
            lastDetail = e?.error?.message || JSON.stringify(e) || lastDetail;
        }
        catch {
            /* ignore */
        }
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
function add(a, b, w = 1) { for (let i = 0; i < a.length; i++)
    a[i] += w * b[i]; return a; }
function l2norm(x) { return Math.sqrt(x.reduce((s, v) => s + v * v, 0)); }
function normalize(x) { const n = l2norm(x) || 1; return x.map(v => v / n); }
async function rebuildUserProfile(userId) {
    const pool = await (0, db_1.getPool)(); // <-- await the pool
    const userRef = db.collection("users").doc(userId);
    const userSnap = await userRef.get();
    if (!userSnap.exists)
        throw new Error("user not found");
    const role = userSnap.get("role");
    const interests = userSnap.get("interests") || [];
    const favRefs = await userRef.collection("favourites").listDocuments();
    const actRefs = await userRef.collection("activities").listDocuments();
    // embed interests
    const interestVecs = interests.length ? await embed(interests) : [];
    // fetch embeddings of seen items
    const favIds = favRefs.map(d => d.id);
    const actIds = actRefs.map(d => d.id);
    const ids = Array.from(new Set([...favIds, ...actIds]));
    let v = new Array(1536).fill(0);
    if (ids.length) {
        const { rows } = await pool.query(`SELECT embedding FROM items WHERE id = ANY($1::text[])`, [ids]);
        // read/watched: +1
        rows.forEach(r => add(v, r.embedding, 1));
        // favourites: +1 more (stronger signal)
        rows.slice(0, favIds.length).forEach(r => add(v, r.embedding, 1));
    }
    // interests: +0.5
    interestVecs.forEach(iv => add(v, iv, 0.5));
    v = normalize(v);
    await pool.query(`INSERT INTO user_profiles (user_id, role, embedding)
   VALUES ($1,$2,$3::float8[]::vector)
   ON CONFLICT (user_id) DO UPDATE
     SET role=EXCLUDED.role, embedding=EXCLUDED.embedding, updated_at=now()`, [userId, role ?? null, v]);
    await userRef.collection("_system").doc("profile").set({ updatedAt: new Date(), hasVector: true });
}
//# sourceMappingURL=users.js.map