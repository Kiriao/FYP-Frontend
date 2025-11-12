// functions/src/recommend.ts
import { onRequest } from "firebase-functions/v2/https";
import { getPool } from "./lib/db";
import { getFirestore, DocumentReference } from "firebase-admin/firestore";
import { getApps, initializeApp } from "firebase-admin/app";
import { findRestrictedTerms } from "./lib/restrictions";
import { embedTexts } from "./lib/openai";
import * as logger from "firebase-functions/logger";

if (!getApps().length) initializeApp();
const db = getFirestore();

/* ---------------------------- weights & knobs ---------------------------- */
const W_QUERY    = Number(process.env.RECO_W_QUERY    ?? 0.60);
const W_INTEREST = Number(process.env.RECO_W_INTEREST ?? 0.20);
const W_AGELANG  = Number(process.env.RECO_W_AGELANG  ?? 0.20);
const MMR_LAMBDA = Number(process.env.MMR_LAMBDA      ?? 0.7);

/* --------------------------------- utils -------------------------------- */
const lc = (s: any) => String(s ?? "").toLowerCase().trim();

const jaccard = (a: string[] = [], b: string[] = []) => {
  if (!a.length || !b.length) return 0;
  const A = new Set(a.map(lc)), B = new Set(b.map(lc));
  let inter = 0;
  A.forEach(x => { if (B.has(x)) inter += 1; });
  return inter / (A.size + B.size - inter);
};

// normalize any "embedding" (pgvector might arrive as string "(..,..)" or array-like)
function asNumberArray(x: any): number[] {
  if (Array.isArray(x)) {
    return x.map((v) => Number(v)).filter((n) => Number.isFinite(n));
  }
  if (x == null) return [];
  const s = String(x).trim();
  if (!s) return [];
  // strip wrapping [] or ()
  const core = s.replace(/^[\[\(]\s*/, "").replace(/[\]\)]\s*$/, "");
  if (!core) return [];
  return core
    .split(",")
    .map((p) => Number(p.trim()))
    .filter((n) => Number.isFinite(n));
}

// cosine similarity (defensive)
function cosine(a: number[], b: number[]) {
  if (!a.length || !b.length || a.length !== b.length) return 0;
  let dot = 0, na = 0, nb = 0;
  for (let i = 0; i < a.length; i++) { dot += a[i]*b[i]; na += a[i]*a[i]; nb += b[i]*b[i]; }
  if (na === 0 || nb === 0) return 0;
  return dot / Math.sqrt(na * nb);
}

// greedy MMR to diversify by embedding
function mmrSelect(
  cands: any[],
  k: number,
  lambda: number,
  simGetter: (x:any)=>number,
  embGetter:(x:any)=>number[]
) {
  const out: any[] = [];
  const used = new Set<string>();
  while (out.length < Math.min(k, cands.length)) {
    let best: any = null;
    let bestScore = -Infinity;
    for (const c of cands) {
      if (used.has(c.id)) continue;
      const relevance = simGetter(c);
      let redundancy = 0;
      if (out.length) {
        let maxSim = 0;
        for (const o of out) {
          const a = embGetter(c);
          const b = embGetter(o);
          const s = (Array.isArray(a) && Array.isArray(b) && a.length && b.length && a.length === b.length)
            ? cosine(a, b) : 0;
          if (s > maxSim) maxSim = s;
        }
        redundancy = maxSim;
      }
      const score = lambda * relevance - (1 - lambda) * redundancy;
      if (score > bestScore) { bestScore = score; best = c; }
    }
    if (!best) break;
    out.push(best);
    used.add(best.id);
  }
  return out;
}

/* -------------------------------- handler ------------------------------- */
export const recommendForUser = onRequest(
  { region: "asia-southeast1", timeoutSeconds: 120 },
  async (req, res) => {
    try {
      const pool = await getPool();

      const userId =
        (typeof req.query.userId === "string" && req.query.userId) ||
        (req.body?.userId as string);
      if (!userId) { res.status(400).json({ error: "userId required" }); return; }

      /* ------------------------------- guardrail ------------------------------ */
      const userSnap = await db.collection("users").doc(userId).get();
      const role = (userSnap.get("role") as string) || "child";
      const restrictions: string[] = userSnap.get("restrictions") || [];

      const queryText: string | undefined =
        (typeof req.body?.query === "string" && req.body.query) ||
        (typeof req.query.query === "string" && req.query.query) ||
        undefined;

      if (role === "child" && queryText && restrictions.length) {
        const hits = findRestrictedTerms(queryText, restrictions);
        if (hits.length) {
          await db.collection("safety_logs").add({
            userId, hits, ts: new Date(), messagePreview: queryText.slice(0, 160)
          });
          res.json({
            mode: "blocked",
            items: [],
            reply:
              "Hey there! I can’t help with that topic. Want to explore fun science books, animal stories, or math videos instead? Tell me a topic you like! 🐼🚀📚"
          });
          return;
        }
      }
      /* -------------------------------- inputs -------------------------------- */
      const limit = Math.min(Number(req.query.limit ?? req.body?.limit ?? 10), 50);
      const type = typeof req.query.type === "string" ? lc(req.query.type) : (req.body?.type ? lc(req.body.type) : undefined);
      // language column does not exist in DB; we accept the param but only use it as a soft boost later (no SQL filter)
      const language = typeof req.query.language === "string" ? lc(req.query.language) : (req.body?.language ? lc(req.body.language) : undefined);
      const excludeIds: string[] = Array.isArray(req.body?.excludeIds) ? req.body.excludeIds.map(String) : [];
      const topic = typeof req.body?.topic === "string" ? lc(req.body.topic) : undefined;
      const genre = typeof req.body?.genre === "string" ? lc(req.body.genre) : undefined;

      const ageFromProfile = userSnap.get("age");
      const ageBody = req.body?.age ?? req.query.age;
      const childAge =
        Number.isFinite(Number(ageBody)) ? Number(ageBody)
        : (Number.isFinite(Number(ageFromProfile)) ? Number(ageFromProfile) : undefined);

      // “seen” set from favourites + activities + caller excludes
      const favRefs = await db.collection("users").doc(userId).collection("favourites").listDocuments();
      const actRefs = await db.collection("users").doc(userId).collection("activities").listDocuments();
      const seenIds = new Set<string>([
        ...excludeIds,
        ...favRefs.map((d: DocumentReference) => d.id),
        ...actRefs.map((d: DocumentReference) => d.id),
      ]);

      // interests from profile (array<string>)
      const profileInterests: string[] = Array.isArray(userSnap.get("interests")) ? userSnap.get("interests") : [];

      // profile vector from user_profiles
      const prof = await pool.query<{ embedding: any }>(
        "SELECT embedding FROM user_profiles WHERE user_id=$1",
        [userId]
      );
      const profVec = asNumberArray(prof.rows?.[0]?.embedding);

      // optional query embedding
      let queryVec: number[] = [];
      if (queryText && queryText.trim()) {
        try {
          const [v] = await embedTexts([queryText.trim()]);
          queryVec = asNumberArray(v);
        } catch (e) {
          logger.warn("reco embedTexts failed", String(e));
        }
      }

      /* ---------------------------- adaptive weights --------------------------- */
      const favCount = favRefs.length;
      const actCount = actRefs.length;
      const historySize = favCount + actCount;
      const hasRichHistory =
        (profileInterests.length >= 3) ||
        (historySize >= 10) ||
        (profVec.length > 0);

      const personalizedFlag =
        String((req.body?.mode || req.query?.mode || "")).toLowerCase() === "personalized";

      let wq = W_QUERY;
      let wi = W_INTEREST;
      let wa = W_AGELANG;
      let mmr = MMR_LAMBDA;

      if (personalizedFlag && hasRichHistory) {
        // Taste-forward
        wq = 0.40; wi = 0.45; wa = 0.15; mmr = 0.65;
      } else if (personalizedFlag) {
        // Moderate taste
        wq = 0.50; wi = 0.35; wa = 0.15; mmr = 0.65;
      }
      const sum = wq + wi + wa;
      if (sum > 0) { wq /= sum; wi /= sum; wa /= sum; }

      logger.info("RECO_WEIGHTS", {
        personalizedFlag, hasRichHistory,
        favCount, actCount, profileInterestsCount: profileInterests.length,
        wq, wi, wa, mmr
      });

      /* ------------------------------ SQL WHERE base --------------------------- */
      const whereParts: string[] = [
        "(tags IS NULL OR NOT (tags && $1))",  // restrictions
        "NOT (id = ANY($2))"                   // not seen
      ];
      const params: any[] = [restrictions, Array.from(seenIds)];
      if (type) {
        whereParts.push("LOWER(type) = $" + (params.length + 1));
        params.push(type);
      }
      if (childAge !== undefined) {
        whereParts.push("((age_min IS NULL OR age_min <= $" + (params.length + 1) + ") AND (age_max IS NULL OR age_max >= $" + (params.length + 1) + "))");
        params.push(childAge);
      }
      // NOTE: no language column in DB; do NOT add SQL filter for language.
      const whereSQL = whereParts.join(" AND ");

      // helpful: increase probes if IVFFLAT is used (ignore error if unindexed)
      try { await pool.query("SET ivfflat.probes = 10"); } catch {}

      /* ------------------------- candidate gathering --------------------------- */
      const CAND_N = Math.max(40, limit * 8);

      type RawItem = {
        id: string;
        title: string;
        description: string | null;
        type: "book" | "video" | string;
        thumb: string | null;
        link: string | null;
        tags: string[] | null;
        authors: string[] | null;
        age_min: number | null;
        age_max: number | null;
        embedding: any;             // normalize later
        created_at: string;
      };

      async function fetchNN(vec: number[]): Promise<RawItem[]> {
        const nv = asNumberArray(vec);
        if (!nv.length) return [];
        const p = params.slice();
        const sql = `
          SELECT id, title, description, type, thumb, link, tags, authors, age_min, age_max, embedding, created_at
            FROM items
           WHERE ${whereSQL}
           ORDER BY embedding <#> $${p.length + 1}::vector
           LIMIT ${CAND_N}
        `;
        p.push(`[${nv.join(",")}]`);
        const r = await pool.query<RawItem>(sql, p);
        // normalize embeddings immediately
        return r.rows.map(row => ({ ...row, embedding: asNumberArray(row.embedding) }))
                     .filter(row => Array.isArray(row.embedding) && row.embedding.length > 0);
      }

      const candSets: RawItem[][] = [];
      if (queryVec.length) candSets.push(await fetchNN(queryVec));
      if (profVec.length)  candSets.push(await fetchNN(profVec));

      let candidates: RawItem[] = [];
      if (candSets.length) {
        // interleave/union by id
        const seen = new Set<string>();
        let i = 0;
        while (candidates.length < CAND_N && candSets.some(set => i < set.length)) {
          for (const set of candSets) {
            if (i < set.length) {
              const it = set[i];
              if (!seen.has(it.id)) { seen.add(it.id); candidates.push(it); }
            }
          }
          i++;
        }
      } else {
        // Generic fallback
        const r = await pool.query<RawItem>(
          `SELECT id, title, description, type, thumb, link, tags, authors, age_min, age_max, embedding, created_at
             FROM items
            WHERE ${whereSQL}
            ORDER BY created_at DESC
            LIMIT ${CAND_N}`,
          params
        );
        candidates = r.rows
          .map(row => ({ ...row, embedding: asNumberArray(row.embedding) }))
          .filter(row => Array.isArray(row.embedding) && row.embedding.length > 0);
        if (!candidates.length) {
          res.json({
            mode: "generic",
            items: [],
            note: "No items yet that meet your filters."
          });
          return;
        }
      }

      /* ------------------------------ scoring --------------------------------- */
      const desiredTopicOrGenre = (topic || genre || "").trim();
      const interestBag = [
        ...profileInterests,
        ...(desiredTopicOrGenre ? [desiredTopicOrGenre] : [])
      ];

      const tagsOf = (x: RawItem) => Array.isArray(x.tags) ? x.tags : [];

      function ageLangBoost(x: RawItem): number {
        // 0..1: age fit contributes; language is not in DB → treat as neutral (1)
        let ageOK = 1;
        if (childAge !== undefined) {
          const minOK = (x.age_min == null) || (x.age_min <= childAge);
          const maxOK = (x.age_max == null) || (x.age_max >= childAge);
          ageOK = (minOK && maxOK) ? 1 : 0;
        }
        const langOK = 1; // no language column; neutral
        return 0.5 * (ageOK + langOK);
      }

      // precompute
      const qSim = new Map<string, number>();
      const iSim = new Map<string, number>();
      const aBoost = new Map<string, number>();
      for (const it of candidates) {
        const jsim = jaccard(tagsOf(it), interestBag);
        const boost = ageLangBoost(it);
        const qsim = queryVec.length ? cosine(queryVec, it.embedding) : 0;
        qSim.set(it.id, qsim);
        iSim.set(it.id, jsim);
        aBoost.set(it.id, boost);
      }

      const finalRelevance = (it: RawItem): number =>
        wq * (qSim.get(it.id) ?? 0) +
        wi * (iSim.get(it.id) ?? 0) +
        wa * (aBoost.get(it.id) ?? 0);

      // diversify
      const diversified = mmrSelect(
        candidates,
        limit,
        mmr,
        (x) => finalRelevance(x),
        (x) => x.embedding
      );

      const out = diversified.map(it => ({
        id: it.id,
        title: it.title,
        description: it.description,
        type: it.type,
        thumb: it.thumb,
        link: it.link,
        authors: Array.isArray(it.authors) ? it.authors : [],
        tags: Array.isArray(it.tags) ? it.tags : []
      }));

      res.json({
        mode: (profVec.length || queryVec.length) ? "personalized" : "generic",
        items: out
      });

    } catch (e: any) {
      logger.error("recommendForUser error", e);
      res.status(500).json({ error: e?.message ?? "internal error" });
    }
  }
);
