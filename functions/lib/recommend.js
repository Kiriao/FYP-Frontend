"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
exports.recommendForUser = void 0;
// functions/src/recommend.ts
const https_1 = require("firebase-functions/v2/https");
const db_1 = require("./lib/db");
const firestore_1 = require("firebase-admin/firestore");
const app_1 = require("firebase-admin/app");
const restrictions_1 = require("./lib/restrictions");
const openai_1 = require("./lib/openai");
const logger = __importStar(require("firebase-functions/logger"));
if (!(0, app_1.getApps)().length)
    (0, app_1.initializeApp)();
const db = (0, firestore_1.getFirestore)();
/* ---------------------------- weights & knobs ---------------------------- */
const W_QUERY = Number(process.env.RECO_W_QUERY ?? 0.60);
const W_INTEREST = Number(process.env.RECO_W_INTEREST ?? 0.20);
const W_AGELANG = Number(process.env.RECO_W_AGELANG ?? 0.20);
const MMR_LAMBDA = Number(process.env.MMR_LAMBDA ?? 0.7);
/* --------------------------------- utils -------------------------------- */
const lc = (s) => String(s ?? "").toLowerCase().trim();
const jaccard = (a = [], b = []) => {
    if (!a.length || !b.length)
        return 0;
    const A = new Set(a.map(lc)), B = new Set(b.map(lc));
    let inter = 0;
    A.forEach(x => { if (B.has(x))
        inter += 1; });
    return inter / (A.size + B.size - inter);
};
// normalize any "embedding" (pgvector might arrive as string "(..,..)" or array-like)
function asNumberArray(x) {
    if (Array.isArray(x)) {
        return x.map((v) => Number(v)).filter((n) => Number.isFinite(n));
    }
    if (x == null)
        return [];
    const s = String(x).trim();
    if (!s)
        return [];
    // strip wrapping [] or ()
    const core = s.replace(/^[\[\(]\s*/, "").replace(/[\]\)]\s*$/, "");
    if (!core)
        return [];
    return core
        .split(",")
        .map((p) => Number(p.trim()))
        .filter((n) => Number.isFinite(n));
}
// cosine similarity (defensive)
function cosine(a, b) {
    if (!a.length || !b.length || a.length !== b.length)
        return 0;
    let dot = 0, na = 0, nb = 0;
    for (let i = 0; i < a.length; i++) {
        dot += a[i] * b[i];
        na += a[i] * a[i];
        nb += b[i] * b[i];
    }
    if (na === 0 || nb === 0)
        return 0;
    return dot / Math.sqrt(na * nb);
}
// greedy MMR to diversify by embedding
function mmrSelect(cands, k, lambda, simGetter, embGetter) {
    const out = [];
    const used = new Set();
    while (out.length < Math.min(k, cands.length)) {
        let best = null;
        let bestScore = -Infinity;
        for (const c of cands) {
            if (used.has(c.id))
                continue;
            const relevance = simGetter(c);
            let redundancy = 0;
            if (out.length) {
                let maxSim = 0;
                for (const o of out) {
                    const a = embGetter(c);
                    const b = embGetter(o);
                    const s = (Array.isArray(a) && Array.isArray(b) && a.length && b.length && a.length === b.length)
                        ? cosine(a, b) : 0;
                    if (s > maxSim)
                        maxSim = s;
                }
                redundancy = maxSim;
            }
            const score = lambda * relevance - (1 - lambda) * redundancy;
            if (score > bestScore) {
                bestScore = score;
                best = c;
            }
        }
        if (!best)
            break;
        out.push(best);
        used.add(best.id);
    }
    return out;
}
/* -------------------------------- handler ------------------------------- */
exports.recommendForUser = (0, https_1.onRequest)({ region: "asia-southeast1", timeoutSeconds: 120 }, async (req, res) => {
    try {
        const pool = await (0, db_1.getPool)();
        const userId = (typeof req.query.userId === "string" && req.query.userId) ||
            req.body?.userId;
        if (!userId) {
            res.status(400).json({ error: "userId required" });
            return;
        }
        /* ------------------------------- guardrail ------------------------------ */
        const userSnap = await db.collection("users").doc(userId).get();
        const role = userSnap.get("role") || "child";
        const restrictions = userSnap.get("restrictions") || [];
        const queryText = (typeof req.body?.query === "string" && req.body.query) ||
            (typeof req.query.query === "string" && req.query.query) ||
            undefined;
        if (role === "child" && queryText && restrictions.length) {
            const hits = (0, restrictions_1.findRestrictedTerms)(queryText, restrictions);
            if (hits.length) {
                await db.collection("safety_logs").add({
                    userId, hits, ts: new Date(), messagePreview: queryText.slice(0, 160)
                });
                res.json({
                    mode: "blocked",
                    items: [],
                    reply: "Hey there! I can’t help with that topic. Want to explore fun science books, animal stories, or math videos instead? Tell me a topic you like! 🐼🚀📚"
                });
                return;
            }
        }
        /* -------------------------------- inputs -------------------------------- */
        const limit = Math.min(Number(req.query.limit ?? req.body?.limit ?? 10), 50);
        const type = typeof req.query.type === "string" ? lc(req.query.type) : (req.body?.type ? lc(req.body.type) : undefined);
        // language column does not exist in DB; we accept the param but only use it as a soft boost later (no SQL filter)
        const language = typeof req.query.language === "string" ? lc(req.query.language) : (req.body?.language ? lc(req.body.language) : undefined);
        const excludeIds = Array.isArray(req.body?.excludeIds) ? req.body.excludeIds.map(String) : [];
        const topic = typeof req.body?.topic === "string" ? lc(req.body.topic) : undefined;
        const genre = typeof req.body?.genre === "string" ? lc(req.body.genre) : undefined;
        const ageFromProfile = userSnap.get("age");
        const ageBody = req.body?.age ?? req.query.age;
        const childAge = Number.isFinite(Number(ageBody)) ? Number(ageBody)
            : (Number.isFinite(Number(ageFromProfile)) ? Number(ageFromProfile) : undefined);
        // “seen” set from favourites + activities + caller excludes
        const favRefs = await db.collection("users").doc(userId).collection("favourites").listDocuments();
        const actRefs = await db.collection("users").doc(userId).collection("activities").listDocuments();
        const seenIds = new Set([
            ...excludeIds,
            ...favRefs.map((d) => d.id),
            ...actRefs.map((d) => d.id),
        ]);
        // interests from profile (array<string>)
        const profileInterests = Array.isArray(userSnap.get("interests")) ? userSnap.get("interests") : [];
        // profile vector from user_profiles
        const prof = await pool.query("SELECT embedding FROM user_profiles WHERE user_id=$1", [userId]);
        const profVec = asNumberArray(prof.rows?.[0]?.embedding);
        // optional query embedding
        let queryVec = [];
        if (queryText && queryText.trim()) {
            try {
                const [v] = await (0, openai_1.embedTexts)([queryText.trim()]);
                queryVec = asNumberArray(v);
            }
            catch (e) {
                logger.warn("reco embedTexts failed", String(e));
            }
        }
        /* ---------------------------- adaptive weights --------------------------- */
        const favCount = favRefs.length;
        const actCount = actRefs.length;
        const historySize = favCount + actCount;
        const hasRichHistory = (profileInterests.length >= 3) ||
            (historySize >= 10) ||
            (profVec.length > 0);
        const personalizedFlag = String((req.body?.mode || req.query?.mode || "")).toLowerCase() === "personalized";
        let wq = W_QUERY;
        let wi = W_INTEREST;
        let wa = W_AGELANG;
        let mmr = MMR_LAMBDA;
        if (personalizedFlag && hasRichHistory) {
            // Taste-forward
            wq = 0.40;
            wi = 0.45;
            wa = 0.15;
            mmr = 0.65;
        }
        else if (personalizedFlag) {
            // Moderate taste
            wq = 0.50;
            wi = 0.35;
            wa = 0.15;
            mmr = 0.65;
        }
        const sum = wq + wi + wa;
        if (sum > 0) {
            wq /= sum;
            wi /= sum;
            wa /= sum;
        }
        logger.info("RECO_WEIGHTS", {
            personalizedFlag, hasRichHistory,
            favCount, actCount, profileInterestsCount: profileInterests.length,
            wq, wi, wa, mmr
        });
        /* ------------------------------ SQL WHERE base --------------------------- */
        const whereParts = [
            "(tags IS NULL OR NOT (tags && $1))", // restrictions
            "NOT (id = ANY($2))" // not seen
        ];
        const params = [restrictions, Array.from(seenIds)];
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
        try {
            await pool.query("SET ivfflat.probes = 10");
        }
        catch { }
        /* ------------------------- candidate gathering --------------------------- */
        const CAND_N = Math.max(40, limit * 8);
        async function fetchNN(vec) {
            const nv = asNumberArray(vec);
            if (!nv.length)
                return [];
            const p = params.slice();
            const sql = `
          SELECT id, title, description, type, thumb, link, tags, authors, age_min, age_max, embedding, created_at
            FROM items
           WHERE ${whereSQL}
           ORDER BY embedding <#> $${p.length + 1}::vector
           LIMIT ${CAND_N}
        `;
            p.push(`[${nv.join(",")}]`);
            const r = await pool.query(sql, p);
            // normalize embeddings immediately
            return r.rows.map(row => ({ ...row, embedding: asNumberArray(row.embedding) }))
                .filter(row => Array.isArray(row.embedding) && row.embedding.length > 0);
        }
        const candSets = [];
        if (queryVec.length)
            candSets.push(await fetchNN(queryVec));
        if (profVec.length)
            candSets.push(await fetchNN(profVec));
        let candidates = [];
        if (candSets.length) {
            // interleave/union by id
            const seen = new Set();
            let i = 0;
            while (candidates.length < CAND_N && candSets.some(set => i < set.length)) {
                for (const set of candSets) {
                    if (i < set.length) {
                        const it = set[i];
                        if (!seen.has(it.id)) {
                            seen.add(it.id);
                            candidates.push(it);
                        }
                    }
                }
                i++;
            }
        }
        else {
            // Generic fallback
            const r = await pool.query(`SELECT id, title, description, type, thumb, link, tags, authors, age_min, age_max, embedding, created_at
             FROM items
            WHERE ${whereSQL}
            ORDER BY created_at DESC
            LIMIT ${CAND_N}`, params);
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
        const tagsOf = (x) => Array.isArray(x.tags) ? x.tags : [];
        function ageLangBoost(x) {
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
        const qSim = new Map();
        const iSim = new Map();
        const aBoost = new Map();
        for (const it of candidates) {
            const jsim = jaccard(tagsOf(it), interestBag);
            const boost = ageLangBoost(it);
            const qsim = queryVec.length ? cosine(queryVec, it.embedding) : 0;
            qSim.set(it.id, qsim);
            iSim.set(it.id, jsim);
            aBoost.set(it.id, boost);
        }
        const finalRelevance = (it) => wq * (qSim.get(it.id) ?? 0) +
            wi * (iSim.get(it.id) ?? 0) +
            wa * (aBoost.get(it.id) ?? 0);
        // diversify
        const diversified = mmrSelect(candidates, limit, mmr, (x) => finalRelevance(x), (x) => x.embedding);
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
    }
    catch (e) {
        logger.error("recommendForUser error", e);
        res.status(500).json({ error: e?.message ?? "internal error" });
    }
});
//# sourceMappingURL=recommend.js.map