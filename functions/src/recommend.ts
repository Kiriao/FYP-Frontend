// functions/src/recommend.ts
import { onRequest } from "firebase-functions/v2/https";
import { getPool } from "./lib/db";
import { getFirestore } from "firebase-admin/firestore";
import { getApps, initializeApp } from "firebase-admin/app";
import { findRestrictedTerms } from "./lib/restrictions";

if (!getApps().length) initializeApp();
const db = getFirestore();

export const recommendForUser = onRequest({ timeoutSeconds: 120 }, async (req, res) => {
  try {
    const pool = await getPool();

    const userId =
      (typeof req.query.userId === "string" && req.query.userId) ||
      (req.body?.userId as string);
    if (!userId) { res.status(400).json({ error: "userId required" }); return; }

    // 🔒 GUARD: load role + restrictions, check the user’s message (req.body.query)
    const userSnap = await db.collection("users").doc(userId).get();
    const role = userSnap.get("role") || "child";
    const restrictions: string[] = userSnap.get("restrictions") || [];
    const queryText: string | undefined =
      (typeof req.body?.query === "string" && req.body.query) ||
      (typeof req.query.query === "string" && req.query.query) ||
      undefined;

    if (role === "child" && queryText && restrictions.length) {
      const hits = findRestrictedTerms(queryText, restrictions);
      if (hits.length) {
        // optional: log
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
    // 🔒 END GUARD

    const limit = Math.min(Number(req.query.limit ?? req.body?.limit ?? 10), 50);
    const type = typeof req.query.type === "string" ? req.query.type : (req.body?.type as string | undefined);

    // Load seenIds from Firestore (de-dup) + age (optional filter)
    const favRefs = await db.collection("users").doc(userId).collection("favourites").listDocuments();
    const actRefs = await db.collection("users").doc(userId).collection("activities").listDocuments();
    const seenIds = Array.from(new Set([...favRefs.map(d => d.id), ...actRefs.map(d => d.id)]));

    const ageFromProfile = userSnap.get("age");
    const ageBody = req.body?.age ?? req.query.age;
    const childAge = Number.isFinite(Number(ageBody)) ? Number(ageBody)
                     : (Number.isFinite(Number(ageFromProfile)) ? Number(ageFromProfile) : undefined);

    // Try personalized profile first
    const prof = await pool.query<{ embedding: number[] }>(
      "SELECT embedding FROM user_profiles WHERE user_id=$1",
      [userId]
    );

    // optional: improve recall
    await pool.query("SET ivfflat.probes = 10");

    const where = [
      "(tags IS NULL OR NOT (tags && $2))",
      "NOT (id = ANY($3))",
      type ? "type = $4" : null,
      childAge !== undefined ? "((age_min IS NULL OR age_min <= $5) AND (age_max IS NULL OR age_max >= $5))" : null
    ].filter(Boolean).join(" AND ");

    if (prof.rows.length > 0) {
      const v = prof.rows[0].embedding;
      const params: any[] = [v, restrictions, seenIds];
      if (type) params.push(type);
      if (childAge !== undefined) params.push(childAge);

      const q = await pool.query(
        `SELECT id, title, description, type
           FROM items
          WHERE ${where}
          ORDER BY embedding <-> $1
          LIMIT ${limit}`,
        params
      );

      if (q.rows.length > 0) {
        res.json({ mode: "profile", items: q.rows });
        return;
      }
    }

    // Generic fallback (no vector)
    const paramsG: any[] = [restrictions, seenIds];
    const whereG = [
      "(tags IS NULL OR NOT (tags && $1))",
      "NOT (id = ANY($2))",
      type ? "type = $3" : null,
      childAge !== undefined ? "((age_min IS NULL OR age_min <= $4) AND (age_max IS NULL OR age_max >= $4))" : null
    ].filter(Boolean).join(" AND ");
    if (type) paramsG.push(type);
    if (childAge !== undefined) paramsG.push(childAge);

    const q = await pool.query(
      `SELECT id, title, description, type
         FROM items
        WHERE ${whereG}
        ORDER BY created_at DESC
        LIMIT ${limit}`,
      paramsG
    );

    res.json({
      mode: "generic",
      items: q.rows,
      note: "These are safe starter picks. Tell me what you like (animals, space, math…), or start reading/watching & ⭐ to personalize your recommendations!"
    });
  } catch (e: any) {
    console.error("recommendForUser error:", e);
    res.status(500).json({ error: e.message ?? "internal error" });
  }
});
