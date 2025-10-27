import { onRequest } from "firebase-functions/v2/https";
import { getPool } from "./lib/db";

export const recommendForUser = onRequest({ timeoutSeconds: 120 }, async (req, res) => {
  try {
    const pool = await getPool(); // <-- await the pool

    const userId =
      (typeof req.query.userId === "string" && req.query.userId) ||
      (req.body?.userId as string);
    if (!userId) { res.status(400).json({ error: "userId required" }); return; }

    const prof = await pool.query<{ embedding: number[]; role: string }>(
      "SELECT embedding, role FROM user_profiles WHERE user_id=$1",
      [userId]
    );
    if (prof.rowCount === 0) { res.status(404).json({ error: "no profile vector yet" }); return; }

    const embedding = prof.rows[0].embedding;
    const restrictions: string[] = Array.isArray(req.body?.restrictions) ? req.body.restrictions : [];
    const seenIds: string[] = Array.isArray(req.body?.seenIds) ? req.body.seenIds : [];

    const q = await pool.query(
      `SELECT id, title, description, type
         FROM items
        WHERE (tags IS NULL OR NOT (tags && $2))
          AND NOT (id = ANY($3))
        ORDER BY embedding <-> $1
        LIMIT 10`,
      [embedding, restrictions, seenIds]
    );

    res.json({ items: q.rows });
  } catch (e: any) {
    console.error("recommendForUser error:", e);
    res.status(500).json({ error: e.message ?? "internal error" });
  }
});
