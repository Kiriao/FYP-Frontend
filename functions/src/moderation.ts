// functions/src/moderation.ts
import { onRequest } from "firebase-functions/v2/https";
import { getFirestore } from "firebase-admin/firestore";
import { getApps, initializeApp } from "firebase-admin/app";
import { findRestrictedTerms } from "./lib/restrictions";

if (!getApps().length) initializeApp();
const db = getFirestore();

export const moderateMessage = onRequest(
    { region: "asia-southeast1", timeoutSeconds: 60 }, async (req, res) => {
  try {
    const { userId, message } = req.body || {};
    if (!userId || !message) {
      res.status(400).json({ error: "userId and message required" });
      return;
    }

    const snap = await db.collection("users").doc(userId).get();
    const role = snap.get("role") || "child";
    const restrictions: string[] = snap.get("restrictions") || [];

    let blocked = false;
    let reply: string | undefined;

    if (role === "child" && restrictions.length) {
      const hits = findRestrictedTerms(String(message), restrictions);
      if (hits.length) {
        blocked = true;
        reply = "I can’t help with that topic. Try asking for animal stories, space books, or math videos!";
        // optional log
        await db.collection("safety_logs").add({
          userId, hits, ts: new Date(), messagePreview: String(message).slice(0, 160)
        });
      }
    }

    res.json({ blocked, reply });
    return;
  } catch (e: any) {
    console.error("moderateMessage error:", e);
    res.status(500).json({ error: e.message ?? "internal error" });
    return;
  }
});
