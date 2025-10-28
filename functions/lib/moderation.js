"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.moderateMessage = void 0;
// functions/src/moderation.ts
const https_1 = require("firebase-functions/v2/https");
const firestore_1 = require("firebase-admin/firestore");
const app_1 = require("firebase-admin/app");
const restrictions_1 = require("./lib/restrictions");
if (!(0, app_1.getApps)().length)
    (0, app_1.initializeApp)();
const db = (0, firestore_1.getFirestore)();
exports.moderateMessage = (0, https_1.onRequest)(async (req, res) => {
    try {
        const { userId, message } = req.body || {};
        if (!userId || !message) {
            res.status(400).json({ error: "userId and message required" });
            return;
        }
        const snap = await db.collection("users").doc(userId).get();
        const role = snap.get("role") || "child";
        const restrictions = snap.get("restrictions") || [];
        let blocked = false;
        let reply;
        if (role === "child" && restrictions.length) {
            const hits = (0, restrictions_1.findRestrictedTerms)(String(message), restrictions);
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
    }
    catch (e) {
        console.error("moderateMessage error:", e);
        res.status(500).json({ error: e.message ?? "internal error" });
        return;
    }
});
//# sourceMappingURL=moderation.js.map