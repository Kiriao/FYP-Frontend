"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.embedTexts = embedTexts;
const undici_1 = require("undici");
const API_KEY = process.env.OPENAI_API_KEY;
const MODEL = process.env.EMBEDDING_MODEL || "text-embedding-3-small";
async function embedTexts(texts) {
    if (!API_KEY)
        throw new Error("OPENAI_API_KEY missing");
    const r = await (0, undici_1.fetch)("https://api.openai.com/v1/embeddings", {
        method: "POST",
        headers: {
            "content-type": "application/json",
            "authorization": `Bearer ${API_KEY}`,
        },
        body: JSON.stringify({ model: MODEL, input: texts }),
    });
    if (!r.ok)
        throw new Error(`OpenAI embeddings HTTP ${r.status}`);
    const j = await r.json();
    return j.data.map(d => d.embedding);
}
//# sourceMappingURL=openai.js.map