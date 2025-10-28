import { fetch } from "undici";

const API_KEY = process.env.OPENAI_API_KEY!;
const MODEL = process.env.EMBEDDING_MODEL || "text-embedding-3-small";

export async function embedTexts(texts: string[]): Promise<number[][]> {
  if (!API_KEY) throw new Error("OPENAI_API_KEY missing");
  const r = await fetch("https://api.openai.com/v1/embeddings", {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "authorization": `Bearer ${API_KEY}`,
    },
    body: JSON.stringify({ model: MODEL, input: texts }),
  });
  if (!r.ok) throw new Error(`OpenAI embeddings HTTP ${r.status}`);
  const j = await r.json() as { data: Array<{ embedding: number[] }> };
  return j.data.map(d => d.embedding);
}
