process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0"; // DEV ONLY: bypass corp TLS inspection

const { Client } = require("pg");

// Use undici fetch + optional corporate proxy + optional TLS relax
const { fetch, setGlobalDispatcher, Agent, ProxyAgent } = require("undici");

const outboundProxy = process.env.HTTPS_PROXY || process.env.HTTP_PROXY;
if (outboundProxy) {
  setGlobalDispatcher(new ProxyAgent(outboundProxy));
} else if (process.env.ALLOW_INSECURE_TLS === "1") {
  // Only for environments doing HTTPS interception; don’t use in prod
  setGlobalDispatcher(new Agent({ connect: { tls: { rejectUnauthorized: false } } }));
}


// ---- env ----
const DATABASE_URL = process.env.DATABASE_URL;                     // postgres://user:pass@host:5432/db[?sslmode=require]
const APP_API_BASE  = (process.env.APP_API_BASE || "").replace(/\/+$/,""); // e.g. https://kidflix-4cda0.web.app
const EMBED_URL     = process.env.EMBED_URL;                        // your Cloud Function /embed endpoint
const DRY_RUN       = process.env.DRY_RUN === "1";

if (!DATABASE_URL) throw new Error("Missing DATABASE_URL");
if (!APP_API_BASE)  throw new Error("Missing APP_API_BASE");
if (!EMBED_URL)     throw new Error("Missing EMBED_URL");

// Reuse same SSL behavior for Client connections (important if hitting public IP with TLS)
const ssl =
  process.env.ALLOW_INSECURE_SSL === "1"
    ? { rejectUnauthorized: false }
    : undefined;

// ---------- helpers ----------
function pickTitle(x){
  return x?.title || x?.name || x?.volumeInfo?.title || x?.snippet?.title || "Untitled";
}
function pickDescription(x){
  return x?.description || x?.snippet || x?.volumeInfo?.description || x?.searchInfo?.textSnippet || "";
}
function pickThumb(x){
  if (x?.thumbnail) return x.thumbnail;
  if (x?.volumeInfo?.imageLinks?.thumbnail) return x.volumeInfo.imageLinks.thumbnail;
  if (x?.snippet?.thumbnails?.medium?.url) return x.snippet.thumbnails.medium.url;
  if (x?.snippet?.thumbnails?.default?.url) return x.snippet.thumbnails.default.url;
  return null;
}
function pickLinkBook(x){
  if (x?.bestLink) return x.bestLink;
  if (x?.previewLink) return x.previewLink;
  if (x?.canonicalVolumeLink) return x.canonicalVolumeLink;
  if (x?.infoLink) return x.infoLink;
  const v = x?.volumeInfo; 
  return v?.previewLink || v?.canonicalVolumeLink || v?.infoLink || null;
}
function pickLinkVideo(x){
  const vid = x?.id?.videoId || x?.videoId;
  if (x?.url) return x.url;
  return vid ? `https://www.youtube.com/watch?v=${vid}` : null;
}

async function getJSON(url){
  const r = await fetch(url, { headers: { accept: "application/json" } });
  if (!r.ok) throw new Error(`HTTP ${r.status} ${url}`);
  return r.json();
}
async function embedText(text){
  const r = await fetch(EMBED_URL, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ text })
  });
  if (!r.ok) throw new Error(`Embed error ${r.status}`);
  const j = await r.json();
  if (!Array.isArray(j.vector) || j.vector.length === 0) throw new Error("Empty vector");
  return j.vector;
}

function bookCategories(){
  return ["fiction","nonfiction","education","children_literature","picture_board_early","middle_grade","poetry_humor","biography","young_adult"];
}
function videoTopics(){
  return ["stories","songs_rhymes","learning","science","math","animals","art_crafts"];
}

function tagsFromBookCategory(c){ return [c]; }
function tagsFromVideoTopic(t){ return [t]; }

async function fetchBooksByCategory(canon, page, pageSize=20){
  const u = new URL(`${APP_API_BASE}/api/books`);
  u.searchParams.set("q", canon);
  u.searchParams.set("page", String(page));
  u.searchParams.set("pageSize", String(pageSize));
  u.searchParams.set("includeYA", "1");
  u.searchParams.set("debug","1");
  return getJSON(u.toString());
}
async function fetchVideosByTopic(topic, page, pageSize=20){
  const u = new URL(`${APP_API_BASE}/api/videos`);
  u.searchParams.set("bucket", topic);
  u.searchParams.set("page", String(page));
  u.searchParams.set("pageSize", String(pageSize));
  return getJSON(u.toString());
}

async function upsertItem(pg, row){
  // Ensure numbers (not strings), then build a pgvector literal: [1,2,3,...]
  const vec = Array.isArray(row.embedding) ? row.embedding.map(Number) : [];
  if (!vec.length) throw new Error("Empty embedding vector");
  const vectorLiteral = `[${vec.join(",")}]`; // pgvector textual format

  const sql = `
    INSERT INTO items (id, title, description, type, tags, age_min, age_max, link, thumb, embedding)
    VALUES ($1,$2,$3,$4,$5::text[],$6,$7,$8,$9,$10::vector)
    ON CONFLICT (id) DO UPDATE
    SET title=EXCLUDED.title,
        description=EXCLUDED.description,
        type=EXCLUDED.type,
        tags=EXCLUDED.tags,
        age_min=EXCLUDED.age_min,
        age_max=EXCLUDED.age_max,
        link=EXCLUDED.link,
        thumb=EXCLUDED.thumb,
        embedding=EXCLUDED.embedding
  `;

  const p = [
    row.id,
    row.title,
    row.description || null,
    row.type,
    row.tags || [],
    row.age_min ?? null,
    row.age_max ?? null,
    row.link || null,
    row.thumb || null,
    vectorLiteral, // IMPORTANT: pass the square-bracket literal
  ];

  if (!DRY_RUN) await pg.query(sql, p);
}


async function run(){
  const pg = new Client({ connectionString: DATABASE_URL, ssl });
  await pg.connect();

  // BOOKS
  for (const cat of bookCategories()){
    for (let page=1; page<=5; page++){
      const data = await fetchBooksByCategory(cat, page, 20);
      const list = Array.isArray(data?.items) ? data.items
                 : Array.isArray(data?.results) ? data.results
                 : [];
      for (const it of list){
        const id = it?.id || it?.volumeId;
        if (!id) continue;
        const title = pickTitle(it);
        const textForEmbedding = `${title}\n${pickDescription(it)}`.slice(0, 4000);
        const vector = await embedText(textForEmbedding);

        const row = {
          id,
          title,
          description: pickDescription(it),
          type: "book",
          tags: tagsFromBookCategory(cat),
          age_min: null,
          age_max: null,
          link: pickLinkBook(it),
          thumb: pickThumb(it),
          embedding: vector
        };
        await upsertItem(pg, row);
      }
    }
  }

  // VIDEOS
  for (const topic of videoTopics()){
    for (let page=1; page<=5; page++){
      const data = await fetchVideosByTopic(topic, page, 20);
      const list = Array.isArray(data?.items) ? data.items
                 : Array.isArray(data?.results) ? data.results
                 : [];
      for (const it of list){
        const id = it?.id?.videoId || it?.videoId || it?.id;
        if (!id) continue;
        const title = pickTitle(it);
        const textForEmbedding = `${title}\n${pickDescription(it)}`.slice(0, 4000);
        const vector = await embedText(textForEmbedding);

        const row = {
          id,
          title,
          description: pickDescription(it),
          type: "video",
          tags: tagsFromVideoTopic(topic),
          age_min: null,
          age_max: null,
          link: pickLinkVideo(it),
          thumb: pickThumb(it),
          embedding: vector
        };
        await upsertItem(pg, row);
      }
    }
  }

  await pg.end();
  console.log("Seeding complete.");
}

async function getJSON(url){
  console.log("HTTP GET:", url);
  const r = await fetch(url, { headers: { accept: "application/json" } });
  if (!r.ok) throw new Error(`HTTP ${r.status} ${url}`);
  return r.json();
}


run().catch(err => { console.error(err); process.exit(1); });
