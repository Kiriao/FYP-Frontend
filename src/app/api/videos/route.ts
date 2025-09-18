// Next.js API route – parity with original Express /api/videos
// Adds `bucket` categories: stories | songs | learning | science | math | animals | artcraft
// Env: YOUTUBE_API_KEY
import { NextRequest, NextResponse } from "next/server";

export const runtime = "nodejs";

/* ---------------- tiny caches ---------------- */
const CACHE_TTL_MS = 10 * 60 * 1000; // 10 min (like your books pool)
const pageCache = new Map<string, { ts: number; value: any }>();
function cacheGet(key: string) {
  const hit = pageCache.get(key);
  if (!hit) return null;
  if (Date.now() - hit.ts > CACHE_TTL_MS) { pageCache.delete(key); return null; }
  return hit.value;
}
function cacheSet(key: string, value: any) {
  pageCache.set(key, { ts: Date.now(), value });
}

// YouTube "madeForKids" verdict cache
const kidsVerdictCache = new Map<string, { ts: number; isKids: boolean }>();
function kidsGet(id: string): boolean | null {
  const hit = kidsVerdictCache.get(id);
  if (!hit) return null;
  if (Date.now() - hit.ts > CACHE_TTL_MS) { kidsVerdictCache.delete(id); return null; }
  return hit.isKids;
}
function kidsSet(id: string, isKids: boolean) {
  kidsVerdictCache.set(id, { ts: Date.now(), isKids });
}

/* ---------------- helpers ---------------- */
const FETCH_TIMEOUT_MS = 8000;
async function fetchWithTimeout(url: string, ms = FETCH_TIMEOUT_MS) {
  const ctl = new AbortController();
  const to = setTimeout(() => ctl.abort(), ms);
  try { return await fetch(url, { signal: ctl.signal }); }
  finally { clearTimeout(to); }
}

/* ---------------- constants (parity) ---------------- */
const YOUTUBE_API_KEY = process.env.YOUTUBE_API_KEY || "";
const YT_MAX = 50;
const PAGES_PER_CAT = 2;
const CATS = [27, 24, 10]; // Education, Entertainment, Music

const WHITELIST = new Set<string>([
  "UCbCmjCuTUZos6Inko4u57UQ", // Cocomelon
  "UCPlwvN0w4qFSP1FllALB92w", // Super Simple
  "UCcdwLMPsaU2ezNSJU1nFoBQ", // Pinkfong
  "UC9x0AN7BWHpCDHSm9NiJFJQ", // Blippi
  "UCXJQ-jqFN8JwXvY4x7R5Q2A", // Mother Goose Club
]);

const POS_MUSIC = /\b(nursery|kids?|children'?s|kinder|toddlers?|preschool|rhymes?|lullab(y|ies)|phonics|abcs?|abc song|123|sing[-\s]?along|cocomelon|pinkfong|super simple|little baby bum|kidzbop|peppa pig|blippi|sesame|mother goose)\b/i;
const NEG_MUSIC = /\b(official music video|explicit|vevo|lyrics?|live performance|mtv|remix|tiktok|club|trap|drill|nsfw)\b/i;
const NEG_GENERIC = /\b(prank|challenge|fail compilation|horror|violent|gore|gun|shooting|war|murder|crime|killer|18\+|NSFW)\b/i;

/* ---------------- seeds for shelves (matches your bot) ---------------- */
function videoSeeds(bucket: string, topic: string) {
  const t = (topic || "").trim();
  switch (bucket) {
    case "songs":    return [ `${t} nursery rhymes`, `${t} kids songs`, `${t} abc song` ].filter(Boolean);
    case "stories":  return [ `${t} bedtime story`, `${t} read aloud`, `${t} stories for kids` ].filter(Boolean);
    case "learning": return [ `${t} for kids`, `${t} phonics`, `${t} alphabet for kids` ].filter(Boolean);
    case "science":  return [ `${t} science for kids`, `${t} stem for kids`, `${t} experiment for kids` ].filter(Boolean);
    case "math":     return [ `${t} math for kids`, `${t} counting for kids`, `${t} numbers for kids` ].filter(Boolean);
    case "animals":  return [ `${t} animals for kids`, `${t} dinosaurs for kids`, `${t} wildlife for kids` ].filter(Boolean);
    case "artcraft": return [ `${t} art for kids`, `${t} crafts for kids`, `${t} drawing for kids` ].filter(Boolean);
    default:         return [ `${t} for kids`, `${t} kids`, `${t}` ].filter(Boolean);
  }
}

function mkSearchUrl(categoryId: number, q: string, pageToken = "") {
  const u = new URL("https://www.googleapis.com/youtube/v3/search");
  u.searchParams.set("part", "snippet");
  u.searchParams.set("type", "video");
  u.searchParams.set("videoEmbeddable", "true");
  u.searchParams.set("safeSearch", "strict");
  u.searchParams.set("maxResults", String(YT_MAX));
  u.searchParams.set("q", q);
  u.searchParams.set("videoCategoryId", String(categoryId));
  if (pageToken) u.searchParams.set("pageToken", pageToken);
  if (YOUTUBE_API_KEY) u.searchParams.set("key", YOUTUBE_API_KEY);
  return u.toString();
}

async function getMadeForKidsSet(ids: string[]) {
  const out = new Set<string>();
  const unknown: string[] = [];

  for (const id of ids) {
    const k = kidsGet(id);
    if (k === true) out.add(id);
    if (k === null) unknown.push(id);
  }

  for (let i = 0; i < unknown.length; i += 50) {
    const slice = unknown.slice(i, i + 50);
    if (!slice.length) continue;
    const u = new URL("https://www.googleapis.com/youtube/v3/videos");
    u.searchParams.set("part", "status");
    u.searchParams.set("id", slice.join(","));
    if (YOUTUBE_API_KEY) u.searchParams.set("key", YOUTUBE_API_KEY);
    const r = await fetchWithTimeout(u.toString());
    if (!r.ok) continue;
    const data = await r.json();
    for (const item of (data.items || [])) {
      const isKids = !!item?.status?.madeForKids;
      if (isKids) out.add(item.id);
      kidsSet(item.id, isKids);
    }
  }
  return out;
}

/* ---------------- core collector (parity with original) ---------------- */
async function collectVideosForQuery(q: string, page: number, pageSize: number) {
  const seen = new Set<string>();
  const keptRaw: Array<{
    title: string; channel: string; channelId: string;
    videoId: string; thumbnail: string | null; publishedAt: string; url: string | null;
    categoryHint: number;
  }> = [];

  const start = (page - 1) * pageSize;
  const BUFFER = 60;
  const targetRaw = start + pageSize + BUFFER;

  let reachedEndAll = true;

  for (const cat of CATS) {
    let token = "";
    let reachedEndThisCat = true;

    for (let p = 0; p < PAGES_PER_CAT; p++) {
      const r = await fetchWithTimeout(mkSearchUrl(cat, q, token));
      if (!r.ok) break;
      const data = await r.json();
      const items = Array.isArray(data.items) ? data.items : [];

      token = data.nextPageToken || "";
      if (token) reachedEndThisCat = false;

      for (const it of items) {
        const s = it.snippet || {};
        const id = it.id?.videoId || "";
        if (!id || seen.has(id)) continue;
        seen.add(id);

        const channelId = s.channelId || "";
        const title = (s.title || "").trim();
        const desc  = (s.description || "").trim();
        const text  = `${title} ${desc}`;

        // global unsafe
        if (NEG_GENERIC.test(text)) continue;

        // music stricter
        if (cat === 10 && !WHITELIST.has(channelId)) {
          if (!POS_MUSIC.test(text)) continue;
          if (NEG_MUSIC.test(text)) continue;
        }

        keptRaw.push({
          title: title || "Untitled",
          channel: s.channelTitle || "",
          channelId,
          videoId: id,
          thumbnail: s.thumbnails?.medium?.url || s.thumbnails?.default?.url || null,
          publishedAt: s.publishedAt || "",
          url: id ? `https://youtu.be/${id}` : null,
          categoryHint: cat
        });

        if (keptRaw.length >= targetRaw) break;
      }

      if (keptRaw.length >= targetRaw) break;
      if (!token) break;
    }

    if (!reachedEndThisCat) reachedEndAll = false;
    if (keptRaw.length >= targetRaw) break;
  }

  // Kids-only filter using Videos API
  const ids = keptRaw.map(v => v.videoId);
  const kidsSetIds = await getMadeForKidsSet(ids);
  const kept = keptRaw.filter(v => kidsSetIds.has(v.videoId));

  const pageItems = kept.slice(start, start + pageSize);
  const hasMore = kept.length > page * pageSize || !reachedEndAll;

  return { items: pageItems, hasMore };
}

/* ---------------- main handler ---------------- */
export async function GET(req: NextRequest) {
  try {
    const url = new URL(req.url);
    const bucket = (url.searchParams.get("bucket") || "").toLowerCase(); // stories, songs, ...
    const topic  = String(url.searchParams.get("q") || "").trim();       // optional, narrows the shelf
    const page   = Math.max(parseInt(url.searchParams.get("page") || "1", 10), 1);
    const pageSize = Math.min(Math.max(parseInt(url.searchParams.get("pageSize") || "20", 10), 1), 20);

    const cacheKey = `videos|${JSON.stringify({ bucket, topic, page, pageSize })}`;
    const cached = cacheGet(cacheKey);
    if (cached) {
      return NextResponse.json(cached, { headers: { "Cache-Control": "public, max-age=60" } });
    }

    // Try seeds for the shelf until we have results
    const seeds = videoSeeds(bucket, topic);
    let best: { items: any[]; hasMore: boolean } = { items: [], hasMore: false };

    for (const q of seeds.length ? seeds : ["stories for kids"]) {
      const data = await collectVideosForQuery(q, page, pageSize);
      if (data.items.length) { best = data; break; }
    }

    // Fallbacks if shelf+topic is too thin
    if (!best.items.length && !topic) {
      const defaults = bucket === "songs"
        ? ["nursery rhymes", "kids songs"]
        : bucket === "stories"
        ? ["bedtime stories", "stories for kids"]
        : ["stories for kids", "learning for kids"];
      for (const q of defaults) {
        const data = await collectVideosForQuery(q, page, pageSize);
        if (data.items.length) { best = data; break; }
      }
    }

    const payload = {
      items: best.items,
      page, pageSize,
      hasMore: best.hasMore,
      bucket,
    };

    cacheSet(cacheKey, payload);
    return NextResponse.json(payload, { headers: { "Cache-Control": "public, max-age=60" } });

  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    console.error("Videos route error:", e);
    return NextResponse.json({ error: "Videos fetch failed", details: msg }, { status: 500 });
  }
}
