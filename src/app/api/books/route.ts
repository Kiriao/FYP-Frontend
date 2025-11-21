// Parity with original Express /api/books, with early-exit-after-step.
// Env: BOOKS_API_KEY
import { NextRequest, NextResponse } from "next/server";

// Ensure Node runtime so the in-memory pool actually persists
export const runtime = "nodejs";

/* ---------------- Pool cache (per warm instance) ---------------- */
type BookRec = {
  id: string | null;
  title: string;
  authors: string[];
  categories: string[];
  maturityRating: string;
  thumbnail: string | null;
  previewLink: string | null;
  canonicalVolumeLink: string | null;
  infoLink: string | null;
  bestLink: string | null;
  snippet?: string | null;
  description?: string | null; // ← added
  synopsis?: string | null;
  buckets?: string[];
};
type PoolVal = { ts: number; items: BookRec[]; approxTotal: number };

const POOL_TTL_MS = 10 * 60 * 1000; // 10 min
const poolCache = new Map<string, PoolVal>();

function poolGet(key: string): PoolVal | null {
  const hit = poolCache.get(key);
  if (!hit) return null;
  if (Date.now() - hit.ts > POOL_TTL_MS) { poolCache.delete(key); return null; }
  return hit;
}
function poolSet(key: string, items: BookRec[], approxTotal: number) {
  poolCache.set(key, { ts: Date.now(), items, approxTotal });
}

/* ---------------- Fetch with timeout ---------------- */
const FETCH_TIMEOUT_MS = 8000;
async function fetchWithTimeout(url: string, ms = FETCH_TIMEOUT_MS) {
  const ctl = new AbortController();
  const to = setTimeout(() => ctl.abort(), ms);
  try { return await fetch(url, { signal: ctl.signal }); }
  finally { clearTimeout(to); }
}

/* ---------------- Slim Google Books fields ---------------- */
const BOOKS_FIELDS =
  "totalItems," +
  "items(id," +
  "  volumeInfo/title," +
  "  volumeInfo/subtitle," +
  "  volumeInfo/authors," +
  "  volumeInfo/categories," +
  "  volumeInfo/description," + // includes long description
  "  volumeInfo/maturityRating," +
  "  volumeInfo/imageLinks/thumbnail," +
  "  volumeInfo/previewLink," +
  "  volumeInfo/canonicalVolumeLink," +
  "  volumeInfo/infoLink" +
  ")," +
  "items/searchInfo/textSnippet";

function makeBooksUrl(
  qExpr: string,
  { startIndex = 0, maxResults = 40, lang = "", key = "" } = {}
) {
  const u = new URL("https://www.googleapis.com/books/v1/volumes");
  u.searchParams.set("q", qExpr);
  u.searchParams.set("printType", "books");
  u.searchParams.set("orderBy", "relevance");
  u.searchParams.set("maxResults", String(maxResults));
  u.searchParams.set("startIndex", String(startIndex));
  if (lang) u.searchParams.set("langRestrict", lang);
  u.searchParams.set("fields", BOOKS_FIELDS);
  if (key) u.searchParams.set("key", key);
  return u.toString();
}

/* ---------------- Classifier (identical logic) ---------------- */
function assignBuckets(catsLC: string[], textAllLC: string): string[] | undefined {
  const buckets: string[] = [];
  const has = (substr: string) => catsLC.some(c => c.includes(substr));
  const re  = (rx: RegExp) => rx.test(textAllLC);

  if (has("young adult") || re(/\byoung[-\s]?adult\b/)) buckets.push("young_adult");
  if (has("juvenile fiction"))    buckets.push("juvenile_fiction");
  if (has("juvenile nonfiction")) buckets.push("juvenile_nonfiction");
  if (
    has("juvenile literature") || has("children's literature") ||
    catsLC.some(c => /literature|classics?|antholog(y|ies)|folklore|myths?|mythology|fables|fairy tales?/i.test(c) && /children|juvenile|kids?/i.test(c)) ||
    re(/\b(children'?s|juvenile)\s+literature\b/) ||
    re(/\b(classic|classics|great books|canon|folklore|myths?|mythology|fables?|fairy[-\s]?tales?|retold|retelling|antholog(?:y|ies)|reader'?s? theater|novel study|novel studies)\b/)
  ) buckets.push("literature");
  if (
    has("juvenile biography") ||
    (has("biography & autobiography") && (has("juvenile") || re(/\bchildren|juvenile|kids?\b/))) ||
    re(/\bbiograph|autobiograph|life of\b/) ||
    re(/\bwho (?:is|was)\b/)
  ) buckets.push("biography");
  if (
    has("juvenile poetry") || has("juvenile humor") || has("juvenile humour") ||
    re(/\b(poem|poems|poetry|rhyme|rhymes|verse|limerick|jokes?|humou?r|funny|giggle)\b/)
  ) buckets.push("poetry_humor");
  if (
    has("picture") || has("picture book") || has("board book") ||
    has("early reader") || has("beginning reader") ||
    re(/\b(picture book|board book|early reader|beginning reader|leveled reader|sight words?)\b/)
  ) buckets.push("early_readers");
  if (
    has("middle grade") || re(/\bmiddle[-\s]?grade\b/) ||
    re(/\b(ages?\s*(8|9|10|11|12)(?:\s*[-–]\s*1?2)?|grade[s]?\s*(3|4|5|6|7)(?:\s*[-–]\s*(6|7))?|upper\s+elementary|middle\s+school|chapter\s*books?|independent\s*reader|MG\b)\b/)
  ) buckets.push("middle_grade");
  if (
    has("education") || has("study aids") ||
    has("language arts") || has("reading") || has("spelling") || has("handwriting") ||
    has("mathematics") || has("math") || has("algebra") || has("geometry") ||
    has("science") || has("technology") || has("computers") ||
    has("curriculum") || has("schools") ||
    has("activity books") || has("workbooks") || has("study guides") ||
    re(/\b(education|educational|study (?:and )?teaching|curriculum|school|classroom|workbook|activity book|worksheet|phonics|sight words?|STEM|counting|shapes|colors?|alphabet|ABCs?)\b/)
  ) buckets.push("education");
  if (catsLC.some(c => c.startsWith("juvenile"))) buckets.push("juvenile_other");
  return buckets.length ? buckets : undefined;
}

function toRec(v: any, includeYA: boolean): BookRec | null {
  const info = v.volumeInfo || {};
  const rawCats = Array.isArray(info.categories) ? info.categories : [];
  const catsLC  = rawCats.map((s: string) => String(s).toLowerCase());
  const titleLC = (info.title || "").toLowerCase();
  const subtitleLC = (info.subtitle || "").toLowerCase();
  const descLC  = (info.description || "").toLowerCase();
  const textAllLC = `${titleLC} ${subtitleLC} ${descLC}`;
  const buckets = assignBuckets(catsLC, textAllLC);
  const kidSafe = (info.maturityRating || "NOT_MATURE") === "NOT_MATURE";
  if (!kidSafe) return null;
  if (!includeYA && Array.isArray(buckets) && buckets.includes("young_adult")) return null;

  // strip HTML that Google Books sometimes includes
  const descRaw  = info.description || "";
  const descText = String(descRaw).replace(/<[^>]+>/g, "").trim();

  return {
    id: v.id || null,
    title: info.title || "Untitled",
    authors: info.authors || [],
    categories: rawCats,
    maturityRating: info.maturityRating || "UNKNOWN",
    thumbnail: info.imageLinks?.thumbnail || null,
    previewLink: info.previewLink || null,
    canonicalVolumeLink: info.canonicalVolumeLink || null,
    infoLink: info.infoLink || null,
    bestLink: info.previewLink || info.canonicalVolumeLink || info.infoLink ||
              (v.id ? `https://books.google.com/books?id=${encodeURIComponent(v.id)}` : null),
    description: info.description ?? null, 
    snippet: v.searchInfo?.textSnippet ?? null,
    synopsis: info.description ?? null,
    buckets
  };
}


function filterByBucket(list: BookRec[], bucket: string) {
  if (!bucket) return orderByCover(list);
  const arr = list.filter(r => Array.isArray(r.buckets) && r.buckets.includes(bucket));
  return orderByCover(arr);
}
function orderByCover(arr: BookRec[]) {
  const withImg = arr.filter(r => !!r.thumbnail);
  const without = arr.filter(r => !r.thumbnail);
  return withImg.concat(without);
}

/* ---------------- Seeds & helpers (identical) ---------------- */
function variantsFor(qs: string): string[] {
  const parts = qs.split(/\s+/).filter(Boolean);
  const vs = new Set([qs]);
  if (parts.length === 1 && parts[0].length >= 3) {
    vs.add(parts[0] + "s");
    if (/y$/i.test(parts[0])) vs.add(parts[0].replace(/y$/i, "ies"));
  }
  return Array.from(vs);
}
const BASE_SEEDS = [
  'subject:juvenile',
  'subject:"early reader"',
  'subject:"board book"',
  'subject:"picture book"',
  'subject:"children\'s"',
  'subject:"children"',
  'subject:"juvenile fiction"',
  'subject:"juvenile nonfiction"',
];
function bucketTopupSeeds(bucket: string) {
  return {
    education: [
      'subject:education','"study and teaching"','phonics','workbook','worksheet',
      'curriculum','"language arts"','math','science','STEM'
    ],
    poetry_humor:     ['poetry','rhyme','jokes','humor','humour','limerick','verse'],
    biography:        ['biography','"life of"','who was','autobiography'],
    early_readers:    ['"picture book"','"board book"','"early reader"','"leveled reader"','"sight words"'],
    middle_grade:     ['"middle grade"','"chapter book"','"upper elementary"','"middle school"','"ages 8-12"','"ages 9-12"','"grades 3-7"','"grades 4-7"','MG'],
    literature:       ['"children\'s literature"','"juvenile literature"','classic','classics','"fairy tales"','folklore','myths','mythology','fables','anthology','retold','retelling','"novel study"','"novel studies"'],
    juvenile_fiction: ['"juvenile fiction"','kids fiction','children fiction'],
    juvenile_nonfiction: ['"juvenile nonfiction"','kids nonfiction','children nonfiction'],
    young_adult:      ['"young adult"','YA']
  }[bucket] || [];
}

/* ---------------- Handler ---------------- */
export async function GET(req: NextRequest) {
  try {
    const url = new URL(req.url);
    const q     = String(url.searchParams.get("q") ?? "");
    const lang  = String(url.searchParams.get("lang") ?? "");
    const wantBucket = String(url.searchParams.get("bucket") || "").toLowerCase();
    const includeYA  = url.searchParams.get("includeYA") === "1" || wantBucket === "young_adult";
    const page     = Math.max(parseInt(url.searchParams.get("page") || "1", 10), 1);
    const pageSize = Math.min(Math.max(parseInt(url.searchParams.get("pageSize") || "20", 10), 1), 40);
    const debug    = url.searchParams.get("debug") === "1";

    // ---- knobs (parity) ----
    const CONCURRENCY  = 6;
    const CONCURRENCY_TOPUP = 10;
    const BASE_STEPS   = [0, 40];
    const TOPUP_STEPS  = [0, 40, 80];
    const RAW_LIMIT_HARD = 4000;

    // Buffer target for the current response (page slice + headroom)
    const targetCount = page * pageSize + pageSize * 2;

    const baseQueries =
      (q && q.trim())
        ? (() => {
            const vs = variantsFor(q.trim());
            const arr: string[] = [];
            for (const term of vs) {
              arr.push(`${term} subject:juvenile`);
              arr.push(`intitle:${term} subject:juvenile`);
              arr.push(`${term} subject:"children's"`);
              arr.push(`${term} subject:"children"`);
              arr.push(`${term} subject:"picture book"`);
              arr.push(`${term} subject:"early reader"`);
              arr.push(`${term} subject:"board book"`);
              arr.push(`${term} subject:"juvenile fiction"`);
              arr.push(`${term} subject:"juvenile nonfiction"`);
              arr.push(`${term} subject:"children's literature"`);
              arr.push(`${term} subject:"juvenile literature"`);
            }
            return Array.from(new Set(arr));
          })()
        : BASE_SEEDS.slice();

    const BOOKS_API_KEY = process.env.BOOKS_API_KEY || "";
    const mkBooksUrl = (expr: string, startIndex = 0) =>
      makeBooksUrl(expr, { startIndex, maxResults: 40, lang, key: BOOKS_API_KEY });

    // Base pool cache key/version (bumped to force refresh with descriptions)
    const BASE_KEY = JSON.stringify({ q, lang, includeYA, v: "base-pool-v3-lit-mg-desc1" }); // ← bumped

    let base = poolGet(BASE_KEY) || { ts: Date.now(), items: [] as BookRec[], approxTotal: 0 };

    // Count current items for requested bucket (for early-exit checks)
    let wantedCount = wantBucket ? filterByBucket(base.items, wantBucket).length : orderByCover(base.items).length;
    const shouldStop = () => wantedCount >= targetCount;

    const onRec = (rec: BookRec) => {
      if (wantBucket) {
        if (rec.buckets?.includes(wantBucket)) wantedCount++;
      } else {
        wantedCount++;
      }
    };

    async function runBatch(urls: string[]) {
      const results = await Promise.allSettled(
        urls.map(u => fetchWithTimeout(u).then(r => r.ok ? r.json() : { items: [], totalItems: 0 }))
      );
      return results.map(r => (r.status === "fulfilled" ? r.value : { items: [], totalItems: 0 })) as any[];
    }

    // Deepen across base queries; EARLY-EXIT only after completing each STEP
    async function deepenBasePool(steps: number[]) {
      const seen = new Set(base.items.map(x => x.id));
      let approxTotal = base.approxTotal || 0;

      for (const step of steps) {
        const urls: string[] = [];
        for (const qExpr of baseQueries) urls.push(mkBooksUrl(qExpr, step));

        for (let i = 0; i < urls.length; i += CONCURRENCY) {
          const batch = urls.slice(i, i + CONCURRENCY);
          const datas = await runBatch(batch);
          for (const data of datas) {
            if (Number.isFinite(data.totalItems)) approxTotal = Math.max(approxTotal, data.totalItems);
            const items = Array.isArray(data.items) ? data.items : [];
            for (const v of items) {
              if (!v || !v.id || seen.has(v.id)) continue;
              const rec = toRec(v, includeYA);
              if (!rec) continue;
              base.items.push(rec);
              seen.add(v.id);
              onRec(rec);
              if (RAW_LIMIT_HARD && base.items.length >= RAW_LIMIT_HARD) break;
            }
            if (RAW_LIMIT_HARD && base.items.length >= RAW_LIMIT_HARD) break;
          }
          if (RAW_LIMIT_HARD && base.items.length >= RAW_LIMIT_HARD) break;
        }

        if (shouldStop()) break; // after finishing whole STEP
      }
      base.approxTotal = approxTotal;
      poolSet(BASE_KEY, base.items, approxTotal);
    }

    // Bucket-focused top-up; EARLY-EXIT after each STEP
    async function topupBucket(bucket: string) {
      const seeds = bucketTopupSeeds(bucket);
      if (!seeds.length) return;
      const seen = new Set(base.items.map(x => x.id));

      for (const step of TOPUP_STEPS) {
        const urls: string[] = [];
        for (const s of seeds) {
          const expr = (bucket === "literature" || bucket === "middle_grade")
            ? [q, s].filter(Boolean).join(" ")
            : [q, s, 'subject:juvenile'].filter(Boolean).join(" ");
          urls.push(mkBooksUrl(expr, step));
        }

        for (let i = 0; i < urls.length; i += CONCURRENCY_TOPUP) {
          const batch = urls.slice(i, i + CONCURRENCY_TOPUP);
          const datas = await runBatch(batch);
          for (const data of datas) {
            const items = Array.isArray(data.items) ? data.items : [];
            for (const v of items) {
              if (!v || !v.id || seen.has(v.id)) continue;
              const rec = toRec(v, includeYA);
              if (!rec) continue;
              base.items.push(rec);
              seen.add(v.id);
              onRec(rec);
              if (RAW_LIMIT_HARD && base.items.length >= RAW_LIMIT_HARD) break;
            }
            if (RAW_LIMIT_HARD && base.items.length >= RAW_LIMIT_HARD) break;
          }
        }

        if (shouldStop()) break; // after finishing whole STEP
      }

      poolSet(BASE_KEY, base.items, base.approxTotal || 0);
    }

    // Build base shallow if missing (warm-up)
    if (!poolGet(BASE_KEY)) {
      base = { ts: Date.now(), items: [], approxTotal: 0 };
      poolSet(BASE_KEY, base.items, base.approxTotal);

      for (const step of BASE_STEPS) {
        const urls: string[] = [];
        for (const qExpr of baseQueries) urls.push(mkBooksUrl(qExpr, step));

        const seen = new Set<string>();
        for (let i = 0; i < urls.length; i += CONCURRENCY) {
          const batch = urls.slice(i, i + CONCURRENCY);
          const datas = await runBatch(batch);
          for (const data of datas) {
            if (Number.isFinite(data.totalItems)) base.approxTotal = Math.max(base.approxTotal, data.totalItems);
            const items = Array.isArray(data.items) ? data.items : [];
            for (const v of items) {
              if (!v || !v.id || seen.has(v.id)) continue;
              const rec = toRec(v, includeYA);
              if (!rec) continue;
              base.items.push(rec);
              seen.add(v.id);
              onRec(rec);
              if (RAW_LIMIT_HARD && base.items.length >= RAW_LIMIT_HARD) break;
            }
            if (RAW_LIMIT_HARD && base.items.length >= RAW_LIMIT_HARD) break;
          }
          if (RAW_LIMIT_HARD && base.items.length >= RAW_LIMIT_HARD) break;
        }
        if (shouldStop()) break;
      }
      poolSet(BASE_KEY, base.items, base.approxTotal);
    }

    // Ensure enough for requested shelf/page
    if (wantBucket) {
      if (filterByBucket(base.items, wantBucket).length < targetCount) {
        await deepenBasePool(TOPUP_STEPS);
      }
      if (filterByBucket(base.items, wantBucket).length < targetCount) {
        await topupBucket(wantBucket);
      }
    } else {
      if (orderByCover(base.items).length < targetCount) {
        await deepenBasePool(TOPUP_STEPS);
      }
    }

    // Final slice & payload
    const start = (page - 1) * pageSize;
    const derived = filterByBucket(base.items, wantBucket);
    const slice  = derived.slice(start, start + pageSize);

    const hasMoreByDerived = derived.length > page * pageSize;
    const hasMoreByApprox  = (base.approxTotal || 0) > page * pageSize;
    const hasMore = hasMoreByDerived || hasMoreByApprox;

    const payload: any = {
      items: slice,
      page, pageSize,
      hasMore,
      totalApprox: base.approxTotal || derived.length,
      baseSize: base.items.length
    };
    if (debug) {
      payload.debug = {
        wantBucket,
        derivedCount: derived.length,
        approxTotal: base.approxTotal || 0,
        baseSize: base.items.length,
        targetCount
      };
    }

    //return NextResponse.json(payload, { headers: { "Cache-Control": "public, max-age=60" } });
    return NextResponse.json(payload, {
    headers: {
    "Cache-Control": "public, max-age=60",
    "x-books-route": "next-books-v1" // ← debug header you can check in DevTools
    }
});

  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    console.error("Books route error:", e);
    return NextResponse.json(
      { error: "Books fetch failed", details: msg },
      { status: 500 }
    );
  }
}
