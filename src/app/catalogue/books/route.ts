import { NextRequest, NextResponse } from "next/server";

const BOOKS_API_KEY = process.env.BOOKS_API_KEY || "";

function variantsFor(q: string) {
  const parts = q.split(/\s+/).filter(Boolean);
  const vs = new Set([q]);
  if (parts.length === 1 && parts[0].length >= 3) {
    vs.add(parts[0] + "s");
    if (/y$/i.test(parts[0])) vs.add(parts[0].replace(/y$/i, "ies"));
  }
  return Array.from(vs);
}

function assignBuckets(rawCats: string[], textAllLC: string) {
  const catsLC = (rawCats || [])
    .flatMap(c => c.split(/\/|:/))
    .map(s => s.trim().toLowerCase());

  const buckets: string[] = [];
  const has = (substr: string) => catsLC.some(c => c.includes(substr));
  const re = (rx: RegExp) => rx.test(textAllLC);

  if (has("young adult") || has("ya") || re(/\b(young[-\s]?adult|ya)\b/i)) buckets.push("young_adult");
  if (has("juvenile fiction")) buckets.push("juvenile_fiction");
  if (has("juvenile nonfiction")) buckets.push("juvenile_nonfiction");
  if (
    has("juvenile literature") ||
    has("children's literature") ||
    has("childrens literature") ||
    re(/\b(children'?s|childrens|juvenile)\s+literature\b/i) ||
    catsLC.some(c => /literature/i.test(c) && /children|childrens|juvenile|kids?/i.test(c))
  ) buckets.push("literature");
  if (
    has("juvenile biography") ||
    (has("biography & autobiography") && (has("juvenile") || re(/\b(children|childrens|juvenile|kids?)\b/i))) ||
    re(/\bbiograph|autobiograph|life of\b/i) ||
    re(/\bwho (?:is|was)\b/i)
  ) buckets.push("biography");
  if (
    has("juvenile poetry") || has("juvenile humor") || has("juvenile humour") ||
    re(/\b(poem|poems|poetry|rhyme|rhymes|verse|limerick|jokes?|humou?r|funny|laugh|giggle)\b/i)
  ) buckets.push("poetry_humor");
  if (
    has("picture") || has("picture book") || has("board book") ||
    has("early reader") || has("beginning reader") ||
    re(/\b(picture book|board book|early reader|beginning reader|leveled reader|sight words?)\b/i)
  ) buckets.push("early_readers");
  if (has("middle grade") || re(/\bmiddle[-\s]?grade\b/i) ||
      re(/\b(ages?\s*8[-–]12|ages?\s*9[-–]12|grade[s]?\s*4[-–]7|age\s*8\s*to\s*12)\b/i)) buckets.push("middle_grade");
  if (has("education") || re(/\b(education|educational|study and teaching|curriculum|phonics|sight words?)\b/i)) buckets.push("education");
  if (catsLC.some(c => c.startsWith("juvenile"))) buckets.push("juvenile_other");

  return buckets.length ? buckets : ["juvenile_other"];
}

export async function GET(req: NextRequest) {
  try {
    const url = new URL(req.url);
    const rawQ = (url.searchParams.get("q") || "").trim();
    const bucket = (url.searchParams.get("shelf") || "").toLowerCase();
    const includeYA = url.searchParams.get("includeYA") === "1" || bucket === "young_adult";
    const page = Math.max(parseInt(url.searchParams.get("page") || "1", 10), 1);
    const pageSize = Math.min(Math.max(parseInt(url.searchParams.get("pageSize") || "20", 10), 1), 40);

    const seedSubjects = [
      'subject:juvenile',
      'subject:"early reader"',
      'subject:"board book"',
      'subject:"picture book"',
      'subject:"children\'s"',
      'subject:"children"',
      'subject:"juvenile fiction"',
      'subject:"juvenile nonfiction"'
    ];

    const queries = rawQ ? variantsFor(rawQ).flatMap(term => [
      `${term} subject:juvenile`,
      `intitle:${term} subject:juvenile`,
      `${term} subject:"children's"`,
      `${term} subject:"children"`,
      `${term} subject:"picture book"`,
      `${term} subject:"early reader"`,
      `${term} subject:"board book"`,
      `${term} subject:"juvenile fiction"`,
      `${term} subject:"juvenile nonfiction"`,
      `${term} subject:"children's literature"`,
      `${term} subject:"juvenile literature"`
    ]) : seedSubjects;

    const seen = new Set<string>();
    const withImgPool: any[] = [];
    const withoutImgPool: any[] = [];
    const GB_MAX = 40;
    const MAX_GB_PAGES = 50;
    const RAW_LIMIT_HARD = 4000;
    let totalRaw = 0;
    let approxTotal = 0;
    const OVERFETCH = Math.max(60, Math.ceil(pageSize * 6));
    const targetCount = page * pageSize + OVERFETCH;

    for (const qExpr of queries) {
      let startIndex = 0;
      for (let p = 0; p < MAX_GB_PAGES; p++) {
        if (totalRaw >= RAW_LIMIT_HARD) break;

        const r = await fetch(
          `https://www.googleapis.com/books/v1/volumes?q=${encodeURIComponent(qExpr)}&printType=books&orderBy=relevance&maxResults=${GB_MAX}${BOOKS_API_KEY ? `&key=${BOOKS_API_KEY}` : ""}&startIndex=${startIndex}`
        );
        if (!r.ok) break;

        const data = await r.json();
        const items = Array.isArray(data.items) ? data.items : [];
        totalRaw += items.length;
        approxTotal = Math.max(approxTotal, data.totalItems || 0);

        for (const v of items) {
          if (!v || !v.id || seen.has(v.id)) continue;
          seen.add(v.id);

          const info = v.volumeInfo || {};
          const rawCats = Array.isArray(info.categories) ? info.categories : [];
          const textAllLC = `${(info.title || "").toLowerCase()} ${(info.subtitle || "").toLowerCase()} ${(info.description || "").toLowerCase()}`;
          const buckets = assignBuckets(rawCats, textAllLC);

          const kidSafe = (info.maturityRating || "NOT_MATURE") === "NOT_MATURE";
          if (!kidSafe) continue;
          if (!includeYA && buckets.includes("young_adult")) continue;
          if (bucket && !buckets.includes(bucket)) continue;

          const rec = {
            id: v.id,
            title: info.title || "Untitled",
            authors: info.authors || [],
            categories: rawCats,
            maturityRating: info.maturityRating || "UNKNOWN",
            thumbnail: info.imageLinks?.thumbnail || null,
            infoLink: info.infoLink || null,
            previewLink: info.previewLink || null,
            canonicalVolumeLink: info.canonicalVolumeLink || null,
            bestLink: info.previewLink || info.canonicalVolumeLink || info.infoLink || (v.id ? `https://books.google.com/books?id=${encodeURIComponent(v.id)}` : null),
            snippet: v.searchInfo?.textSnippet || null,
            buckets
          };

          if (rec.thumbnail) withImgPool.push(rec);
          else withoutImgPool.push(rec);

          if (withImgPool.length + withoutImgPool.length >= targetCount) break;
        }

        if (withImgPool.length + withoutImgPool.length >= targetCount || items.length < GB_MAX) break;
        startIndex += GB_MAX;
      }
      if (withImgPool.length + withoutImgPool.length >= targetCount) break;
    }

    const ordered = withImgPool.concat(withoutImgPool);
    const start = (page - 1) * pageSize;
    const pageItems = ordered.slice(start, start + pageSize);
    const hasMore = ordered.length > page * pageSize || totalRaw < approxTotal;

    return NextResponse.json({ items: pageItems, page, pageSize, hasMore, totalApprox: approxTotal });
  } catch (e: any) {
    console.error(e);
    return NextResponse.json({ error: "Books fetch failed", details: e?.message || String(e) }, { status: 500 });
  }
}
