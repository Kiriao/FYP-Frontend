import { onRequest } from "firebase-functions/v2/https";
import * as logger from "firebase-functions/logger";
import { setGlobalOptions } from "firebase-functions/v2";
import { fetch } from "undici";
import { embedTexts } from "./lib/openai";
import { Pool, QueryResultRow } from "pg";
export { recommendForUser } from "./recommend";
export { moderateMessage } from "./moderation";
import { upsertItems } from "./items";
import { rebuildUserProfile } from "./users";
import { getApps, initializeApp } from "firebase-admin/app";
import { getFirestore } from "firebase-admin/firestore";
import { findRestrictedTerms } from "./lib/restrictions";

/* -------------------------------- bootstrap -------------------------------- */
if (!getApps().length) initializeApp();
const db = getFirestore();
setGlobalOptions({ region: "asia-southeast1", timeoutSeconds: 120 });

/* --------------------------------- config --------------------------------- */
const API_BASE = (process.env.APP_API_BASE || "").replace(/\/+$/, "");
const APP_ORIGIN = (process.env.APP_PUBLIC_ORIGIN || "https://kidflix-4cda0.web.app").replace(/\/+$/, "");
const RECOMMENDER_URL =
  process.env.RECOMMENDER_URL ||
  "https://asia-southeast1-kidflix-4cda0.cloudfunctions.net/recommendForUser";

// --------- Default safety blocklist (applies to all child chats) ---------
const DEFAULT_RESTRICTED_TERMS: string[] = [
  "sex",
  "sexual",
  "porn",
  "porno",
  "pornography",
  "xxx",
  "naked",
  "nudity",
  "erotic",
  "bdsm",
  "fetish",
  "rape",
  "rapist",
  "nsfw",
  "18+",
  "adult content",
  "strip",
  "stripping",

  "gun",
  "guns",
  "weapon",
  "weapons",
  "shooting",
  "rifle",
  "pistol",
  "shotgun",
  "bomb",
  "explosive",

  "murder",
  "killer",
  "killing",
  "homicide",
  "gore",
  "gory",
  "torture",
  "beheading",

  "drug",
  "drugs",
  "cocaine",
  "heroin",
  "meth",
  "ecstasy",
  "weed",
  "marijuana",

  "gang",
  "gangs",
  "gangster",
  "mafia",
  "cartel"
];

/* -------------------------------- helpers --------------------------------- */
function isKnownGenreTerm(s?: string) {
  if (!s) return false;
  const canon = normGenre(s);
  return !!GENRE_ALIASES[canon];
}

/** Build 3–5 personalized suggestion chips from profile/history. */
async function buildPersonalizedSuggestions(userId?: string, lang = "en") {
  const suggestions: string[] = [];

  try {
    if (userId) {
      const uref = db.collection("users").doc(userId);
      const usnap = await uref.get();

      // try explicit interests first (array of strings)
      const interests: string[] = Array.isArray(usnap.get("interests")) ? usnap.get("interests") : [];

      // try last categories/topics we saved in chat thread state
      // (we store these in chat_threads via reply())
      const historySnap = await db.collection("chat_threads")
        .where("updatedAt", ">=", Date.now() - 30*24*3600*1000)
        .limit(10).get();

      const recentTags = new Set<string>();
      historySnap.forEach(doc => {
        const d = doc.data() || {};
        if (typeof d.category === "string" && d.category) recentTags.add(d.category);
        if (typeof d.topic === "string" && d.topic) recentTags.add(d.topic);
        if (typeof d.genre === "string" && d.genre) recentTags.add(d.genre);
      });

      // compose a candidate pool (interests first, then recents)
      const pool = [...interests, ...Array.from(recentTags)];

      // simple canonicalization & de-dup
      const uniq = Array.from(new Set(pool
        .map(s => s.trim())
        .filter(Boolean)
        .slice(0, 10)));

      // turn into natural asks for kids
      for (const t of uniq) {
        const canon = t.toLowerCase();
        if (canon.includes("video")) suggestions.push(`${t}`);
        else suggestions.push(`${t} books`);
        if (suggestions.length >= 5) break;
      }
    }
  } catch { /* non-fatal */ }

  // as a final safety, add some generic-but-useful kid topics
  if (suggestions.length < 3) {
    ["animal stories", "space books", "science videos", "mystery books", "math videos"]
      .forEach(s => { if (!suggestions.includes(s)) suggestions.push(s); });
  }
  return suggestions.slice(0, 5);
}

function unquote(s: string) { return s.replace(/^["']|["']$/g, "").trim(); }

type BookQueryResolution =
  | { mode: "category"; canon: Canon; display: string }
  | { mode: "author";  q: string;   display: string }
  | { mode: "title";   q: string;   display: string }
  | { mode: "topic";   q: string;   display: string };

// Render a numbered list like "1. Title (book/video)" for the first n items
function asNumbered(
  items: Array<{ title: string; type?: "book" | "video"; kind?: "book" | "video" }>,
  n = 5
): string {
  return items.slice(0, n)
    .map((it, i) => {
      const k = (it.type || it.kind) === "video" ? "video" : "book";
      return `${i + 1}. ${it.title} (${k})`;
    })
    .join("\n");
}

function readAuthorParam(p: any, utterance: string): string {
  // DF may send @sys.person as a string or an object { name, givenName, ... }
  const cand =
    (typeof p?.author === "string" && p.author) ||
    (typeof p?.author?.name === "string" && p.author.name) ||
    (typeof p?.person === "string" && p.person) ||
    (typeof p?.person?.name === "string" && p.person.name) ||
    "";

  if (cand) return cand.trim();

  // last-resort: pull “… books by|from X” or “X books”
  const ql = (utterance || "").toLowerCase().trim();
  const mBy   = ql.match(/\bbooks?\s+(?:by|from)\s+([\p{L}\p{N}\s.\-'"&]+)$/u);
  const mTail = ql.match(/^([\p{L}\p{N}\s.\-'"&]+)\s+books?$/u);
  return (mBy?.[1] || mTail?.[1] || "").trim();
}

// ---- NEW (global): author candidate extractor (forgiving) ----
function extractAuthorCandidate(q: string): string {
  const ql = (q || "").toLowerCase().trim();

  // patterns: "books by X", "books from X"
  const mBy = ql.match(/\bbooks?\s+(?:by|from)\s+([\p{L}\p{N}\s.\-'"&]+)$/u);
  if (mBy) return (mBy[1] || "").trim();

  // "X books"
  const mTail = ql.match(/^([\p{L}\p{N}\s.\-'"&]+)\s+books?$/u);
  if (mTail) return (mTail[1] || "").trim();

  // bare name fallback if user typed two+ tokens and the intent/tag says books
  const mBare = ql.match(/^([a-z][a-z'.\-]+(?:\s+[a-z'.\-]+){1,3})$/i);
  if (mBare) return (mBare[1] || "").trim();

  return "";
}

// --- helpers
function ensureAnonId(req: any): string {
  // stable device ID for signed-out users
  const h = req.headers["x-device-id"] as string | undefined;
  return h &&
  h.length > 10 ? h : hashKey((req.ip || "") + (req.headers["user-agent"] || "guest"));
}

function currentUserId(req: any, params: any): string {
  const hdr = (req.headers["x-user-id"] as string | undefined)?.trim() || "";
  const param = (typeof params?.userId === "string" ? params.userId : "").trim();
  const uid = hdr || param;
  return uid ? `uid:${uid}` : `anon:${ensureAnonId(req)}`;
}


function threadKey(u: string, session?: string) {
  // 1 user can have many threads (tabs/devices)
  const s = (session && String(session).slice(-36)) || "default";
  return `${u}::${s}`;
}

async function loadThreadState(u: string, s: string) {
  const ref = db.collection("chat_threads").doc(threadKey(u, s));
  const snap = await ref.get();
  return snap.exists ? snap.data() : {};
}
async function saveThreadState(u: string, s: string, patch: Record<string, any>) {
  const ref = db.collection("chat_threads").doc(threadKey(u, s));
  await ref.set({ ...patch, updatedAt: Date.now() }, { merge: true });
}

/** Decide whether a free text like "nonfiction" is category/title/author/topic. */
async function resolveBookQuery(term: string, lang: string): Promise<BookQueryResolution> {
  const raw = (term || "").trim();
  if (!raw) return { mode: "topic", q: "", display: "" };

  // 1) category?
  if (isKnownGenreTerm(raw)) {
    const canon = normGenre(raw);
    return { mode: "category", canon, display: raw };
  }

  // 2) quoted text biases to title
  const quoted = /^["'].*["']$/.test(raw);
  const clean = unquote(raw);

  // 3) probe author vs title (1 small page each)
  const [aProbe, tProbe] = await Promise.all([
    fetchBooksBySearch(`inauthor:"${clean}"`, { page: 1, pageSize: 8, lang }),
    fetchBooksBySearch(`intitle:"${clean}"`,  { page: 1, pageSize: 8, lang }),
  ]);
  const aN = (aProbe.items || []).length;
  const tN = (tProbe.items || []).length;

  // thresholds: need at least a few, and 20% lead
  if (aN >= 3 && aN >= Math.max(3, tN * 1.1)) return { mode: "author", q: `inauthor:"${clean}"`, display: clean };
  if (tN >= 4 && (tN >= aN * 1.2 || quoted)) return { mode: "title", q: `intitle:"${clean}"`, display: clean };

  // 4) default → topic free text
  return { mode: "topic", q: clean, display: clean };
}

/** Lowercase + remove helper verbs/stopwords commonly seen in “recommend me books”. */
function normalizeForIntent(s?: string) {
  if (!s) return "";
  return s
    .toLowerCase()
    .replace(/\u00a0/g, " ")
    .replace(
      /\b(recommend|suggest|show|find|search|give|tell|list|want|need|like|some|any|pls|please|for|from|by|kids|kid|children|child|me|us|the|a|an|on|about|regarding|around)\b/g,
      " "
    )
    .replace(/[^\p{L}\p{N}\s]+/gu, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/** True if user basically asked only for “books” or “videos” (after stripping helpers). */
function isGenericAsk(raw?: string): { books: boolean; videos: boolean; generic: boolean } {
  const t = normalizeForIntent(raw);
  const books = t === "book" || t === "books";
  const videos = t === "video" || t === "videos";
  return { books, videos, generic: books || videos || t === "" };
}

function isLowInfo(text?: string) {
  const t = (text || "").trim();
  if (!t) return true;

  const norm = normalizeForIntent(t);

  // NEW: known book/video genres (e.g., "fiction", "nonfiction", "education") are informative
  if (isKnownGenreTerm(norm)) return false;

  const words = norm.split(/\s+/).filter(Boolean);
  if (words.length <= 1) return true;
  if (norm.length <= 3) return true;

  const uniq = new Set(norm.replace(/\s+/g, "").split(""));
  return uniq.size <= 3;
}


/** Make “topic for kids” bias if user didn’t already say it. */
function ensureForKids(s: string) {
  return /\bfor\s+kids\b/i.test(s) ? s : `${s} for kids`;
}


function httpsify(u?: string | null): string | null {
  if (!u) return null;
  try {
    const url = new URL(u);
    url.protocol = "https:";
    if (/^books\.google\./i.test(url.hostname) && url.pathname.startsWith("/books/content")) {
      url.hostname = "books.google.com";
    }
    return url.toString();
  } catch {
    return u.replace(/^http:\/\//i, "https://");
  }
}

function getJSON<T = any>(url: string): Promise<T> {
  return fetch(url, { headers: { accept: "application/json" } }).then(async r => {
    if (!r.ok) throw new Error(`HTTP ${r.status} for ${url}`);
    return (await r.json()) as T;
  });
}

function hashKey(s: string): string {
  let h = 0, i = 0;
  while (i < s.length) h = (h * 31 + s.charCodeAt(i++)) | 0;
  return String(h >>> 0);
}


const pickItems = (d: any): any[] =>
  Array.isArray(d) ? d :
  Array.isArray(d?.items) ? d.items :
  Array.isArray(d?.results) ? d.results : [];

const pickTitle = (x: any) => x?.title || x?.name || x?.volumeInfo?.title || x?.snippet?.title || "";
const pickAuthorsArray = (x: any): string[] =>
  Array.isArray(x?.authors) ? x.authors :
  Array.isArray(x?.volumeInfo?.authors) ? x.volumeInfo.authors : [];
const pickDescription = (x: any) =>
  x?.description || x?.snippet || x?.volumeInfo?.description || x?.searchInfo?.textSnippet || "";

const looksLikePlaceholder = (s?: any) =>
  typeof s === "string" &&
  (/^\s*\$intent\.params/i.test(s) || /^\s*\$page\.params/i.test(s) || /^\s*\$session\.params/i.test(s));

const clean = (s?: any): string => {
  if (s == null) return "";
  if (typeof s !== "string") return String(s ?? "");
  const t = s.trim();
  if (!t || t === "null" || t === "undefined" || t === '""' || t === "''" || looksLikePlaceholder(t)) return "";
  return t;
};

const idForBook = (x: any): string | null =>
  x?.id || x?.volumeId || x?.volumeInfo?.industryIdentifiers?.[0]?.identifier || null;
const idForVideo = (x: any): string | null =>
  x?.id?.videoId || x?.videoId || null;

function pickThumb(x: any): string | null {
  if (x?.thumbnail) return httpsify(x.thumbnail);
  if (x?.volumeInfo?.imageLinks?.thumbnail) return httpsify(x.volumeInfo.imageLinks.thumbnail);
  if (x?.snippet?.thumbnails?.medium?.url) return httpsify(x.snippet.thumbnails.medium.url);
  if (x?.snippet?.thumbnails?.default?.url) return httpsify(x.snippet.thumbnails.default.url);
  return null;
}

function pickLinkBook(x: any): string | null {
  if (x?.bestLink) return httpsify(x.bestLink);
  if (x?.previewLink) return httpsify(x.previewLink);
  if (x?.canonicalVolumeLink) return httpsify(x.canonicalVolumeLink);
  if (x?.infoLink) return httpsify(x.infoLink);
  const v = x?.volumeInfo;
  return httpsify(v?.previewLink || v?.canonicalVolumeLink || v?.infoLink || null);
}

function pickLinkVideo(x: any): string | null {
  if (x?.url) return httpsify(x.url);
  const vid = x?.id?.videoId || x?.videoId;
  return vid ? `https://www.youtube.com/watch?v=${vid}` : null;
}

function makeInfoCard(title: string, subtitle: string | null, img: string | null, href: string | null) {
  const card: any = { type: "info", title: title || "Untitled" };
  if (subtitle) card.subtitle = subtitle;
  if (img) card.image = { rawUrl: img };
  if (href) card.actionLink = href;
  return card;
}

function buildPreviewLink(kind: "book" | "video", data: Record<string, string | number | null | undefined>) {
  const u = new URL(`${APP_ORIGIN}/preview`);
  u.searchParams.set("type", kind);
  for (const [k, v] of Object.entries(data)) {
    if (v != null && v !== "") u.searchParams.set(k, String(v));
  }
  return u.toString();
}
const dfLink = (kind: "book" | "video", raw: any, extra: Record<string, string | number | null | undefined>) => {
  const id = kind === "book" ? idForBook(raw) : idForVideo(raw);
  const link = kind === "book" ? (pickLinkBook(raw) || "") : (pickLinkVideo(raw) || "");
  return buildPreviewLink(kind, { id, title: pickTitle(raw), image: pickThumb(raw) || "", link, ...extra });
};


/* ----------------------------- genres & topics ----------------------------- */
type Canon =
  | "all" | "fiction" | "nonfiction" | "education" | "children_literature"
  | "picture_board_early" | "middle_grade" | "poetry_humor" | "biography" | "other_kids" | "young_adult"
  | "stories" | "songs_rhymes" | "learning" | "science" | "math" | "animals" | "art_crafts"
  | string;

const GENRE_ALIASES: Record<string, Canon> = {
  "all":"all","fiction":"fiction","fiction book":"fiction","fiction books":"fiction",
  "non fiction":"nonfiction","non-fiction":"nonfiction","nonfiction":"nonfiction","non fiction book":"nonfiction","nonfiction book":"nonfiction",
  "education":"education","educational":"education",
  "children s literature":"children_literature","childrens literature":"children_literature",
  "picture board early":"picture_board_early","picture books":"picture_board_early","board books":"picture_board_early","early reader":"picture_board_early","early readers":"picture_board_early",
  "middle grade":"middle_grade","poetry humor":"poetry_humor","poetry & humor":"poetry_humor","funny":"poetry_humor",
  "biography":"biography","other kids":"other_kids","young adult":"young_adult","ya":"young_adult",
  "stories":"stories","story":"stories","songs rhymes":"songs_rhymes","song":"songs_rhymes","songs":"songs_rhymes","nursery rhymes":"songs_rhymes",
  "learning":"learning","learning videos":"learning","science":"science","stem":"science",
  "math":"math","mathematics":"math","animals":"animals","wildlife":"animals","pets":"animals",
  "art crafts":"art_crafts","arts crafts":"art_crafts","art and crafts":"art_crafts","art & crafts":"art_crafts",
  "space":"science","fantasy":"fiction","mystery":"fiction","coding":"education","programming":"education"
};

function normTag(s: any): string {
  return String(s ?? "")
    .replace(/\u00A0/g, " ")
    .toLowerCase()
    .trim()
    .replace(/\s+/g, " ")
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}
function normGenre(raw?: string): Canon {
  if (!raw) return "";
  const k = String(raw).toLowerCase().replace(/&/g, " and ").replace(/[^a-z0-9]+/g, " ").replace(/\s+/g, " ").trim();
  return GENRE_ALIASES[k] ?? k;
}
function mapAgeToGroup(n?: number | string) {
  const v = Number(n);
  if (!Number.isFinite(v)) return "";
  if (v <= 5) return "3-5"; if (v <= 8) return "6-8"; if (v <= 12) return "9-12"; return "13-15";
}

/* ----------------------------- modes & parsing ----------------------------- */
type SearchMode = "ann" | "personalized";

function isTruthy(x: any) {
  if (typeof x === "boolean") return x;
  if (typeof x === "string") return /^(1|true|yes|on)$/i.test(x.trim());
  if (typeof x === "number") return x !== 0;
  return false;
}
function getSearchMode(params: any): SearchMode {
  const raw = (params?.mode ?? params?.Mode ?? params?.search_mode ?? params?.searchMode ?? "")
    .toString().toLowerCase().trim();
  const personalizedFlag =
    isTruthy(params?.personalize) || isTruthy(params?.personalised) ||
    isTruthy(params?.personalized) || isTruthy(params?.use_profile);
  if (raw === "personalized" || personalizedFlag) return "personalized";
  if (["ann","vector","unbiased"].includes(raw)) return "ann";
  return "ann";
}

/* ----------------------- topic matching (strict filter) -------------------- */
function topicVariants(raw: string): string[] {
  const t = (raw || "").trim().toLowerCase();
  if (!t) return [];
  const out = new Set<string>([t]);
  if (t.endsWith("s")) out.add(t.slice(0, -1)); else out.add(`${t}s`);
  if (t === "dinosaur" || t === "dinosaurs") {
    ["dino","dinos","t. rex","trex","tyrannosaurus","triceratops","stegosaurus","paleontolog"].forEach(s => out.add(s));
  }
  return Array.from(out);
}
const containsAny = (hay?: string, needles: string[] = []) => !!hay && needles.some(n => hay.toLowerCase().includes(n));
const bookMatchesTopic = (raw: any, topic: string) =>
  containsAny(pickTitle(raw), topicVariants(topic)) ||
  containsAny(pickAuthorsArray(raw).join(", "), topicVariants(topic)) ||
  containsAny(pickDescription(raw), topicVariants(topic));
const videoMatchesTopic = (raw: any, topic: string) =>
  containsAny(pickTitle(raw), topicVariants(topic)) ||
  containsAny(raw?.description || raw?.snippet?.description || "", topicVariants(topic));

/* ------------------------------ queries/maps ------------------------------- */
function bookQueryFor(canon: Canon): { term: string; juvenile: boolean } {
  switch (canon) {
    case "all": return { term: "children books", juvenile: true };
    case "fiction": return { term: "juvenile fiction", juvenile: true };
    case "nonfiction": return { term: "juvenile nonfiction", juvenile: true };
    case "education":
    case "learning": return { term: "education for children", juvenile: true };
    case "children_literature": return { term: "children's literature", juvenile: true };
    case "picture_board_early": return { term: "picture books", juvenile: true };
    case "middle_grade": return { term: "middle grade", juvenile: true };
    case "poetry_humor": return { term: "children poetry humor", juvenile: true };
    case "biography": return { term: "biography for children", juvenile: true };
    case "other_kids": return { term: "children books", juvenile: true };
    case "young_adult": return { term: "young adult", juvenile: false };
    default: return { term: String(canon || "children books"), juvenile: true };
  }
}
function videoQueryFor(canon: Canon): string {
  switch (canon) {
    case "stories": return "bedtime stories for kids";
    case "songs_rhymes": return "nursery rhymes kids songs";
    case "learning": return "educational videos for kids";
    case "science": return "science for kids";
    case "math": return "math for kids";
    case "animals": return "animals for kids";
    case "art_crafts": return "arts and crafts for kids";
    default: return String(canon || "kids");
  }
}

/* ------------------------------- fetchers ---------------------------------- */
async function fetchBooksByCategory(
  canon: Canon,
  opts: { startIndex: number; lang?: string; age?: any; ageGroup?: any }
) {
  if (API_BASE) {
    try {
      const u = new URL(`${API_BASE}/api/books`);
      const { term } = bookQueryFor(canon);
      u.searchParams.set("q", term);
      u.searchParams.set("query", term);
      u.searchParams.set("category", String(canon));
      if (opts.age) u.searchParams.set("age", String(opts.age));
      if (opts.ageGroup) u.searchParams.set("ageGroup", String(opts.ageGroup));
      if (opts.lang) u.searchParams.set("lang", String(opts.lang));
      u.searchParams.set("limit", "6");
      u.searchParams.set("offset", String(opts.startIndex));
      u.searchParams.set("debug", "1");
      const data: any = await getJSON(u.toString());
      return { items: pickItems(data), usedUrl: u.toString(), source: "app" as const };
    } catch (e) {
      logger.warn("App /api/books failed; fallback to Google Books", { e: String(e) });
    }
  }
  const { term, juvenile } = bookQueryFor(canon);
  const g = new URL("https://www.googleapis.com/books/v1/volumes");
  g.searchParams.set("q", `${term}${juvenile ? " subject:juvenile" : ""}`);
  if (opts.lang) g.searchParams.set("langRestrict", String(opts.lang));
  g.searchParams.set("maxResults", "6");
  g.searchParams.set("startIndex", String(opts.startIndex));
  if (process.env.BOOKS_API_KEY) g.searchParams.set("key", process.env.BOOKS_API_KEY);
  const data: any = await getJSON(g.toString());
  return { items: Array.isArray(data?.items) ? data.items : [], usedUrl: g.toString(), source: "google_books" as const };
}

async function fetchBooksBySearch(term: string, opts: { page: number; pageSize: number; lang?: string }) {
  const q = term.trim();
  if (API_BASE) {
    const u = new URL(`${API_BASE}/api/books`);
    u.searchParams.set("q", q);
    if (opts.lang) u.searchParams.set("lang", String(opts.lang));
    u.searchParams.set("page", String(opts.page));
    u.searchParams.set("pageSize", String(opts.pageSize));
    u.searchParams.set("includeYA", "1");
    u.searchParams.set("debug", "1");
    u.searchParams.set("ts", String(Date.now()));
    const data: any = await getJSON(u.toString());
    return { items: pickItems(data), usedUrl: u.toString(), source: "app" as const };
  }
  const g = new URL("https://www.googleapis.com/books/v1/volumes");
  g.searchParams.set("q", q);
  g.searchParams.set("printType", "books");
  g.searchParams.set("orderBy", "relevance");
  g.searchParams.set("maxResults", String(opts.pageSize));
  g.searchParams.set("startIndex", String((opts.page - 1) * opts.pageSize));
  if (opts.lang) g.searchParams.set("langRestrict", String(opts.lang));
  if (process.env.BOOKS_API_KEY) g.searchParams.set("key", process.env.BOOKS_API_KEY);
  const data: any = await getJSON(g.toString());
  return { items: Array.isArray(data?.items) ? data.items : [], usedUrl: g.toString(), source: "google_books" as const };
}

async function fetchVideosByTopic(
  topic: Canon,
  opts: { startIndex: number; lang?: string; pageToken?: string | null; freeQuery?: string | null }
) {
  const q = opts.freeQuery ? String(opts.freeQuery) : videoQueryFor(topic);
  if (API_BASE) {
    try {
      const u = new URL(`${API_BASE}/api/videos`);
      u.searchParams.set("q", q);
      u.searchParams.set("query", q);
      u.searchParams.set("topic", String(opts.freeQuery ? (opts.freeQuery || topic) : topic));
      if (opts.lang) u.searchParams.set("lang", String(opts.lang));
      u.searchParams.set("limit", "6");
      u.searchParams.set("offset", String(opts.startIndex));
      u.searchParams.set("debug", "1");
      if (opts.pageToken) u.searchParams.set("pageToken", String(opts.pageToken));
      const data: any = await getJSON(u.toString());
      return { items: pickItems(data), usedUrl: u.toString(), source: "app" as const, nextPageToken: (data?.nextPageToken as string) || null };
    } catch (e) {
      logger.warn("App /api/videos failed; fallback to YouTube", { e: String(e) });
    }
  }
  const y = new URL("https://www.googleapis.com/youtube/v3/search");
  y.searchParams.set("part", "snippet");
  y.searchParams.set("type", "video");
  y.searchParams.set("videoEmbeddable", "true");
  y.searchParams.set("safeSearch", "strict");
  y.searchParams.set("maxResults", "6");
  y.searchParams.set("q", q);
  if (opts.pageToken) y.searchParams.set("pageToken", String(opts.pageToken));
  if (process.env.YOUTUBE_API_KEY) y.searchParams.set("key", process.env.YOUTUBE_API_KEY);
  const data: any = await getJSON(y.toString());
  return {
    items: Array.isArray(data?.items) ? data.items : [],
    usedUrl: y.toString(),
    source: "youtube" as const,
    nextPageToken: (data?.nextPageToken as string) || null
  };
}

/* ------------------------------ CX reply shim ------------------------------ */
function reply(res: any, text: string, extras: Record<string, any> = {}, payload?: any) {

    try {
    if (res.headersSent) {
      try {
        logger.warn("REPLY_SKIPPED_HEADERS_SENT", {
          preview: String(text || "").slice(0, 80),
        });
      } catch {}
      return;
    }
  } catch {}

  try { res.setHeader?.("Content-Type", "application/json"); } catch {}

  // --- persist per user+session ---
  try {
    // NOTE: set earlier in cxWebhook before calling reply()
    const sessionId = (globalThis as any).__kidflix_sessionId as string || "default";
    const u = (globalThis as any).__kidflix_userKey as string || "anon:device";

    const resolvedUserId =
    typeof (extras as any).userId === "string" && (extras as any).userId
    ? (extras as any).userId
    : ((globalThis as any).__kidflix_userId as string | null) || undefined;

    // choose what we store (keep it small)
    const toStore: Record<string, any> = {
      userId: resolvedUserId,
      last_list: extras.last_list ?? undefined,
      last_algo: extras.last_algo ?? undefined,
      seen_ids: Array.isArray(extras.seen_ids) ? extras.seen_ids.slice(-200) : undefined,
      seen_video_ids: Array.isArray(extras.seen_video_ids) ? extras.seen_video_ids.slice(-200) : undefined,
      next_offset: typeof extras.next_offset === "number" ? extras.next_offset : undefined,
      last_video_page_token: extras.last_video_page_token ?? undefined,
      category: extras.category ?? undefined,
      topic: extras.topic ?? undefined,
      genre: extras.genre ?? undefined,
    };

    // drop undefined keys
    const pruned = Object.fromEntries(
      Object.entries(toStore).filter(([, v]) => v !== undefined)
    ) as Record<string, any>;

    // fire & forget
    saveThreadState(u, sessionId, pruned).catch(() => {});
  } catch {}

  const messages: any[] = [{ text: { text: [text] } }];
  if (payload) messages.push({ payload });

  res.status(200).json({
    fulfillment_response: { messages },
    sessionInfo: { parameters: { ...extras } }
  });
}

/* --------------------------------- copy ----------------------------------- */
const WELCOME_LINE =
  `Hi! I’m Kidflix Assistant 👋. I can help to recommend books or videos for kids. ` +
  `Would you like to start with Books or Videos?`;

const PROMPT_BOOKS =
  `Which book category are you after? (Fiction, Non Fiction, Education, Children’s Literature, ` +
  `Picture/Board/Early, Middle Grade, Poetry & Humor, Biography, Young Adult)`;

const PROMPT_VIDEOS =
  `What kind of videos are you looking for? (Stories, Songs & Rhymes, Learning, Science, Math, Animals, Art & Crafts)`;

/* ----------------------------- utterance pickup ---------------------------- */
function extractUtterance(body: any, params: any): string {
  const cand = [
    params?.lastUserText,
    body?.text,
    body?.queryResult?.queryText,
    body?.queryResult?.transcript,
    body?.transcript,
    body?.sessionInfo?.parameters?.q,
    body?.sessionInfo?.parameters?.query,
    body?.sessionInfo?.parameters?.free_query,
    params?.q, params?.query, params?.free_query
  ].map(clean).filter(Boolean);
  return String(cand[0] || "");
}

function extractExplicitTopic(utter: string): { kind: "book" | "video" | null; term: string | null; author?: string|null } {
  const u0 = (utter || "").trim();
  if (!u0) return { kind: null, term: null };

  const u = u0.toLowerCase();

  // Pattern A: "<topic> books/videos"
  const mA = u.match(/^([\p{L}\p{N}\s\-'"&]+?)\s+(books?|videos?)$/u);
  if (mA) {
    const kind = mA[2].startsWith("video") ? "video" : "book";
    const term = normalizeForIntent(mA[1] || "").replace(/\b(books?|videos?)\b/g, "").trim();
    return { kind, term: term || null };
  }

  // Pattern B: "books/videos (on|about|for) <topic>"
  const mB = u.match(/\b(books?|videos?)\b.*?\b(on|about|regarding|around|for)\s+([\p{L}\p{N}\s\-'"&]+)$/u);
  if (mB) {
    const kind = mB[1].startsWith("video") ? "video" : "book";
    const term = normalizeForIntent(mB[3] || "").replace(/\b(books?|videos?)\b/g, "").trim();
    return { kind, term: term || null };
  }

  // Pattern C: "books/videos by/from <author>"
  const mC = u.match(/\b(books?|videos?)\b\s+(by|from)\s+([\p{L}\p{N}\s\.\-'"&]+)$/u);
  if (mC) {
    const kind = mC[1].startsWith("video") ? "video" : "book";
    const author = mC[3].trim();
    return { kind, term: `inauthor:"${author}"`, author };
  }

  // Pattern D: lone author-ish input → let resolver probe author vs title
  const tokens = u.split(/\s+/).filter(Boolean);
  if (tokens.length >= 2 && tokens.length <= 5) {
    return { kind: null, term: u0 };
  }

  return { kind: null, term: null };
}

const addForKids = (s: string) => (/\bfor\s+kids\b/i.test(s) ? s : `${s} for kids`);

/* ------------------------------- webhook ----------------------------------- */
export const cxWebhook = onRequest(
  { region: "asia-southeast1", memory: "512MiB", timeoutSeconds: 120 },
  async (req, res) => {

    const SOFT_TIMEOUT_MS = 15000; // 15s < Dialogflow CX 20s

    const timeoutHandle = setTimeout(() => {
      try {
        if (!res.headersSent) {
          logger.warn("CX_SOFT_TIMEOUT", { ms: SOFT_TIMEOUT_MS });

          reply(
            res,
            "Hmm, I’m taking a bit longer than usual. Could you please try asking that again?",
            { algo_used: "soft_timeout" }
          );
        }
      } catch (e) {
        logger.warn("CX_SOFT_TIMEOUT_HANDLER_ERR", String(e));
      }
    }, SOFT_TIMEOUT_MS);

    try {
    const rawTag = req.body?.fulfillmentInfo?.tag ?? "";
    const tagKey = normTag(rawTag);
    const body = req.body || {};
    const intentName = body?.intentInfo?.displayName ?? "";
    let params = (body?.sessionInfo?.parameters as Record<string, any>) || {};

    // session & user key
    const sessionId = (typeof body?.sessionInfo?.session === "string" && body.sessionInfo.session) || "default";
    const userKey = currentUserId(req, params);

    // make them available to reply()
    (globalThis as any).__kidflix_sessionId = sessionId;
    (globalThis as any).__kidflix_userKey = userKey;

    // merge persisted → then request params (request overrides)
    try {
      const persisted = await loadThreadState(userKey, sessionId);
      params = { ...(persisted || {}), ...(params || {}) };
    } catch (e) {
      logger.warn("STATE_LOAD_ERR", String(e));
    }

    // pick up utterance robustly
    const rawUtterance = extractUtterance(body, params);
    logger.info("TRACE_UTTER", { rawTag, tagKey, intentName, rawUtterance });

    // // mode & personalization
    // const mode: SearchMode = getSearchMode(params);
    // const userId: string | undefined = typeof params.userId === "string" ? params.userId : undefined;
    // const canPersonalize = !!userId;
    // const preferPersonal = mode === "personalized" && canPersonalize;

    // mode & personalization
    const mode: SearchMode = getSearchMode(params);

    // accept user id from either params or header
    const headerUid = (req.headers["x-user-id"] as string | undefined)?.trim() || "";
    const paramUid  = (typeof params.userId === "string" ? params.userId : "").trim();
    const userId: string | undefined = (paramUid || headerUid) || undefined;

    // make it visible to reply()
    (globalThis as any).__kidflix_userId = userId || null;


    // write it back so downstream logic (and next turns) see it
    if (userId && !params.userId) params.userId = userId;

    const canPersonalize = !!userId;
    const preferPersonal = mode === "personalized" && canPersonalize;


    // genres/topic from parameters or explicit text
    const rawBook = clean(params.genre);
    const rawVideo = clean(params.genre_video);
    // const explicit = extractExplicitTopic(rawUtterance);
    // const bookCanon = explicit.kind === "book" ? "" : normGenre(rawBook);
    // const videoCanon = explicit.kind === "video" ? "" : normGenre(rawVideo);
    // const freeBookQuery = explicit.kind === "book" ? explicit.term || "" : "";
    // const freeVideoQuery = explicit.kind === "video" && explicit.term ? addForKids(explicit.term) : "";
    const explicit = extractExplicitTopic(rawUtterance);

// If user typed "<genre> books", treat it as a category (fiction, nonfiction, education, etc.)
const explicitGenreCanon =
  (explicit.kind === "book" && explicit.term && isKnownGenreTerm(explicit.term))
    ? normGenre(explicit.term)
    : "";

// Keep genre in bookCanon and blank the free query when we recognized a genre.
// Otherwise fall back to the previous logic.
const bookCanon =
  explicitGenreCanon || (explicit.kind === "book" ? "" : normGenre(rawBook));

const videoCanon =
  explicit.kind === "video" ? "" : normGenre(rawVideo);

const freeBookQuery =
  explicitGenreCanon ? "" : (explicit.kind === "book" ? (explicit.term || "") : "");

const freeVideoQuery =
  (explicit.kind === "video" && explicit.term) ? addForKids(explicit.term) : "";


    // --- Robust generic intent gating
    const gen = isGenericAsk(rawUtterance);

    // Effective queries that decide routing below
    let effFreeBookQuery = freeBookQuery;
    let effFreeVideoQuery = freeVideoQuery;

    // If the user said “books/videos” without a real topic → force category prompts
    if (gen.books && !effFreeBookQuery && !bookCanon) effFreeBookQuery = "";
    if (gen.videos && !effFreeVideoQuery && !videoCanon) effFreeVideoQuery = "";

    // Also guard ANN from low-info text like: “recommend books”, “show videos”
    const annEligibleSeed = normalizeForIntent(
      effFreeBookQuery || effFreeVideoQuery || rawUtterance
    );
    const blockAnn = isLowInfo(annEligibleSeed);

    // Keep a clear, kids-biased free video term when user typed plain topic words
    if (effFreeVideoQuery) effFreeVideoQuery = ensureForKids(effFreeVideoQuery);

    // age / lang
    const rawAge = params.age ?? params.child_age ?? params.kid_age ?? params.number ?? "";
    const age: number | undefined = Number.isFinite(Number(rawAge)) ? Number(rawAge) : undefined;
    const ageGroup = params.age_group || mapAgeToGroup(rawAge);
    const lang = String(params.language ?? "en");

/* ----------- safety & restrictions (runs whenever there is text) ----------- */
try {
  // collect all user-facing text we might route on
  const textsToScan: string[] = [];

  // 1) main utterance & generic free queries
  if (rawUtterance) textsToScan.push(rawUtterance);
  if (params?.q) textsToScan.push(String(params.q));
  if (params?.query) textsToScan.push(String(params.query));
  if (params?.free_query) textsToScan.push(String(params.free_query));
  if (params?.book_query) textsToScan.push(String(params.book_query));
  if (params?.video_query) textsToScan.push(String(params.video_query));

  // 2) domain-specific params where topics often live
  if (params?.genre) textsToScan.push(String(params.genre));
  if (params?.genre_video) textsToScan.push(String(params.genre_video));
  if (params?.category) textsToScan.push(String(params.category));
  if (params?.topic) textsToScan.push(String(params.topic));

  // 3) also include the normalized genres derived earlier
  if (rawBook) textsToScan.push(String(rawBook));
  if (rawVideo) textsToScan.push(String(rawVideo));

  // 4) (optional) tag name, just in case you ever encode topics in tags
  if (rawTag) textsToScan.push(String(rawTag));
  if (tagKey) textsToScan.push(String(tagKey));

  const scanText = textsToScan
    .map(t => String(t || "").trim())
    .filter(Boolean)
    .join(" ")
    .slice(0, 500); // avoid scanning huge blobs

  if (scanText) {
    let role = "child";

    // Default preset restrictions (always on)
    let userSpecific: string[] = [];

    if (userId) {
      const userSnap = await db.collection("users").doc(userId).get();
      role = (userSnap.get("role") as string) || "child";

      // restrictions[] field on users/{userId}
      const r = userSnap.get("restrictions");
      if (Array.isArray(r)) userSpecific = r;

      // OPTIONAL: if you already have a separate Firestore collection
      // for restrictions, merge it too (uncomment and adjust collection name):
      //
      // const presetSnap = await db.collection("restrictions").doc(userId).get();
      // const extra = presetSnap.get("terms");
      // if (Array.isArray(extra)) userSpecific.push(...extra);
    }

    // Merge default preset + Firestore restrictions
    const mergedRestrictions = Array.from(
      new Set(
        [
          ...DEFAULT_RESTRICTED_TERMS,
          ...userSpecific,
        ]
          .map(s => String(s).toLowerCase().trim())
          .filter(Boolean)
      )
    );

    const hits = findRestrictedTerms(scanText, mergedRestrictions);

    logger.info("GUARD_CHECK", {
      userId: userId || null,
      role,
      restrictionCount: mergedRestrictions.length,
      sampleText: scanText.slice(0, 60),
      sampleHits: hits.slice(0, 3),
    });

    if (role === "child" && hits.length) {
      if (userId) {
        await db.collection("safety_logs").add({
          userId,
          hits,
          ts: new Date(),
          messagePreview: scanText.slice(0, 160),
          source: "preset+user",   // helpful for debugging later
        });
      }

      reply(
        res,
        "Hey there! I can’t help with that topic. Want to explore fun science books, animal stories, or math videos instead? 🐼🚀📚",
        { algo_used: "guard_block" }
      );
      return;
    }
  }
} catch (e) {
  logger.warn("moderation guard error", String(e));
  // fail-open (still have API-level safety and kid-friendly queries)
}

    // convenience: kind from intent
    const forcedType: "book" | "video" | undefined =
      /book/i.test(intentName) || /book/i.test(tagKey) ? "book" :
      (/video/i.test(intentName) || /video/i.test(tagKey) ? "video" : undefined);

    // Decide whether to try ANN first:
    const freeQuery = clean(
      params?.q || params?.query || params?.free_query || params?.book_query || params?.video_query || rawUtterance
    );

    // Build a seed query + desired type from context
    function buildSeed(
      freeQuery: string,
      forcedType: "book" | "video" | undefined,
      explicit: { kind: "book" | "video" | null; term: string | null },
      rawBook: string,
      bookCanon: string,
      rawVideo: string,
      videoCanon: string,
      rawUtterance: string
    ): { seed: string; desiredType: "book" | "video" | null; categoryOrTopic: string } {
      const desiredType: "book" | "video" | null = forcedType ?? (explicit.kind ?? null);

      // Prefer free text; else explicit term; else chosen category/topic
      let seed = freeQuery || explicit.term || "";
      let categoryOrTopic = "";

      if (!seed) {
        if (desiredType === "book") {
          seed = rawBook || bookCanon || "";
          categoryOrTopic = bookCanon || rawBook || "";
        } else if (desiredType === "video") {
          seed = rawVideo || videoCanon || "";
          categoryOrTopic = videoCanon || rawVideo || "";
        }
      }
      if (!seed) {
        seed = rawBook || rawVideo || bookCanon || videoCanon || "";
        categoryOrTopic = bookCanon || videoCanon || rawBook || rawVideo || "";
      }
      if (!seed) seed = rawUtterance || "kids reading and learning";

        // If the seed itself is a known book genre, interpret it as the category
      if (!categoryOrTopic && desiredType === "book" && isKnownGenreTerm(seed)) {
      categoryOrTopic = normGenre(seed);
      }


      return { seed, desiredType, categoryOrTopic };
    }

    // --- Guard: personalized requested but off
    if (mode === "personalized" && !canPersonalize) {
      reply(
        res,
        "Personalized recommendations are currently disabled. Please sign in and turn on Personalization in Settings to get picks tailored to you.",
        { algo_used: "personalization_off", mode }
      );
      return;
    }

    // --- Canonicalize author (used by author flow)
    async function resolveAuthorCanonical(raw: string, lang?: string): Promise<string | null> {
      try {
        const g = new URL("https://www.googleapis.com/books/v1/volumes");
        g.searchParams.set("q", `inauthor:${JSON.stringify(raw)}`);
        g.searchParams.set("maxResults", "5");
        if (lang) g.searchParams.set("langRestrict", String(lang));
        if (process.env.BOOKS_API_KEY) g.searchParams.set("key", process.env.BOOKS_API_KEY);

        const data: any = await getJSON(g.toString());
        const items: any[] = Array.isArray(data?.items) ? data.items : [];
        for (const it of items) {
          const authors = pickAuthorsArray(it);
          if (authors.length) return authors[0];
        }
      } catch (e) {
        logger.warn("resolveAuthorCanonical error", String(e));
      }
      return null;
    }

    // --- PERSONALIZER helper
    async function tryPersonalizer(
      seedQuery: string,
      desiredType: "book" | "video" | null,
      categoryOrTopic: string
    ) {
      if (!canPersonalize) return null;
      try {
        const seenIds = new Set<string>([
          ...(Array.isArray((params as any)?.seen_ids) ? (params as any).seen_ids.map(String) : []),
          ...(Array.isArray((params as any)?.seen_video_ids) ? (params as any).seen_video_ids.map(String) : []),
        ]);

        const enrichedQuery = [seedQuery, categoryOrTopic].filter(Boolean).join(" ").trim();

        const requestHash = hashKey(JSON.stringify({
          userId,
          q: enrichedQuery || seedQuery || "",
          type: desiredType || undefined,
          ctx: categoryOrTopic || "",
        }));

        const sessionIdLocal =
          params.session_id ||
          (typeof req.body?.sessionInfo?.session === "string" ? req.body.sessionInfo.session : undefined);

        const r = await fetch(RECOMMENDER_URL, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            userId,
            query: enrichedQuery || seedQuery || "",
            type: desiredType || undefined,
            age,
            language: lang,
            limit: 12,
            excludeIds: Array.from(seenIds),
            topic: categoryOrTopic || undefined,
            genre: categoryOrTopic || undefined,
            sessionId: sessionIdLocal,
            requestHash,
          }),
        });
        if (!r.ok) return null;

        const j: any = await r.json();
        if (j?.mode === "blocked") {
          reply(res, j?.reply || "I can’t help with that topic. Try animals, space, or math instead!");
          return "DONE" as const;
        }

        const all = Array.isArray(j?.items) ? j.items : [];
        if (!all.length) return null;

        const listKind: "book" | "video" =
          all[0]?.type === "video" ? "video" :
          all[0]?.type === "book"  ? "book"  :
          (desiredType ?? "book");

        const topicText = (categoryOrTopic || seedQuery || (listKind === "book" ? "books" : "videos")).trim();

        const items = all.slice(0, 5);
        const numbered = items
          .map((it: any, i: number) => `${i + 1}. ${it.title} (${it.type === "video" ? "video" : "book"})`)
          .join("\n");

        const cards = items.map((it: any) => {
          const authorsArr = Array.isArray(it.authors) ? it.authors : [];
          const author = authorsArr[0] || "";
          const authorCount = String(authorsArr.length || 0);
          return makeInfoCard(
            it.title,
            authorsArr.length ? authorsArr.join(", ") : null,
            it.thumb || null,
            buildPreviewLink(it.type === "video" ? "video" : "book", {
              id: it.id,
              title: it.title,
              image: it.thumb || "",
              link: it.link || "",
              author,
              authors: authorsArr.join(", "),
              authorCount,
              category: listKind === "book" ? (categoryOrTopic || "") : undefined,
              topic:    listKind === "video" ? (categoryOrTopic || "") : undefined,
              source: "recommender",
            })
          );
        });

        const remembered: any = {
          kind: listKind,
          seedTitle: items[0]?.title || "",
          items: items.map((it: any) => ({ id: it.id, title: it.title, thumb: it.thumb || null, type: it.type })),
          ...(listKind === "book" ? { category: categoryOrTopic || "" } : { topic: categoryOrTopic || "" }),
        };

        reply(
          res,
          `Here are some personalized recommendations for ${listKind === "book" ? "books" : "videos"} on "${topicText}" because you like similar ${listKind === "book" ? "books" : "videos"}.\n\n${numbered}`,
          { algo_used: "recommender", last_algo: "recommender", mode, last_list: remembered },
          { richContent: [cards.length ? [...cards] : [], [{ type: "chips", options: [{ text: "Show more" }]}]] }
        );
        return "DONE" as const;
      } catch (e) {
        logger.warn("recommender error", String(e));
        return null;
      }
    }

    // --- ANN helper (with global author extractor usage)
    async function tryANN(
      query: string,
      opts: { forcedType: "book" | "video" | null; personalize: boolean; mode: "ann" | "personalized"; contextTag?: string; }
    ) {

      try {
        const limit = 12;
        const biasSuffix = opts.personalize ? " for kids" : "";
        const q1 = (query || "").trim();
        const q2 = (q1 + biasSuffix).trim();

        logger.info("DECISION_SEED", { preferPersonal, seed: q1, desiredType, categoryOrTopic: opts.contextTag || "", tagKey });
        logger.info("ANN_START", { q1, q2, forcedType: opts.forcedType, personalize: opts.personalize, contextTag: opts.contextTag });

        let rows = await annNearest(q1, limit, opts.forcedType || undefined);
        logger.info("ANN_AFTER_SQL", { count: rows?.length ?? 0 });

        if (!rows || rows.length === 0) {
          logger.info("ANN_RETRY", { step: "no-results → drop type filter", q: q1 });
          rows = await annNearest(q1, limit, undefined);
        }
        if (!rows || rows.length === 0) {
          logger.info("ANN_RETRY", { step: "no-results → add kids bias", q: q2, forcedType: opts.forcedType });
          rows = await annNearest(q2, limit, opts.forcedType || undefined);
        }
        if (!rows || rows.length === 0) {
          logger.warn("ANN_SQL_EMPTY_TRY_HTTP");
          try {
            rows = await annNearestHttp(q1, limit, opts.forcedType || undefined);
            logger.info("ANN_AFTER_HTTP", { count: rows?.length ?? 0 });
          } catch (e) {
            logger.warn("ANN_HTTP_ERROR", String(e));
          }
        }
        if (!rows || rows.length === 0) {
          logger.info("ANN_GIVE_UP");
          return null;
        }

        // --- (3) Low-confidence guard on base ANN similarity
        const baseSim = (r: any) =>
          typeof r.score === "number"
            ? r.score
            : (typeof r.dist === "number" ? (1 - r.dist) : 0);

        let bestBaseSim = 0;
        for (const r of rows) {
          const s = baseSim(r);
          if (s > bestBaseSim) bestBaseSim = s;
        }

        if (bestBaseSim < 0.38) {
          logger.info("ANN_CONF_LOW → fallback to category/search", { bestBaseSim });
          return null;
        }

        // --- (2) Hybrid re-ranking: favor exact/near author & title matches
        function softIncludes(hay?: string, needle?: string) {
          return !!(hay && needle) && hay.toLowerCase().includes(needle.toLowerCase());
        }

        const qAuthor = extractAuthorCandidate(q1);
        const titleText = q1; // original query text for title contains

        const authorSignal = (rowAuthors?: string[]) => {
          if (!qAuthor || !Array.isArray(rowAuthors) || rowAuthors.length === 0) return 0;
          const lower = qAuthor.toLowerCase();
          if (rowAuthors.some(a => a && a.toLowerCase() === lower)) return 1.0;   // exact author
          if (rowAuthors.some(a => softIncludes(a, qAuthor))) return 0.6;         // partial author
          return 0;
        };

        const titleSignal = (title?: string) => softIncludes(title || "", titleText) ? 0.5 : 0;

        // Blend: 50% ANN similarity + 50% textual boost
        rows = rows
          .map((r: any) => {
            const sim   = baseSim(r);
            const aBoost = authorSignal(r.authors);
            const tBoost = titleSignal(r.title);
            const final  = 0.50 * sim + 0.50 * Math.max(aBoost, tBoost);
            return { ...r, _final: final };
          })
          .sort((a: any, b: any) => (b._final ?? 0) - (a._final ?? 0));

        const minAcceptable = 0.35;
        if (!rows.some((r:any)=> (r._final ?? 0) >= minAcceptable)) {
          logger.info("ANN_AUTHOR_NO_GOOD_CANDIDATES → fallback");
          return null;
        }

        const items = rows.slice(0, 5).map((r) => {
          const authorsArr = Array.isArray(r.authors) ? r.authors : (r.authors ? [String(r.authors)] : []);
          return { ...r, authors: authorsArr };
        });

        const cards = items.map((r) => {
          const subtitle = r.authors.length ? r.authors.join(", ") : null;
          const kind: "book" | "video" = r.kind === "video" ? "video" : "book";
          const author = r.authors[0] || "";
          const authorCount = String(r.authors.length || 0);
          return makeInfoCard(
            r.title,
            subtitle,
            r.thumb || null,
            buildPreviewLink(kind, {
              id: r.id,
              title: r.title,
              image: r.thumb || "",
              link: r.link || "",
              author,
              authors: r.authors.join(", "),
              authorCount,
              category: kind === "book" ? (opts.contextTag || "") : undefined,
              topic:    kind === "video" ? (opts.contextTag || "") : undefined,
              description: (r.description || "").slice(0, 500),
              snippet: (r.description || "").slice(0, 500),
              source: "ann",
            })
          );
        });

        const annKind: "book" | "video" = items[0].kind === "video" ? "video" : "book";
        const remembered: any = {
          kind: annKind,
          seedTitle: items[0].title || "",
          items: items.map((r) => ({ id: r.id, title: r.title, thumb: r.thumb || null })),
        };
        if (annKind === "book")  remembered.category = opts.contextTag || "";
        if (annKind === "video") remembered.topic    = opts.contextTag || "";

        const topicText = (opts.contextTag || q1 || (annKind === "book" ? "books" : "videos")).trim();
        const lastQueryUrl = `ANN(local/sql+fallback):{"q":${JSON.stringify(q1)},"type":"${opts.forcedType ?? "auto"}"}`;
        const numbered = items.map((r, i) => `${i + 1}. ${r.title} (${r.kind})`).join("\n");

        reply(
          res,
          `Here are some recommendation picks for ${annKind === "book" ? "books" : "videos"} on "${topicText}".\n\n${numbered}`,
          { algo_used: "ann", last_algo: "ann", mode: opts.mode, last_list: remembered, lastQueryUrl },
          { richContent: [cards.length ? [...cards] : [], [{ type: "chips", options: [{ text: "Show more" }]}]] }
        );
        return "DONE" as const;
      } catch (e) {
        logger.warn("ANN failed", String(e));
        return null;
      }
    }

    const looksLikeAuthorAsk = (u: string) => /\bbooks?\s+(?:by|from)\s+/.test(u.toLowerCase().trim())
      || /\b[a-z][a-z'.-]+\s+[a-z'.-]+(?:\s+[a-z'.-]+)?\s+books?\b/i.test(u);

    // ---- NEW AUTHOR FLOW (place before generic routing) ----
    if (tagKey === "bookbyauthor" || intentName === "BookByAuthor" || looksLikeAuthorAsk(rawUtterance)) {
      const rawAuthor = readAuthorParam(params, rawUtterance) || extractAuthorCandidate(rawUtterance);
      if (!rawAuthor) {
        reply(res, "Which author are you looking for? You can say things like “books by Roald Dahl” or “J K Rowling books”.");
        return;
      }

      const canonAuthor = await resolveAuthorCanonical(rawAuthor, lang) || rawAuthor;

      // 1) ANN with strict author filter
      const annRows = await annNearest(`${canonAuthor} books`, 24, "book");
      const filtered = (annRows || []).filter(r => {
        const arr = Array.isArray(r.authors) ? r.authors.map((a: any) => String(a).toLowerCase()) : [];
        const target = canonAuthor.toLowerCase();
        return arr.includes(target) || arr.some((a: string) => a.includes(target));
      });

      const ranked = filtered
        .map((r:any) => {
          const sim = typeof r.score === "number" ? r.score : (typeof r.dist === "number" ? (1 - r.dist) : 0);
          const aBoost = 1.0; // already filtered by author; strong boost
          const tBoost = (r.title || "").toLowerCase().includes(canonAuthor.toLowerCase()) ? 0.2 : 0;
          return { ...r, _final: 0.5*sim + 0.5*Math.max(aBoost, tBoost) };
        })
        .sort((a:any,b:any)=> (b._final??0)-(a._final??0));

      if (ranked.length >= 3) {
        const items = ranked.slice(0,5).map((r:any)=> ({ ...r, authors: Array.isArray(r.authors)? r.authors : (r.authors? [String(r.authors)] : []) }));
        const numbered = items.map((r:any,i:number)=>`${i+1}. ${r.title} (book)`).join("\n");
        const cards = items.map((r:any)=> makeInfoCard(
          r.title,
          r.authors.length ? r.authors.join(", ") : null,
          r.thumb || null,
          buildPreviewLink("book", {
            id: r.id, title: r.title, image: r.thumb || "", link: r.link || "",
            author: r.authors[0] || "", authors: r.authors.join(", "), authorCount: String(r.authors.length||0),
            source: "ann", category: ""
          })
        ));
        reply(
          res,
          `Here are some books by “${canonAuthor}”:\n\n${numbered}`,
          {
            algo_used: "ann_author",
            last_algo: "ann",
            mode,
            last_list: {
              kind: "book", category: "", seedTitle: items[0]?.title || "",
              items: items.map((x:any)=>({ id:x.id, title:x.title, thumb:x.thumb||null }))
            }
          },
          { richContent: [cards.length ? [...cards] : [], [{ type:"chips", options:[{ text:"Show more"}]}]] }
        );
        return;
      }

      // 2) Fallback to Google Books inauthor: (RELAXED + post-filter for kids)
try {
  const g = new URL("https://www.googleapis.com/books/v1/volumes");
  // primary: just author (no 'subject:juvenile' hard filter)
  g.searchParams.set("q", `inauthor:"${canonAuthor}"`);
  g.searchParams.set("printType", "books");
  g.searchParams.set("orderBy", "relevance");
  if (lang) g.searchParams.set("langRestrict", String(lang));
  g.searchParams.set("maxResults", "20");
  if (process.env.BOOKS_API_KEY) g.searchParams.set("key", process.env.BOOKS_API_KEY);

  const data:any = await getJSON(g.toString());
  let all: any[] = Array.isArray(data?.items) ? data.items : [];

  // soft child/YA filter from categories/snippets/titles
  const isKid = (it:any) => {
    const vi = it?.volumeInfo || {};
    const cats = Array.isArray(vi.categories) ? vi.categories.join(" ") : String(vi.categories || "");
    const title = String(vi.title || "");
    const snip  = String(it?.searchInfo?.textSnippet || vi.description || "");
    const hay   = `${cats} ${title} ${snip}`.toLowerCase();
    return /\b(juvenile|children|child|kids?|young adult|ya|middle[-\s]?grade|picture\s*book|board\s*book|early\s*reader|chapter\s*book)\b/.test(hay);
  };

  let kid = all.filter(isKid);

  // if the kid-filter is too aggressive, relax to all author results
  if (kid.length < 3) kid = all;

  // still too few? try a second pass with a softer query variant
  if (kid.length < 3 && canonAuthor.split(" ").length >= 2) {
    const g2 = new URL("https://www.googleapis.com/books/v1/volumes");
    g2.searchParams.set("q", `inauthor:${JSON.stringify(canonAuthor)} OR "${canonAuthor}"`);
    g2.searchParams.set("printType", "books");
    g2.searchParams.set("orderBy", "relevance");
    if (lang) g2.searchParams.set("langRestrict", String(lang));
    g2.searchParams.set("maxResults", "20");
    if (process.env.BOOKS_API_KEY) g2.searchParams.set("key", process.env.BOOKS_API_KEY);
    const data2:any = await getJSON(g2.toString());
    const all2 = Array.isArray(data2?.items) ? data2.items : [];
    const kid2 = all2.filter(isKid);
    if (kid2.length > kid.length) kid = kid2;
    else if (!kid.length) kid = all2; // at least show something
  }

  const top = kid.slice(0, 5);
  if (!top.length) {
    reply(res, `I couldn’t find books by “${canonAuthor}”. Try another author?`);
    return;
  }

  const numbered = top.map((it:any,i:number)=> `${i+1}. ${pickTitle(it) || "Untitled"}`).join("\n");
  const cards = top.map((it:any)=>{
    const title = pickTitle(it) ?? "Untitled";
    const authorsArr = pickAuthorsArray(it);
    const img = pickThumb(it);
    const desc = String(pickDescription(it)).slice(0,500);
    return makeInfoCard(
      title,
      authorsArr.length ? authorsArr.join(", ") : null,
      img,
      buildPreviewLink("book", {
        id: idForBook(it),
        title,
        image: img || "",
        link: pickLinkBook(it) || "",
        author: authorsArr[0] || "",
        authors: authorsArr.join(", "),
        authorCount: String(authorsArr.length || 0),
        description: desc,
        snippet: desc,
        category: "",
        source: "google_books"
      })
    );
  });

  reply(
    res,
    `Here are some books by “${canonAuthor}”:\n\n${numbered}`,
    {
      algo_used: "author_search",
      last_algo: "author_search",
      mode,
      last_list: {
        kind: "book" as const,
        category: "",
        seedTitle: top[0] ? (pickTitle(top[0]) || "") : "",
        items: top.map((it:any)=>({ id: idForBook(it), title: pickTitle(it) || "Untitled", thumb: pickThumb(it) }))
      },
      lastQueryUrl: "GoogleBooks:inauthor"
    },
    { richContent: [cards.length ? [...cards] : [], [{ type:"chips", options:[{ text:"Show more"}]}]] }
  );
  return;
} catch (e) {
  logger.warn("author inauthor fallback error", String(e));
  reply(res, `I had trouble searching for books by “${canonAuthor}”. Try again or another author?`);
  return;
}
    }

    // ----------------------- existing routing continues -----------------------

    if (tagKey === "noop_guard" && !looksLikeAuthorAsk(rawUtterance)) {
      reply(res, "I didn’t get that. Try asking for another topic (e.g., “fiction books”, “educational videos”).", { algo_used: "noop" });
      return;
    }

    // MORE LIKE THIS
    if (tagKey === "more_like_this") {
      const p = params || {};
      const last = p.last_list || {};
      const lastAlgo: string = String(p.last_algo || p.algo_used || "").toLowerCase();
      const kind: "book" | "video" | undefined = last.kind;

      if (!kind) {
        reply(res, "Do you want more books or more videos?", {}, {
          richContent: [[{ type: "chips", options: [{ text: "Books" }, { text: "Videos" }]}]]
        });
        return;
      }

      // Build a clean exclude set
      const excludeIds: string[] = [
        ...(Array.isArray(p.seen_ids) ? (p.seen_ids as string[]).map(String) : []),
        ...(Array.isArray(p.seen_video_ids) ? (p.seen_video_ids as string[]).map(String) : []),
        ...(Array.isArray(last.items) ? last.items.map((x: any) => String(x?.id)).filter(Boolean) : []),
      ];
      const exclude = new Set<string>(excludeIds);

      // Prefer genre/topic → query → seedTitle
      const baseLabel = (kind === "book" ? (last.category || p.category || "") : (last.topic || p.topic || "")) || "";
      const enrichedQuery = [baseLabel, (p.query || ""), (last.seedTitle || "")].filter(Boolean).join(" ").trim();

      // --- A) Personalizer branch
      if (lastAlgo === "recommender") {
        try {
          const r = await fetch(RECOMMENDER_URL, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              userId,
              query: enrichedQuery,
              type: kind,
              age: p.age || undefined,
              limit: 12,
              excludeIds: Array.from(exclude),
              topic: last.topic || undefined,
              genre: last.category || undefined,
            }),
          });

          if (r.ok) {
            const j: any = await r.json();
            const all = Array.isArray(j?.items) ? j.items : [];
            const items = all.filter((it: any) => it?.id && !exclude.has(String(it.id))).slice(0, 5);

            if (items.length) {
              const cards = items.map((it: any) =>
                makeInfoCard(
                  it.title,
                  Array.isArray(it.authors) && it.authors.length ? it.authors.join(", ") : null,
                  it.thumb || null,
                  buildPreviewLink(it.type === "video" ? "video" : "book", {
                    id: it.id,
                    title: it.title,
                    image: it.thumb || "",
                    link: it.link || "",
                    author: Array.isArray(it.authors) ? (it.authors[0] || "") : "",
                    authors: Array.isArray(it.authors) ? it.authors.join(", ") : "",
                    authorCount: String(Array.isArray(it.authors) ? it.authors.length : 0),
                    category: kind === "book" ? (last.category || "") : undefined,
                    topic:    kind === "video" ? (last.topic || "") : undefined,
                    source: "recommender",
                  })
                )
              );

              items.forEach((it: any) => exclude.add(String(it.id)));

              const remembered: any = {
                kind,
                seedTitle: items[0]?.title || last.seedTitle || "",
                items: items.map((it: any) => ({ id: it.id, title: it.title, thumb: it.thumb || null })),
                ...(kind === "book" ? { category: last.category || "" } : { topic: last.topic || "" })
              };

              const label = baseLabel || last.seedTitle || (kind === "book" ? "books" : "videos");
              const numbered = items.map((it: any, i: number) => `${i + 1}. ${it.title} (${it.type})`).join("\n");

              reply(
                res,
                `Here are more personalized picks related to "${label}":\n\n${numbered}`,
                {
                  last_list: remembered,
                  last_algo: "recommender",
                  algo_used: "recommender_more",
                  seen_ids: Array.from(exclude),
                  seen_video_ids: Array.from(exclude),
                  mode
                },
                { richContent: [cards.length ? [...cards] : [], [{ type: "chips", options: [{ text: "Show more" }]}]] }
              );
              return;
            }
          } else {
            logger.warn("recommender-more HTTP not ok", String(r.status));
          }
        } catch (e) {
          logger.warn("recommender-more error", String(e));
        }
      }

      // --- B) ANN branch
      if (lastAlgo === "ann") {
        const baseQuery = enrichedQuery || "kids books and videos";
        const ann = await annNearest(baseQuery, 12, kind);
        const fresh = (ann || []).filter(x => x?.id && !exclude.has(String(x.id))).slice(0, 5);

        if (fresh.length) {
          const cards = fresh.map((r) =>
            makeInfoCard(
              r.title,
              Array.isArray(r.authors) && r.authors.length ? r.authors.join(", ") : null,
              r.thumb || null,
              buildPreviewLink(r.kind, {
                id: r.id,
                title: r.title,
                image: r.thumb || "",
                link: r.link || "",
                author: Array.isArray(r.authors) ? (r.authors[0] || "") : "",
                authors: Array.isArray(r.authors) ? r.authors.join(", ") : "",
                authorCount: String(Array.isArray(r.authors) ? r.authors.length : 0),
                category: kind === "book" ? (last.category || "") : undefined,
                topic:    kind === "video" ? (last.topic || "") : undefined,
                description: (r.description || "").slice(0, 500),
                snippet: (r.description || "").slice(0, 500),
                source: "ann",
              })
            )
          );

          fresh.forEach((it) => exclude.add(String(it.id)));

          const remembered: any = {
            kind,
            seedTitle: fresh[0]?.title || last.seedTitle || "",
            items: fresh.map((it: any) => ({ id: it.id, title: it.title, thumb: it.thumb || null })),
            ...(kind === "book" ? { category: last.category || "" } : { topic: last.topic || "" })
          };

          const label = baseLabel || last.seedTitle || (kind === "book" ? "books" : "videos");
          const numbered = fresh.map((it: any, i: number) => `${i + 1}. ${it.title} (${kind})`).join("\n");

          reply(
            res,
            `Here are more similar picks related to "${label}":\n\n${numbered}`,
            {
              last_list: remembered,
              last_algo: "ann",
              algo_used: "ann_more",
              seen_ids: Array.from(exclude),
              seen_video_ids: Array.from(exclude),
              mode
            },
            { richContent: [cards.length ? [...cards] : [], [{ type: "chips", options: [{ text: "Show more" }]}]] }
          );
          return;
        }
      }
      // ---------- C) BOOKS pagination fallback ----------
      if (kind === "book") {
        const category: string | undefined = last.category || p.category;
        if (!category) { reply(res, "Which book category should I continue? (e.g., Fiction, Non-fiction)"); return; }

        let startIndex = Math.max(0, Number(p.next_offset) || 0);
        const seen = new Set<string>(Array.isArray(p.seen_ids) ? p.seen_ids : []);
        const fresh: any[] = [];
        let usedUrl = ""; let source: "app" | "google_books" = "app";

        for (let tries = 0; tries < 10 && fresh.length < 5; tries += 1) {
          const page = await fetchBooksByCategory(String(category) as any, { startIndex, lang: p.language, age: p.age, ageGroup: p.age_group });
          const uniques: any[] = page.items.filter((it: any) => { const id = idForBook(it); return id && !seen.has(id); });
          if (uniques.length > 0) { fresh.push(...uniques); usedUrl = page.usedUrl; source = page.source; }
          else if (page.source === "app") {
            const { term, juvenile } = bookQueryFor(String(category) as any);
            const g = new URL("https://www.googleapis.com/books/v1/volumes");
            g.searchParams.set("q", `${term}${juvenile ? " subject:juvenile" : ""}`);
            if (p.language) g.searchParams.set("langRestrict", String(p.language));
            g.searchParams.set("maxResults", "6");
            g.searchParams.set("startIndex", String(startIndex));
            if (process.env.BOOKS_API_KEY) g.searchParams.set("key", process.env.BOOKS_API_KEY);
            const data: any = await getJSON(g.toString());
            const uniques2: any[] = (Array.isArray(data?.items) ? data.items : []).filter((it: any) => {
              const id = idForBook(it); return id && !seen.has(id);
            });
            if (uniques2.length > 0) { fresh.push(...uniques2); usedUrl = g.toString(); source = "google_books"; }
          } else {
            usedUrl = page.usedUrl; source = page.source;
          }
          startIndex += 6;
        }

        const topItems: any[] = fresh.slice(0, 5);
        const text: string = topItems.length
          ? `Here are more similar picks related to "${category}":\n` +
            asNumbered(topItems.map((it:any)=>({ title: pickTitle(it) || "Untitled", type: "book" })), 5)
          : `I couldn't find more results for "${category}". Try another category?`;

        const cards = topItems.map((it: any) => {
          const title = pickTitle(it) ?? "Untitled";
          const authorsArr = pickAuthorsArray(it);
          const author = authorsArr[0] || "";
          const authorCount = String(authorsArr.length || 0);
          const img = pickThumb(it);
          const desc = String(pickDescription(it)).slice(0, 500);

          const href = buildPreviewLink("book", {
            id: idForBook(it),
            title,
            image: img || "",
            link: pickLinkBook(it) || "",
            author,
            authorCount,
            authors: authorsArr.join(", "),
            description: desc,
            snippet: desc,
            category: String(category),
            source,
          });

          return makeInfoCard(title, authorsArr.length ? authorsArr.join(", ") : null, img, href);
        });

        topItems.forEach(it => { const id = idForBook(it); if (id) seen.add(id); });
        const remembered = {
          kind: "book" as const,
          category,
          seedTitle: topItems[0] ? (pickTitle(topItems[0]) || "") : (last.seedTitle || ""),
          items: topItems.map((it: any) => ({ id: idForBook(it), title: pickTitle(it) || "Untitled", thumb: pickThumb(it) }))
        };

        reply(res, text, {
          lastQueryAt: new Date().toISOString(),
          lastQueryUrl: usedUrl,
          source,
          last_list: remembered,
          last_selected_index: null,
          next_offset: Math.max(Number(p.next_offset) || 0, startIndex),
          seen_ids: Array.from(seen),
          last_algo: "category",
          algo_used: "category_more",
          mode
        }, { richContent: [cards, [{ type: "chips", options: [{ text: "Show more" }]}]] });
        return;
      }

      // ---------- D) VIDEOS pagination fallback ----------
      const topic: string | undefined = last.topic || p.category || p.topic;
      if (!topic) { reply(res, "Which video topic should I continue? (e.g., Stories, Animals)"); return; }

      let startIndex = Math.max(0, Number(p.next_offset) || 0);
      let pageToken: string | null = p.last_video_page_token || null;
      const seenV = new Set<string>(Array.isArray(p.seen_video_ids) ? p.seen_video_ids : []);
      const freshV: any[] = [];
      let usedUrlV = ""; let sourceV: "app" | "youtube" = "app"; let lastToken: string | null = pageToken;

      for (let tries = 0; tries < 10 && freshV.length < 5; tries += 1) {
        const page = await fetchVideosByTopic(String(topic) as any, { startIndex, lang: p.language, pageToken });
        const uniques: any[] = page.items.filter((it: any) => { const id = idForVideo(it); return id && !seenV.has(id); });
        if (uniques.length > 0) { freshV.push(...uniques); usedUrlV = page.usedUrl; sourceV = page.source; lastToken = page.nextPageToken || lastToken; }
        else if (page.source === "app") {
          const q = videoQueryFor(String(topic) as any);
          const y = new URL("https://www.googleapis.com/youtube/v3/search");
          y.searchParams.set("part", "snippet");
          y.searchParams.set("type", "video");
          y.searchParams.set("videoEmbeddable", "true");
          y.searchParams.set("safeSearch", "strict");
          y.searchParams.set("maxResults", "6");
          y.searchParams.set("q", q);
          if (pageToken) y.searchParams.set("pageToken", String(pageToken));
          if (process.env.YOUTUBE_API_KEY) y.searchParams.set("key", process.env.YOUTUBE_API_KEY);
          const data: any = await getJSON(y.toString());
          const uniques2: any[] = (Array.isArray(data?.items) ? data.items : []).filter((it: any) => {
            const id = idForVideo(it); return id && !seenV.has(id);
          });
          if (uniques2.length > 0) { freshV.push(...uniques2); usedUrlV = y.toString(); sourceV = "youtube"; lastToken = (data?.nextPageToken as string) || lastToken; }
        } else {
          usedUrlV = page.usedUrl; sourceV = page.source; lastToken = page.nextPageToken || lastToken;
        }
        startIndex += 6;
        pageToken = lastToken;
      }

      const topItemsV: any[] = freshV.slice(0, 5);
      const textV: string = topItemsV.length
        ? `Here are more similar picks related to "${topic}":\n` +
          asNumbered(topItemsV.map((it:any)=>({ title: pickTitle(it) || "Untitled", type: "video" })), 5)
        : `I couldn't find more videos about "${topic}".`;

      const cardsV = topItemsV.map((it: any) => {
        const title = pickTitle(it) ?? "Untitled";
        const vAuthor = it?.channel || it?.channelTitle || it?.snippet?.channelTitle || "";
        const authorCount = vAuthor ? "1" : "0";
        const img = pickThumb(it);
        const vid = idForVideo(it);
        const watch = pickLinkVideo(it) || (vid ? `https://www.youtube.com/watch?v=${vid}` : "");
        const embed = vid ? `https://www.youtube.com/embed/${vid}` : watch;
        const vDesc = String(it?.snippet?.description || "").slice(0, 500);

        const href = buildPreviewLink("video", {
          id: vid,
          title,
          image: img || "",
          link: embed,
          url: watch,
          author: vAuthor,
          authorCount,
          description: vDesc,
          snippet: vDesc,
          topic: String(topic),
          source: sourceV
        });

        return makeInfoCard(title, vAuthor || null, img, href);
      });

      topItemsV.forEach(it => { const id = idForVideo(it); if (id) seenV.add(id); });
      const rememberedV = {
        kind: "video" as const,
        topic,
        seedTitle: topItemsV[0] ? (pickTitle(topItemsV[0]) || "") : (last.seedTitle || ""),
        items: topItemsV.map((it: any) => ({ id: idForVideo(it), title: pickTitle(it) || "Untitled", thumb: pickThumb(it) }))
      };

      reply(res, textV, {
        lastQueryAt: new Date().toISOString(),
        lastQueryUrl: usedUrlV,
        source: sourceV,
        last_list: rememberedV,
        last_selected_index: null,
        next_offset: Math.max(Number(p.next_offset) || 0, startIndex),
        last_video_page_token: lastToken || null,
        seen_video_ids: Array.from(seenV),
        last_algo: "category",
        algo_used: "category_more",
        mode
      }, { richContent: [cardsV, [{ type: "chips", options: [{ text: "Show more" }]}]] });
      return;
    }

    // ------------------------ Decision Tree (re-ordered) ------------------------
    if (tagKey === "more_like_this") {
      // early handled
      logger.info("DECISION_DBG", { branch: "skip: more_like_this handled earlier" });
      return;
    }

    const { seed, desiredType, categoryOrTopic } = buildSeed(
      freeQuery, forcedType, explicit, rawBook, bookCanon, rawVideo, videoCanon, rawUtterance
    );

    // Helpful breadcrumbs in logs
    logger.info("DECISION_DBG", {
      tagKey, preferPersonal, seed, desiredType, categoryOrTopic,
      bookCanon, videoCanon, freeBookQuery, freeVideoQuery
    });

    // If generic ask / missing seed, prompt category instead of ANN
    if (
      !preferPersonal && (
      gen.generic ||
      (explicit.kind === "book"  && !explicit.term) ||
      (explicit.kind === "video" && !explicit.term) ||
      (["books","findbooks","book"].includes(tagKey)  && !effFreeBookQuery && !bookCanon) ||
      (["videos","findvideos","video"].includes(tagKey) && !effFreeVideoQuery && !videoCanon)
      )
    ) {
      const wantBooks  = gen.books  || explicit.kind === "book"  || ["books","findbooks","book"].includes(tagKey);
      const wantVideos = gen.videos || explicit.kind === "video" || ["videos","findvideos","video"].includes(tagKey);

      if (wantBooks && !wantVideos) { reply(res, PROMPT_BOOKS, { algo_used: "category_prompt", mode }); return; }
      if (wantVideos && !wantBooks) { reply(res, PROMPT_VIDEOS, { algo_used: "category_prompt", mode }); return; }
      reply(res, WELCOME_LINE, { algo_used: "welcome", mode }); return;
    }

    // Keep TS happy: best type hint
    const typeHint: "book" | "video" | null =
      desiredType ?? (["findvideos","videos","video"].includes(tagKey) ? "video"
                : ["findbooks","books","book"].includes(tagKey) ? "book"
                : null);

        // --- Personalized "nudge" branch: prompt user to be specific when seed is low-info ---
if (preferPersonal && (!seed || !seed.trim() || blockAnn) && !categoryOrTopic) {
  // Build suggestion chips tailored to the user
  const chips = await buildPersonalizedSuggestions(userId, lang);
  const chipPayload = chips.length
    ? { richContent: [[{ type: "chips", options: chips.map(t => ({ text: t })) }]] }
    : undefined;

  reply(
    res,
    "To personalize better, tell me a topic or author.\nTry one of these:",
    { userId, algo_used: "personalize_prompt" },
    chipPayload
  );
  return;
}

// If personalization is on but seed is low-info, use category/topic as the seed
if (preferPersonal && (!seed || !seed.trim() || blockAnn) && categoryOrTopic) {
  const effectiveSeed = categoryOrTopic.trim();
  const rRec = await tryPersonalizer(effectiveSeed, typeHint, categoryOrTopic);
  if (rRec === "DONE") return;

  // ANN fallback (with kids bias) before category lists
  const rAnn = await tryANN(effectiveSeed, {
    forcedType: typeHint,
    personalize: true,
    mode,
    contextTag: categoryOrTopic
  });
  if (rAnn === "DONE") return;
}

    // ANN vs Personalizer
    if (!seed || !seed.trim() || blockAnn) {
      // fall-through to category/topic flows
      logger.info("DECISION_DBG", { branch: (!seed || !seed.trim()) ? "no-seed → category/topic" : "blocked-low-info → category/topic", annEligibleSeed });
    } else {
      if (preferPersonal) {
        logger.info("DECISION_DBG", { branch: "personalized → recommender first", seed, desiredType, categoryOrTopic });

        const rRec = await tryPersonalizer(seed, typeHint, categoryOrTopic);
        if (rRec === "DONE") return;

        logger.info("DECISION_DBG", { branch: "personalized → recommender empty → ANN fallback" });
        const rAnn = await tryANN(seed, {
          forcedType: typeHint,
          personalize: true,
          mode,
          contextTag: categoryOrTopic
        });
        if (rAnn === "DONE") return;

      } else {
        logger.info("DECISION_DBG", { branch: "ann-mode → ANN first", seed, desiredType, categoryOrTopic });

        const rAnn = await tryANN(seed, {
          forcedType: typeHint,
          personalize: false,
          mode,
          contextTag: categoryOrTopic
        });
        if (rAnn === "DONE") return;
      }
    }

    // Category/Topic fallbacks
    const isBooks  = ["findbooks","books","book"].includes(tagKey);
    const isVideos = ["findvideos","videos","video"].includes(tagKey);

    if (!isBooks && !isVideos && tagKey !== "more_like_this") {
      reply(res, WELCOME_LINE, { algo_used: "welcome", mode });
      return;
    }

    // BOOKS first page
    if (isBooks) {
      let bookPath: BookQueryResolution | null = null;

      const utterNonGeneric = !isGenericAsk(rawUtterance).generic;
      const looksLikeBooks = ["findbooks","books","book"].includes(tagKey) || /\bbooks?\b/i.test(rawUtterance);
      if (effFreeBookQuery) {
        try { bookPath = await resolveBookQuery(freeBookQuery!, lang); } catch {}
      }

      const haveFree = !!bookPath;

            // --- EXTRA SAFETY GUARD: block restricted topics for books (child role) ---
      try {
        let role = "child";
        let userSpecific: string[] = [];

        if (userId) {
          const snap = await db.collection("users").doc(userId).get();
          role = (snap.get("role") as string) || "child";
          const r = snap.get("restrictions");
          if (Array.isArray(r)) userSpecific = r;
        }

        const parts: string[] = [];

        // what the kid actually typed
        if (rawUtterance) parts.push(rawUtterance);

        // book-related query bits
        if (freeBookQuery) parts.push(freeBookQuery);
        if (rawBook) parts.push(rawBook);
        if (bookCanon) parts.push(bookCanon);
        if (params?.category) parts.push(String(params.category));

        const searchText = parts
          .map(t => String(t || "").toLowerCase().trim())
          .filter(Boolean)
          .join(" ")
          .slice(0, 500);

        if (role === "child" && searchText) {
          const mergedRestrictions = Array.from(
            new Set(
              [...DEFAULT_RESTRICTED_TERMS, ...userSpecific]
                .map(s => String(s).toLowerCase().trim())
                .filter(Boolean),
            ),
          );

          // simple substring match on all restricted terms
          const hits = mergedRestrictions.filter(term => term && searchText.includes(term));

          if (hits.length > 0) {
            logger.info("GUARD_BLOCK_BOOKS", {
              userId: userId || null,
              role,
              searchTextSample: searchText.slice(0, 120),
              sampleHits: hits.slice(0, 5),
            });

            reply(
              res,
              "I can’t help with that topic. Try asking for animals, science, mystery or other fun kids’ books instead! 🐼🚀📚",
              { algo_used: "guard_block" },
            );
            return;
          }
        }
      } catch (e) {
        logger.warn("BOOK_GUARD_ERROR", String(e));
      }
      // --- END EXTRA SAFETY GUARD ---

      if (!bookCanon && !haveFree) {
        reply(res, PROMPT_BOOKS, { algo_used: "category_prompt", mode });
        return;
      }

      if (!bookPath && (looksLikeBooks || utterNonGeneric) && !bookCanon) {
        try { bookPath = await resolveBookQuery(rawUtterance, lang); } catch {}
      }

      let items: any[] = [];
      let usedUrl = "";
      let source: "app" | "google_books" = "app";
      let display = effFreeBookQuery || rawBook || bookCanon || "books";

      if (haveFree) {
        if (bookPath!.mode === "category") {
          const first = await fetchBooksByCategory(bookPath!.canon, { startIndex: 0, lang, age, ageGroup });
          items = first.items; usedUrl = first.usedUrl; source = first.source;
          display = bookPath!.display || String(bookPath!.canon);
        } else {
          const first = await fetchBooksBySearch(bookPath!.q, { page: 1, pageSize: 12, lang });
          items = first.items; usedUrl = first.usedUrl; source = first.source; display = bookPath!.display;

          if (bookPath!.mode === "topic") {
            const filtered = items.filter((it: any) => bookMatchesTopic(it, bookPath!.q));
            if (filtered.length >= 3) items = filtered;
          }
        }
      } else {
        const first = await fetchBooksByCategory(bookCanon, { startIndex: 0, lang, age, ageGroup });
        items = first.items; usedUrl = first.usedUrl; source = first.source;
        display = rawBook || bookCanon || "books";
      }

      const topItems: any[] = items.slice(0, 6);
      const topText = topItems.map((it: any, i: number) => `${i + 1}. ${pickTitle(it) || "Untitled"}`).join("\n");
      const text = topItems.length
        ? `Here are some book picks on "${display}":\n${topText}`
        : `I couldn't find books on "${display}". Try another topic or category?`;

      const categoryForPreview =
        (haveFree && bookPath?.mode === "category")
          ? String(bookPath!.canon)
          : String(effFreeBookQuery || bookCanon);

      const cards = topItems.map((it: any) => {
        const title = pickTitle(it) ?? "Untitled";
        const authorList = pickAuthorsArray(it);
        const author = authorList[0] || "";
        const authorCount = String(authorList.length || 0);
        const img = pickThumb(it);
        const desc = String(pickDescription(it)).slice(0, 500);

        const href = buildPreviewLink("book", {
          id: idForBook(it),
          title,
          image: img || "",
          link: pickLinkBook(it) || "",
          author,
          authorCount,
          authors: authorList.join(", "),
          description: desc,
          snippet: desc,
          category: categoryForPreview,
          age: age ? String(age) : "",
          source,
        });

        return makeInfoCard(title, authorList.length ? authorList.join(", ") : null, img, href);
      });

      const seen = new Set<string>();
      topItems.forEach(it => { const id = idForBook(it); if (id) seen.add(id); });

      const remembered = {
        kind: "book" as const,
        category: categoryForPreview,
        seedTitle: topItems[0] ? (pickTitle(topItems[0]) || "") : "",
        items: topItems.map((it: any) => ({ id: idForBook(it), title: pickTitle(it) || "Untitled", thumb: pickThumb(it) }))
      };

      reply(res, text, {
        books_done: true,
        genre: haveFree ? "" : (rawBook ?? ""),
        category: categoryForPreview,
        lastQueryAt: new Date().toISOString(),
        lastQueryUrl: usedUrl,
        source,
        last_list: remembered,
        last_selected_index: null,
        next_offset: 6,
        seen_ids: Array.from(seen),
        video_order_idx: Number(params.video_order_idx) || 0,
        last_video_page_token: null,
        seen_video_ids: [] as string[],
        algo_used: "category",
        mode
      }, { richContent: [cards.length ? [...cards] : [], [{ type: "chips", options: [{ text: "Show more" }]}]] });
      return;
    }

    // VIDEOS first page
    if (isVideos) {
      const haveFree = !!effFreeVideoQuery;
      if (!videoCanon && !haveFree) {
        reply(res, PROMPT_VIDEOS, { algo_used: "category_prompt", mode });
        return;
      }

      const vFirst = await fetchVideosByTopic((videoCanon || "kids") as any, {
        startIndex: 0,
        lang,
        pageToken: null,
        freeQuery: haveFree ? effFreeVideoQuery : null
      });

      let items: any[] = vFirst.items;
      const usedUrl: string = vFirst.usedUrl;
      const source: "app" | "youtube" = vFirst.source;
      const nextPageToken: string | null = vFirst.nextPageToken || null;

      if (haveFree) {
        const topicOnly = effFreeVideoQuery.replace(/\s+for\s+kids\b/i, "").trim();
        const filtered = items.filter((it: any) => videoMatchesTopic(it, topicOnly));
        if (filtered.length >= 3) items = filtered;
      }

      const display = effFreeVideoQuery || rawVideo || videoCanon || "kids";
      const topItems: any[] = items.slice(0, 6);
      const topText = topItems.map((it: any, i: number) => `${i + 1}. ${pickTitle(it) || "Untitled"}`).join("\n");
      const text = topItems.length
        ? `Here are some videos on "${display}":\n${topText}`
        : `I couldn't find videos on "${display}". Try another topic?`;

      const topicForPreview = String(effFreeVideoQuery || videoCanon || "kids");

      const cards = topItems.map((it: any) => {
        const title = pickTitle(it) ?? "Untitled";
        const vAuthor = it?.channel || it?.channelTitle || it?.snippet?.channelTitle || "";
        const authorCount = vAuthor ? "1" : "0";
        const img = pickThumb(it);
        const vid = idForVideo(it);
        const watch = pickLinkVideo(it) || (vid ? `https://www.youtube.com/watch?v=${vid}` : "");
        const embed = vid ? `https://www.youtube.com/embed/${vid}` : watch;
        const vDesc = String(it?.snippet?.description || "").slice(0, 500);

        const href = buildPreviewLink("video", {
          id: vid,
          title,
          image: img || "",
          link: embed,
          url: watch,
          author: vAuthor,
          authorCount,
          description: vDesc,
          snippet: vDesc,
          topic: topicForPreview,
          source
        });

        return makeInfoCard(title, vAuthor || null, img, href);
      });

      const seenV = new Set<string>();
      topItems.forEach(it => { const id = idForVideo(it); if (id) seenV.add(id); });

      const remembered = {
        kind: "video" as const,
        topic: topicForPreview,
        seedTitle: topItems[0] ? (pickTitle(topItems[0]) || "") : "",
        items: topItems.map((it: any) => ({ id: idForVideo(it), title: pickTitle(it) || "Untitled", thumb: pickThumb(it) }))
      };

      reply(res, text, {
        videos_done: true,
        genre: "",
        genre_video: String(freeVideoQuery || rawVideo || videoCanon || ""),
        category: topicForPreview,
        lastQueryAt: new Date().toISOString(),
        lastQueryUrl: usedUrl,
        source,
        last_list: remembered,
        last_selected_index: null,
        next_offset: 6,
        video_order_idx: 0,
        last_video_page_token: nextPageToken,
        seen_video_ids: Array.from(seenV),
        algo_used: "category",
        mode
      }, { richContent: [cards.length ? [...cards] : [], [{ type: "chips", options: [{ text: "Show more" }]}]] });
      return;
    }

    // default welcome
    reply(res, WELCOME_LINE, { algo_used: "welcome", mode });
  } finally {
    clearTimeout(timeoutHandle);
  }
}
);

/* =================== EMBEDDING + ANN (Postgres + pgvector) =================== */
const socketPath = process.env.INSTANCE_UNIX_SOCKET || ""; // e.g. /cloudsql/project:region:instance
const isSocket = !!socketPath;

const pool = new Pool({
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  database: process.env.PGDATABASE,
  host: isSocket ? socketPath : process.env.PGHOST,
  port: Number(process.env.PGPORT || 5432),
  ssl: isSocket
    ? undefined
    : (process.env.PGSSL ? { rejectUnauthorized: false } : undefined),
  max: 5,
});
async function pgQuery<T extends QueryResultRow = QueryResultRow>(text: string, params?: any[]): Promise<{ rows: T[] }> {
  const res = await pool.query<T>(text, params);
  return { rows: res.rows };
}

async function embedTextOpenAI(text: string): Promise<number[]> {
  const r = await fetch("https://api.openai.com/v1/embeddings", {
    method: "POST",
    headers: {
      "Authorization": `Bearer ${process.env.OPENAI_API_KEY}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({ model: "text-embedding-3-small", input: text })
  });
  if (!r.ok) {
    const t = await r.text().catch(() => "");
    throw new Error(`OpenAI embed failed: ${r.status} ${t}`);
  }
  const data: any = await r.json().catch(() => ({}));
  const vec: number[] = data?.data?.[0]?.embedding || [];
  if (!Array.isArray(vec) || !vec.length) throw new Error("empty embedding");
  return vec;
}

async function embedText(text: string): Promise<number[]> {
  // prefer direct OpenAI; fall back to local embedTexts util
  try {
    return await embedTextOpenAI(text);
  } catch (e) {
    logger.warn("embedTextOpenAI failed, fallback to local embedTexts()", String((e as any)?.message || e));
    const out = await embedTexts([text]);
    if (!Array.isArray(out) || !out[0] || !Array.isArray(out[0])) {
      throw new Error("embedTexts fallback returned invalid vector");
    }
    return out[0];
  }
}

/* --------- ANN Types --------- */
type AnnRow = {
  id: string;
  kind: "book" | "video";
  title: string;
  authors: string[];
  description: string | null;
  link: string | null;
  thumb: string | null;
  dist?: number;   // when using SQL
  score?: number;  // when using HTTP
};

/* -------------------- Direct SQL (pgvector) -------------------- */
async function annNearest(query: string, limit = 6, type?: "book" | "video") {
  const emb = await embedText(query);
  const vecLit = `[${emb.join(",")}]`;

  const params: any[] = [];
  const where: string[] = [];

  if (type) {
    params.push(type);
    // NOTE: in your DB the column is "type" (items.type), not "kind"
    where.push(`LOWER(type) = $${params.length}`);
  }
  const whereSQL = where.length ? `WHERE ${where.join(" AND ")}` : "";

  const sql = `
    SELECT
      id,
      LOWER(type) AS type,
      title,
      authors,
      description,
      link,
      thumb,
      (embedding <#> $${params.length + 1}::vector) AS dist
    FROM public.items
    ${whereSQL}
    ORDER BY embedding <#> $${params.length + 1}::vector
    LIMIT $${params.length + 2}
  `;
  params.push(vecLit, limit);

  const { rows } = await pgQuery(sql, params);

  return rows.map((r: any) => ({
    id: r.id,
    kind: r.type === "video" ? "video" : "book",
    title: r.title,
    authors: Array.isArray(r.authors) ? r.authors : (r.authors ? [String(r.authors)] : []),
    description: r.description ?? null,
    link: r.link ?? null,
    thumb: r.thumb ?? null,
    dist: Number(r.dist) || 0,
  })) as AnnRow[];
}

/* -------------------- HTTP fallback (annSearch CF) -------------------- */
const ANN_HTTP_URL =
  process.env.ANN_URL ||
  "https://asia-southeast1-kidflix-4cda0.cloudfunctions.net/annSearch";

async function annNearestHttp(text: string, k = 6, forcedType?: "book" | "video") {
  const body: any = { text, k };
  if (forcedType) body.filters = { kind: forcedType };

  const r = await fetch(ANN_HTTP_URL, {
    method: "POST",
    headers: { "content-type": "application/json", accept: "application/json" },
    body: JSON.stringify(body),
  });

  if (!r.ok) {
    const t = await r.text().catch(() => "");
    throw new Error(`ANN HTTP ${r.status} ${t}`);
  }

  let j: any;
  try {
    j = await r.json();
  } catch {
    const t = await r.text().catch(() => "");
    logger.warn("ANN_HTTP_BAD_JSON", t.slice(0, 300));
    return [];
  }

  const raw = Array.isArray(j?.results) ? j.results
           : Array.isArray(j?.items)   ? j.items
           : [];

  logger.info("ANN_HTTP_RESP", { count: raw.length, sample: raw[0]?.id || null });

  return raw.map((x: any) => ({
    id: x.id,
    kind: (x.kind === "video" ? "video" : "book") as "book" | "video",
    title: x.title,
    authors: Array.isArray(x.authors) ? x.authors : (x.authors ? [String(x.authors)] : []),
    description: x.description ?? null,
    link: x?.metadata?.link ?? x.link ?? null,
    thumb: x?.metadata?.thumb ?? x.thumb ?? null,
    score: typeof x.score === "number" ? x.score : undefined,
  })) as AnnRow[];
}

/* -------------------- Optional: MMR helper (diversity) -------------------- */
function cosine(a: number[], b: number[]) {
  let dot = 0, na = 0, nb = 0;
  for (let i = 0; i < a.length; i++) { dot += a[i]*b[i]; na += a[i]*a[i]; nb += b[i]*b[i]; }
  if (!na || !nb) return 0;
  return dot / (Math.sqrt(na)*Math.sqrt(nb));
}
function mmrRerank(
  cands: { id:string; embedding:number[]; score:number }[],
  k = 6,
  lambda = 0.7
) {
  const chosen: typeof cands = [];
  const rest = [...cands];
  while (chosen.length < k && rest.length) {
    let bestIdx = 0, bestVal = -Infinity;
    for (let i = 0; i < rest.length; i++) {
      const r = rest[i];
      const maxSim = chosen.length
        ? Math.max(...chosen.map(c => cosine(r.embedding, c.embedding)))
        : 0;
      const val = lambda * r.score - (1 - lambda) * maxSim;
      if (val > bestVal) { bestVal = val; bestIdx = i; }
    }
    chosen.push(rest.splice(bestIdx, 1)[0]);
  }
  return chosen;
}

/* ----------------------------- Health/debug endpoints ----------------------------- */
export const health = onRequest(async (_req, res) => {
  try {
    const e = await embedTextOpenAI("hello kids");
    const { rows } = await pgQuery<{ c: number }>("SELECT COUNT(*)::int AS c FROM contents");
    res.json({ ok: true, embedDims: e.length, rows: rows[0]?.c ?? 0 });
  } catch (err: any) {
    res.status(500).json({ ok: false, error: String(err?.message || err) });
  }
});

export const annCount = onRequest(async (_req, res) => {
  try {
    const { rows } = await pgQuery<{ c: number }>("SELECT COUNT(*)::int AS c FROM contents");
    res.json({ rows: rows[0]?.c ?? 0 });
  } catch (e: any) {
    res.status(500).json({ error: e?.message || "count error" });
  }
});

/* ----------------------------- Vector utilities ----------------------------- */
export const embed = onRequest(
  { region: "asia-southeast1", memory: "512MiB", timeoutSeconds: 120 },
  async (req, res) => {
    try {
      const { text } = (req.body ?? {}) as { text?: string };
      if (!text) { res.status(400).json({ error: "text required" }); return; }
      const vector = await embedTextOpenAI(String(text));
      res.json({ vector });
    } catch (e: any) {
      logger.error(e);
      res.status(500).json({ error: e?.message || "embed error" });
    }
  }
);

export const annUpsert = onRequest(
  { region: "asia-southeast1", memory: "512MiB", timeoutSeconds: 120 },
  async (req, res) => {
    try {
      const { items } = (req.body ?? {}) as {
        items?: Array<{
          id: string;
          text?: string;
          vector?: number[];
          kind?: "book" | "video";
          title?: string;
          authors?: string[];
          description?: string;
          metadata?: Record<string, any>;
        }>;
      };
      if (!Array.isArray(items) || items.length === 0) {
        res.status(400).json({ error: "items required" });
        return;
      }

      const upsertSQL = `
        INSERT INTO contents (id, kind, title, authors, description, metadata, embedding, updated_at)
        VALUES ($1, $2, $3, $4::text[], $5, $6::jsonb, $7::vector, now())
        ON CONFLICT (id) DO UPDATE
        SET kind = EXCLUDED.kind,
            title = EXCLUDED.title,
            authors = EXCLUDED.authors,
            description = EXCLUDED.description,
            metadata = EXCLUDED.metadata,
            embedding = EXCLUDED.embedding,
            updated_at = now()
      `;

      let count = 0;
      for (const it of items) {
        if (!it?.id) throw new Error("each item requires id");
        let vec = it.vector as number[] | undefined;
        if (!vec && it.text) vec = await embedTextOpenAI(String(it.text));
        if (!vec || !Array.isArray(vec) || vec.length === 0) {
          throw new Error(`item ${it.id} missing vector or text`);
        }

        const params = [
          String(it.id),
          String(it.kind || "book"),
          String(it.title || "Untitled"),
          Array.isArray(it.authors) ? it.authors : [],
          String(it.description || ""),
          it.metadata ? JSON.stringify(it.metadata) : "{}",
          `[${vec.join(",")}]`,
        ];
        await pgQuery(upsertSQL, params);
        count += 1;
      }
      res.json({ upserted: count });
    } catch (e: any) {
      logger.error(e);
      res.status(500).json({ error: e?.message || "upsert error" });
    }
  }
);

export const annSearch = onRequest(
  { region: "asia-southeast1", memory: "512MiB", timeoutSeconds: 120 },
  async (req, res) => {
    try {
      type Filters = { kind?: string; title_ilike?: string };
      const { text, vector, filters, k } = (req.body ?? {}) as {
        text?: string;
        vector?: number[];
        filters?: Filters;
        k?: number;
      };

      // 1) Get a query vector
      let qv: number[] | undefined = Array.isArray(vector) ? vector : undefined;
      if (!qv && text) qv = await embedTextOpenAI(String(text));
      if (!qv || qv.length === 0) {
        res.status(400).json({ error: "provide text or vector" });
        return;
      }

      const K = Math.min(Math.max(Number(k) || 20, 1), 200);
      const vecLit = `[${qv.join(",")}]`;

      // 2) WHERE filters
      const where: string[] = [];
      const params: any[] = [];

      if (filters?.kind) {
        params.push(String(filters.kind).toLowerCase());
        where.push(`LOWER(type) = $${params.length}`);
      }
      if (filters?.title_ilike) {
        params.push(`%${String(filters.title_ilike)}%`.toLowerCase());
        where.push(`LOWER(title) LIKE $${params.length}`);
      }
      const whereSQL = where.length ? `WHERE ${where.join(" AND ")}` : "";

      // 3) Query items (production table)
      const sql = `
        SELECT
          id,
          LOWER(type) AS type,
          title,
          authors,
          description,
          link,
          thumb,
          1 - (embedding <#> $${params.length + 1}::vector) AS score
        FROM public.items
        ${whereSQL}
        ORDER BY embedding <#> $${params.length + 1}::vector
        LIMIT ${K}
      `;
      params.push(vecLit);

      const { rows } = await pgQuery(sql, params);

      // 4) Normalize to response shape
      const results = rows.map((r: any) => ({
        id: r.id,
        kind: r.type === "video" ? "video" : "book",
        title: r.title,
        authors: Array.isArray(r.authors) ? r.authors : (r.authors ? [String(r.authors)] : []),
        description: r.description ?? null,
        metadata: { link: r.link ?? null, thumb: r.thumb ?? null },
        score: Number(r.score) || 0,
      }));

      res.json({ results });
    } catch (e: any) {
      logger.error(e);
      res.status(500).json({ error: e?.message || "search error" });
    }
  }
);

/* ----------------------------- App integration shims ----------------------------- */
export const embedItems = onRequest(async (req, res) => {
  try {
    const body = typeof req.body === "string" ? JSON.parse(req.body) : req.body;
    const texts: string[] = body?.texts ?? [];
    if (!Array.isArray(texts) || !texts.length) { res.status(400).json({ error: "Provide texts: string[]" }); return; }
    const vectors = await embedTexts(texts);
    res.json({ vectors });
  } catch (e: any) {
    res.status(500).json({ error: e?.message || "Internal error" });
  }
});

export const upsertItemsHttp = onRequest({ timeoutSeconds: 120 }, async (req, res) => {
  try {
    const items = Array.isArray(req.body?.items) ? req.body.items : [];
    if (!items.length) { res.status(400).json({ error: "items[] required" }); return; }
    await upsertItems(items);
    res.json({ ok: true, count: items.length });
  } catch (e: any) {
    console.error("upsertItemsHttp error:", e);
    res.status(500).json({ error: e.message || "internal error" });
  }
});

export const rebuildUserProfileHttp = onRequest(async (req, res) => {
  try {
    const userId = (req.body?.userId as string) || (req.query.userId as string);
    if (!userId) { res.status(400).json({ error: "userId required" }); return; }
    await rebuildUserProfile(userId);
    res.json({ ok: true });
  } catch (e:any) {
    res.status(500).json({ error: e.message });
  }
});

/* ----------------------------- Clear per-session history ----------------------------- */
export const clearHistory = onRequest(async (req, res) => {
  try {
    const sessionId = (req.query.session as string) || "default";

    const uidFromHeader = (req.headers["x-user-id"] as string || "").trim();
    const uidFromQuery  = (req.query.userId as string || "").trim();

    // derive device anon key (for pre-auth clears)
    const anonKey = `anon:${ensureAnonId(req)}`;

    const keysToNuke = new Set<string>();
    if (uidFromHeader) keysToNuke.add(uidFromHeader);
    if (uidFromQuery)  keysToNuke.add(uidFromQuery);
    keysToNuke.add(anonKey);

    await Promise.all(
      Array.from(keysToNuke).map(k =>
        db.collection("chat_threads").doc(threadKey(k, sessionId)).delete().catch(() => {})
      )
    );

    res.json({ ok: true, clearedFor: Array.from(keysToNuke) });
  } catch (e:any) {
    res.status(500).json({ ok: false, error: e?.message || "clear error" });
  }
});

