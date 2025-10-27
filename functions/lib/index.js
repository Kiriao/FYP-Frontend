"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
exports.rebuildUserProfileHttp = exports.upsertItemsHttp = exports.embedItems = exports.annSearch = exports.annUpsert = exports.embed = exports.cxWebhook = exports.recommendForUser = void 0;
const https_1 = require("firebase-functions/v2/https");
const logger = __importStar(require("firebase-functions/logger"));
const undici_1 = require("undici");
const openai_1 = require("./lib/openai");
const pg_1 = require("pg");
var recommend_1 = require("./recommend");
Object.defineProperty(exports, "recommendForUser", { enumerable: true, get: function () { return recommend_1.recommendForUser; } });
const items_1 = require("./items");
const users_1 = require("./users");
// ⬇️ removed: import { embedText, upsertVectors, findNeighbors } from "./vertex";
/* ------------ config ------------ */
const API_BASE = process.env.APP_API_BASE || "";
const APP_ORIGIN = (process.env.APP_PUBLIC_ORIGIN || "https://kidflix-4cda0.web.app").replace(/\/+$/, "");
/* ------------ tiny helpers ------------ */
async function getJSON(url) {
    const r = await (0, undici_1.fetch)(url, { headers: { accept: "application/json" } });
    if (!r.ok)
        throw new Error(`HTTP ${r.status} for ${url}`);
    return (await r.json());
}
const pickItems = (d) => Array.isArray(d) ? d : Array.isArray(d?.items) ? d.items : Array.isArray(d?.results) ? d.results : [];
const pickTitle = (x) => x?.title || x?.name || x?.volumeInfo?.title || x?.snippet?.title;
const pickAuthorsArray = (x) => {
    if (Array.isArray(x?.authors))
        return x.authors;
    if (Array.isArray(x?.volumeInfo?.authors))
        return x.volumeInfo.authors;
    return [];
};
const pickDescription = (x) => x?.description || x?.snippet || x?.volumeInfo?.description || x?.searchInfo?.textSnippet || "";
/** Force HTTPS (avoid mixed-content blocking) */
function httpsify(u) {
    if (!u)
        return null;
    try {
        const url = new URL(u);
        url.protocol = "https:";
        if (/^books\.google\./i.test(url.hostname) && url.pathname.startsWith("/books/content")) {
            url.hostname = "books.google.com";
        }
        return url.toString();
    }
    catch {
        return u.replace(/^http:\/\//i, "https://");
    }
}
/** Build a deep link your web app will intercept to open the modal */
function buildPreviewLink(kind, data) {
    const u = new URL(`${APP_ORIGIN}/preview`);
    u.searchParams.set("type", kind);
    for (const [k, v] of Object.entries(data)) {
        if (v != null && v !== "")
            u.searchParams.set(k, String(v));
    }
    return u.toString();
}
function idForBook(x) {
    return x?.id || x?.volumeId || x?.volumeInfo?.industryIdentifiers?.[0]?.identifier || null;
}
function idForVideo(x) {
    return x?.id?.videoId || x?.videoId || null;
}
function pickThumb(x) {
    if (x?.thumbnail)
        return httpsify(x.thumbnail);
    if (x?.volumeInfo?.imageLinks?.thumbnail)
        return httpsify(x.volumeInfo.imageLinks.thumbnail);
    if (x?.snippet?.thumbnails?.medium?.url)
        return httpsify(x.snippet.thumbnails.medium.url);
    if (x?.snippet?.thumbnails?.default?.url)
        return httpsify(x.snippet.thumbnails.default.url);
    return null;
}
function pickLinkBook(x) {
    if (x?.bestLink)
        return httpsify(x.bestLink);
    if (x?.previewLink)
        return httpsify(x.previewLink);
    if (x?.canonicalVolumeLink)
        return httpsify(x.canonicalVolumeLink);
    if (x?.infoLink)
        return httpsify(x.infoLink);
    const v = x?.volumeInfo;
    return httpsify(v?.previewLink || v?.canonicalVolumeLink || v?.infoLink || null);
}
function pickLinkVideo(x) {
    if (x?.url)
        return httpsify(x.url);
    const vid = x?.id?.videoId || x?.videoId;
    return vid ? `https://www.youtube.com/watch?v=${vid}` : null;
}
function makeInfoCard(title, subtitle, img, href) {
    const card = { type: "info", title: title || "Untitled" };
    if (subtitle)
        card.subtitle = subtitle;
    if (img)
        card.image = { rawUrl: img };
    if (href)
        card.actionLink = href;
    return card;
}
const GENRE_ALIASES = {
    // books
    "all": "all",
    "fiction": "fiction",
    "fiction book": "fiction",
    "fiction books": "fiction",
    "non fiction": "nonfiction",
    "non-fiction": "nonfiction",
    "nonfiction": "nonfiction",
    "non fiction book": "nonfiction",
    "nonfiction book": "nonfiction",
    "education": "education", "educational": "education",
    "children s literature": "children_literature", "childrens literature": "children_literature",
    "picture board early": "picture_board_early", "picture books": "picture_board_early",
    "board books": "picture_board_early", "early reader": "picture_board_early", "early readers": "picture_board_early",
    "middle grade": "middle_grade",
    "poetry humor": "poetry_humor", "poetry & humor": "poetry_humor", "funny": "poetry_humor",
    "biography": "biography", "other kids": "other_kids",
    "young adult": "young_adult", "ya": "young_adult",
    // videos
    "stories": "stories", "story": "stories",
    "songs rhymes": "songs_rhymes", "song": "songs_rhymes", "songs": "songs_rhymes", "nursery rhymes": "songs_rhymes",
    "learning": "learning", "learning videos": "learning",
    "science": "science", "stem": "science",
    "math": "math", "mathematics": "math",
    "animals": "animals", "wildlife": "animals", "pets": "animals",
    "art crafts": "art_crafts", "arts crafts": "art_crafts", "art and crafts": "art_crafts", "art & crafts": "art_crafts",
    // topical extras (kept for 1-word genre taps)
    "space": "science", "fantasy": "fiction", "mystery": "fiction",
    "coding": "education", "programming": "education"
};
const looksLikePlaceholder = (s) => typeof s === "string" && (/^\s*\$intent\.params/i.test(s) || /^\s*\$page\.params/i.test(s) || /^\s*\$session\.params/i.test(s));
const clean = (s) => {
    if (s == null)
        return "";
    if (typeof s !== "string")
        return String(s ?? "");
    const t = s.trim();
    if (!t || t === "null" || t === "undefined" || t === '""' || t === "''" || looksLikePlaceholder(t))
        return "";
    return t;
};
function normTag(s) {
    return String(s ?? "")
        .replace(/\u00A0/g, " ")
        .toLowerCase()
        .trim()
        .replace(/\s+/g, " ")
        .replace(/[^a-z0-9]+/g, "_")
        .replace(/^_+|_+$/g, "");
}
function normGenre(raw) {
    if (!raw)
        return "";
    const k = String(raw).toLowerCase().replace(/&/g, " and ").replace(/[^a-z0-9]+/g, " ").replace(/\s+/g, " ").trim();
    return GENRE_ALIASES[k] ?? k;
}
function mapAgeToGroup(n) {
    const v = Number(n);
    if (!Number.isFinite(v))
        return "";
    if (v <= 5)
        return "3-5";
    if (v <= 8)
        return "6-8";
    if (v <= 12)
        return "9-12";
    return "13-15";
}
/** Extract “books/videos … on|about …” and “<topic> books/videos” */
function extractExplicitTopic(utter) {
    const u = (utter || "").toLowerCase().trim();
    if (!u)
        return { kind: null, term: null };
    // “books/videos … on|about …”
    const pat1 = /(book|books|video|videos)\b[^]*?\b(?:on|about|regarding|around|for)\s+([^].*)$/i;
    const m1 = u.match(pat1);
    if (m1) {
        const kind = m1[1].includes("video") ? "video" : "book";
        let term = (m1[2] || "").replace(/\b(for|to|please|pls)\b.*$/i, "").trim();
        term = term.replace(/^[^a-z0-9]+|[^a-z0-9]+$/gi, "");
        if (term)
            return { kind, term };
    }
    // “<topic> books/videos”
    const m2 = /(.*)\s+(videos?|books?)$/i.exec(u);
    if (m2) {
        const kind = m2[2].startsWith("video") ? "video" : "book";
        const term = m2[1].trim().replace(/^[^a-z0-9]+|[^a-z0-9]+$/gi, "");
        if (term && !GENRE_ALIASES[term])
            return { kind, term };
    }
    return { kind: null, term: null };
}
/* ---------- strict topic filtering helpers ---------- */
function topicVariants(raw) {
    const t = (raw || "").trim().toLowerCase();
    if (!t)
        return [];
    const out = new Set([t]);
    if (t.endsWith("s"))
        out.add(t.slice(0, -1));
    else
        out.add(`${t}s`);
    if (t === "dinosaur" || t === "dinosaurs") {
        ["dino", "dinos", "t. rex", "trex", "tyrannosaurus", "triceratops", "stegosaurus", "paleontolog"].forEach(s => out.add(s));
    }
    return Array.from(out);
}
function containsAny(hay, needles) {
    if (!hay)
        return false;
    const lc = hay.toLowerCase();
    return needles.some(n => lc.includes(n));
}
function bookMatchesTopic(raw, topic) {
    const vars = topicVariants(topic);
    const title = pickTitle(raw);
    const authors = pickAuthorsArray(raw).join(", ");
    const desc = pickDescription(raw);
    return containsAny(title, vars) || containsAny(authors, vars) || containsAny(desc, vars);
}
function videoMatchesTopic(raw, topic) {
    const vars = topicVariants(topic);
    const title = pickTitle(raw);
    const desc = raw?.description || raw?.snippet?.description || "";
    return containsAny(title, vars) || containsAny(desc, vars);
}
/* ---------- genre → queries ---------- */
function bookQueryFor(canon) {
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
function videoQueryFor(canon) {
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
/* ---- display simplifier ---- */
function simplifyItem(kind, raw) {
    return {
        id: kind === "book"
            ? (raw?.id || raw?.volumeId || raw?.volumeInfo?.industryIdentifiers?.[0]?.identifier || null)
            : (raw?.id?.videoId || raw?.videoId || null),
        title: pickTitle(raw) || "Untitled",
        thumb: pickThumb(raw),
    };
}
/* ---- de-dupe helper ---- */
function uniqNew(items, seen, kind) {
    const out = [];
    for (const it of items) {
        const id = kind === "book" ? idForBook(it) : idForVideo(it);
        if (!id || seen.has(id))
            continue;
        seen.add(id);
        out.push(it);
    }
    return out;
}
/* ---- category/search fetchers ---- */
async function fetchBooksByCategory(canon, opts) {
    if (API_BASE) {
        try {
            const u = new URL(`${API_BASE.replace(/\/+$/, "")}/api/books`);
            const { term } = bookQueryFor(canon);
            u.searchParams.set("q", term);
            u.searchParams.set("query", term);
            u.searchParams.set("category", String(canon));
            if (opts.age)
                u.searchParams.set("age", String(opts.age));
            if (opts.ageGroup)
                u.searchParams.set("ageGroup", String(opts.ageGroup));
            if (opts.lang)
                u.searchParams.set("lang", String(opts.lang));
            // (keep offset/limit path for category browse)
            u.searchParams.set("limit", "6");
            u.searchParams.set("offset", String(opts.startIndex));
            u.searchParams.set("debug", "1");
            const data = await getJSON(u.toString());
            return { items: pickItems(data), usedUrl: u.toString(), source: "app" };
        }
        catch (e) {
            logger.warn("App /api/books (category page) failed; fallback to Google Books", { e: String(e) });
        }
    }
    const { term, juvenile } = bookQueryFor(canon);
    const g = new URL("https://www.googleapis.com/books/v1/volumes");
    g.searchParams.set("q", `${term}${juvenile ? " subject:juvenile" : ""}`);
    if (opts.lang)
        g.searchParams.set("langRestrict", String(opts.lang));
    g.searchParams.set("maxResults", "6");
    g.searchParams.set("startIndex", String(opts.startIndex));
    if (process.env.BOOKS_API_KEY)
        g.searchParams.set("key", process.env.BOOKS_API_KEY);
    const data = await getJSON(g.toString());
    return { items: Array.isArray(data?.items) ? data.items : [], usedUrl: g.toString(), source: "google_books" };
}
/** free-text books search (page/pageSize) */
async function fetchBooksBySearch(term, opts) {
    const q = term.trim();
    if (API_BASE) {
        const u = new URL(`${API_BASE.replace(/\/+$/, "")}/api/books`);
        u.searchParams.set("q", q);
        if (opts.lang)
            u.searchParams.set("lang", String(opts.lang));
        u.searchParams.set("page", String(opts.page));
        u.searchParams.set("pageSize", String(opts.pageSize));
        u.searchParams.set("includeYA", "1");
        u.searchParams.set("debug", "1");
        u.searchParams.set("ts", String(Date.now()));
        const data = await getJSON(u.toString());
        return { items: pickItems(data), usedUrl: u.toString(), source: "app" };
    }
    const g = new URL("https://www.googleapis.com/books/v1/volumes");
    g.searchParams.set("q", q);
    g.searchParams.set("printType", "books");
    g.searchParams.set("orderBy", "relevance");
    g.searchParams.set("maxResults", String(opts.pageSize));
    g.searchParams.set("startIndex", String((opts.page - 1) * opts.pageSize));
    if (opts.lang)
        g.searchParams.set("langRestrict", String(opts.lang));
    if (process.env.BOOKS_API_KEY)
        g.searchParams.set("key", process.env.BOOKS_API_KEY);
    const data = await getJSON(g.toString());
    return { items: Array.isArray(data?.items) ? data.items : [], usedUrl: g.toString(), source: "google_books" };
}
async function fetchVideosByTopic(topic, opts) {
    const q = opts.freeQuery ? String(opts.freeQuery) : videoQueryFor(topic);
    if (API_BASE) {
        try {
            const u = new URL(`${API_BASE.replace(/\/+$/, "")}/api/videos`);
            u.searchParams.set("q", q);
            u.searchParams.set("query", q);
            u.searchParams.set("topic", String(opts.freeQuery ? (opts.freeQuery || topic) : topic));
            if (opts.lang)
                u.searchParams.set("lang", String(opts.lang));
            u.searchParams.set("limit", "6");
            u.searchParams.set("offset", String(opts.startIndex));
            u.searchParams.set("debug", "1");
            if (opts.pageToken)
                u.searchParams.set("pageToken", String(opts.pageToken));
            const data = await getJSON(u.toString());
            return {
                items: pickItems(data),
                usedUrl: u.toString(),
                source: "app",
                nextPageToken: data?.nextPageToken || null
            };
        }
        catch (e) {
            logger.warn("App /api/videos (category page) failed; fallback to YouTube", { e: String(e) });
        }
    }
    const y = new URL("https://www.googleapis.com/youtube/v3/search");
    y.searchParams.set("part", "snippet");
    y.searchParams.set("type", "video");
    y.searchParams.set("videoEmbeddable", "true");
    y.searchParams.set("safeSearch", "strict");
    y.searchParams.set("maxResults", "6");
    y.searchParams.set("q", q);
    if (opts.pageToken)
        y.searchParams.set("pageToken", String(opts.pageToken));
    if (process.env.YOUTUBE_API_KEY)
        y.searchParams.set("key", process.env.YOUTUBE_API_KEY);
    const data = await getJSON(y.toString());
    return {
        items: Array.isArray(data?.items) ? data.items : [],
        usedUrl: y.toString(),
        source: "youtube",
        nextPageToken: data?.nextPageToken || null
    };
}
/* unified reply (Dialogflow CX) */
function reply(res, text, extras = {}, payload) {
    try {
        res.setHeader?.("Content-Type", "application/json");
    }
    catch { }
    const messages = [{ text: { text: [text] } }];
    if (payload)
        messages.push({ payload });
    res.status(200).json({ fulfillment_response: { messages }, sessionInfo: { parameters: { ...extras } } });
}
const WELCOME_LINE = `Hi! I’m Kidflix Assistant 👋. I can help to recommend books or videos for kids. ` +
    `Would you like to start with Books or Videos?`;
const PROMPT_BOOKS = `Which book category are you after? (Fiction, Non Fiction, Education, Children’s Literature, ` +
    `Picture/Board/Early, Middle Grade, Poetry & Humor, Biography, Young Adult)`;
const PROMPT_VIDEOS = `What kind of videos are you looking for? (Stories, Songs & Rhymes, Learning, Science, Math, Animals, Art & Crafts)`;
/* =================== WEBHOOK: cxWebhook =================== */
exports.cxWebhook = (0, https_1.onRequest)({ region: "asia-southeast1", memory: "512MiB", timeoutSeconds: 120 }, async (req, res) => {
    const rawTag = req.body?.fulfillmentInfo?.tag ?? "";
    const tagKey = normTag(rawTag);
    logger.info("TAG_DEBUG", { rawTag, tagKey, bodyHasFulfillmentInfo: !!req.body?.fulfillmentInfo });
    const params = req.body?.sessionInfo?.parameters || {};
    const rawUtterance = clean(req.body?.text || req.body?.queryResult?.queryText || "");
    const rawBook = clean(params.genre);
    const rawVideo = clean(params.genre_video);
    const explicit = extractExplicitTopic(rawUtterance);
    const bookCanon = explicit.kind === "book" ? "" : normGenre(rawBook);
    const videoCanon = explicit.kind === "video" ? "" : normGenre(rawVideo);
    const freeBookQuery = explicit.kind === "book" ? explicit.term : "";
    const freeVideoQuery = explicit.kind === "video" ? (explicit.term ? `${explicit.term} for kids` : "") : "";
    const rawAge = params.age ?? params.child_age ?? params.kid_age ?? params.number ?? "";
    const age = rawAge;
    const ageGroup = params.age_group || mapAgeToGroup(rawAge);
    const lang = String(params.language ?? "en");
    try {
        const isBooks = tagKey === "findbooks" || tagKey === "books" || tagKey === "book";
        const isVideos = tagKey === "findvideos" || tagKey === "videos" || tagKey === "video";
        if (!isBooks && !isVideos && tagKey !== "more_like_this") {
            reply(res, WELCOME_LINE);
            return;
        }
        /* -------- BOOKS (first page) -------- */
        if (isBooks) {
            if (!bookCanon && !freeBookQuery) {
                reply(res, PROMPT_BOOKS);
                return;
            }
            let items = [];
            let usedUrl = "";
            let source = "app";
            if (freeBookQuery) {
                const first = await fetchBooksBySearch(freeBookQuery, { page: 1, pageSize: 12, lang });
                items = first.items;
                usedUrl = first.usedUrl;
                source = first.source;
                // strict filtering for the topic; fallback if too few
                const filtered = items.filter((it) => bookMatchesTopic(it, freeBookQuery));
                if (filtered.length >= 3)
                    items = filtered;
            }
            else {
                const first = await fetchBooksByCategory(bookCanon, { startIndex: 0, lang, age, ageGroup });
                items = first.items;
                usedUrl = first.usedUrl;
                source = first.source;
            }
            const display = freeBookQuery || rawBook || bookCanon || "books";
            const topItems = items.slice(0, 5);
            const top = topItems.map((it, i) => `${i + 1}. ${pickTitle(it) ?? "Untitled"}`).filter(Boolean);
            const text = top.length
                ? `Here are some book picks on "${display}":\n${top.join("\n")}`
                : `I couldn't find books on "${display}". Try another topic or category?`;
            const cards = topItems.map((it) => {
                const title = pickTitle(it) ?? "Untitled";
                const authorList = pickAuthorsArray(it);
                const subtitle = authorList.length ? authorList.join(", ") : null;
                const img = pickThumb(it);
                const desc = pickDescription(it);
                const href = buildPreviewLink("book", {
                    id: idForBook(it),
                    title,
                    image: img || "",
                    link: pickLinkBook(it) || "",
                    authors: authorList.join(", "),
                    snippet: String(desc).slice(0, 500),
                    category: String(freeBookQuery || bookCanon),
                    age: age ? String(age) : "",
                    source
                });
                return makeInfoCard(title, subtitle, img, href);
            });
            const payload = { richContent: [cards.length ? [...cards] : [], [{ type: "chips", options: [{ text: "More like this" }, { text: "Recommend similar" }] }]] };
            const seenBooks = new Set();
            for (const it of topItems) {
                const id = idForBook(it);
                if (id)
                    seenBooks.add(id);
            }
            const remembered = {
                kind: "book",
                category: String(freeBookQuery || bookCanon),
                seedTitle: topItems[0] ? (pickTitle(topItems[0]) || "") : "",
                items: topItems.map((it) => simplifyItem("book", it))
            };
            reply(res, text, {
                books_done: true,
                genre: freeBookQuery ? "" : (rawBook ?? ""),
                category: String(freeBookQuery || bookCanon),
                lastQueryAt: new Date().toISOString(),
                lastQueryUrl: usedUrl,
                source,
                last_list: remembered,
                last_selected_index: null,
                next_offset: 6, // page size alignment
                seen_ids: Array.from(seenBooks),
                video_order_idx: Number(params.video_order_idx) || 0,
                last_video_page_token: null,
                seen_video_ids: []
            }, payload);
            return;
        }
        /* -------- VIDEOS (first page) -------- */
        if (isVideos) {
            if (!videoCanon && !freeVideoQuery) {
                reply(res, PROMPT_VIDEOS);
                return;
            }
            const vFirst = await fetchVideosByTopic((videoCanon || "kids"), { startIndex: 0, lang, pageToken: null, freeQuery: freeVideoQuery || null });
            let items = vFirst.items;
            const usedUrl = vFirst.usedUrl;
            const source = vFirst.source;
            const nextPageToken = vFirst.nextPageToken || null;
            // strict topic filter only for free-text video queries
            if (freeVideoQuery) {
                const topicOnly = freeVideoQuery.replace(/\s+for\s+kids\b/i, "").trim();
                const filtered = items.filter((it) => videoMatchesTopic(it, topicOnly));
                if (filtered.length >= 3)
                    items = filtered;
            }
            const display = freeVideoQuery || rawVideo || videoCanon;
            const topItems = items.slice(0, 5);
            const top = topItems.map((it, i) => `${i + 1}. ${pickTitle(it) ?? "Untitled"}`).filter(Boolean);
            const text = top.length
                ? `Here are some videos about "${display}":\n${top.join("\n")}`
                : `I couldn't find videos about "${display}". Try another topic?`;
            const cards = topItems.map((it) => {
                const title = pickTitle(it) ?? "Untitled";
                const subtitle = it?.channel || it?.channelTitle || it?.snippet?.channelTitle || null;
                const img = pickThumb(it);
                const vid = idForVideo(it);
                const watch = pickLinkVideo(it) || (vid ? `https://www.youtube.com/watch?v=${vid}` : "");
                const embed = vid ? `https://www.youtube.com/embed/${vid}` : watch;
                const href = buildPreviewLink("video", {
                    id: vid, title, image: img || "", link: embed, url: watch,
                    topic: String(freeVideoQuery || videoCanon), source
                });
                return makeInfoCard(title, subtitle, img, href);
            });
            const payload = { richContent: [cards.length ? [...cards] : [], [{ type: "chips", options: [{ text: "More like this" }, { text: "Recommend similar" }] }]] };
            const seenV = new Set();
            for (const it of topItems) {
                const id = idForVideo(it);
                if (id)
                    seenV.add(id);
            }
            const remembered = {
                kind: "video",
                topic: String(freeVideoQuery || videoCanon),
                seedTitle: topItems[0] ? (pickTitle(topItems[0]) || "") : "",
                items: topItems.map((it) => simplifyItem("video", it))
            };
            reply(res, text, {
                videos_done: true,
                genre: "",
                genre_video: String(freeVideoQuery || rawVideo || videoCanon || ""),
                category: String(freeVideoQuery || videoCanon),
                lastQueryAt: new Date().toISOString(),
                lastQueryUrl: usedUrl,
                source,
                last_list: remembered,
                last_selected_index: null,
                next_offset: 6, // align with page size
                video_order_idx: 0,
                last_video_page_token: nextPageToken,
                seen_video_ids: Array.from(seenV)
            }, payload);
            return;
        }
        /* -------- “MORE LIKE THIS” -------- */
        if (tagKey === "more_like_this") {
            const p = req.body?.sessionInfo?.parameters || {};
            const last = p.last_list || {};
            const kind = last.kind;
            if (!kind) {
                reply(res, "Do you want more books or more videos?", {}, {
                    richContent: [[{ type: "chips", options: [{ text: "Books" }, { text: "Videos" }] }]]
                });
                return;
            }
            if (kind === "book") {
                const category = last.category || p.category;
                if (!category) {
                    reply(res, "Which book category should I continue? (e.g., Fiction, Non-fiction)");
                    return;
                }
                let startIndex = Math.max(0, Number(p.next_offset) || 0);
                const seen = new Set(Array.isArray(p.seen_ids) ? p.seen_ids : []);
                const fresh = [];
                let usedUrl = "";
                let source = "app";
                // Try up to 10 mini-pages (6 each) or until we gather 5 fresh
                for (let tries = 0; tries < 10 && fresh.length < 5; tries += 1) {
                    const page = await fetchBooksByCategory(String(category), {
                        startIndex, lang: p.language, age: p.age, ageGroup: p.age_group
                    });
                    const uniques = uniqNew(page.items, seen, "book");
                    if (uniques.length > 0) {
                        fresh.push(...uniques);
                        usedUrl = page.usedUrl;
                        source = page.source;
                    }
                    else {
                        // fallback to Google Books if app endpoint yields nothing new
                        if (page.source === "app") {
                            const { term, juvenile } = bookQueryFor(String(category));
                            const g = new URL("https://www.googleapis.com/books/v1/volumes");
                            g.searchParams.set("q", `${term}${juvenile ? " subject:juvenile" : ""}`);
                            if (p.language)
                                g.searchParams.set("langRestrict", String(p.language));
                            g.searchParams.set("maxResults", "6");
                            g.searchParams.set("startIndex", String(startIndex));
                            if (process.env.BOOKS_API_KEY)
                                g.searchParams.set("key", process.env.BOOKS_API_KEY);
                            const data = await getJSON(g.toString());
                            const uniques2 = uniqNew(Array.isArray(data?.items) ? data.items : [], seen, "book");
                            if (uniques2.length > 0) {
                                fresh.push(...uniques2);
                                usedUrl = g.toString();
                                source = "google_books";
                            }
                        }
                        else {
                            usedUrl = page.usedUrl;
                            source = page.source;
                        }
                    }
                    startIndex += 6; // advance by true fetch size
                }
                const topItems = fresh.slice(0, 5);
                const list = topItems.map((it, i) => `${i + 1}. ${pickTitle(it) ?? "Untitled"}`).join("\n");
                const text = topItems.length
                    ? `Here are more book picks on "${category}":\n${list}`
                    : `I couldn't find more results for "${category}". Try another category?`;
                const cards = topItems.map((it) => {
                    const title = pickTitle(it) ?? "Untitled";
                    const authorsArr = pickAuthorsArray(it);
                    const subtitle = authorsArr.length ? authorsArr.join(", ") : null;
                    const img = pickThumb(it);
                    const desc = pickDescription(it);
                    const href = buildPreviewLink("book", {
                        id: idForBook(it),
                        title,
                        image: img || "",
                        link: pickLinkBook(it) || "",
                        authors: authorsArr.join(", "),
                        snippet: String(desc).slice(0, 500),
                        category: String(category),
                        source
                    });
                    return makeInfoCard(title, subtitle, img, href);
                });
                const payload = { richContent: [cards.length ? [...cards] : [], [{ type: "chips", options: [{ text: "More like this" }, { text: "Recommend similar" }] }]] };
                const remembered = {
                    kind: "book",
                    category,
                    seedTitle: topItems[0] ? (pickTitle(topItems[0]) || "") : (last.seedTitle || ""),
                    items: topItems.map((it) => simplifyItem("book", it))
                };
                for (const it of topItems) {
                    const id = idForBook(it);
                    if (id)
                        seen.add(id);
                }
                reply(res, text, {
                    lastQueryAt: new Date().toISOString(),
                    lastQueryUrl: usedUrl,
                    source,
                    last_list: remembered,
                    last_selected_index: null,
                    next_offset: Math.max(Number(p.next_offset) || 0, startIndex), // keep moving window
                    seen_ids: Array.from(seen)
                }, payload);
                return;
            }
            // ---- videos ----
            const topic = last.topic || p.category || p.topic;
            if (!topic) {
                reply(res, "Which video topic should I continue? (e.g., Stories, Animals)");
                return;
            }
            let startIndex = Math.max(0, Number(p.next_offset) || 0);
            let pageToken = p.last_video_page_token || null;
            const seenV = new Set(Array.isArray(p.seen_video_ids) ? p.seen_video_ids : []);
            const freshV = [];
            let usedUrlV = "";
            let sourceV = "app";
            let lastToken = pageToken;
            for (let tries = 0; tries < 10 && freshV.length < 5; tries += 1) {
                const page = await fetchVideosByTopic(String(topic), { startIndex, lang: p.language, pageToken });
                const uniques = uniqNew(page.items, seenV, "video");
                if (uniques.length > 0) {
                    freshV.push(...uniques);
                    usedUrlV = page.usedUrl;
                    sourceV = page.source;
                    lastToken = page.nextPageToken || lastToken;
                }
                else {
                    if (page.source === "app") {
                        const q = videoQueryFor(String(topic));
                        const y = new URL("https://www.googleapis.com/youtube/v3/search");
                        y.searchParams.set("part", "snippet");
                        y.searchParams.set("type", "video");
                        y.searchParams.set("videoEmbeddable", "true");
                        y.searchParams.set("safeSearch", "strict");
                        y.searchParams.set("maxResults", "6");
                        y.searchParams.set("q", q);
                        if (pageToken)
                            y.searchParams.set("pageToken", String(pageToken));
                        if (process.env.YOUTUBE_API_KEY)
                            y.searchParams.set("key", process.env.YOUTUBE_API_KEY);
                        const data = await getJSON(y.toString());
                        const uniques2 = uniqNew(Array.isArray(data?.items) ? data.items : [], seenV, "video");
                        if (uniques2.length > 0) {
                            freshV.push(...uniques2);
                            usedUrlV = y.toString();
                            sourceV = "youtube";
                            lastToken = data?.nextPageToken || lastToken;
                        }
                    }
                    else {
                        usedUrlV = page.usedUrl;
                        sourceV = page.source;
                        lastToken = page.nextPageToken || lastToken;
                    }
                }
                startIndex += 6;
                pageToken = lastToken;
            }
            const topItemsV = freshV.slice(0, 5);
            const listV = topItemsV.map((it, i) => `${i + 1}. ${pickTitle(it) ?? "Untitled"}`).join("\n");
            const textV = topItemsV.length ? `Here are more videos about "${topic}":\n${listV}` : `I couldn't find more videos about "${topic}".`;
            const cardsV = topItemsV.map((it) => {
                const title = pickTitle(it) ?? "Untitled";
                const subtitle = it?.channel || it?.channelTitle || it?.snippet?.channelTitle || null;
                const img = pickThumb(it);
                const vid = idForVideo(it);
                const watch = pickLinkVideo(it) || (vid ? `https://www.youtube.com/watch?v=${vid}` : "");
                const embed = vid ? `https://www.youtube.com/embed/${vid}` : watch;
                const href = buildPreviewLink("video", { id: vid, title, image: img || "", link: embed, url: watch, topic: String(topic), source: sourceV });
                return makeInfoCard(title, subtitle, img, href);
            });
            const payloadV = { richContent: [cardsV.length ? [...cardsV] : [], [{ type: "chips", options: [{ text: "More like this" }, { text: "Recommend similar" }] }]] };
            const rememberedV = {
                kind: "video",
                topic,
                seedTitle: topItemsV[0] ? (pickTitle(topItemsV[0]) || "") : (last.seedTitle || ""),
                items: topItemsV.map((it) => simplifyItem("video", it))
            };
            for (const it of topItemsV) {
                const id = idForVideo(it);
                if (id)
                    seenV.add(id);
            }
            reply(res, textV, {
                lastQueryAt: new Date().toISOString(),
                lastQueryUrl: usedUrlV,
                source: sourceV,
                last_list: rememberedV,
                last_selected_index: null,
                next_offset: Math.max(Number(p.next_offset) || 0, startIndex),
                last_video_page_token: lastToken || null,
                seen_video_ids: Array.from(seenV)
            }, payloadV);
            return;
        }
        // Fallback for unexpected tags
        reply(res, WELCOME_LINE);
    }
    catch (err) {
        logger.error("Webhook error", { err: String(err?.message || err) });
        reply(res, "Something went wrong fetching results. Please try again.");
    }
});
/* =================== UTILS: EMBEDDING + ANN (Postgres + pgvector) =================== */
/** shared pool (warm instances reuse connections) */
const pool = new pg_1.Pool({
    host: process.env.PGHOST,
    port: Number(process.env.PGPORT || 5432),
    user: process.env.PGUSER,
    password: process.env.PGPASSWORD,
    database: process.env.PGDATABASE,
    ssl: process.env.PGSSL ? { rejectUnauthorized: false } : undefined,
    max: 5
});
async function pgQuery(text, params) {
    const res = await pool.query(text, params);
    return { rows: res.rows };
}
/** Embed with OpenAI (cheap model) */
async function embedTextOpenAI(text) {
    const r = await (0, undici_1.fetch)("https://api.openai.com/v1/embeddings", {
        method: "POST",
        headers: {
            "Authorization": `Bearer ${process.env.OPENAI_API_KEY}`,
            "Content-Type": "application/json"
        },
        body: JSON.stringify({
            model: "text-embedding-3-small",
            input: text
        })
    });
    if (!r.ok) {
        const t = await r.text();
        throw new Error(`OpenAI embed failed: ${r.status} ${t}`);
    }
    const data = await r.json();
    const vec = data?.data?.[0]?.embedding || [];
    if (!Array.isArray(vec) || !vec.length)
        throw new Error("empty embedding");
    return vec;
}
/** POST /embed  { text } -> { vector }  (parity with your old function name) */
exports.embed = (0, https_1.onRequest)({ region: "asia-southeast1", memory: "512MiB", timeoutSeconds: 120 }, async (req, res) => {
    try {
        const { text } = (req.body ?? {});
        if (!text) {
            res.status(400).json({ error: "text required" });
            return;
        }
        const vector = await embedTextOpenAI(String(text));
        res.json({ vector });
    }
    catch (e) {
        logger.error(e);
        res.status(500).json({ error: e?.message || "embed error" });
    }
});
/** POST /ann/upsert
 * body: { items: [{ id, text?, vector?, kind?, title?, authors?, description?, metadata? }] }
 */
exports.annUpsert = (0, https_1.onRequest)({ region: "asia-southeast1", memory: "512MiB", timeoutSeconds: 120 }, async (req, res) => {
    try {
        const { items } = (req.body ?? {});
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
            if (!it?.id)
                throw new Error("each item requires id");
            let vec = it.vector;
            if (!vec && it.text)
                vec = await embedTextOpenAI(String(it.text));
            if (!vec || !Array.isArray(vec) || vec.length === 0)
                throw new Error(`item ${it.id} missing vector or text`);
            const params = [
                String(it.id),
                String(it.kind || "book"),
                String(it.title || "Untitled"),
                Array.isArray(it.authors) ? it.authors : [],
                String(it.description || ""),
                it.metadata ? JSON.stringify(it.metadata) : "{}",
                `[${vec.join(",")}]` // pgvector accepts array literal cast by ::vector
            ];
            await pgQuery(upsertSQL, params);
            count += 1;
        }
        res.json({ upserted: count });
    }
    catch (e) {
        logger.error(e);
        res.status(500).json({ error: e?.message || "upsert error" });
    }
});
/** POST /ann/search
 * body: { text?: string, vector?: number[], k?: number, filters?: { kind?: string, title_ilike?: string } }
 */
exports.annSearch = (0, https_1.onRequest)({ region: "asia-southeast1", memory: "512MiB", timeoutSeconds: 120 }, async (req, res) => {
    try {
        const { text, vector, filters, k } = (req.body ?? {});
        let qv = vector;
        if (!qv && text)
            qv = await embedTextOpenAI(String(text));
        if (!qv || !Array.isArray(qv) || qv.length === 0) {
            res.status(400).json({ error: "provide text or vector" });
            return;
        }
        const K = Math.min(Math.max(Number(k) || 20, 1), 200);
        const vecLit = `[${qv.join(",")}]`;
        const where = [];
        const params = [];
        if (filters?.kind) {
            params.push(String(filters.kind));
            where.push(`kind = $${params.length}`);
        }
        if (filters?.title_ilike) {
            params.push(`%${String(filters.title_ilike)}%`.toLowerCase());
            where.push(`LOWER(title) LIKE $${params.length}`);
        }
        const whereSQL = where.length ? `WHERE ${where.join(" AND ")}` : "";
        // cosine distance (<=>). Lower is closer; also compute similarity=1 - distance
        const sql = `
        SELECT id, kind, title, authors, description, metadata,
               1 - (embedding <=> $${params.length + 1}::vector) AS score
        FROM contents
        ${whereSQL}
        ORDER BY embedding <=> $${params.length + 1}::vector
        LIMIT ${K}
      `;
        params.push(vecLit);
        const { rows } = await pgQuery(sql, params);
        res.json({ results: rows });
    }
    catch (e) {
        logger.error(e);
        res.status(500).json({ error: e?.message || "search error" });
    }
});
exports.embedItems = (0, https_1.onRequest)(async (req, res) => {
    try {
        const body = typeof req.body === "string" ? JSON.parse(req.body) : req.body;
        const texts = body?.texts ?? [];
        if (!Array.isArray(texts) || !texts.length) {
            res.status(400).json({ error: "Provide texts: string[]" });
            return;
        }
        // tip: dedupe & trim; keep each <= ~8000 tokens
        const vectors = await (0, openai_1.embedTexts)(texts);
        res.json({ vectors });
    }
    catch (e) {
        res.status(500).json({ error: e?.message || "Internal error" });
    }
});
exports.upsertItemsHttp = (0, https_1.onRequest)({ timeoutSeconds: 120 }, async (req, res) => {
    try {
        const items = Array.isArray(req.body?.items) ? req.body.items : [];
        if (!items.length) {
            res.status(400).json({ error: "items[] required" });
            return;
        }
        await (0, items_1.upsertItems)(items);
        res.json({ ok: true, count: items.length });
    }
    catch (e) {
        console.error("upsertItemsHttp error:", e);
        res.status(500).json({ error: e.message || "internal error" });
    }
});
exports.rebuildUserProfileHttp = (0, https_1.onRequest)(async (req, res) => {
    try {
        const userId = req.body?.userId || req.query.userId;
        if (!userId) {
            res.status(400).json({ error: "userId required" });
            return;
        }
        await (0, users_1.rebuildUserProfile)(userId);
        res.json({ ok: true });
    }
    catch (e) {
        res.status(500).json({ error: e.message });
    }
});
//# sourceMappingURL=index.js.map