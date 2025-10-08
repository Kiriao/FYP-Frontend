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
exports.cxWebhook = void 0;
const https_1 = require("firebase-functions/v2/https");
const logger = __importStar(require("firebase-functions/logger"));
const undici_1 = require("undici");
const API_BASE = process.env.APP_API_BASE || "";
/* ------------ tiny helpers ------------ */
async function getJSON(url) {
    const r = await (0, undici_1.fetch)(url, { headers: { accept: "application/json" } });
    if (!r.ok)
        throw new Error(`HTTP ${r.status} for ${url}`);
    return (await r.json());
}
const pickItems = (d) => Array.isArray(d) ? d : Array.isArray(d?.items) ? d.items : Array.isArray(d?.results) ? d.results : [];
const pickTitle = (x) => x?.title || x?.name || x?.volumeInfo?.title || x?.snippet?.title;
const GENRE_ALIASES = {
    "all": "all", "fiction": "fiction", "non fiction": "nonfiction", "non-fiction": "nonfiction", "nonfiction": "nonfiction",
    "education": "education", "educational": "education", "children s literature": "children_literature", "childrens literature": "children_literature",
    "picture board early": "picture_board_early", "picture books": "picture_board_early", "board books": "picture_board_early", "early reader": "picture_board_early", "early readers": "picture_board_early",
    "middle grade": "middle_grade", "poetry humor": "poetry_humor", "poetry & humor": "poetry_humor", "biography": "biography", "other kids": "other_kids",
    "young adult": "young_adult", "ya": "young_adult",
    "stories": "stories", "story": "stories", "songs rhymes": "songs_rhymes", "song": "songs_rhymes", "songs": "songs_rhymes", "nursery rhymes": "songs_rhymes",
    "learning": "learning", "learning videos": "learning", "science": "science", "stem": "science", "math": "math", "mathematics": "math",
    "animals": "animals", "wildlife": "animals", "pets": "animals",
    "art crafts": "art_crafts", "arts crafts": "art_crafts", "art and crafts": "art_crafts", "art & crafts": "art_crafts",
    "dinosaur": "animals", "dinosaurs": "animals", "space": "science", "fantasy": "fiction", "mystery": "fiction", "coding": "education", "programming": "education"
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
function replyText(res, text, extras = {}, payload) {
    try {
        res.setHeader?.("Content-Type", "application/json");
    }
    catch { }
    const messages = [{ text: { text: [text] } }];
    if (payload)
        messages.push({ payload });
    res.status(200).json({ fulfillment_response: { messages }, session_info: { parameters: { ...extras } } });
}
exports.cxWebhook = (0, https_1.onRequest)({ region: "asia-southeast1", secrets: ["BOOKS_API_KEY", "YOUTUBE_API_KEY", "APP_API_BASE"] }, async (req, res) => {
    const rawTag = req.body?.fulfillmentInfo?.tag || "";
    const tag = rawTag.toLowerCase();
    const params = req.body?.sessionInfo?.parameters || {};
    // keep books/videos separate
    const rawBook = clean(params.genre);
    const rawVideo = clean(params.genre_video);
    const bookCanon = normGenre(rawBook);
    const videoCanon = normGenre(rawVideo);
    const rawAge = params.age ?? params.child_age ?? params.kid_age ?? params.number ?? "";
    const age = rawAge;
    const ageGroup = params.age_group || mapAgeToGroup(rawAge);
    const lang = String(params.language ?? "en");
    try {
        const isBooks = tag === "findbooks" || tag === "books" || tag === "book";
        const isVideos = tag === "findvideos" || tag === "videos" || tag === "video";
        if (isBooks) {
            if (!bookCanon) {
                replyText(res, "Which book category are you after? (Fiction, Non Fiction, Education, Children’s Literature, Picture/Board/Early, Middle Grade, Poetry & Humor, Biography, Young Adult)");
                return;
            }
            const { term: bookTerm, juvenile } = bookQueryFor(bookCanon);
            let items = [];
            let usedUrl = "";
            let source = "";
            if (API_BASE) {
                try {
                    const u = new URL(`${API_BASE.replace(/\/+$/, "")}/api/books`);
                    u.searchParams.set("q", bookTerm);
                    u.searchParams.set("query", bookTerm);
                    u.searchParams.set("category", String(bookCanon));
                    if (age)
                        u.searchParams.set("age", String(age));
                    if (ageGroup)
                        u.searchParams.set("ageGroup", String(ageGroup));
                    if (lang)
                        u.searchParams.set("lang", lang);
                    u.searchParams.set("limit", "6");
                    u.searchParams.set("debug", "1");
                    const data = await getJSON(u.toString());
                    items = pickItems(data);
                    usedUrl = u.toString();
                    source = "app";
                }
                catch (e) {
                    logger.warn("App /api/books failed, falling back to Google Books", { e: String(e) });
                }
            }
            if (items.length === 0) {
                const u = new URL("https://www.googleapis.com/books/v1/volumes");
                const q = `${bookTerm}${juvenile ? " subject:juvenile" : ""}`;
                u.searchParams.set("q", q);
                if (lang)
                    u.searchParams.set("langRestrict", lang);
                u.searchParams.set("maxResults", "6");
                if (process.env.BOOKS_API_KEY)
                    u.searchParams.set("key", process.env.BOOKS_API_KEY);
                const data = await getJSON(u.toString());
                items = Array.isArray(data?.items) ? data.items : [];
                usedUrl = u.toString();
                source = "google_books";
            }
            const display = rawBook || bookCanon;
            const top = items.slice(0, 3).map((it, i) => `${i + 1}. ${pickTitle(it) ?? "Untitled"}`).filter(Boolean);
            const text = top.length
                ? `Here are some book picks${display ? ` on "${display}"` : ""}${age ? ` (age ${age})` : ""}:\n${top.join("\n")}`
                : `I couldn't find books${display ? ` on "${display}"` : ""}${age ? ` for age ${age}` : ""}. Try another category?`;
            logger.info("BOOKS", { canon: bookCanon, q: bookTerm, count: items.length, usedUrl, source });
            replyText(res, text, {
                books_done: true,
                genre: rawBook ?? "",
                category: bookCanon,
                lastQueryAt: new Date().toISOString(),
                lastQueryUrl: usedUrl,
                source
            });
            return;
        }
        if (isVideos) {
            if (!videoCanon) {
                replyText(res, "What kind of videos are you looking for? (Stories, Songs & Rhymes, Learning, Science, Math, Animals, Art & Crafts)");
                return;
            }
            const vq = videoQueryFor(videoCanon);
            let items = [];
            let usedUrl = "";
            let source = "";
            if (API_BASE) {
                try {
                    const u = new URL(`${API_BASE.replace(/\/+$/, "")}/api/videos`);
                    u.searchParams.set("q", vq);
                    u.searchParams.set("query", vq);
                    u.searchParams.set("topic", String(videoCanon));
                    if (lang)
                        u.searchParams.set("lang", lang);
                    u.searchParams.set("limit", "6");
                    u.searchParams.set("debug", "1");
                    const data = await getJSON(u.toString());
                    items = pickItems(data);
                    usedUrl = u.toString();
                    source = "app";
                }
                catch (e) {
                    logger.warn("App /api/videos failed, falling back to YouTube", { e: String(e) });
                }
            }
            if (items.length === 0) {
                const u = new URL("https://www.googleapis.com/youtube/v3/search");
                u.searchParams.set("part", "snippet");
                u.searchParams.set("type", "video");
                u.searchParams.set("videoEmbeddable", "true");
                u.searchParams.set("safeSearch", "strict");
                u.searchParams.set("maxResults", "6");
                u.searchParams.set("q", vq);
                if (process.env.YOUTUBE_API_KEY)
                    u.searchParams.set("key", process.env.YOUTUBE_API_KEY);
                const data = await getJSON(u.toString());
                items = Array.isArray(data?.items) ? data.items : [];
                usedUrl = u.toString();
                source = "youtube";
            }
            const display = rawVideo || videoCanon;
            const top = items.slice(0, 3).map((it, i) => `${i + 1}. ${pickTitle(it) ?? "Untitled"}`).filter(Boolean);
            const text = top.length
                ? `Here are some videos${display ? ` about "${display}"` : ""}:\n${top.join("\n")}`
                : `I couldn't find videos${display ? ` about "${display}"` : ""}. Try another topic?`;
            logger.info("VIDEOS", { canon: videoCanon, q: vq, count: items.length, usedUrl, source });
            replyText(res, text, {
                videos_done: true,
                genre: "",
                genre_video: rawVideo ?? "",
                category: videoCanon,
                lastQueryAt: new Date().toISOString(),
                lastQueryUrl: usedUrl,
                source
            });
            return;
        }
        replyText(res, `No fulfillment defined for tag "${rawTag || "(empty)"}".`);
    }
    catch (err) {
        logger.error("Webhook error", { err: String(err?.message || err) });
        replyText(res, "Something went wrong fetching results. Please try again.");
    }
});
