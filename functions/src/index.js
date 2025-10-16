"use strict";
var __assign = (this && this.__assign) || function () {
    __assign = Object.assign || function(t) {
        for (var s, i = 1, n = arguments.length; i < n; i++) {
            s = arguments[i];
            for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p))
                t[p] = s[p];
        }
        return t;
    };
    return __assign.apply(this, arguments);
};
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g = Object.create((typeof Iterator === "function" ? Iterator : Object).prototype);
    return g.next = verb(0), g["throw"] = verb(1), g["return"] = verb(2), typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (g && (g = 0, op[0] && (_ = 0)), _) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.cxWebhook = void 0;
var https_1 = require("firebase-functions/v2/https");
var logger = require("firebase-functions/logger");
var undici_1 = require("undici");
var API_BASE = process.env.APP_API_BASE || "";
function getJSON(url) {
    return __awaiter(this, void 0, void 0, function () {
        var r;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0: return [4 /*yield*/, (0, undici_1.fetch)(url, { headers: { accept: "application/json" } })];
                case 1:
                    r = _a.sent();
                    if (!r.ok)
                        throw new Error("HTTP ".concat(r.status, " for ").concat(url));
                    return [4 /*yield*/, r.json()];
                case 2: return [2 /*return*/, (_a.sent())];
            }
        });
    });
}
var pickItems = function (d) {
    return Array.isArray(d) ? d : Array.isArray(d === null || d === void 0 ? void 0 : d.items) ? d.items : Array.isArray(d === null || d === void 0 ? void 0 : d.results) ? d.results : [];
};
var pickTitle = function (x) { var _a, _b; return (x === null || x === void 0 ? void 0 : x.title) || (x === null || x === void 0 ? void 0 : x.name) || ((_a = x === null || x === void 0 ? void 0 : x.volumeInfo) === null || _a === void 0 ? void 0 : _a.title) || ((_b = x === null || x === void 0 ? void 0 : x.snippet) === null || _b === void 0 ? void 0 : _b.title); };
var GENRE_ALIASES = {
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
var looksLikePlaceholder = function (s) {
    return typeof s === "string" && (/^\s*\$intent\.params/i.test(s) || /^\s*\$page\.params/i.test(s) || /^\s*\$session\.params/i.test(s));
};
var clean = function (s) {
    if (s == null)
        return "";
    if (typeof s !== "string")
        return String(s !== null && s !== void 0 ? s : "");
    var t = s.trim();
    if (!t || t === "null" || t === "undefined" || t === '""' || t === "''" || looksLikePlaceholder(t))
        return "";
    return t;
};
function normGenre(raw) {
    var _a;
    if (!raw)
        return "";
    var k = String(raw).toLowerCase().replace(/&/g, " and ").replace(/[^a-z0-9]+/g, " ").replace(/\s+/g, " ").trim();
    return (_a = GENRE_ALIASES[k]) !== null && _a !== void 0 ? _a : k;
}
function mapAgeToGroup(n) {
    var v = Number(n);
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
function replyText(res, text, extras, payload) {
    var _a;
    if (extras === void 0) { extras = {}; }
    try {
        (_a = res.setHeader) === null || _a === void 0 ? void 0 : _a.call(res, "Content-Type", "application/json");
    }
    catch (_b) { }
    var messages = [{ text: { text: [text] } }];
    if (payload)
        messages.push({ payload: payload });
    res.status(200).json({ fulfillment_response: { messages: messages }, session_info: { parameters: __assign({}, extras) } });
}
exports.cxWebhook = (0, https_1.onRequest)({ region: "asia-southeast1", secrets: ["BOOKS_API_KEY", "YOUTUBE_API_KEY", "APP_API_BASE"] }, function (req, res) { return __awaiter(void 0, void 0, void 0, function () {
    var rawTag, tag, params, rawBook, rawVideo, bookCanon, videoCanon, rawAge, age, ageGroup, lang, isBooks, isVideos, _a, bookTerm, juvenile, items, usedUrl, source, u, data, e_1, u, q, data, display, top_1, text, vq, items, usedUrl, source, u, data, e_2, u, data, display, top_2, text, err_1;
    var _b, _c, _d, _e, _f, _g, _h, _j, _k;
    return __generator(this, function (_l) {
        switch (_l.label) {
            case 0:
                rawTag = ((_c = (_b = req.body) === null || _b === void 0 ? void 0 : _b.fulfillmentInfo) === null || _c === void 0 ? void 0 : _c.tag) || "";
                tag = rawTag.toLowerCase();
                params = ((_e = (_d = req.body) === null || _d === void 0 ? void 0 : _d.sessionInfo) === null || _e === void 0 ? void 0 : _e.parameters) || {};
                rawBook = clean(params.genre);
                rawVideo = clean(params.genre_video);
                bookCanon = normGenre(rawBook);
                videoCanon = normGenre(rawVideo);
                rawAge = (_j = (_h = (_g = (_f = params.age) !== null && _f !== void 0 ? _f : params.child_age) !== null && _g !== void 0 ? _g : params.kid_age) !== null && _h !== void 0 ? _h : params.number) !== null && _j !== void 0 ? _j : "";
                age = rawAge;
                ageGroup = params.age_group || mapAgeToGroup(rawAge);
                lang = String((_k = params.language) !== null && _k !== void 0 ? _k : "en");
                _l.label = 1;
            case 1:
                _l.trys.push([1, 16, , 17]);
                isBooks = tag === "findbooks" || tag === "books" || tag === "book";
                isVideos = tag === "findvideos" || tag === "videos" || tag === "video";
                if (!isBooks) return [3 /*break*/, 8];
                if (!bookCanon) {
                    replyText(res, "Which book category are you after? (Fiction, Non Fiction, Education, Children’s Literature, Picture/Board/Early, Middle Grade, Poetry & Humor, Biography, Young Adult)");
                    return [2 /*return*/];
                }
                _a = bookQueryFor(bookCanon), bookTerm = _a.term, juvenile = _a.juvenile;
                items = [];
                usedUrl = "";
                source = "";
                if (!API_BASE) return [3 /*break*/, 5];
                _l.label = 2;
            case 2:
                _l.trys.push([2, 4, , 5]);
                u = new URL("".concat(API_BASE.replace(/\/+$/, ""), "/api/books"));
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
                return [4 /*yield*/, getJSON(u.toString())];
            case 3:
                data = _l.sent();
                items = pickItems(data);
                usedUrl = u.toString();
                source = "app";
                return [3 /*break*/, 5];
            case 4:
                e_1 = _l.sent();
                logger.warn("App /api/books failed, falling back to Google Books", { e: String(e_1) });
                return [3 /*break*/, 5];
            case 5:
                if (!(items.length === 0)) return [3 /*break*/, 7];
                u = new URL("https://www.googleapis.com/books/v1/volumes");
                q = "".concat(bookTerm).concat(juvenile ? " subject:juvenile" : "");
                u.searchParams.set("q", q);
                if (lang)
                    u.searchParams.set("langRestrict", lang);
                u.searchParams.set("maxResults", "6");
                if (process.env.BOOKS_API_KEY)
                    u.searchParams.set("key", process.env.BOOKS_API_KEY);
                return [4 /*yield*/, getJSON(u.toString())];
            case 6:
                data = _l.sent();
                items = Array.isArray(data === null || data === void 0 ? void 0 : data.items) ? data.items : [];
                usedUrl = u.toString();
                source = "google_books";
                _l.label = 7;
            case 7:
                display = rawBook || bookCanon;
                top_1 = items.slice(0, 3).map(function (it, i) { var _a; return "".concat(i + 1, ". ").concat((_a = pickTitle(it)) !== null && _a !== void 0 ? _a : "Untitled"); }).filter(Boolean);
                text = top_1.length
                    ? "Here are some book picks".concat(display ? " on \"".concat(display, "\"") : "").concat(age ? " (age ".concat(age, ")") : "", ":\n").concat(top_1.join("\n"))
                    : "I couldn't find books".concat(display ? " on \"".concat(display, "\"") : "").concat(age ? " for age ".concat(age) : "", ". Try another category?");
                logger.info("BOOKS", { canon: bookCanon, q: bookTerm, count: items.length, usedUrl: usedUrl, source: source });
                replyText(res, text, {
                    books_done: true,
                    genre: rawBook !== null && rawBook !== void 0 ? rawBook : "",
                    category: bookCanon,
                    lastQueryAt: new Date().toISOString(),
                    lastQueryUrl: usedUrl,
                    source: source
                });
                return [2 /*return*/];
            case 8:
                if (!isVideos) return [3 /*break*/, 15];
                if (!videoCanon) {
                    replyText(res, "What kind of videos are you looking for? (Stories, Songs & Rhymes, Learning, Science, Math, Animals, Art & Crafts)");
                    return [2 /*return*/];
                }
                vq = videoQueryFor(videoCanon);
                items = [];
                usedUrl = "";
                source = "";
                if (!API_BASE) return [3 /*break*/, 12];
                _l.label = 9;
            case 9:
                _l.trys.push([9, 11, , 12]);
                u = new URL("".concat(API_BASE.replace(/\/+$/, ""), "/api/videos"));
                u.searchParams.set("q", vq);
                u.searchParams.set("query", vq);
                u.searchParams.set("topic", String(videoCanon));
                if (lang)
                    u.searchParams.set("lang", lang);
                u.searchParams.set("limit", "6");
                u.searchParams.set("debug", "1");
                return [4 /*yield*/, getJSON(u.toString())];
            case 10:
                data = _l.sent();
                items = pickItems(data);
                usedUrl = u.toString();
                source = "app";
                return [3 /*break*/, 12];
            case 11:
                e_2 = _l.sent();
                logger.warn("App /api/videos failed, falling back to YouTube", { e: String(e_2) });
                return [3 /*break*/, 12];
            case 12:
                if (!(items.length === 0)) return [3 /*break*/, 14];
                u = new URL("https://www.googleapis.com/youtube/v3/search");
                u.searchParams.set("part", "snippet");
                u.searchParams.set("type", "video");
                u.searchParams.set("videoEmbeddable", "true");
                u.searchParams.set("safeSearch", "strict");
                u.searchParams.set("maxResults", "6");
                u.searchParams.set("q", vq);
                if (process.env.YOUTUBE_API_KEY)
                    u.searchParams.set("key", process.env.YOUTUBE_API_KEY);
                return [4 /*yield*/, getJSON(u.toString())];
            case 13:
                data = _l.sent();
                items = Array.isArray(data === null || data === void 0 ? void 0 : data.items) ? data.items : [];
                usedUrl = u.toString();
                source = "youtube";
                _l.label = 14;
            case 14:
                display = rawVideo || videoCanon;
                top_2 = items.slice(0, 3).map(function (it, i) { var _a; return "".concat(i + 1, ". ").concat((_a = pickTitle(it)) !== null && _a !== void 0 ? _a : "Untitled"); }).filter(Boolean);
                text = top_2.length
                    ? "Here are some videos".concat(display ? " about \"".concat(display, "\"") : "", ":\n").concat(top_2.join("\n"))
                    : "I couldn't find videos".concat(display ? " about \"".concat(display, "\"") : "", ". Try another topic?");
                logger.info("VIDEOS", { canon: videoCanon, q: vq, count: items.length, usedUrl: usedUrl, source: source });
                replyText(res, text, {
                    videos_done: true,
                    genre: "",
                    genre_video: rawVideo !== null && rawVideo !== void 0 ? rawVideo : "",
                    category: videoCanon,
                    lastQueryAt: new Date().toISOString(),
                    lastQueryUrl: usedUrl,
                    source: source
                });
                return [2 /*return*/];
            case 15:
                replyText(res, "No fulfillment defined for tag \"".concat(rawTag || "(empty)", "\"."));
                return [3 /*break*/, 17];
            case 16:
                err_1 = _l.sent();
                logger.error("Webhook error", { err: String((err_1 === null || err_1 === void 0 ? void 0 : err_1.message) || err_1) });
                replyText(res, "Something went wrong fetching results. Please try again.");
                return [3 /*break*/, 17];
            case 17: return [2 /*return*/];
        }
    });
}); });
