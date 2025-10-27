import { onRequest } from "firebase-functions/v2/https";
import * as logger from "firebase-functions/logger";
import { fetch } from "undici";

const API_BASE = process.env.APP_API_BASE || "";
const APP_ORIGIN = (process.env.APP_PUBLIC_ORIGIN || "https://kidflix-4cda0.web.app").replace(/\/+$/, "");

/* ------------ tiny helpers ------------ */
async function getJSON<T = any>(url: string): Promise<T> {
  const r = await fetch(url, { headers: { accept: "application/json" } });
  if (!r.ok) throw new Error(`HTTP ${r.status} for ${url}`);
  return (await r.json()) as T;
}
const pickItems = (d: any): any[] =>
  Array.isArray(d) ? d : Array.isArray(d?.items) ? d.items : Array.isArray(d?.results) ? d.results : [];
const pickTitle = (x: any) => x?.title || x?.name || x?.volumeInfo?.title || x?.snippet?.title;

/** Force HTTPS (avoid mixed-content blocking) */
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

/** Build a deep link your web app will intercept to open the modal */
function buildPreviewLink(kind: "book" | "video", data: Record<string, string | number | null | undefined>) {
  const u = new URL(`${APP_ORIGIN}/preview`);
  u.searchParams.set("type", kind);
  for (const [k, v] of Object.entries(data)) {
    if (v != null && v !== "") u.searchParams.set(k, String(v));
  }
  return u.toString();
}

function idForBook(x: any): string | null {
  return x?.id || x?.volumeId || x?.volumeInfo?.industryIdentifiers?.[0]?.identifier || null;
}
function idForVideo(x: any): string | null {
  return x?.id?.videoId || x?.videoId || null;
}

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
  if (href) card.actionLink = href; // Dialogflow Messenger "info" card link
  return card;
}

type Canon =
  | "all" | "fiction" | "nonfiction" | "education" | "children_literature"
  | "picture_board_early" | "middle_grade" | "poetry_humor" | "biography" | "other_kids" | "young_adult"
  | "stories" | "songs_rhymes" | "learning" | "science" | "math" | "animals" | "art_crafts"
  | string;

const GENRE_ALIASES: Record<string, Canon> = {
  "all":"all","fiction":"fiction","non fiction":"nonfiction","non-fiction":"nonfiction","nonfiction":"nonfiction",
  "education":"education","educational":"education","children s literature":"children_literature","childrens literature":"children_literature",
  "picture board early":"picture_board_early","picture books":"picture_board_early","board books":"picture_board_early","early reader":"picture_board_early","early readers":"picture_board_early",
  "middle grade":"middle_grade","poetry humor":"poetry_humor","poetry & humor":"poetry_humor","biography":"biography","other kids":"other_kids",
  "young adult":"young_adult","ya":"young_adult",
  "stories":"stories","story":"stories","songs rhymes":"songs_rhymes","song":"songs_rhymes","songs":"songs_rhymes","nursery rhymes":"songs_rhymes",
  "learning":"learning","learning videos":"learning","science":"science","stem":"science","math":"math","mathematics":"math",
  "animals":"animals","wildlife":"animals","pets":"animals",
  "art crafts":"art_crafts","arts crafts":"art_crafts","art and crafts":"art_crafts","art & crafts":"art_crafts",
  "dinosaur":"animals","dinosaurs":"animals","space":"science","fantasy":"fiction","mystery":"fiction","coding":"education","programming":"education"
};

const looksLikePlaceholder = (s?: any) =>
  typeof s === "string" && (/^\s*\$intent\.params/i.test(s) || /^\s*\$page\.params/i.test(s) || /^\s*\$session\.params/i.test(s));

const clean = (s?: any): string => {
  if (s == null) return "";
  if (typeof s !== "string") return String(s ?? "");
  const t = s.trim();
  if (!t || t === "null" || t === "undefined" || t === '""' || t === "''" || looksLikePlaceholder(t)) return "";
  return t;
};

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

/* unified reply that also supports Messenger richContent */
function reply(res: any, text: string, extras: Record<string, any> = {}, payload?: any) {
  try { res.setHeader?.("Content-Type", "application/json"); } catch {}
  const messages: any[] = [{ text: { text: [text] } }];
  if (payload) messages.push({ payload });
  res.status(200).json({ fulfillment_response: { messages }, sessionInfo: { parameters: { ...extras } } });
}

export const cxWebhook = onRequest(
  { region: "asia-southeast1", secrets: ["BOOKS_API_KEY", "YOUTUBE_API_KEY", "APP_API_BASE"] },
  async (req, res): Promise<void> => {
    const rawTag = (req.body?.fulfillmentInfo?.tag as string) || "";
    const tag = rawTag.toLowerCase();
    const params = (req.body?.sessionInfo?.parameters as Record<string, any>) || {};

    const rawBook  = clean(params.genre as string | undefined);
    const rawVideo = clean(params.genre_video as string | undefined);
    const bookCanon  = normGenre(rawBook);
    const videoCanon = normGenre(rawVideo);

    const rawAge = params.age ?? params.child_age ?? params.kid_age ?? params.number ?? "";
    const age = rawAge;
    const ageGroup = params.age_group || mapAgeToGroup(rawAge);
    const lang = String(params.language ?? "en");

    try {
      const isBooks  = tag === "findbooks"  || tag === "books"  || tag === "book";
      const isVideos = tag === "findvideos" || tag === "videos" || tag === "video";

      if (isBooks) {
        if (!bookCanon) {
          reply(res, "Which book category are you after? (Fiction, Non Fiction, Education, Children’s Literature, Picture/Board/Early, Middle Grade, Poetry & Humor, Biography, Young Adult)");
          return;
        }
        const { term: bookTerm, juvenile } = bookQueryFor(bookCanon);
        let items: any[] = []; let usedUrl = ""; let source = "";

        if (API_BASE) {
          try {
            const u = new URL(`${API_BASE.replace(/\/+$/, "")}/api/books`);
            u.searchParams.set("q", bookTerm);
            u.searchParams.set("query", bookTerm);
            u.searchParams.set("category", String(bookCanon));
            if (age) u.searchParams.set("age", String(age));
            if (ageGroup) u.searchParams.set("ageGroup", String(ageGroup));
            if (lang) u.searchParams.set("lang", lang);
            u.searchParams.set("limit", "6");
            u.searchParams.set("debug", "1");
            const data: any = await getJSON(u.toString());
            items = pickItems(data);
            usedUrl = u.toString(); source = "app";
          } catch (e) {
            logger.warn("App /api/books failed, falling back to Google Books", { e: String(e) });
          }
        }
        if (items.length === 0) {
          const u = new URL("https://www.googleapis.com/books/v1/volumes");
          const q = `${bookTerm}${juvenile ? " subject:juvenile" : ""}`;
          u.searchParams.set("q", q);
          if (lang) u.searchParams.set("langRestrict", lang);
          u.searchParams.set("maxResults", "6");
          if (process.env.BOOKS_API_KEY) u.searchParams.set("key", process.env.BOOKS_API_KEY);
          const data: any = await getJSON(u.toString());
          items = Array.isArray(data?.items) ? data.items : [];
          usedUrl = u.toString(); source = "google_books";
        }

        const display = rawBook || bookCanon;
        const topItems = items.slice(0, 5);
        const top = topItems.map((it: any, i: number) => `${i + 1}. ${pickTitle(it) ?? "Untitled"}`).filter(Boolean);
        const text = top.length
          ? `Here are some book picks${display ? ` on "${display}"` : ""}${age ? ` (age ${age})` : ""}:\n${top.join("\n")}`
          : `I couldn't find books${display ? ` on "${display}"` : ""}${age ? ` for age ${age}` : ""}. Try another category?`;

        const cards = topItems.map((it: any) => {
          const title = pickTitle(it) ?? "Untitled";
          const authorList = (it?.authors && Array.isArray(it.authors) ? it.authors
                              : it?.volumeInfo?.authors && Array.isArray(it.volumeInfo.authors) ? it.volumeInfo.authors
                              : []) as string[];
          const subtitle = authorList.length ? authorList.join(", ") : null;
          const img = pickThumb(it);

          const desc =
            it?.description ||
            it?.snippet ||
            it?.volumeInfo?.description ||
            "";

          const href = buildPreviewLink("book", {
            id: idForBook(it),
            title,
            image: img || "",
            link: pickLinkBook(it) || "",
            authors: authorList.join(", "),
            snippet: String(desc).slice(0, 500),
            category: String(bookCanon),
            age: age ? String(age) : "",
            source
          });
          return makeInfoCard(title, subtitle, img, href);
        });

        const chips = {
          type: "chips",
          options: [
            { text: "More like this" },
            { text: "Younger age" },
            { text: "Non-fiction" }
          ]
        };

        const payload = { richContent: [ cards.length ? [ ...cards ] : [], [chips] ] };
        logger.info("BOOKS", { canon: bookCanon, q: bookTerm, count: items.length, usedUrl, source });

        reply(res, text, {
          books_done: true,
          genre: rawBook ?? "",
          category: bookCanon,
          lastQueryAt: new Date().toISOString(),
          lastQueryUrl: usedUrl,
          source
        }, payload);
        return;
      }

      if (isVideos) {
        if (!videoCanon) {
          reply(res, "What kind of videos are you looking for? (Stories, Songs & Rhymes, Learning, Science, Math, Animals, Art & Crafts)");
          return;
        }
        const vq = videoQueryFor(videoCanon);
        let items: any[] = []; let usedUrl = ""; let source = "";

        if (API_BASE) {
          try {
            const u = new URL(`${API_BASE.replace(/\/+$/, "")}/api/videos`);
            u.searchParams.set("q", vq);
            u.searchParams.set("query", vq);
            u.searchParams.set("topic", String(videoCanon));
            if (lang) u.searchParams.set("lang", lang);
            u.searchParams.set("limit", "6");
            u.searchParams.set("debug", "1");
            const data: any = await getJSON(u.toString());
            items = pickItems(data);
            usedUrl = u.toString(); source = "app";
          } catch (e) {
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
          if (process.env.YOUTUBE_API_KEY) u.searchParams.set("key", process.env.YOUTUBE_API_KEY);
          const data: any = await getJSON(u.toString());
          items = Array.isArray(data?.items) ? data.items : [];
          usedUrl = u.toString(); source = "youtube";
        }

        const display = rawVideo || videoCanon;
        const topItems = items.slice(0, 5);
        const top = topItems.map((it: any, i: number) => `${i + 1}. ${pickTitle(it) ?? "Untitled"}`).filter(Boolean);
        const text = top.length
          ? `Here are some videos${display ? ` about "${display}"` : ""}:\n${top.join("\n")}`
          : `I couldn't find videos${display ? ` about "${display}"` : ""}. Try another topic?`;

        // 🔻 Only this block changed (embed + watch URL in the deep-link)
        const cards = topItems.map((it: any) => {
          const title = pickTitle(it) ?? "Untitled";
          const subtitle = it?.channel || it?.channelTitle || it?.snippet?.channelTitle || null;
          const img = pickThumb(it);

          const vid = idForVideo(it);
          const watch = pickLinkVideo(it) || (vid ? `https://www.youtube.com/watch?v=${vid}` : "");
          const embed = vid ? `https://www.youtube.com/embed/${vid}` : watch;

          const href = buildPreviewLink("video", {
            id: vid,
            title,
            image: img || "",
            link: embed,           // for your iframe
            url: watch,            // for "Watch on YouTube"
            topic: String(videoCanon),
            source
          });

          return makeInfoCard(title, subtitle, img, href);
        });

        const chips = {
          type: "chips",
          options: [
            { text: "More like this" },
            { text: "Shorter videos" },
            { text: "Songs & Rhymes" }
          ]
        };

        const payload = { richContent: [ cards.length ? [ ...cards ] : [], [chips] ] };
        logger.info("VIDEOS", { canon: videoCanon, q: vq, count: items.length, usedUrl, source });

        reply(res, text, {
          videos_done: true,
          genre: "",
          genre_video: rawVideo ?? "",
          category: videoCanon,
          lastQueryAt: new Date().toISOString(),
          lastQueryUrl: usedUrl,
          source
        }, payload);
        return;
      }

      reply(res, `No fulfillment defined for tag "${rawTag || "(empty)"}".`);
    } catch (err: any) {
      logger.error("Webhook error", { err: String(err?.message || err) });
      reply(res, "Something went wrong fetching results. Please try again.");
    }
  }
);
