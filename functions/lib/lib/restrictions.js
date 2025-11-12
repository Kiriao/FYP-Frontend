"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.normalizeForMatch = normalizeForMatch;
exports.findRestrictedTerms = findRestrictedTerms;
// functions/src/lib/restrictions.ts
function normalizeForMatch(s) {
    // lowercase + NFC unicode normalize
    let t = s.normalize("NFC").toLowerCase();
    // collapse common obfuscations (leet-ish)
    const map = {
        "0": "o", "1": "i", "3": "e", "4": "a", "5": "s", "7": "t",
        "$": "s", "@": "a", "!": "i", "|": "l", "€": "e"
    };
    t = t.replace(/[013457$@!|€]/g, ch => map[ch] || ch);
    // reduce separators/spaces
    t = t.replace(/[_\-\.\s]+/g, " ");
    return t;
}
function buildWordBoundaryRegex(terms) {
    // word boundaries; allow spaces between letters: v i o l e n t
    const escaped = terms
        .map(x => x.trim())
        .filter(Boolean)
        .map(x => x.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"))
        .map(x => x.split("").join("\\s*"));
    const pattern = `\\b(?:${escaped.join("|")})\\b`;
    return new RegExp(pattern, "i");
}
// export function findRestrictedTerms(input: string, restrictedTerms: string[]): string[] {
//   const norm = normalizeForMatch(input);
//   const hits = new Set<string>();
//   for (const term of restrictedTerms) {
//     const reSingle = buildWordBoundaryRegex([term]);
//     if (reSingle.test(norm)) hits.add(term);
//   }
//   return Array.from(hits);
// }
// liberal match: case-insensitive, substring (with basic word-ish boundaries)
function findRestrictedTerms(text, restricted) {
    if (!text || !Array.isArray(restricted))
        return [];
    const t = String(text).toLowerCase();
    const hits = [];
    for (const raw of restricted) {
        const q = String(raw || "").trim().toLowerCase();
        if (!q)
            continue;
        // allow "blood", "bloodshed", "bloody" to match "blood"
        // but avoid matching super tiny tokens inside words accidentally
        const pat = q.length <= 3 ? new RegExp(`\\b${escapeRx(q)}\\b`, "i")
            : new RegExp(`${escapeRx(q)}`, "i");
        if (pat.test(t))
            hits.push(raw);
    }
    return hits;
}
function escapeRx(s) {
    return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}
//# sourceMappingURL=restrictions.js.map