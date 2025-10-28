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
function findRestrictedTerms(input, restrictedTerms) {
    const norm = normalizeForMatch(input);
    const hits = new Set();
    for (const term of restrictedTerms) {
        const reSingle = buildWordBoundaryRegex([term]);
        if (reSingle.test(norm))
            hits.add(term);
    }
    return Array.from(hits);
}
//# sourceMappingURL=restrictions.js.map