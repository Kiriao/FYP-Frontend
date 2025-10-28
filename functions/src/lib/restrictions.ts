// functions/src/lib/restrictions.ts
export function normalizeForMatch(s: string): string {
  // lowercase + NFC unicode normalize
  let t = s.normalize("NFC").toLowerCase();

  // collapse common obfuscations (leet-ish)
  const map: Record<string, string> = {
    "0":"o","1":"i","3":"e","4":"a","5":"s","7":"t",
    "$":"s","@":"a","!":"i","|":"l","€":"e"
  };
  t = t.replace(/[013457$@!|€]/g, ch => map[ch] || ch);

  // reduce separators/spaces
  t = t.replace(/[_\-\.\s]+/g, " ");

  return t;
}

function buildWordBoundaryRegex(terms: string[]): RegExp {
  // word boundaries; allow spaces between letters: v i o l e n t
  const escaped = terms
    .map(x => x.trim())
    .filter(Boolean)
    .map(x => x.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"))
    .map(x => x.split("").join("\\s*"));
  const pattern = `\\b(?:${escaped.join("|")})\\b`;
  return new RegExp(pattern, "i");
}

export function findRestrictedTerms(input: string, restrictedTerms: string[]): string[] {
  const norm = normalizeForMatch(input);
  const hits = new Set<string>();
  for (const term of restrictedTerms) {
    const reSingle = buildWordBoundaryRegex([term]);
    if (reSingle.test(norm)) hits.add(term);
  }
  return Array.from(hits);
}
