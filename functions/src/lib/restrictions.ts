// functions/src/lib/restrictions.ts

// Categorized restricted terms
export const RESTRICTED_TERMS = {
  violence: ["kill", "murder", "assault", "weapon", "blood", "stab", "shoot", "attack", "fight", "war"],
  drugs: ["cocaine", "heroin", "meth", "drug", "narcotic", "marijuana", "cannabis"],
  hate: ["racist", "slur", "discrimination", "hate", "nazi", "supremacist"],
  explicit: ["explicit", "nsfw", "adult", "sexual", "porn", "nude"],
  selfharm: ["suicide", "selfharm", "cutting", "overdose", "kill myself"]
};

// Flatten all categories into one array
export const ALL_RESTRICTED_TERMS = Object.values(RESTRICTED_TERMS).flat();

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

// liberal match: case-insensitive, substring (with basic word-ish boundaries)
export function findRestrictedTerms(text: string, restricted: string[]): string[] {
  if (!text || !Array.isArray(restricted)) return [];
  const t = String(text).toLowerCase();
  const hits: string[] = [];
  for (const raw of restricted) {
    const q = String(raw || "").trim().toLowerCase();
    if (!q) continue;
    // allow "blood", "bloodshed", "bloody" to match "blood"
    // but avoid matching super tiny tokens inside words accidentally
    const pat = q.length <= 3 ? new RegExp(`\\b${escapeRx(q)}\\b`, "i")
                              : new RegExp(`${escapeRx(q)}`, "i");
    if (pat.test(t)) hits.push(raw);
  }
  return hits;
}

function escapeRx(s: string) {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

// Check against all restricted terms
export function checkRestrictions(text: string): string[] {
  return findRestrictedTerms(text, ALL_RESTRICTED_TERMS);
}

// Check specific category
export function checkCategory(text: string, category: keyof typeof RESTRICTED_TERMS): string[] {
  return findRestrictedTerms(text, RESTRICTED_TERMS[category]);
}

// Check and return which categories were violated
export function checkCategoriesViolated(text: string): {
  violations: string[];
  categories: string[];
} {
  const violations: string[] = [];
  const categories: string[] = [];
  
  for (const [category, terms] of Object.entries(RESTRICTED_TERMS)) {
    const hits = findRestrictedTerms(text, terms);
    if (hits.length > 0) {
      violations.push(...hits);
      categories.push(category);
    }
  }
  
  return {
    violations: [...new Set(violations)], // remove duplicates
    categories: [...new Set(categories)]
  };
}