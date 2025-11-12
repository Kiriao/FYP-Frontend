// functions/src/lib/personalize.ts
// Lightweight heuristic to detect if the user explicitly wants personalization.
export function wantsPersonalized(raw: string): boolean {
  if (!raw) return false;
  const t = raw.toLowerCase();

  // Feel free to extend this list as you see real queries
  return Boolean(
    t.match(
      /\b(for me|for my kid|for my child|based on (my|our) (likes|history|profile)|personal(?:ized)?|like (last time|what i liked)|my preferences?)\b/
    )
  );
}
