// src/lib/previewBus.ts
export type PreviewDetail = {
  type?: "book" | "video";
  id?: string;
  title?: string;
  image?: string;
  link?: string;   // embed or info link
  url?: string;    // watch/info link
  authors?: string;
  snippet?: string;
  category?: string;
  topic?: string;
  age?: string;
  source?: string;
};

const EVT = "kidflix:open-preview";

export function openPreview(detail: PreviewDetail) {
  window.dispatchEvent(new CustomEvent(EVT, { detail }));
}

export function listenPreview(handler: (d: PreviewDetail) => void) {
  const fn = (e: Event) => handler((e as CustomEvent).detail as PreviewDetail);
  window.addEventListener(EVT, fn as any);
  return () => window.removeEventListener(EVT, fn as any);
}
