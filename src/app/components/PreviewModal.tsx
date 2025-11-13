"use client";
import React, { useEffect, useMemo, useState } from "react";
import { listenPreview, openPreview, type PreviewDetail } from "@/lib/previewBus";

/** --- Shape used by your /catalogue modal markup --- */
type BookItem = {
  id: string;
  title: string;
  thumbnail: string;
  link: string;
  url: string;
  authors: string[];
  snippet: string;
  description: string;
  source: string;
  category: string;
  age: string;
};

type VideoItem = {
  videoId: string;
  title: string;
  thumbnail: string;
  link: string;  // embed URL
  url: string;   // watch URL
  source: string;
};

type SelectedItem = BookItem | VideoItem;

/** Utility that mirrors what you already do in /catalogue for videos/books */
function fromPreview(detail: PreviewDetail): SelectedItem {
  if (detail.type === "video") {
    const vid =
      detail.id ||
      (detail.url && /[?&]v=([^&]+)/.exec(String(detail.url))?.[1]) ||
      "";

    const embed =
      detail.link && String(detail.link).includes("/embed/")
        ? detail.link
        : vid
        ? `https://www.youtube.com/embed/${vid}`
        : detail.link || "";

    const watch = detail.url || (vid ? `https://www.youtube.com/watch?v=${vid}` : "");

    return {
      videoId: vid || "",
      title: detail.title || "",
      thumbnail: detail.image || "",
      link: embed,
      url: watch,
      source: detail.source || "chat",
    } as VideoItem;
  }

  // book
  return {
    id: detail.id || crypto.randomUUID(),
    title: detail.title || "",
    thumbnail: detail.image || "",
    link: detail.link || "",
    url: detail.url || detail.link || "",
    authors: detail.authors
      ? String(detail.authors)
          .split(",")
          .map((s) => s.trim())
          .filter(Boolean)
      : [],
    snippet: detail.snippet || "",
    description: detail.snippet || "",
    source: detail.source || "chat",
    category: detail.category || "",
    age: detail.age || "",
  } as BookItem;
}

export default function GlobalPreviewModalHost() {
  const [open, setOpen] = useState(false);
  const [item, setItem] = useState<SelectedItem | null>(null);

  // listen once, globally
  useEffect(() => {
    const off = listenPreview((d) => {
      setItem(fromPreview(d));
      setOpen(true);
    });
    return () => off();
  }, []);

  if (!open || !item) return null;

  const isVideo = (i: SelectedItem): i is VideoItem => (i as any).videoId !== undefined;

  return (
    <div
      aria-modal
      role="dialog"
      className="fixed inset-0 z-[1000] flex items-center justify-center bg-black/50"
      onClick={() => {
        setOpen(false);
        setItem(null);
      }}
    >
      <div
        className="w-[92vw] max-w-[720px] rounded-2xl bg-white shadow-xl p-4 md:p-6 max-h-[90vh] overflow-y-auto"
        onClick={(e) => e.stopPropagation()}
      >
        {/* --- Video modal --- */}
        {isVideo(item) ? (
          <>
            <h2 className="text-xl font-bold mb-3">{item.title}</h2>
            <div className="aspect-video w-full rounded-lg overflow-hidden bg-black">
              <iframe
                src={item.link}
                title={item.title}
                className="w-full h-full"
                allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture"
                allowFullScreen
              />
            </div>
            <div className="mt-4 flex gap-2">
              <a
                href={item.url}
                target="_blank"
                rel="noreferrer"
                className="px-3 py-2 text-sm rounded-lg bg-blue-600 text-white"
              >
                Watch on YouTube
              </a>
              <button
                onClick={() => {
                  setOpen(false);
                  setItem(null);
                }}
                className="px-3 py-2 text-sm rounded-lg border border-gray-300"
              >
                Close
              </button>
            </div>
          </>
        ) : (
          /* --- Book modal (condensed version of your /catalogue UI) --- */
          <>
            <div className="flex gap-4">
              {item.thumbnail ? (
                <img
                  src={item.thumbnail}
                  alt={item.title}
                  className="w-24 h-36 object-cover rounded-lg flex-shrink-0"
                />
              ) : null}
              <div className="min-w-0">
                <h2 className="text-xl font-bold mb-1">{item.title}</h2>
                {!!item.authors?.length && (
                  <p className="text-xs text-gray-600">{item.authors.join(", ")}</p>
                )}
                {!!item.category && (
                  <p className="text-[11px] text-gray-500 mt-1">Category: {item.category}</p>
                )}
                {!!item.age && <p className="text-[11px] text-gray-500">Age: {item.age}</p>}
                {!!item.description && (
                  <p className="text-sm text-gray-700 mt-3">{item.description}</p>
                )}
                <div className="mt-4 flex gap-2">
                  {item.link && (
                    <a
                      href={item.link}
                      target="_blank"
                      rel="noreferrer"
                      className="px-3 py-2 text-sm rounded-lg bg-blue-600 text-white"
                    >
                      Read sample
                    </a>
                  )}
                  {item.url && (
                    <a
                      href={item.url}
                      target="_blank"
                      rel="noreferrer"
                      className="px-3 py-2 text-sm rounded-lg border border-gray-300"
                    >
                      View on Google Books
                    </a>
                  )}
                  <button
                    onClick={() => {
                      setOpen(false);
                      setItem(null);
                    }}
                    className="px-3 py-2 text-sm rounded-lg border border-gray-300"
                  >
                    Close
                  </button>
                </div>
              </div>
            </div>
          </>
        )}
      </div>
    </div>
  );
}
