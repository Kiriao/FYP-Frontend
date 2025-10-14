"use client";
import { useEffect, useState } from "react";

type PreviewData = {
  type?: "book" | "video";
  id?: string;
  title?: string;
  image?: string;
  link?: string;
  category?: string;
  age?: string;
  topic?: string;
  source?: string;
};

export default function PreviewModalHost() {
  const [open, setOpen] = useState(false);
  const [data, setData] = useState<PreviewData | null>(null);

  useEffect(() => {
    const handler = (e: any) => {
      const detail = e.detail as PreviewData;

      // 👇 If your app exposes a modal opener, use it and bail.
      const openAppModal = (window as any).kidflixOpenPreview as ((d: PreviewData) => void) | undefined;
      if (typeof openAppModal === "function") {
        openAppModal(detail);
        return;
      }

      // Fallback: show the lightweight modal here
      setData(detail);
      setOpen(true);
    };
    window.addEventListener("kidflix:open-preview", handler);
    return () => window.removeEventListener("kidflix:open-preview", handler);
  }, []);

  if (!open || !data) return null;

  // (fallback modal UI omitted for brevity — keep what you already have)

  // 🔁 Replace this quick modal with your existing modal component if you have one
  return (
    <div
      aria-modal
      role="dialog"
      className="fixed inset-0 z-[1000] flex items-center justify-center bg-black/50"
      onClick={() => setOpen(false)}
    >
      <div
        className="w-[90vw] max-w-3xl rounded-2xl bg-white shadow-xl p-4 md:p-6"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex gap-4">
          {data.image ? (
            <img
              src={data.image}
              alt={data.title || "preview"}
              className="w-36 h-52 object-cover rounded-lg flex-shrink-0"
            />
          ) : null}
          <div className="min-w-0">
            <h2 className="text-xl font-semibold line-clamp-2">{data.title}</h2>
            <p className="text-sm text-gray-500 mt-1">
              {data.type === "book" ? `Category: ${data.category || "-"}` : `Topic: ${data.topic || "-"}`}
              {data.age ? ` • Age: ${data.age}` : ""}
            </p>
            <p className="text-xs text-gray-400 mt-1">Source: {data.source || "unknown"}</p>
            <div className="mt-4 flex gap-2">
              {data.link ? (
                <a
                  href={data.link}
                  target="_blank"
                  rel="noreferrer"
                  className="px-3 py-2 text-sm rounded-lg bg-blue-600 text-white"
                >
                  Open original
                </a>
              ) : null}
              <button
                onClick={() => setOpen(false)}
                className="px-3 py-2 text-sm rounded-lg border border-gray-300"
              >
                Close
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
