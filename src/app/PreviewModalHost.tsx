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

const handleOpenOriginal = () => {
  if (!data?.link) return;

  let finalUrl = data.link;

  // Replace any Firebase URLs with Vercel URL
  if (data.link.includes('kidflix-4cda0.web.app')) {
    const baseUrl = process.env.NEXT_PUBLIC_APP_URL || 'https://fyp-frontend-coral.vercel.app';
    const path = data.link.replace(/https?:\/\/kidflix-4cda0\.web\.app/g, '');
    finalUrl = `${baseUrl}${path}`;
  }
  // Handle relative URLs
  else if (!data.link.startsWith('http')) {
    const baseUrl = process.env.NEXT_PUBLIC_APP_URL || 'https://fyp-frontend-coral.vercel.app';
    finalUrl = `${baseUrl}${data.link.startsWith('/') ? '' : '/'}${data.link}`;
  }

  window.open(finalUrl, "_blank");
};

  if (!open || !data) return null;

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/50"
      onClick={() => setOpen(false)}
    >
      <div
        className="bg-white rounded-xl shadow-xl max-w-md w-full mx-4 overflow-hidden"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="p-6">
          {data.image ? (
            <img
              src={data.image}
              alt={data.title}
              className="w-full h-48 object-cover rounded-lg mb-4"
            />
          ) : null}
          <div className="space-y-3">
            <h3 className="text-xl font-bold text-gray-900">{data.title}</h3>
            <p className="text-sm text-gray-600">
              {data.type === "book" ? `Category: ${data.category || "-"}` : `Topic: ${data.topic || "-"}`}
              {data.age ? ` • Age: ${data.age}` : ""}
            </p>
            <p className="text-xs text-gray-500">Source: {data.source || "unknown"}</p>
            <div className="flex gap-3 pt-3">
              {data.link ? (
                <button
                  onClick={handleOpenOriginal}
                  className="px-4 py-2 text-sm font-medium text-white bg-blue-600 rounded-lg hover:bg-blue-700"
                >
                  Open original
                </button>
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