"use client";

import { useEffect } from "react";
import { useRouter } from "next/navigation";

export default function PreviewPage() {
  const router = useRouter();

  useEffect(() => {
    // Prevent double-run if user navigates back/forward fast
    if (sessionStorage.getItem("__kidflix_preview_once__")) return;
    sessionStorage.setItem("__kidflix_preview_once__", "1");

    try {
      const sp = new URLSearchParams(window.location.search);
      const params: Record<string, string> = {};
      sp.forEach((v, k) => (params[k] = v));

      // Prefer the real in-app modal if present
      const openApp = (window as any).kidflixOpenPreview;
      if (typeof openApp === "function") {
        openApp(params);
      } else {
        window.dispatchEvent(new CustomEvent("kidflix:open-preview", { detail: params }));
      }
    } finally {
      // Navigate away exactly once
      setTimeout(() => {
        try {
          if (window.history.length > 1) router.back();
          else router.push("/");
        } catch {
          router.push("/");
        } finally {
          // allow future previews after we leave this page
          setTimeout(() => sessionStorage.removeItem("__kidflix_preview_once__"), 500);
        }
      }, 0);
    }
  }, [router]);

  return null;
}
