"use client";

import { useEffect } from "react";
import { useRouter } from "next/navigation";

export default function PreviewPage() {
  const router = useRouter();

  useEffect(() => {
    // Read query params from the URL on the client
    const sp = new URLSearchParams(window.location.search);
    const params: Record<string, string> = {};
    sp.forEach((v, k) => (params[k] = v));

    // Fire the same event your app listens for to open the modal
    window.dispatchEvent(new CustomEvent("kidflix:open-preview", { detail: params }));

    // Navigate back so the modal shows over the previous page.
    // If there is no history (opened in a new tab), go home instead.
    if (window.history.length > 1) {
      router.back();
    } else {
      router.push("/");
    }
  }, [router]);

  return null; // nothing to render
}
