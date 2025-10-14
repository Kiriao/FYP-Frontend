"use client";
import Script from "next/script";
import { useEffect } from "react";

export default function DialogflowMessenger() {
  useEffect(() => {
<<<<<<< HEAD
    // Prevent duplicate <df-messenger> creation
    if (document.querySelector("df-messenger")) return;
=======
    // avoid double wiring
    // @ts-ignore
    if ((window as any).__kidflix_click_wired) return;
    // @ts-ignore
    (window as any).__kidflix_click_wired = true;

    const parsePreview = (href: string) => {
      try {
        const u = new URL(href, window.location.origin);
        if (u.pathname !== "/preview") return null;
        const detail: Record<string, string> = {};
        u.searchParams.forEach((v, k) => (detail[k] = v));
        return detail;
      } catch {
        return null;
      }
    };

    const stop = (e: Event) => {
      try { e.preventDefault?.(); e.stopPropagation?.(); (e as any).stopImmediatePropagation?.(); } catch {}
    };

    // 👉 Only one path: prefer real modal; else fall back to broadcast
    const openPreview = (detail: Record<string, string>) => {
      const opener = (window as any).openAppModal;
      if (typeof opener === "function") {
        opener(detail);
      } else {
        window.dispatchEvent(new CustomEvent("kidflix:open-preview", { detail }));
      }
    };

    // Capture clicks from Messenger (works through shadow DOM)
    const clickCapture = (ev: MouseEvent) => {
      const path = (ev.composedPath && ev.composedPath()) || [];
      const a = path.find((n: any) => n?.tagName?.toLowerCase?.() === "a") as HTMLAnchorElement | undefined;
      if (!a) return;
      const detail = parsePreview(a.getAttribute("href") || "");
      if (!detail) return;
      stop(ev);
      openPreview(detail);
    };
    window.addEventListener("click", clickCapture, true);

    // Some builds emit custom events; handle them too
    const onInfo = (e: any) => {
      const detail = parsePreview(e?.detail?.actionLink || "");
      if (!detail) return;
      stop(e);
      openPreview(detail);
    };
    const onUrl = (e: any) => {
      const detail = parsePreview(e?.detail?.url || "");
      if (!detail) return;
      stop(e);
      openPreview(detail);
    };
    window.addEventListener("df-info-card-clicked" as any, onInfo as any);
    window.addEventListener("df-url-clicked" as any, onUrl as any);

    return () => {
      window.removeEventListener("click", clickCapture, true);
      window.removeEventListener("df-info-card-clicked" as any, onInfo as any);
      window.removeEventListener("df-url-clicked" as any, onUrl as any);
    };
>>>>>>> main
  }, []);

  return (
    <>
<<<<<<< HEAD
      {/* Load the CX Messenger script only once */}
=======
>>>>>>> main
      <Script
        src="https://www.gstatic.com/dialogflow-console/fast/df-messenger/prod/v1/df-messenger.js"
        strategy="afterInteractive"
      />
<<<<<<< HEAD
      {/* Optionally load CSS theme */}
=======
>>>>>>> main
      <link
        rel="stylesheet"
        href="https://www.gstatic.com/dialogflow-console/fast/df-messenger/prod/v1/themes/df-messenger-default.css"
      />
<<<<<<< HEAD

      {/* Messenger element */}
=======
>>>>>>> main
      <df-messenger
        location="asia-southeast1"
        project-id="kidflix-4cda0"
        agent-id="9a0bbfa5-d4cd-490f-bb51-531a5d2b3d84"
        language-code="en"
        max-query-length="-1"
      >
        <df-messenger-chat-bubble chat-title="FlixBot"></df-messenger-chat-bubble>
      </df-messenger>

      <style jsx global>{`
        df-messenger {
          z-index: 999;
          position: fixed;
          --df-messenger-font-color: #000;
          --df-messenger-font-family: Google Sans;
          --df-messenger-chat-background: #f3f6fc;
          --df-messenger-message-user-background: #d3e3fd;
          --df-messenger-message-bot-background: #fff;
          bottom: 16px;
          right: 16px;
        }
      `}</style>
    </>
  );
<<<<<<< HEAD
}
=======
}
>>>>>>> main
