"use client";

import { useEffect, useMemo, useState } from "react";
import Script from "next/script";
import { auth } from "@/lib/firebase";

/* ---------------- utilities ---------------- */
function nukeDfCaches() {
  try {
    const wipe = (s: Storage) => {
      const ks: string[] = [];
      for (let i = 0; i < s.length; i++) ks.push(s.key(i) || "");
      ks.forEach((k) => {
        const kk = (k || "").toLowerCase();
        if (kk.includes("df-messenger") || kk.includes("dfmessenger") || kk.includes("dialogflow") || kk.includes("chat_history") || kk.includes("conv_state")) {
          try { s.removeItem(k); } catch {}
        }
      });
    };
    wipe(localStorage);
    wipe(sessionStorage);
  } catch {}
  try {
    const anyIDB: any = indexedDB as any;
    if (anyIDB?.databases) {
      anyIDB.databases().then((dbs: Array<{ name?: string }>) => {
        (dbs || []).forEach((db) => {
          const n = (db?.name || "").toLowerCase();
          if (n.includes("df") || n.includes("dialogflow")) {
            try { indexedDB.deleteDatabase(db!.name!); } catch {}
          }
        });
      }).catch(() => {});
    } else {
      ["df-messenger", "dialogflow-messenger", "dialogflow"].forEach((n) => { try { indexedDB.deleteDatabase(n); } catch {} });
    }
  } catch {}
}

function getTabId(): string {
  try {
    const k = "kidflix_tab_id";
    let v = sessionStorage.getItem(k);
    if (!v) {
      v = crypto.randomUUID?.() || String(Math.random()).slice(2);
      sessionStorage.setItem(k, v);
    }
    return v;
  } catch { return String(Math.random()).slice(2); }
}

/* ---------------- component ---------------- */
export default function DialogflowMessenger() {
  const [uid, setUid] = useState<string | null>(auth.currentUser?.uid || null);
  const tabId = useMemo(getTabId, []);

  const sessionId = useMemo(() => {
    const base = uid ? `user:${uid}` : `anon:${navigator.userAgent || "ua"}:${navigator.language || "lang"}`;
    return `${base}::${tabId}`;
  }, [uid, tabId]);

  const widgetKey = useMemo(() => `df-${sessionId}`, [sessionId]);

  /* ---- personalization + preview interception ---- */
  useEffect(() => {
    if ((window as any).__kidflix_df_wired) return;
    (window as any).__kidflix_df_wired = true;

    try {
      const mode = localStorage.getItem("kidflix_mode");
      (window as any).__kidflix_personalize_on = mode === "personalized";
    } catch {}
    (window as any).kidflixSetPersonalize = (on: boolean) => {
      try { localStorage.setItem("kidflix_mode", on ? "personalized" : "ann"); } catch {}
      (window as any).__kidflix_personalize_on = !!on;
    };

    // --- preview interception for /preview links (cards + anchors) ---
    const parsePreview = (href: string | null) => {
      if (!href) return null;
      try {
        const u = new URL(href, window.location.origin);
        const endsWithPreview = u.pathname === "/preview" || u.pathname.endsWith("/preview") || u.pathname.split("/").pop() === "preview";
        if (!endsWithPreview) return null;
        const detail: Record<string, string> = {};
        u.searchParams.forEach((v, k) => (detail[k] = v));
        return detail;
      } catch { return null; }
    };
    const stop = (e: Event) => { try { e.preventDefault?.(); e.stopPropagation?.(); (e as any).stopImmediatePropagation?.(); } catch {} };
    const openPreviewEv = (detail: Record<string, string>) => {
      const opener = (window as any).kidflixOpenPreview || (window as any).openAppModal;
      if (typeof opener === "function") opener(detail);
      else window.dispatchEvent(new CustomEvent("kidflix:open-preview", { detail }));
    };

    const clickCapture = (ev: MouseEvent) => {
      try {
        const path: any[] = (ev.composedPath && ev.composedPath()) || [];
        const a = path.find((n: any) => (n?.tagName?.toLowerCase?.() === "a" && n?.getAttribute?.("href")) || n?.href);
        const href = (a?.getAttribute?.("href") as string) || (typeof a?.href === "string" ? (a.href as string) : null);
        const detail = parsePreview(href);
        if (!detail) return;
        stop(ev);
        openPreviewEv(detail);
      } catch {}
    };
    document.addEventListener("click", clickCapture, true);

    const onInfo   = (e: any) => { const d = parsePreview(e?.detail?.actionLink || ""); if (!d) return; stop(e); openPreviewEv(d); };
    const onUrl    = (e: any) => { const d = parsePreview(e?.detail?.url || "");        if (!d) return; stop(e); openPreviewEv(d); };
    const onAnchor = (e: any) => { const d = parsePreview(e?.detail?.href || e?.detail?.url || ""); if (!d) return; stop(e); openPreviewEv(d); };
    window.addEventListener("df-info-card-clicked" as any, onInfo as any);
    window.addEventListener("df-url-clicked" as any, onUrl as any);
    window.addEventListener("df-anchor-clicked" as any, onAnchor as any);

    // --- inject userId / lastUserText / mode on each request ---
    if (!(window as any).__kidflix_df_hook_wired) {
      (window as any).__kidflix_df_hook_wired = true;

      const ensureParams = (body: any) => {
        body.queryParams = body.queryParams || {};
        body.queryParams.parameters = body.queryParams.parameters || {};
        return body.queryParams.parameters;
      };
      const pickUtter = (body: any): string | undefined =>
        body?.text?.text || body?.queryInput?.text?.text || body?.query?.text || body?.query?.query || undefined;

      const onRequestSent = (event: any) => {
        try {
          const body: any = event?.detail?.data?.requestBody;
          if (!body || typeof body !== "object") return;

          const p = ensureParams(body);
          const curUid =
            localStorage.getItem("kidflix_uid") ||
            (window as any).kidflix_uid ||
            auth.currentUser?.uid ||
            undefined;
          if (curUid) p.userId = curUid;

          const personalize =
            (window as any).__kidflix_personalize_on ??
            (localStorage.getItem("kidflix_mode") === "personalized");

          const userText = pickUtter(body);
          if (typeof userText === "string" && userText.trim()) p.lastUserText = userText.trim();

          p.personalize = !!personalize;
          p.canPersonalize = !!personalize;
          p.mode = personalize ? "personalized" : "ann";
        } catch (e) {
          console.warn("df-request-sent hook error:", e);
        }
      };

      window.addEventListener("df-request-sent" as any, onRequestSent as any, { passive: true });
      window.addEventListener("dfRequestSent" as any, onRequestSent as any, { passive: true });

      (window as any).__kidflix_df_hook_cleanup = () => {
        window.removeEventListener("df-request-sent" as any, onRequestSent as any);
        window.removeEventListener("dfRequestSent" as any, onRequestSent as any);
      };
    }

    return () => {
      try { (window as any).__kidflix_df_hook_cleanup?.(); } catch {}
      document.removeEventListener("click", clickCapture, true);
      window.removeEventListener("df-info-card-clicked" as any, onInfo as any);
      window.removeEventListener("df-url-clicked" as any, onUrl as any);
      window.removeEventListener("df-anchor-clicked" as any, onAnchor as any);
      (window as any).__kidflix_df_wired = false;
    };
  }, []);

  /* ---- clear caches ONLY when auth changes ---- */
  useEffect(() => {
    const unsub = auth.onAuthStateChanged((next) => {
      const nextUid = next?.uid || null;
      if (uid !== nextUid) {
        nukeDfCaches();
        setUid(nextUid); // triggers remount via widgetKey
      }
    });
    return () => unsub();
  }, [uid]);

  /* ---- fire WELCOME event + safe bot-bubble fallback (force agent side) ---- */
useEffect(() => {
  const WELCOME_TEXT =
    "Hi! I’m Kidflix Assistant 👋. I can help to recommend books or videos for kids. Would you like to start with Books or Videos?";

  // Try all known shapes that render as **bot/agent** across df-messenger builds
  const renderBotText = (m: any, text: string) => {
    // 1) Modern: explicit 'agent'
    try { m.renderCustomText?.(text, "agent"); return true; } catch {}
    // 2) Legacy: boolean flag means "isUserMessage"
    try { m.renderCustomText?.(text, false); return true; } catch {}
    // 3) Some builds: 'bot'
    try { m.renderCustomText?.(text, "bot"); return true; } catch {}
    // 4) Object arg (rare experimental)
    try { m.renderCustomText?.({ text, sender: "agent" }); return true; } catch {}
    return false;
  };

  const countMessages = () => {
    try {
      const el: any = document.querySelector("df-messenger");
      const root = (el as any)?.shadowRoot as ShadowRoot | undefined;
      if (!root) return 0;
      return root.querySelectorAll("[message]").length || 0;
    } catch { return 0; }
  };

  let fired = false;
  let settleTimer: any = null;
  let safetyTimer: any = null;

  const sendWelcome = () => {
    if (fired) return;
    fired = true;

    const m: any = document.querySelector("df-messenger");
    if (!m) return;

    // Ask CX to handle the Start-page event (preferred)
    try { m.renderCustomEvent?.({ event: "WELCOME", languageCode: "en" }); } catch {}
    try { m.renderCustomEvent?.({ name: "WELCOME", languageCode: "en" }); } catch {}
    try { m.renderCustomEvent?.("WELCOME"); } catch {}

    // If no agent message appears, inject our own **agent** bubble
    const before = countMessages();
    settleTimer = setTimeout(() => {
      const after = countMessages();
      if (after <= before) {
        renderBotText(m, WELCOME_TEXT);
      }
    }, 1400);
  };

  const onLoaded = () => {
    if (safetyTimer) { clearTimeout(safetyTimer); safetyTimer = null; }
    sendWelcome();
  };

  window.addEventListener("df-messenger-loaded" as any, onLoaded as any, { once: true } as any);

  // Fire anyway if load event is missed
  safetyTimer = setTimeout(() => {
    if (document.querySelector("df-messenger")) sendWelcome();
  }, 1200);

  return () => {
    window.removeEventListener("df-messenger-loaded" as any, onLoaded as any);
    if (settleTimer) clearTimeout(settleTimer);
    if (safetyTimer) clearTimeout(safetyTimer);
  };
}, [widgetKey]);

  return (
    <>
      <Script src="https://www.gstatic.com/dialogflow-console/fast/df-messenger/prod/v1/df-messenger.js" strategy="afterInteractive" />
      <link
        rel="stylesheet"
        href="https://www.gstatic.com/dialogflow-console/fast/df-messenger/prod/v1/themes/df-messenger-default.css"
      />

      <div key={widgetKey}>
        <df-messenger
          location="asia-southeast1"
          project-id="kidflix-4cda0"
          agent-id="9a0bbfa5-d4cd-490f-bb51-531a5d2b3d84"
          language-code="en"
          max-query-length="-1"
          session-id={sessionId}
        >
          <df-messenger-chat-bubble chat-title="FlixBot"></df-messenger-chat-bubble>
        </df-messenger>
      </div>

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
}
