"use client";

import React, { useEffect, useState } from "react";

// Define wrapper components for custom elements
const DFMessenger = (props: any) => React.createElement("df-messenger", props);
const DFBubble = (props: any) =>
  React.createElement("df-messenger-chat-bubble", props);

// --- Utility helpers ---
function newSessionId() {
  return `kidflix-${Date.now()}-${Math.random().toString(36).slice(2)}`;
}

function clearDfStorage() {
  try {
    for (const store of [localStorage, sessionStorage]) {
      Object.keys(store).forEach((k) => {
        const kk = k.toLowerCase();
        if (kk.includes("df") || kk.includes("dialogflow")) store.removeItem(k);
      });
    }
  } catch {}
}

export default function DialogflowMessenger() {
  const [ready, setReady] = useState(false);
  const [sessionId, setSessionId] = useState<string | null>(null);

  const projectId = process.env.NEXT_PUBLIC_DF_PROJECT_ID!;
  const agentId = process.env.NEXT_PUBLIC_DF_AGENT_ID!;
  const location = process.env.NEXT_PUBLIC_DF_LOCATION || "asia-southeast1";
  const chatTitle = process.env.NEXT_PUBLIC_CHAT_TITLE || "FlixBot";

  const SESSION_KEY = "kidflix_df_session_id";

  // --- 1️⃣ Inject Trusted Types shim *immediately* ---
  useEffect(() => {
    if (typeof window === "undefined") return;

    const shim = document.createElement("script");
    shim.type = "text/javascript";
    shim.textContent = `
      (function() {
        try {
          if (!window.trustedTypes || !window.trustedTypes.createPolicy) {
            window.trustedTypes = {
              createPolicy: function(name, rules) {
                return {
                  createHTML: rules.createHTML || ((x) => x),
                  createScript: rules.createScript || ((x) => x),
                  createScriptURL: rules.createScriptURL || ((x) => x)
                };
              }
            };
            console.log("✅ TrustedTypes shim injected before Dialogflow");
          }
        } catch(e) {
          console.error("Trusted Types shim error", e);
        }
      })();
    `;
    document.head.prepend(shim); // run before any async script
  }, []);

  // --- 2️⃣ Load Dialogflow script only *after* shim is ready ---
  useEffect(() => {
    if (typeof window === "undefined") return;

    const existing = document.querySelector("script#df-messenger-js");
    if (existing) {
      setReady(true);
      return;
    }

    const script = document.createElement("script");
    script.id = "df-messenger-js";
    script.src =
      "https://www.gstatic.com/dialogflow-console/fast/df-messenger/prod/v1/df-messenger.js";
    script.async = true;
    script.onload = () => {
      console.log("✅ Dialogflow script loaded safely");
      setReady(true);
    };
    document.head.appendChild(script);
  }, []);

  // --- 3️⃣ Maintain stable session id ---
  useEffect(() => {
    if (typeof window === "undefined") return;
    let sid = sessionStorage.getItem(SESSION_KEY);
    if (!sid) {
      sid = newSessionId();
      sessionStorage.setItem(SESSION_KEY, sid);
    }
    setSessionId(sid);

    (window as any).resetKidflixChat = () => {
      clearDfStorage();
      const fresh = newSessionId();
      sessionStorage.setItem(SESSION_KEY, fresh);
      setSessionId(fresh);
    };
  }, []);

  return (
    <>
      {/* Dialogflow Default Theme */}
      <link
        rel="stylesheet"
        href="https://www.gstatic.com/dialogflow-console/fast/df-messenger/prod/v1/themes/df-messenger-default.css"
      />

      {/* Render only when ready */}
      {ready && sessionId && (
        <DFMessenger
          key={sessionId}
          location={location}
          project-id={projectId}
          agent-id={agentId}
          language-code="en"
          session-id={sessionId}
          max-query-length="-1"
          style={{
            position: "fixed",
            bottom: "16px",
            right: "16px",
            zIndex: 2147483647,
            ["--df-messenger-font-color" as any]: "#000",
            ["--df-messenger-font-family" as any]: "Google Sans",
            ["--df-messenger-chat-background" as any]: "#f3f6fc",
            ["--df-messenger-message-user-background" as any]: "#d3e3fd",
            ["--df-messenger-message-bot-background" as any]: "#fff",
          }}
        >
          <DFBubble chat-title={chatTitle} />
        </DFMessenger>
      )}
    </>
  );
}
