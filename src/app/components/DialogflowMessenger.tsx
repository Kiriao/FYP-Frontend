"use client";

import React from "react";
import Script from "next/script";

// Render custom elements via createElement so TS doesn't complain
const DFMessenger = (props: any) => React.createElement("df-messenger", props);
const DFBubble = (props: any) =>
  React.createElement("df-messenger-chat-bubble", props);

// Generate a stable session id per tab
function newSessionId() {
  return `kidflix-${Date.now()}-${Math.random().toString(36).slice(2)}`;
}

// Optional: clear widget’s own storage & rotate session
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
  const [ready, setReady] = React.useState(false);

  const projectId = process.env.NEXT_PUBLIC_DF_PROJECT_ID!;
  const agentId = process.env.NEXT_PUBLIC_DF_AGENT_ID!;
  const location = process.env.NEXT_PUBLIC_DF_LOCATION || "asia-southeast1";
  const chatTitle = process.env.NEXT_PUBLIC_CHAT_TITLE || "Kidflix Assistant";

  // Stable session per browser tab
  const SESSION_KEY = "kidflix_df_session_id";
  const [sessionId, setSessionId] = React.useState<string | null>(null);

  React.useEffect(() => {
    if (typeof window === "undefined") return;
    let sid = sessionStorage.getItem(SESSION_KEY);
    if (!sid) {
      sid = newSessionId();
      sessionStorage.setItem(SESSION_KEY, sid);
    }
    setSessionId(sid);

    // Expose a reset helper in DevTools
    (window as any).resetKidflixChat = () => {
      clearDfStorage();
      const fresh = newSessionId();
      sessionStorage.setItem(SESSION_KEY, fresh);
      setSessionId(fresh);
    };
  }, []);

  return (
    <>
      {/* --- Tiny Trusted Types shim BEFORE df-messenger loads --- */}
      <Script
        id="df-tt-shim"
        strategy="beforeInteractive"
        dangerouslySetInnerHTML={{
          __html: `
            try {
              if (!window.trustedTypes || !window.trustedTypes.createPolicy) {
                window.trustedTypes = {
                  // Return the rules object which includes createScriptURL etc.
                  createPolicy: function(_name, rules) { return rules; }
                };
              }
            } catch (e) { /* ignore */ }
          `,
        }}
      />

      {/* DF Messenger CSS */}
      <link
        rel="stylesheet"
        href="https://www.gstatic.com/dialogflow-console/fast/df-messenger/prod/v1/themes/df-messenger-default.css"
      />

      {/* DF Messenger script */}
      <Script
        id="df-messenger-js"
        src="https://www.gstatic.com/dialogflow-console/fast/df-messenger/prod/v1/df-messenger.js"
        strategy="afterInteractive"
        onLoad={() => setReady(true)}
      />

      {/* Render only once the script is ready and we have a session */}
      {ready && sessionId && (
        <DFMessenger
          key={sessionId} // remounts on reset
          location={location}
          project-id={projectId}
          agent-id={agentId}
          language-code="en"
          // Avoid unsupported attributes that can break the widget
          session-id={sessionId}
          style={{
            position: "fixed",
            bottom: "16px",
            right: "16px",
            zIndex: 2147483647, // beat anything
            // Optional theming via CSS vars:
            // @ts-ignore - custom CSS vars
            ["--df-messenger-font-color" as any]: "#000",
            ["--df-messenger-chat-background" as any]: "#f3f6fc",
            ["--df-messenger-message-user-background" as any]: "#d3e3fd",
            ["--df-messenger-message-bot-background" as any]: "#fff",
          }}
        >
          {/* Bubble via createElement so TS is happy */}
          <DFBubble chat-title={chatTitle} />
        </DFMessenger>
      )}
    </>
  );
}
