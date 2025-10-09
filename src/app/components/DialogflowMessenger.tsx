"use client";

import React from "react";
import Script from "next/script";

// Utility: create elements safely for TS
const DFMessenger = (props: any) => React.createElement("df-messenger", props);
const DFBubble = (props: any) =>
  React.createElement("df-messenger-chat-bubble", props);

// Generate a unique, stable session ID per tab
function newSessionId() {
  return `kidflix-${Date.now()}-${Math.random().toString(36).slice(2)}`;
}

// Optional: clear Dialogflow storage
function clearDfStorage() {
  try {
    for (const store of [localStorage, sessionStorage]) {
      Object.keys(store).forEach((key) => {
        const lower = key.toLowerCase();
        if (lower.includes("df") || lower.includes("dialogflow")) {
          store.removeItem(key);
        }
      });
    }
  } catch {}
}

export default function DialogflowMessenger() {
  const [ready, setReady] = React.useState(false);
  const [sessionId, setSessionId] = React.useState<string | null>(null);

  const projectId = process.env.NEXT_PUBLIC_DF_PROJECT_ID!;
  const agentId = process.env.NEXT_PUBLIC_DF_AGENT_ID!;
  const location = process.env.NEXT_PUBLIC_DF_LOCATION || "asia-southeast1";
  const chatTitle = process.env.NEXT_PUBLIC_CHAT_TITLE || "Kidflix Assistant";

  const SESSION_KEY = "kidflix_df_session_id";

  // Setup session ID
  React.useEffect(() => {
    if (typeof window === "undefined") return;

    let sid = sessionStorage.getItem(SESSION_KEY);
    if (!sid) {
      sid = newSessionId();
      sessionStorage.setItem(SESSION_KEY, sid);
    }
    setSessionId(sid);

    // expose a reset function in dev tools
    (window as any).resetKidflixChat = () => {
      clearDfStorage();
      const fresh = newSessionId();
      sessionStorage.setItem(SESSION_KEY, fresh);
      setSessionId(fresh);
    };
  }, []);

  // Ensure Messenger script loaded properly
  const handleScriptLoad = () => {
    console.log("✅ Dialogflow script loaded");
    setReady(true);
  };

  return (
    <>
      {/* --- Trusted Types shim for Dialogflow --- */}
      <Script
        id="df-tt-shim"
        strategy="beforeInteractive"
        dangerouslySetInnerHTML={{
          __html: `
            (function(){
              try {
                if (!window.trustedTypes) {
                  window.trustedTypes = {
                    createPolicy: function(_name, rules) { return rules; }
                  };
                }
              } catch(e){}
            })();
          `,
        }}
      />

      {/* --- Dialogflow Messenger styles --- */}
      <link
        rel="stylesheet"
        href="https://www.gstatic.com/dialogflow-console/fast/df-messenger/prod/v1/themes/df-messenger-default.css"
      />

      {/* --- Dialogflow Messenger script --- */}
      <Script
        id="df-messenger-js"
        src="https://www.gstatic.com/dialogflow-console/fast/df-messenger/prod/v1/df-messenger.js"
        strategy="afterInteractive"
        onLoad={handleScriptLoad}
      />

      {/* --- Render chat widget only when ready --- */}
      {ready && sessionId && (
        <DFMessenger
          key={sessionId}
          location={location}
          project-id={projectId}
          agent-id={agentId}
          language-code="en"
          session-id={sessionId}
          expand={false}
          style={{
            position: "fixed",
            bottom: "16px",
            right: "16px",
            zIndex: 9999999999,
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
