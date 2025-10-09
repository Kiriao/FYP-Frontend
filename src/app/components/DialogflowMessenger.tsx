"use client";

import React from "react";
import Script from "next/script";

const DFMessenger = (props: any) => React.createElement("df-messenger", props);
const DFBubble = (props: any) =>
  React.createElement("df-messenger-chat-bubble", props);

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
  const [ready, setReady] = React.useState(false);

  const projectId = process.env.NEXT_PUBLIC_DF_PROJECT_ID!;
  const agentId = process.env.NEXT_PUBLIC_DF_AGENT_ID!;
  const location = process.env.NEXT_PUBLIC_DF_LOCATION || "asia-southeast1";
  const chatTitle = process.env.NEXT_PUBLIC_CHAT_TITLE || "Kidflix Assistant";

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

    (window as any).resetKidflixChat = () => {
      clearDfStorage();
      const fresh = newSessionId();
      sessionStorage.setItem(SESSION_KEY, fresh);
      setSessionId(fresh);
    };
  }, []);

  return (
    <>
      <Script
        id="df-tt-shim"
        strategy="beforeInteractive"
        dangerouslySetInnerHTML={{
          __html: `
            try {
              if (!window.trustedTypes || !window.trustedTypes.createPolicy) {
                window.trustedTypes = {
                  createPolicy: function(_name, rules) { return rules; }
                };
              }
            } catch (e) { }
          `,
        }}
      />

      <link
        rel="stylesheet"
        href="https://www.gstatic.com/dialogflow-console/fast/df-messenger/prod/v1/themes/df-messenger-default.css"
      />

      <Script
        id="df-messenger-js"
        src="https://www.gstatic.com/dialogflow-console/fast/df-messenger/prod/v1/df-messenger.js"
        strategy="afterInteractive"
        onLoad={() => setReady(true)}
      />

      {ready && sessionId && (
        <DFMessenger
          key={sessionId}
          location={location}
          project-id={projectId}
          agent-id={agentId}
          language-code="en"
          session-id={sessionId}
          style={{
            position: "fixed",
            bottom: "16px",
            right: "16px",
            zIndex: 2147483647,
            ["--df-messenger-font-color" as any]: "#000",
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
