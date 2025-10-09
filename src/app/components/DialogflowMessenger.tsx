"use client";

import React, { useEffect, useState } from "react";
import Script from "next/script";

// Define wrapper components
const DFMessenger = (props: any) => React.createElement("df-messenger", props);
const DFBubble = (props: any) =>
  React.createElement("df-messenger-chat-bubble", props);

// Generate unique session IDs
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
  const chatTitle = process.env.NEXT_PUBLIC_CHAT_TITLE || "Kidflix Assistant";

  const SESSION_KEY = "kidflix_df_session_id";

  // Ensure the Trusted Types shim runs before anything else
  useEffect(() => {
    const shim = document.createElement("script");
    shim.innerHTML = `
      (function() {
        try {
          if (!window.trustedTypes) {
            window.trustedTypes = {
              createPolicy: function(name, rules) {
                return {
                  createHTML: rules.createHTML || ((x) => x),
                  createScript: rules.createScript || ((x) => x),
                  createScriptURL: rules.createScriptURL || ((x) => x)
                };
              }
            };
          } else if (!window.trustedTypes.createPolicy) {
            window.trustedTypes.createPolicy = function(name, rules) {
              return {
                createHTML: rules.createHTML || ((x) => x),
                createScript: rules.createScript || ((x) => x),
                createScriptURL: rules.createScriptURL || ((x) => x)
              };
            };
          }
        } catch(e) {
          console.error("Trusted Types shim error", e);
        }
      })();
    `;
    document.head.prepend(shim); // ensure it executes first
  }, []);

  // Session setup
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

  const handleScriptLoad = () => {
    console.log("✅ Dialogflow script loaded");
    setReady(true);
  };

  return (
    <>
      {/* Dialogflow styles */}
      <link
        rel="stylesheet"
        href="https://www.gstatic.com/dialogflow-console/fast/df-messenger/prod/v1/themes/df-messenger-default.css"
      />

      {/* Dialogflow script */}
      <Script
        id="df-messenger-js"
        src="https://www.gstatic.com/dialogflow-console/fast/df-messenger/prod/v1/df-messenger.js"
        strategy="afterInteractive"
        onLoad={handleScriptLoad}
      />

      {/* Render chat once ready */}
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
