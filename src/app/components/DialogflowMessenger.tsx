"use client";
import Script from "next/script";
import { useEffect } from "react";

export default function DialogflowMessenger() {
  useEffect(() => {
    // Prevent duplicate <df-messenger> creation
    if (document.querySelector("df-messenger")) return;
  }, []);

  return (
    <>
      {/* Load the CX Messenger script only once */}
      <Script
        src="https://www.gstatic.com/dialogflow-console/fast/df-messenger/prod/v1/df-messenger.js"
        strategy="afterInteractive"
      />
      {/* Optionally load CSS theme */}
      <link
        rel="stylesheet"
        href="https://www.gstatic.com/dialogflow-console/fast/df-messenger/prod/v1/themes/df-messenger-default.css"
      />

      {/* Messenger element */}
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
}
