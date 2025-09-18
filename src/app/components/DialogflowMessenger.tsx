"use client";

import Script from "next/script";
import React from "react";

// Prevent duplicates across HMR/StrictMode
declare global {
  // eslint-disable-next-line no-var
  var __DF_WIDGET_ADDED__: boolean | undefined;
}

// Tiny wrapper avoids TS IntrinsicElements typing issues
const DFMessenger = (props: any) => React.createElement("df-messenger", props);

export default function DialogflowMessenger() {
  const [scriptReady, setScriptReady] = React.useState(false);
  const [canMount, setCanMount] = React.useState(false);
  const [shouldWelcome, setShouldWelcome] = React.useState(false);

  // Mount exactly once per page lifetime
  React.useEffect(() => {
    if (!globalThis.__DF_WIDGET_ADDED__) {
      globalThis.__DF_WIDGET_ADDED__ = true;
      setCanMount(true);
    }
  }, []);

  // Greet only once per tab
  React.useEffect(() => {
    const KEY = "df_has_welcomed";
    if (!sessionStorage.getItem(KEY)) {
      setShouldWelcome(true);
      sessionStorage.setItem(KEY, "1");
    }
  }, []);

  return (
    <>
      <Script
        id="df-bootstrap"
        src="https://www.gstatic.com/dialogflow-console/fast/messenger/bootstrap.js?v=1"
        strategy="afterInteractive"
        onLoad={() => setScriptReady(true)}
      />

      {scriptReady && canMount && (
        <DFMessenger
          {...(shouldWelcome ? { intent: "WELCOME" } : {})}
          chat-title="kidflix-bot"
          agent-id="80aba165-ba45-424b-a08b-530cfed40536"
          language-code="en"
        />
      )}
    </>
  );
}
