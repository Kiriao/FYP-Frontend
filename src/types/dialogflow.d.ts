// src/types/dialogflow.d.ts
import type * as React from "react";

declare global {
  namespace JSX {
    interface IntrinsicElements {
      "df-messenger": React.DetailedHTMLProps<
        React.HTMLAttributes<HTMLElement>,
        HTMLElement
      > & {
        "agent-id": string;
        "chat-title"?: string;
        "intent"?: string;
        "language-code"?: string;
      };
    }
  }
}
export {};
