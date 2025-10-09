// src/types/dialogflow-messenger.d.ts

declare namespace JSX {
  interface IntrinsicElements {
    "df-messenger": React.DetailedHTMLProps<React.HTMLAttributes<HTMLElement>, HTMLElement> & {
      "project-id"?: string;
      "agent-id"?: string;
      "location"?: string;
      "language-code"?: string;
      "max-query-length"?: string;
    };
    "df-messenger-chat-bubble": React.DetailedHTMLProps<React.HTMLAttributes<HTMLElement>, HTMLElement> & {
      "chat-title"?: string;
    };
  }
}
