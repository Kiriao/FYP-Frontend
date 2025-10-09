declare namespace JSX {
  interface IntrinsicElements {
    'df-messenger': React.DetailedHTMLProps<React.HTMLAttributes<HTMLElement>, HTMLElement> & {
      location?: string;
      'project-id'?: string;
      'agent-id'?: string;
      'language-code'?: string;
      'max-query-length'?: string;
    };
    'df-messenger-chat-bubble': React.DetailedHTMLProps<React.HTMLAttributes<HTMLElement>, HTMLElement> & {
      'chat-title'?: string;
    };
  }
}

export {};