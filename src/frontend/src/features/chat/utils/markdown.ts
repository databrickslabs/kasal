/**
 * Detect if a string contains markdown formatting.
 */
export function containsMarkdown(text: string): boolean {
  const patterns = [
    /^#{1,6}\s/m,           // headers
    /\*\*.+?\*\*/,          // bold
    /\*.+?\*/,              // italic
    /`.+?`/,                // inline code
    /```[\s\S]*?```/,       // code blocks
    /^\s*[-*+]\s/m,         // unordered lists
    /^\s*\d+\.\s/m,         // ordered lists
    /\[.+?\]\(.+?\)/,       // links
    /^\|.+\|$/m,            // tables
    /^>\s/m,                // blockquotes
  ];
  return patterns.some((p) => p.test(text));
}

/**
 * Format a timestamp for display.
 */
export function formatTime(date: Date): string {
  return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
}

/**
 * Generate a unique message ID.
 */
export function generateId(): string {
  return `msg-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}
