import React from 'react';
import MessageContent from '../MessageContent';

/**
 * Renderer for a text-answer tool result (e.g. Perplexity, scraped page) shown
 * INSIDE the collapsible trace pill: the answer as Markdown in an indented,
 * scrollable panel — readable but tucked away (these answers are often long).
 * Returns null when there's no content.
 *
 * Accepts the full inline-trace props (label / sublabel / durationMs) for
 * registry compatibility, but renders only the answer body.
 */
export const ToolResultCard: React.FC<{
  detail: string;
  label?: string;
  sublabel?: string;
  durationMs?: number;
  indentClass?: string;
}> = ({ detail, indentClass = 'ml-3' }) => {
  if (!detail || !detail.trim()) return null;
  return (
    <div
      className={`mt-1 ${indentClass} max-w-[85%] rounded p-3 max-h-96 overflow-y-auto text-sm leading-relaxed`}
      style={{
        color: 'var(--text-primary)',
        backgroundColor: 'var(--bg-primary)',
        border: '1px solid var(--border-color)',
      }}
    >
      <MessageContent content={detail} />
    </div>
  );
};
