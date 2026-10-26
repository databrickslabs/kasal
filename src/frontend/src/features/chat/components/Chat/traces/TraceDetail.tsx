import React from 'react';
import { TRACE_DETAIL_RENDERERS } from './registry';

/**
 * Generic expanded-trace-detail dispatcher. Picks the first registered
 * renderer that matches (e.g. Perplexity → answer as Markdown); otherwise falls
 * back to a plain monospace block. Tool-specific UI lives in the registry, not
 * here — so this stays small and ChatMessage stays lean.
 */
export const TraceDetail: React.FC<{ detail: string; label?: string; indentClass?: string }> = ({
  detail,
  label,
  indentClass = 'ml-3',
}) => {
  const renderer = TRACE_DETAIL_RENDERERS.find((r) => r.match(detail, label));
  if (renderer) {
    const Component = renderer.Component;
    return <Component detail={detail} indentClass={indentClass} />;
  }
  return (
    <pre
      className={`mt-1 ${indentClass} text-[11px] whitespace-pre-wrap break-words rounded p-2 max-w-[85%] max-h-72 overflow-y-auto`}
      style={{
        color: 'var(--text-primary)',
        backgroundColor: 'var(--bg-primary)',
        border: '1px solid var(--border-color)',
      }}
    >
      {detail}
    </pre>
  );
};
