import React from 'react';

/**
 * The expanded view of a "Reasoning" activity step.
 *
 * Exists purely to render the model's thinking UNCAPPED. The generic
 * `TraceDetail` block is `max-h-72` with an inner scrollbar, which is right for
 * a raw JSON tool payload you glance at but wrong here: expanding reasoning is
 * an explicit request to READ it, and a ~2,000-character train of thought inside
 * a 288px box becomes a nested scrollbar you have to fight while the chat
 * scrolls underneath. This lets it grow and leaves scrolling to the chat pane.
 */
export const ReasoningTraceDetail: React.FC<{ detail: string; indentClass?: string }> = ({
  detail,
  indentClass = 'ml-3',
}) => (
  <pre
    className={`mt-1 ${indentClass} text-[11px] whitespace-pre-wrap break-words rounded p-2 max-w-[85%]`}
    style={{
      color: 'var(--text-secondary)',
      backgroundColor: 'var(--bg-primary)',
      border: '1px solid var(--border-color)',
    }}
  >
    {detail}
  </pre>
);

/** The activity step this renders is labelled exactly "Reasoning". */
export function matchesReasoning(_detail: string, label?: string): boolean {
  return (label || '').trim().toLowerCase() === 'reasoning';
}

export default ReasoningTraceDetail;
