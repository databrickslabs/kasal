import React from 'react';
import { GenieTraceDetail, GenieAnswerCard, matchesGenieDetail } from './GenieTraceDetail';
import { ToolResultCard } from './ToolResultCard';
import { SerperResultCard, matchesSerper } from './SerperResultCard';

/** Props an inline trace renderer receives (the whole resolved trace entry). */
export interface InlineTraceProps {
  detail: string;
  label?: string;
  sublabel?: string;
  durationMs?: number;
  /** Left-indent Tailwind class applied to the rendered block. Default 'ml-3'. */
  indentClass?: string;
}

/**
 * Registry of per-tool trace-detail renderers. Each tool that wants a custom
 * expanded view for its trace output registers one entry here — `ChatMessage`
 * and the generic `TraceDetail` dispatcher never change. This is the extension
 * point: messages vary by tool, but the rendering stays modular and isolated.
 *
 * To add a tool:
 *   1. create `MyToolTraceDetail.tsx` exporting a `{ detail, indentClass }` component
 *      and a `matchesMyTool(detail, label?)` predicate;
 *   2. add `{ match: matchesMyTool, Component: MyToolTraceDetail }` below.
 *
 * `Inline` is optional: a tool can render its result DIRECTLY in the chat (not
 * behind the collapsed trace pill) — e.g. Genie shows its answer Perplexity-
 * style. When set, ChatMessage renders it inline for that tool's `tool_result`.
 */
export interface TraceDetailRenderer {
  /** Return true if this renderer should handle the given detail / tool label. */
  match: (detail: string, label?: string) => boolean;
  /** Renders the expanded detail (inside the collapsed trace pill). */
  Component: React.FC<{ detail: string; indentClass?: string }>;
  /** Optional: render the result inline in the chat instead of a collapsed pill. */
  Inline?: React.FC<InlineTraceProps>;
}

export const TRACE_DETAIL_RENDERERS: TraceDetailRenderer[] = [
  // Genie renders its answer INLINE in the chat (Perplexity-style card).
  { match: (detail) => matchesGenieDetail(detail), Component: GenieTraceDetail, Inline: GenieAnswerCard },
  // Perplexity, Serper and scrape stay in the COLLAPSIBLE trace pill (their full
  // answers can be long/ugly inline). No `Inline` → they render as the normal
  // collapsible pill; expanding shows the FORMATTED answer (Markdown / linked
  // search results), not the raw JSON, via these `Component` renderers.
  { match: (_detail, label) => /perplexity/i.test(label || ''), Component: ToolResultCard },
  { match: (detail, label) => matchesSerper(detail, label), Component: SerperResultCard },
  { match: (_detail, label) => /read website|website content|scrape/i.test(label || ''), Component: ToolResultCard },
  // Future tools register here, e.g.:
  // { match: (detail, label) => label === 'SomeTool', Component: SomeToolTraceDetail },
];

/**
 * Find the inline renderer for a tool result, if one is registered and matches.
 * Used by ChatMessage to render certain tool results (e.g. Genie) directly in
 * the conversation rather than behind the collapsed trace pill.
 */
export function findInlineTraceRenderer(
  detail: string,
  label?: string,
): TraceDetailRenderer['Inline'] {
  const renderer = TRACE_DETAIL_RENDERERS.find((r) => r.match(detail, label));
  return renderer?.Inline;
}
