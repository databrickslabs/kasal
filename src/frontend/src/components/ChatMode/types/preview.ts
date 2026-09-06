// The preview pane renders structured A2UI documents ONLY — the UI document is
// the single source of truth for generated deliverables. Raw HTML, JSON,
// markdown and plain text deliberately get NO preview: crews are steered toward
// A2UI by the UI Configurator, and anything else stays in the chat transcript.
// 'ui' = a structured A2UI deliverable (the canonical generated surface).
// 'text' = a plain-text / markdown answer (chat-mode responses), shown in the
// pane on demand via the run-activity "Show in panel" icon — it has no A2UI
// controls (no Customize/refine, no PPTX export), just the rendered markdown.
// 'memory' = a run's memory (concept graph + records); `data` holds the run's
// job id and MemoryPane fetches/derives the rest. Like 'text' it has none of
// the A2UI machinery.
export type PreviewContentType = 'ui' | 'text' | 'memory';

export interface PreviewContent {
  type: PreviewContentType;
  data: string;
  title?: string;
  /** The chat message this content was derived from (the run's assistant
   *  message). Lets a pane restyle round-trip to that message's `resultData`
   *  (persisted via the session API), so a "Customize → Look" palette survives
   *  session switches instead of living only in this in-memory slot. */
  sourceMessageId?: string;
}

