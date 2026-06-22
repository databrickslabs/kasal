import React, { useRef, useEffect, useMemo, useState } from 'react';
import {
  parseUiDocument,
  findUiDocument,
  applyConfiguredTheme,
  inferSurfaceDeliverable,
  setSurfaceTheme,
  DELIVERABLE_LABELS,
} from '../../utils/uiDocument';
import type { Theme } from '../../../Configuration/uiConfigShared';
import { useWorkspaceThemes } from '../../hooks/useWorkspaceThemes';
import { downloadSurfacePdf } from '../../utils/surfacePdf';
import UiRenderer from './UiRenderer';
import RefinePanel from './RefinePanel';

// The preview pane renders structured A2UI documents ONLY — the UI document is
// the single source of truth for generated deliverables. Raw HTML, JSON,
// markdown and plain text deliberately get NO preview: crews are steered toward
// A2UI by the UI Configurator, and anything else stays in the chat transcript.
export type PreviewContentType = 'ui';

export interface PreviewContent {
  type: PreviewContentType;
  data: string;
  title?: string;
}

interface PreviewPanelProps {
  content: PreviewContent;
  onClose: () => void;
  chatCollapsed: boolean;
  onToggleChat: () => void;
  /** Refine the current artifact with a natural-language instruction (AI). */
  onRefine?: (instruction: string) => void;
  /** Replace the current artifact's document in place (deterministic restyle, no AI). */
  onStyleChange?: (updatedData: string) => void;
  /** All previewable task outputs of the run, oldest → newest. */
  history?: PreviewContent[];
  /** Index into `history` currently shown. */
  index?: number;
  /** Switch the displayed preview to another history entry. */
  onNavigate?: (index: number) => void;
}

/**
 * Strip a leading `**Task name**\n\n` prefix the chat layer prepends to
 * task-output messages. The prefix is for chat display only; the preview
 * should render just the body.
 */
function stripTaskTitlePrefix(raw: string): string {
  const match = raw.match(/^\s*\*\*[^\n*][^\n]*?\*\*\s*\n\n+/);
  return match ? raw.slice(match[0].length) : raw;
}

/**
 * Strip markdown code fences if present.
 * Handles: ```html\n...\n``` or ```json\n...\n``` etc.
 */
function stripCodeFences(raw: string): string {
  const trimmed = raw.trim();
  const fenceMatch = trimmed.match(/^```\w*\s*\n([\s\S]*?)\n\s*```\s*$/);
  if (fenceMatch) {
    return fenceMatch[1];
  }
  // Also handle when there's text before/after the code fence
  const innerMatch = trimmed.match(/```(?:html|json|xml)\s*\n([\s\S]*?)\n\s*```/);
  if (innerMatch) {
    return innerMatch[1];
  }
  return raw;
}

/**
 * A task output is previewable IFF it is (or contains) a parseable A2UI
 * document. Everything else — HTML, generic JSON, markdown, plain text —
 * returns null and stays in the chat transcript.
 */
export function parsePreviewContent(raw: string): PreviewContent | null {
  if (!raw || raw.length < 10) return null;

  // Drop the chat layer's bold-title prefix so the preview shows only the body.
  const body = stripTaskTitlePrefix(raw);

  // Strip markdown code fences that often wrap the JSON document.
  const cleaned = stripCodeFences(body);

  // Search the text directly first (handles prose/fenced/bare/double-encoded and
  // keeps the original string for a clean top-level doc). If that misses, parse
  // to an object and search RECURSIVELY so a doc WRAPPED in a result envelope
  // ({result:{…}}, {output:"<json>"}, nested) is still found — a top-level-only
  // parse leaks it into the chat as raw JSON instead of rendering in the preview.
  let doc = findUiDocument(cleaned);
  if (!doc) {
    try {
      doc = findUiDocument(JSON.parse(cleaned));
    } catch {
      /* not a JSON wrapper */
    }
  }
  if (doc) {
    return { type: 'ui', data: typeof doc === 'string' ? doc : JSON.stringify(doc) };
  }

  return null;
}

const PreviewPanel: React.FC<PreviewPanelProps> = ({ content, onClose, chatCollapsed, onToggleChat, onRefine, onStyleChange, history, index, onNavigate }) => {
  const [refineOpen, setRefineOpen] = useState(false);
  const asideRef = useRef<HTMLElement>(null);
  const [fullscreen, setFullscreen] = useState(false);
  const [downloading, setDownloading] = useState(false);

  // TRUE browser full screen (hides the browser chrome / top menu) via the
  // Fullscreen API, kept in sync so an Esc / browser-driven exit also flips the
  // toggle back.
  useEffect(() => {
    const onChange = () => setFullscreen(document.fullscreenElement === asideRef.current);
    document.addEventListener('fullscreenchange', onChange);
    return () => document.removeEventListener('fullscreenchange', onChange);
  }, []);

  // The toggle only shows when NOT full screen (the whole header is hidden in
  // full screen), so it always *enters*; exiting is the browser's Esc (synced
  // back via the fullscreenchange listener above).
  const enterFullscreen = () => {
    setRefineOpen(false);
    void asideRef.current!.requestFullscreen().catch(() => {});
  };

  // Heal already-stored previews that include the chat layer's bold-title prefix.
  const displayData = useMemo(() => stripTaskTitlePrefix(content.data), [content]);

  // Parse the A2UI document for the brand-consistent renderer, then re-resolve
  // its theme from the workspace UI-Configurator palettes (source of truth —
  // the agent-embedded theme is frequently the wrong palette).
  const workspaceThemes = useWorkspaceThemes();
  const uiSurface = useMemo(() => {
    const parsed = parseUiDocument(displayData);
    return parsed ? applyConfiguredTheme(parsed, workspaceThemes) : null;
  }, [displayData, workspaceThemes]);

  // What this deliverable is (presentation, dashboard, …) drives the "Customize"
  // panel: a friendly title + the matching per-type content controls.
  // inferSurfaceDeliverable only ever returns keys present in DELIVERABLE_LABELS.
  const deliverable = useMemo(() => (uiSurface ? inferSurfaceDeliverable(uiSurface) : 'default'), [uiSurface]);
  const deliverableLabel = DELIVERABLE_LABELS[deliverable];
  const currentTheme = uiSurface?.theme;

  // Apply a deterministic Look change: rewrite the doc's theme and hand it back
  // to the owner to swap in place + persist. No AI, no crew run.
  const applyStyle = (theme: Theme) => {
    if (!onStyleChange) return;
    onStyleChange(setSurfaceTheme(displayData, theme));
  };

  // Download the rendered surface as a PDF file (no print dialog): decks land
  // one slide per landscape page; other deliverables as one content-sized page.
  const downloadPdf = async () => {
    if (!uiSurface || downloading) return;
    setDownloading(true);
    try {
      await downloadSurfacePdf(uiSurface, content.title || 'kasal-app');
    } finally {
      setDownloading(false);
    }
  };

  return (
    <aside
      ref={asideRef}
      className="flex flex-col h-full"
      style={{
        flex: chatCollapsed ? '1 1 100%' : '1 1 50%',
        minWidth: '300px',
        backgroundColor: 'var(--bg-primary)',
        borderLeft: chatCollapsed ? 'none' : '1px solid var(--border-color)',
      }}
    >
      {/* Header — hidden entirely in full screen for a chrome-free view (exit with Esc) */}
      {!fullscreen && (
      <div
        className="flex items-center justify-between px-4 py-3 flex-shrink-0"
        style={{ borderBottom: '1px solid var(--border-color)' }}
      >
        <div className="flex items-center gap-2">
          {/* Toggle chat button */}
          <button
            onClick={onToggleChat}
            className="w-7 h-7 rounded-lg flex items-center justify-center transition-colors hover:opacity-70"
            style={{ color: 'var(--text-muted)' }}
            title={chatCollapsed ? 'Show chat' : 'Hide chat'}
          >
            {/* The preview sits to the RIGHT of the chat, so expanding it to
                full width grows LEFTWARD: "hide chat" points left (◀◀), and
                restoring the chat points right (▶▶). */}
            <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              {chatCollapsed ? (
                <path strokeLinecap="round" strokeLinejoin="round" d="M11.25 4.5l7.5 7.5-7.5 7.5m-6-15l7.5 7.5-7.5 7.5" />
              ) : (
                <path strokeLinecap="round" strokeLinejoin="round" d="M18.75 19.5l-7.5-7.5 7.5-7.5m-6 15L5.25 12l7.5-7.5" />
              )}
            </svg>
          </button>
          <svg
            className="w-4 h-4"
            style={{ color: 'var(--accent)' }}
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor"
            strokeWidth={2}
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              d="M3.375 19.5h17.25m-17.25 0a1.125 1.125 0 01-1.125-1.125M3.375 19.5h7.5c.621 0 1.125-.504 1.125-1.125m-9.75 0V5.625m0 12.75v-1.5c0-.621.504-1.125 1.125-1.125m18.375 2.625V5.625m0 12.75c0 .621-.504 1.125-1.125 1.125m1.125-1.125v-1.5c0-.621-.504-1.125-1.125-1.125m0 3.75h-7.5A1.125 1.125 0 0112 18.375m9.75-12.75c0-.621-.504-1.125-1.125-1.125H3.375c-.621 0-1.125.504-1.125 1.125m19.5 0v1.5c0 .621-.504 1.125-1.125 1.125M2.25 5.625v1.5c0 .621.504 1.125 1.125 1.125m0 0h17.25m-17.25 0h7.5c.621 0 1.125.504 1.125 1.125M3.375 8.25c-.621 0-1.125.504-1.125 1.125v1.5c0 .621.504 1.125 1.125 1.125m17.25-3.75h-7.5c-.621 0-1.125.504-1.125 1.125m8.625-1.125c.621 0 1.125.504 1.125 1.125v1.5c0 .621-.504 1.125-1.125 1.125m-17.25 0h7.5m-7.5 0c-.621 0-1.125.504-1.125 1.125v1.5c0 .621.504 1.125 1.125 1.125M12 10.875v-1.5m0 1.5c0 .621-.504 1.125-1.125 1.125M12 10.875c0 .621.504 1.125 1.125 1.125m-2.25 0c.621 0 1.125.504 1.125 1.125M13.125 12h7.5m-7.5 0c-.621 0-1.125.504-1.125 1.125M20.625 12c.621 0 1.125.504 1.125 1.125v1.5c0 .621-.504 1.125-1.125 1.125m-17.25 0h7.5M12 14.625v-1.5m0 1.5c0 .621-.504 1.125-1.125 1.125M12 14.625c0 .621.504 1.125 1.125 1.125m-2.25 0c.621 0 1.125.504 1.125 1.125m0 0v1.5c0 .621-.504 1.125-1.125 1.125"
            />
          </svg>
          <span
            className="text-sm font-medium"
            style={{ color: 'var(--text-primary)' }}
          >
            {content.title || 'App'}
          </span>
          <span
            className="text-[10px] px-1.5 py-0.5 rounded font-mono"
            style={{
              color: 'var(--text-muted)',
              backgroundColor: 'var(--bg-secondary)',
            }}
          >
            {content.type.toUpperCase()}
          </span>
        </div>
        <div className="flex items-center gap-1">
          {history && history.length > 1 && onNavigate && (() => {
            const current = index ?? history.length - 1;
            return (
              <div
                className="flex items-center gap-0.5 mr-1 rounded-lg"
                style={{ backgroundColor: 'var(--bg-secondary)' }}
              >
                <button
                  onClick={() => onNavigate(current - 1)}
                  disabled={current <= 0}
                  className="w-7 h-7 rounded-lg flex items-center justify-center transition-colors hover:opacity-70 disabled:opacity-30 disabled:cursor-not-allowed"
                  style={{ color: 'var(--text-muted)' }}
                  title="Previous output"
                  aria-label="Previous output"
                >
                  <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                    <path strokeLinecap="round" strokeLinejoin="round" d="M15.75 19.5L8.25 12l7.5-7.5" />
                  </svg>
                </button>
                <span
                  className="text-[11px] font-mono tabular-nums px-1 select-none"
                  style={{ color: 'var(--text-muted)' }}
                  title="Task output (latest shown first)"
                >
                  {current + 1}/{history.length}
                </span>
                <button
                  onClick={() => onNavigate(current + 1)}
                  disabled={current >= history.length - 1}
                  className="w-7 h-7 rounded-lg flex items-center justify-center transition-colors hover:opacity-70 disabled:opacity-30 disabled:cursor-not-allowed"
                  style={{ color: 'var(--text-muted)' }}
                  title="Next output"
                  aria-label="Next output"
                >
                  <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                    <path strokeLinecap="round" strokeLinejoin="round" d="M8.25 4.5l7.5 7.5-7.5 7.5" />
                  </svg>
                </button>
              </div>
            );
          })()}
          {onRefine && (
            <button
              onClick={() => setRefineOpen((v) => !v)}
              className="flex items-center gap-1.5 h-7 px-2.5 rounded-lg text-xs font-medium transition-colors hover:opacity-80"
              style={{
                color: refineOpen ? 'var(--accent)' : 'var(--text-secondary)',
                backgroundColor: 'var(--bg-secondary)',
              }}
              title="Customize the look and content of this result"
            >
              <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.8}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M9.813 15.904L9 18.75l-.813-2.846a4.5 4.5 0 00-3.09-3.09L2.25 12l2.846-.813a4.5 4.5 0 003.09-3.09L9 5.25l.813 2.846a4.5 4.5 0 003.09 3.09L15.75 12l-2.846.813a4.5 4.5 0 00-3.09 3.09z" />
              </svg>
              Customize
            </button>
          )}
          <button
            onClick={downloadPdf}
            disabled={downloading}
            className="w-7 h-7 rounded-lg flex items-center justify-center transition-colors hover:opacity-70 disabled:opacity-40 disabled:cursor-wait"
            style={{ color: 'var(--text-muted)' }}
            title="Download as PDF"
            aria-label="Download as PDF"
          >
            <svg
              className={`w-4 h-4 ${downloading ? 'animate-pulse' : ''}`}
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
              strokeWidth={2}
            >
              <path strokeLinecap="round" strokeLinejoin="round" d="M3 16.5v2.25A2.25 2.25 0 005.25 21h13.5A2.25 2.25 0 0021 18.75V16.5M16.5 12L12 16.5m0 0L7.5 12m4.5 4.5V3" />
            </svg>
          </button>
          <button
            onClick={enterFullscreen}
            className="w-7 h-7 rounded-lg flex items-center justify-center transition-colors hover:opacity-70"
            style={{ color: 'var(--text-muted)' }}
            title="Full screen"
            aria-label="Full screen"
          >
            <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M3.75 3.75v4.5m0-4.5h4.5m-4.5 0L9 9m11.25-5.25h-4.5m4.5 0v4.5m0-4.5L15 9M3.75 20.25v-4.5m0 4.5h4.5m-4.5 0L9 15m11.25 5.25h-4.5m4.5 0v-4.5m0 4.5L15 15" />
            </svg>
          </button>
          <button
            onClick={onClose}
            className="w-7 h-7 rounded-lg flex items-center justify-center transition-colors hover:opacity-70"
            style={{ color: 'var(--text-muted)' }}
            title="Close preview"
          >
            <svg
              className="w-4 h-4"
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
              strokeWidth={2}
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                d="M6 18L18 6M6 6l12 12"
              />
            </svg>
          </button>
        </div>
      </div>
      )}

      {/* Customize panel — instant "Look" (deterministic) + AI "Content" refine */}
      {onRefine && refineOpen && !fullscreen && (
        <RefinePanel
          deliverable={deliverable}
          deliverableLabel={deliverableLabel}
          currentTheme={currentTheme}
          onApplyStyle={applyStyle}
          onRefine={onRefine}
          onClose={() => setRefineOpen(false)}
        />
      )}

      {/* Content — A2UI only; non-UI documents are never previewable. */}
      <div className="flex-1 overflow-auto">
        {uiSurface && (
          // Key on the displayed content so a refine (or history navigation)
          // REMOUNTS the renderer instead of reusing the instance. The refined
          // deck reuses the same component ids (root/slide1…), so without a key
          // React keeps the old instance — and its internal useState (slide
          // index, surface.data model) — leaving the preview on the stale
          // version. Index + content length changes whenever the artifact does.
          <UiRenderer key={`ui-${index ?? 0}-${displayData.length}`} surface={uiSurface} />
        )}
      </div>
    </aside>
  );
};

export default PreviewPanel;
