import React, { useEffect, useMemo, useState } from 'react';
import { PanelLeft } from 'lucide-react';
import { DELIVERABLE_LABELS } from '../../../Configuration/uiConfigShared';
import { toSurface } from '../../utils/surfaceAdapter';
import { themeToDeck, getDeckTheme, DEFAULT_DECK_THEME_ID } from '../../../../shared/a2ui';
import type { Surface } from '../../../../shared/a2ui';
import { downloadPptx } from '../../../../shared/a2ui/lib/download';
import type { Theme } from '../../../Configuration/uiConfigShared';
import { downloadSurfacePdf } from '../../utils/surfacePdf';
import { useA2uiThemes } from '../../hooks/useA2uiThemes';
import A2uiSurface from '../Chat/A2uiSurface';
import MessageContent from '../Chat/MessageContent';
import { friendlyStep, type RunStep } from './RunTimeline';
import ThinkingStream from './ThinkingStream';
import LogSurface from './LogSurface';
import RefinePanel from './RefinePanel';

/** A surface carrying a per-surface "Look" restyle (see A2uiSurface). */
type ThemedSurface = Surface & { theme?: Theme };

// A2UI surfaceKind → UIConfigurator deliverable key (drives the "Customize" panel
// title + per-type controls). 'document' maps to the closest configurable type.
const SURFACE_TO_DELIVERABLE: Record<string, string> = {
  presentation: 'presentation',
  dashboard: 'dashboard',
  mindmap: 'mindmap',
  quiz: 'quiz',
  flashcards: 'flashcards',
  map: 'map',
  document: 'report',
  conversation: 'default',
};

// The preview pane renders structured A2UI documents ONLY — the UI document is
// the single source of truth for generated deliverables. Raw HTML, JSON,
// markdown and plain text deliberately get NO preview: crews are steered toward
// A2UI by the UI Configurator, and anything else stays in the chat transcript.
// 'ui' = a structured A2UI deliverable (the canonical generated surface).
// 'text' = a plain-text / markdown answer (chat-mode responses), shown in the
// pane on demand via the run-activity "Show in panel" icon — it has no A2UI
// controls (no Customize/refine, no PPTX export), just the rendered markdown.
export type PreviewContentType = 'ui' | 'text';

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

interface PreviewPanelProps {
  content: PreviewContent;
  onClose: () => void;
  chatCollapsed: boolean;
  /** Toggle the adjacent chat column. Omitted when there's no chat beside the pane
   *  (e.g. the Jobs "Show result" dialog) — then no toggle button shows. */
  onToggleChat?: () => void;
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
  /** The run's step timeline (from the persistent chat trace), shown collapsed
   *  above the result so the activity is never lost once the deliverable lands. */
  runSteps?: RunStep[];
  /** Open the pane directly on THIS step's content (master→detail pre-selected)
   *  — set when the user clicks a step ROW in the chat's activity dropdown. */
  focusStep?: RunStep | null;
  /** Dock the activity into the chat's "Working…" bar instead of this pane. */
  onMoveActivityToChat?: () => void;
  /** Embedded in a host shell that already provides fullscreen + close (e.g. the
   *  Jobs "Show result" dialog). Hides this pane's own fullscreen/close so they're
   *  not duplicated; the title + download menu + Customize stay. */
  embedded?: boolean;
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

  // Strip markdown code fences that often wrap the JSON.
  const cleaned = stripCodeFences(body);

  // toSurface accepts the new {text,a2ui} envelope, a bare Surface, a JSON string
  // of either, or an older legacy document found anywhere in the tree (adapted).
  // PreviewContent.data always holds the canonical NEW Surface JSON afterwards.
  const surface = toSurface(cleaned);
  return surface ? { type: 'ui', data: JSON.stringify(surface) } : null;
}

const PreviewPanel: React.FC<PreviewPanelProps> = ({ content, onClose, chatCollapsed, onToggleChat, onRefine, onStyleChange, history, index, onNavigate, runSteps = [], focusStep, onMoveActivityToChat, embedded }) => {
  const [refineOpen, setRefineOpen] = useState(false);
  const [downloading, setDownloading] = useState(false);
  // The single top-toolbar download is a menu: PDF or PowerPoint.
  const [downloadAnchor, setDownloadAnchor] = useState<HTMLElement | null>(null);
  const a2uiThemes = useA2uiThemes();
  // The run's step timeline lives ON THE RIGHT the whole time: while building
  // (PreviewSkeleton) and — collapsed above the result — once it's done, so it's
  // never lost. `runSteps` comes from the persistent chat trace, so it survives
  // the run finishing. Collapsed by default.
  const [activityOpen, setActivityOpen] = useState(false);
  // Master→detail inside the activity view: which step's context is open full-page
  // (null = show the step list). A step is chosen from the list; "Back" clears it.
  const [activeStep, setActiveStep] = useState<RunStep | null>(null);
  const toggleActivity = () => {
    setActivityOpen((v) => !v);
    setActiveStep(null); // toggling always returns to the list / deliverable
  };
  // A step ROW clicked in the chat's activity dropdown lands the pane directly
  // on that step's content; "Back" then returns to the (opened) step list.
  useEffect(() => {
    if (focusStep) {
      setActiveStep(focusStep);
      setActivityOpen(true);
    }
  }, [focusStep]);

  // Heal already-stored previews that include the chat layer's bold-title prefix.
  const displayData = useMemo(() => stripTaskTitlePrefix(content.data), [content]);

  // A plain-text / markdown deliverable (chat-mode answer). It renders as markdown
  // and has none of the A2UI machinery (no Surface, theme, Customize, or export) —
  // every A2UI-only control below additionally gates on `uiSurface`, which is null
  // here, so they hide automatically.
  const isText = content.type === 'text';

  // Coerce the stored content into the shared Surface (handles the new envelope,
  // a bare surface, or an older legacy doc — adapted). A2uiSurface re-resolves the
  // workspace branding (and any per-surface "Look" restyle) at render time.
  const uiSurface = useMemo(() => toSurface(displayData), [displayData]) as ThemedSurface | null;
  // A deck is fit to the available HEIGHT (letterboxed, no scroll) so the whole
  // slide shows. Other surfaces (documents, dashboards) are tall content — they
  // keep the natural width + vertical scroll. Detect a deck by an actual SlideDeck
  // component, not just surfaceKind: messages-format / legacy results can carry a
  // SlideDeck while their surfaceKind was inferred to 'document'.
  const isDeck =
    uiSurface?.surfaceKind === 'presentation' ||
    !!uiSurface?.components?.some((c) => c.component === 'SlideDeck');
  // Fit-to-height (letterbox) only makes sense when the host gives the pane a
  // FIXED height to fill — i.e. the chat side pane. When `embedded` (the Jobs
  // "Show result" dialog) the host shrink-wraps its content, so a fitted deck
  // would just leave a tall empty band below it; there we render the deck
  // WIDTH-driven (natural 16:9 height) and let the dialog size to it.
  const fitDeck = isDeck && !embedded;

  // What this deliverable is (presentation, dashboard, …) drives the "Customize"
  // panel: a friendly title + the matching per-type content controls.
  const deliverable = uiSurface
    ? SURFACE_TO_DELIVERABLE[uiSurface.surfaceKind] || 'default'
    : 'default';
  const deliverableLabel = DELIVERABLE_LABELS[deliverable];
  const currentTheme = uiSurface?.theme;

  // Apply a deterministic Look change: stamp the picked palette onto the surface
  // and hand it back to the owner to swap in place + persist. The renderer reads
  // surface.theme as the override, so the preview restyles INSTANTLY — no AI run.
  const applyStyle = (theme: Theme) => {
    if (!onStyleChange || !uiSurface) return;
    const restyled: ThemedSurface = {
      ...uiSurface,
      theme: { ...uiSurface.theme, ...theme },
    };
    onStyleChange(JSON.stringify(restyled));
  };

  // The deck theme resolved exactly as A2uiSurface does (per-surface "Look"
  // override → workspace palette → built-in default), so a PowerPoint export
  // matches the on-screen palette.
  const deckTheme = useMemo(() => {
    if (!uiSurface) return undefined;
    const key = SURFACE_TO_DELIVERABLE[uiSurface.surfaceKind] || 'default';
    const palette = uiSurface.theme ?? a2uiThemes?.[key];
    return palette ? themeToDeck(palette) : getDeckTheme(DEFAULT_DECK_THEME_ID);
  }, [uiSurface, a2uiThemes]);

  // The single top-toolbar download offers PDF or PowerPoint (replacing both the
  // old icon-only PDF button AND the surface's own "PowerPoint" button, which is
  // suppressed in the pane via `hideDownloads`). Decks land one slide per page;
  // other deliverables as one content-sized page (PDF) or slides (PPTX).
  // Name the file after the deliverable's title; fall back to its kind
  // (dashboard, quiz, document, …) so a downloaded dashboard isn't "kasal-app".
  const baseName = content.title || uiSurface?.surfaceKind || 'kasal-app';
  const runDownload = async (kind: 'pdf' | 'pptx') => {
    setDownloadAnchor(null);
    if (!uiSurface || downloading) return;
    setDownloading(true);
    try {
      if (kind === 'pdf') {
        await downloadSurfacePdf(uiSurface, baseName);
      } else {
        await downloadPptx(uiSurface, deckTheme, `${baseName}.pptx`);
      }
    } finally {
      setDownloading(false);
    }
  };

  return (
    <aside
      className="flex flex-col h-full"
      style={{
        flex: chatCollapsed ? '1 1 100%' : '1 1 50%',
        minWidth: '300px',
        backgroundColor: 'var(--bg-primary)',
        borderLeft: chatCollapsed ? 'none' : '1px solid var(--border-color)',
      }}
    >
      {/* Header — hidden entirely in full screen for a chrome-free view (exit with
          Esc), and when embedded: the host dialog (Jobs "Show result") already
          provides its own title bar + controls, so a second header would just
          stack and steal vertical height (forcing a scroll). */}
      {!embedded && (
      <div
        className="flex items-center justify-between px-4 py-3 flex-shrink-0"
        style={{ borderBottom: '1px solid var(--border-color)' }}
      >
        <div className="flex items-center gap-2">
          {/* Toggle chat button — only when there's a chat column beside the pane */}
          {onToggleChat && (
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
          )}
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
          {runSteps.length > 0 && (
            <button
              onClick={toggleActivity}
              className="flex items-center gap-1.5 h-7 px-2.5 rounded-lg text-xs font-medium transition-colors hover:opacity-80"
              style={{
                color: activityOpen ? 'var(--accent)' : 'var(--text-secondary)',
                backgroundColor: 'var(--bg-secondary)',
              }}
              title="Show the run activity timeline (steps + context)"
              aria-pressed={activityOpen}
            >
              <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.8}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M3.75 6.75h.007v.008H3.75V6.75zm0 5.25h.007v.008H3.75V12zm0 5.25h.007v.008H3.75v-.008zM8.25 6.75h12M8.25 12h12m-12 5.25h12" />
              </svg>
              Activity
            </button>
          )}
          {onRefine && uiSurface && (
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
          {/* The download is a small menu (PDF / PowerPoint) anchored under its
              button. `downloadAnchor` doubles as the open flag; a transparent
              backdrop catches the click-away to close. A2UI-only — a plain-text
              answer has no surface to export, so the menu hides for text. */}
          {uiSurface && (
          <div className="relative">
            <button
              onClick={(e: React.MouseEvent<HTMLButtonElement>) => setDownloadAnchor(e.currentTarget)}
              disabled={downloading}
              className="w-7 h-7 rounded-lg flex items-center justify-center transition-colors hover:opacity-70 disabled:opacity-40 disabled:cursor-wait"
              style={{ color: 'var(--text-muted)' }}
              title="Download"
              aria-label="Download"
              aria-haspopup="menu"
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
            {Boolean(downloadAnchor) && (
              <>
                <div className="fixed inset-0 z-40" onClick={() => setDownloadAnchor(null)} />
                <div
                  role="menu"
                  className="kasal-popover absolute right-0 top-full mt-1 z-50 min-w-[8rem] rounded-lg py-1"
                  style={{ backgroundColor: 'var(--bg-primary)', border: '1px solid var(--border-color)' }}
                >
                  <button
                    type="button"
                    role="menuitem"
                    onClick={() => runDownload('pdf')}
                    className="block w-full text-left px-3 py-1.5 text-sm transition-colors hover:bg-[var(--bg-rail-hover)]"
                    style={{ color: 'var(--text-primary)' }}
                  >
                    PDF
                  </button>
                  <button
                    type="button"
                    role="menuitem"
                    onClick={() => runDownload('pptx')}
                    className="block w-full text-left px-3 py-1.5 text-sm transition-colors hover:bg-[var(--bg-rail-hover)]"
                    style={{ color: 'var(--text-primary)' }}
                  >
                    PowerPoint
                  </button>
                </div>
              </>
            )}
          </div>
          )}
          {!embedded && (
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
          )}
        </div>
      </div>
      )}

      {/* Customize panel — instant "Look" (deterministic) + AI "Content" refine */}
      {onRefine && refineOpen && (
        <RefinePanel
          deliverable={deliverable}
          deliverableLabel={deliverableLabel}
          currentTheme={currentTheme}
          onApplyStyle={applyStyle}
          onRefine={onRefine}
          onClose={() => setRefineOpen(false)}
        />
      )}

      {/* Content — A2UI only; non-UI documents are never previewable. The deck-fit
          layout (overflow-hidden flex column, so the deck letterboxes to height)
          applies ONLY when the deck DELIVERABLE is actually shown. While the run
          activity list or a step's context (LogSurface, minHeight:100%) is open it
          REPLACES the deliverable, and those need the natural scrolling box —
          otherwise the fit container collapses/clips them to an empty pane. */}
      <div
        data-testid="preview-body"
        className={`flex-1 min-h-0 ${
          fitDeck && !activityOpen && !activeStep
            ? 'overflow-hidden flex flex-col'
            : 'overflow-auto'
        }`}
      >
        {/* Run activity — collapsed above the result so it's never lost. The list
            shows plain-English steps; clicking one opens its context full-page. */}
        {runSteps.length > 0 && (
          <div style={{ borderBottom: '1px solid var(--border-color)' }}>
            {activeStep ? (
              // Detail header: a Back affordance to return to the step list.
              <button
                type="button"
                onClick={() => setActiveStep(null)}
                className="flex items-center gap-1.5 w-full px-4 py-2 text-left text-[11px] font-medium transition-colors hover:opacity-80"
                style={{ color: 'var(--text-muted)' }}
                aria-label="Back to the run activity"
              >
                <svg className="w-3 h-3 flex-shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M15.75 19.5L8.25 12l7.5-7.5" />
                </svg>
                Back · {friendlyStep(activeStep.label)}
              </button>
            ) : (
              <div className="flex items-center">
                <button
                  type="button"
                  onClick={toggleActivity}
                  className="flex items-center gap-1.5 flex-1 px-4 py-2 text-left text-[11px] font-medium transition-colors hover:opacity-80"
                  style={{ color: 'var(--text-muted)' }}
                  aria-expanded={activityOpen}
                >
                  <svg
                    className="w-3 h-3 flex-shrink-0 transition-transform"
                    style={{ transform: activityOpen ? 'rotate(90deg)' : 'rotate(0deg)' }}
                    fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}
                  >
                    <path strokeLinecap="round" strokeLinejoin="round" d="M8.25 4.5l7.5 7.5-7.5 7.5" />
                  </svg>
                  Run activity · {runSteps.length} step{runSteps.length === 1 ? '' : 's'}
                </button>
                {onMoveActivityToChat && (
                  <button
                    type="button"
                    onClick={onMoveActivityToChat}
                    aria-label="Show in chat"
                    className="w-7 h-7 mr-2 rounded-md flex items-center justify-center flex-shrink-0 transition-colors hover:opacity-80"
                    style={{ color: 'var(--text-muted)' }}
                    title="Show the activity in the chat instead"
                  >
                    <PanelLeft size={14} aria-hidden="true" />
                  </button>
                )}
              </div>
            )}
            {activityOpen && !activeStep && (
              <div className="px-4 pb-3">
                <ThinkingStream steps={runSteps} onSelect={setActiveStep} />
              </div>
            )}
          </div>
        )}
        {/* A chosen step's context, on its own full page (replaces the deliverable
            and fills the pane). */}
        {activeStep && (
          <div data-testid="run-step-context" style={{ minHeight: '100%' }}>
            <LogSurface body={activeStep.detail || ''} />
          </div>
        )}
        {/* When the activity is open (list or detail) it REPLACES the deliverable
            — the pane shows one or the other, never stacked. */}
        {!activityOpen && !activeStep && uiSurface && (
          // Key on the displayed content so a refine (or history navigation)
          // REMOUNTS the renderer instead of reusing the instance. The refined
          // deck reuses the same component ids (root/slide1…), so without a key
          // React keeps the old instance — and its internal useState (slide
          // index) — leaving the preview on the stale version. Index + content
          // length changes whenever the artifact does.
          // A deck gets a flex-fill wrapper + `fit` so it letterboxes to the height;
          // other surfaces render naturally inside the scrolling content area.
          fitDeck ? (
            <div className="flex-1 min-h-0 flex p-3">
              <A2uiSurface key={`ui-${index ?? 0}-${displayData.length}`} surface={uiSurface} hideDownloads fit />
            </div>
          ) : (
            // Non-deck surfaces (dashboard, document, quiz) scroll naturally. Center
            // them in a padded, max-width column so they don't glue to the top-left
            // edge and leave a vast empty band when the pane is wide (fullscreen).
            <div className="mx-auto w-full max-w-6xl p-4 sm:p-6">
              <A2uiSurface key={`ui-${index ?? 0}-${displayData.length}`} surface={uiSurface} hideDownloads />
            </div>
          )
        )}
        {/* Plain-text / markdown answer (chat-mode deliverable). Rendered with the
            same markdown renderer the chat uses, in a roomy, scrollable column. */}
        {!activityOpen && !activeStep && !uiSurface && isText && (
          <div
            className="mx-auto w-full max-w-3xl p-4 sm:p-6 text-[15px] leading-[1.7]"
            style={{ color: 'var(--text-primary)' }}
          >
            <MessageContent content={displayData} />
          </div>
        )}
      </div>
    </aside>
  );
};

export default PreviewPanel;
