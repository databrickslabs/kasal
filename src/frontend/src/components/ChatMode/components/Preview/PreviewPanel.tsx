import React, { useRef, useEffect, useMemo, useState } from 'react';
import { Menu, MenuItem } from '@mui/material';
import Box from '@mui/material/Box';
import { buttonResetSx, pulseSx } from '../../chatSx';
import { DELIVERABLE_LABELS } from '../../../Configuration/uiConfigShared';
import { toSurface } from '../../utils/surfaceAdapter';
import { themeToDeck, getDeckTheme, DEFAULT_DECK_THEME_ID } from '../../../../shared/a2ui';
import type { Surface } from '../../../../shared/a2ui';
import { downloadPptx } from '../../../../shared/a2ui/lib/download';
import type { Theme } from '../../../Configuration/uiConfigShared';
import { downloadSurfacePdf } from '../../utils/surfacePdf';
import { useA2uiThemes } from '../../hooks/useA2uiThemes';
import A2uiSurface from '../Chat/A2uiSurface';
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
  document: 'report',
  conversation: 'default',
};

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

// Shared toolbar button styles — ALWAYS spread into a fresh `sx` literal at the
// call site (`sx={{ ...iconBtnSx }}`), never passed directly, so inferred string
// props don't widen out of SxProps. `iconBtnSx`: the 28×28 icon buttons;
// `pillBtnSx`: the labelled Activity/Customize pills (caller adds bg + active color).
const iconBtnSx = {
  ...buttonResetSx,
  width: 28,
  height: 28,
  borderRadius: '8px',
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'center',
  transition: 'color 0.15s, background-color 0.15s',
  color: 'text.disabled',
  '&:hover': { opacity: 0.7 },
};
const pillBtnSx = {
  ...buttonResetSx,
  display: 'flex',
  alignItems: 'center',
  gap: 0.75,
  height: 28,
  px: 1.25,
  borderRadius: '8px',
  fontSize: 12,
  fontWeight: 500,
  transition: 'color 0.15s, background-color 0.15s',
  '&:hover': { opacity: 0.8 },
};

const PreviewPanel: React.FC<PreviewPanelProps> = ({ content, onClose, chatCollapsed, onToggleChat, onRefine, onStyleChange, history, index, onNavigate, runSteps = [], onMoveActivityToChat, embedded }) => {
  const [refineOpen, setRefineOpen] = useState(false);
  const asideRef = useRef<HTMLElement>(null);
  const [fullscreen, setFullscreen] = useState(false);
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
  const baseName = content.title || 'kasal-app';
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
    <Box
      component="aside"
      ref={asideRef}
      sx={{
        display: 'flex',
        flexDirection: 'column',
        height: '100%',
        flex: chatCollapsed ? '1 1 100%' : '1 1 50%',
        minWidth: '300px',
        backgroundColor: 'background.default',
        ...(chatCollapsed ? {} : { borderLeft: 1, borderColor: 'divider' }),
      }}
    >
      {/* Header — hidden entirely in full screen for a chrome-free view (exit with
          Esc), and when embedded: the host dialog (Jobs "Show result") already
          provides its own title bar + controls, so a second header would just
          stack and steal vertical height (forcing a scroll). */}
      {!fullscreen && !embedded && (
      <Box
        sx={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          px: 2,
          py: 1.5,
          flexShrink: 0,
          borderBottom: 1,
          borderColor: 'divider',
        }}
      >
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          {/* Toggle chat button — only when there's a chat column beside the pane */}
          {onToggleChat && (
          <Box
            component="button"
            onClick={onToggleChat}
            sx={{ ...iconBtnSx }}
            title={chatCollapsed ? 'Show chat' : 'Hide chat'}
          >
            {/* The preview sits to the RIGHT of the chat, so expanding it to
                full width grows LEFTWARD: "hide chat" points left (◀◀), and
                restoring the chat points right (▶▶). */}
            <Box component="svg" sx={{ width: 16, height: 16 }} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              {chatCollapsed ? (
                <path strokeLinecap="round" strokeLinejoin="round" d="M11.25 4.5l7.5 7.5-7.5 7.5m-6-15l7.5 7.5-7.5 7.5" />
              ) : (
                <path strokeLinecap="round" strokeLinejoin="round" d="M18.75 19.5l-7.5-7.5 7.5-7.5m-6 15L5.25 12l7.5-7.5" />
              )}
            </Box>
          </Box>
          )}
          <Box
            component="svg"
            sx={{ width: 16, height: 16, color: 'primary.main' }}
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
          </Box>
          <Box
            component="span"
            sx={{ fontSize: 14, fontWeight: 500, color: 'text.primary' }}
          >
            {content.title || 'App'}
          </Box>
          <Box
            component="span"
            sx={{
              fontSize: 10,
              px: 0.75,
              py: 0.25,
              borderRadius: '4px',
              fontFamily: 'monospace',
              color: 'text.disabled',
              backgroundColor: (t) => t.chat.bgSecondary,
            }}
          >
            {content.type.toUpperCase()}
          </Box>
        </Box>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
          {history && history.length > 1 && onNavigate && (() => {
            const current = index ?? history.length - 1;
            return (
              <Box
                sx={{ display: 'flex', alignItems: 'center', gap: 0.25, mr: 0.5, borderRadius: '8px', backgroundColor: (t) => t.chat.bgSecondary }}
              >
                <Box
                  component="button"
                  onClick={() => onNavigate(current - 1)}
                  disabled={current <= 0}
                  sx={{ ...iconBtnSx, '&:disabled': { opacity: 0.3, cursor: 'not-allowed' } }}
                  title="Previous output"
                  aria-label="Previous output"
                >
                  <Box component="svg" sx={{ width: 16, height: 16 }} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                    <path strokeLinecap="round" strokeLinejoin="round" d="M15.75 19.5L8.25 12l7.5-7.5" />
                  </Box>
                </Box>
                <Box
                  component="span"
                  sx={{ fontSize: 11, fontFamily: 'monospace', fontVariantNumeric: 'tabular-nums', px: 0.5, userSelect: 'none', color: 'text.disabled' }}
                  title="Task output (latest shown first)"
                >
                  {current + 1}/{history.length}
                </Box>
                <Box
                  component="button"
                  onClick={() => onNavigate(current + 1)}
                  disabled={current >= history.length - 1}
                  sx={{ ...iconBtnSx, '&:disabled': { opacity: 0.3, cursor: 'not-allowed' } }}
                  title="Next output"
                  aria-label="Next output"
                >
                  <Box component="svg" sx={{ width: 16, height: 16 }} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                    <path strokeLinecap="round" strokeLinejoin="round" d="M8.25 4.5l7.5 7.5-7.5 7.5" />
                  </Box>
                </Box>
              </Box>
            );
          })()}
          {runSteps.length > 0 && (
            <Box
              component="button"
              onClick={toggleActivity}
              sx={{ ...pillBtnSx, color: activityOpen ? 'primary.main' : 'text.secondary', backgroundColor: (t) => t.chat.bgSecondary }}
              title="Show the run activity timeline (steps + context)"
              aria-pressed={activityOpen}
            >
              <Box component="svg" sx={{ width: 14, height: 14 }} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.8}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M3.75 6.75h.007v.008H3.75V6.75zm0 5.25h.007v.008H3.75V12zm0 5.25h.007v.008H3.75v-.008zM8.25 6.75h12M8.25 12h12m-12 5.25h12" />
              </Box>
              Activity
            </Box>
          )}
          {onRefine && (
            <Box
              component="button"
              onClick={() => setRefineOpen((v) => !v)}
              sx={{ ...pillBtnSx, color: refineOpen ? 'primary.main' : 'text.secondary', backgroundColor: (t) => t.chat.bgSecondary }}
              title="Customize the look and content of this result"
            >
              <Box component="svg" sx={{ width: 14, height: 14 }} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.8}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M9.813 15.904L9 18.75l-.813-2.846a4.5 4.5 0 00-3.09-3.09L2.25 12l2.846-.813a4.5 4.5 0 003.09-3.09L9 5.25l.813 2.846a4.5 4.5 0 003.09 3.09L15.75 12l-2.846.813a4.5 4.5 0 00-3.09 3.09z" />
              </Box>
              Customize
            </Box>
          )}
          <Box
            component="button"
            onClick={(e: React.MouseEvent<HTMLButtonElement>) => setDownloadAnchor(e.currentTarget)}
            disabled={downloading}
            sx={{ ...iconBtnSx, '&:disabled': { opacity: 0.4, cursor: 'wait' } }}
            title="Download"
            aria-label="Download"
            aria-haspopup="menu"
          >
            <Box
              component="svg"
              sx={{ width: 16, height: 16, ...(downloading ? pulseSx : {}) }}
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
              strokeWidth={2}
            >
              <path strokeLinecap="round" strokeLinejoin="round" d="M3 16.5v2.25A2.25 2.25 0 005.25 21h13.5A2.25 2.25 0 0021 18.75V16.5M16.5 12L12 16.5m0 0L7.5 12m4.5 4.5V3" />
            </Box>
          </Box>
          <Menu
            anchorEl={downloadAnchor}
            open={Boolean(downloadAnchor)}
            onClose={() => setDownloadAnchor(null)}
            anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
            transformOrigin={{ vertical: 'top', horizontal: 'right' }}
          >
            <MenuItem onClick={() => runDownload('pdf')}>PDF</MenuItem>
            <MenuItem onClick={() => runDownload('pptx')}>PowerPoint</MenuItem>
          </Menu>
          {!embedded && (
          <Box
            component="button"
            onClick={enterFullscreen}
            sx={{ ...iconBtnSx }}
            title="Full screen"
            aria-label="Full screen"
          >
            <Box component="svg" sx={{ width: 16, height: 16 }} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M3.75 3.75v4.5m0-4.5h4.5m-4.5 0L9 9m11.25-5.25h-4.5m4.5 0v4.5m0-4.5L15 9M3.75 20.25v-4.5m0 4.5h4.5m-4.5 0L9 15m11.25 5.25h-4.5m4.5 0v-4.5m0 4.5L15 15" />
            </Box>
          </Box>
          )}
          {!embedded && (
          <Box
            component="button"
            onClick={onClose}
            sx={{ ...iconBtnSx }}
            title="Close preview"
          >
            <Box
              component="svg"
              sx={{ width: 16, height: 16 }}
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
            </Box>
          </Box>
          )}
        </Box>
      </Box>
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

      {/* Content — A2UI only; non-UI documents are never previewable. The deck-fit
          layout (overflow-hidden flex column, so the deck letterboxes to height)
          applies ONLY when the deck DELIVERABLE is actually shown. While the run
          activity list or a step's context (LogSurface, minHeight:100%) is open it
          REPLACES the deliverable, and those need the natural scrolling box —
          otherwise the fit container collapses/clips them to an empty pane. */}
      <Box
        data-testid="preview-body"
        sx={{
          flex: 1,
          minHeight: 0,
          ...(fitDeck && !activityOpen && !activeStep
            ? { overflow: 'hidden', display: 'flex', flexDirection: 'column' }
            : { overflow: 'auto' }),
        }}
      >
        {/* Run activity — collapsed above the result so it's never lost. The list
            shows plain-English steps; clicking one opens its context full-page. */}
        {runSteps.length > 0 && !fullscreen && (
          <Box sx={{ borderBottom: 1, borderColor: 'divider' }}>
            {activeStep ? (
              // Detail header: a Back affordance to return to the step list.
              <Box
                component="button"
                type="button"
                onClick={() => setActiveStep(null)}
                sx={{ ...buttonResetSx, display: 'flex', alignItems: 'center', gap: 0.75, width: '100%', px: 2, py: 1, textAlign: 'left', fontSize: 11, fontWeight: 500, transition: 'color 0.15s, background-color 0.15s', color: 'text.disabled', '&:hover': { opacity: 0.8 } }}
                aria-label="Back to the run activity"
              >
                <Box component="svg" sx={{ width: 12, height: 12, flexShrink: 0 }} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M15.75 19.5L8.25 12l7.5-7.5" />
                </Box>
                Back · {friendlyStep(activeStep.label)}
              </Box>
            ) : (
              <Box sx={{ display: 'flex', alignItems: 'center' }}>
                <Box
                  component="button"
                  type="button"
                  onClick={toggleActivity}
                  sx={{ ...buttonResetSx, display: 'flex', alignItems: 'center', gap: 0.75, flex: 1, px: 2, py: 1, textAlign: 'left', fontSize: 11, fontWeight: 500, transition: 'color 0.15s, background-color 0.15s', color: 'text.disabled', '&:hover': { opacity: 0.8 } }}
                  aria-expanded={activityOpen}
                >
                  <Box
                    component="svg"
                    sx={{ width: 12, height: 12, flexShrink: 0, transition: 'transform 0.15s', transform: activityOpen ? 'rotate(90deg)' : 'rotate(0deg)' }}
                    fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}
                  >
                    <path strokeLinecap="round" strokeLinejoin="round" d="M8.25 4.5l7.5 7.5-7.5 7.5" />
                  </Box>
                  Run activity · {runSteps.length} step{runSteps.length === 1 ? '' : 's'}
                </Box>
                {onMoveActivityToChat && (
                  <Box
                    component="button"
                    type="button"
                    onClick={onMoveActivityToChat}
                    sx={{ ...buttonResetSx, px: 1.5, py: 1, fontSize: 11, flexShrink: 0, transition: 'color 0.15s, background-color 0.15s', color: 'text.disabled', '&:hover': { opacity: 0.8 } }}
                    title="Show the activity in the chat instead"
                  >
                    Show in chat
                  </Box>
                )}
              </Box>
            )}
            {activityOpen && !activeStep && (
              <Box sx={{ px: 2, pb: 1.5 }}>
                <ThinkingStream steps={runSteps} onSelect={setActiveStep} />
              </Box>
            )}
          </Box>
        )}
        {/* A chosen step's context, on its own full page (replaces the deliverable
            and fills the pane). */}
        {activeStep && (
          <Box data-testid="run-step-context" sx={{ minHeight: '100%' }}>
            <LogSurface body={activeStep.detail || ''} />
          </Box>
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
            <Box sx={{ flex: 1, minHeight: 0, display: 'flex', p: 1.5 }}>
              <A2uiSurface key={`ui-${index ?? 0}-${displayData.length}`} surface={uiSurface} hideDownloads fit />
            </Box>
          ) : (
            <A2uiSurface key={`ui-${index ?? 0}-${displayData.length}`} surface={uiSurface} hideDownloads />
          )
        )}
      </Box>
    </Box>
  );
};

export default PreviewPanel;
