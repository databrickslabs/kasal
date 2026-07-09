import React, { useMemo, useRef, useState } from 'react';
import { Maximize2, Palette, PanelRight } from 'lucide-react';
import {
  A2UIRenderer,
  DeckThemeContext,
  SurfaceChromeContext,
  getDeckTheme,
  DEFAULT_DECK_THEME_ID,
  themeToDeck,
  themeToTokens,
  type Surface,
} from '../../../../shared/a2ui';
import { useA2uiThemes } from '../../hooks/useA2uiThemes';
import { THEME_PRESETS, DEFAULT_THEME, type Theme } from '../../../Configuration/uiConfigShared';

/** A per-surface palette persisted by the preview's "Customize → Look" restyle.
 *  Stored on the surface (the composer never emits it) so it survives reload/PDF. */
type ThemedSurface = Surface & { theme?: Theme };

// A2UI surfaceKind → UIConfigurator deliverable key, for resolving the workspace
// branding palette. 'document' falls to the closest configurable type ('report').
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

// Some deliverables are COMPONENTS (usable inside dashboard/document), not their
// own surfaceKind — so a surface WHOSE ROOT is one of these resolves its branding
// from the component, letting the UIConfigurator's per-type palette + directives
// apply (e.g. a document rooted on a Forecast uses the 'forecast' palette). When
// the component is only PART of a larger surface, the surfaceKind palette wins.
const ROOT_COMPONENT_TO_DELIVERABLE: Record<string, string> = {
  Forecast: 'forecast',
  Graph: 'graph',
  Sequence: 'sequence',
  Album: 'album',
  Diagram: 'diagram',
};

/**
 * The ONE place a composed A2UI {@link Surface} is rendered with Kasal workspace
 * branding. Shared by the chat thread, the preview pane, the workflow chat, and
 * the Jobs result viewer so every host renders identically — and the same way the
 * exported Databricks App does (which vendors the same `shared/a2ui` renderer).
 *
 * Resolves the workspace palette for the surface's deliverable and applies it two
 * ways: the deck theme (presentations) via `DeckThemeContext`, and `--a2-*` tokens
 * (cards/tables/dashboards) via inline CSS vars. Decks keep their built-in theme
 * unless the admin gave that deliverable a palette (mirrors the UIConfigurator).
 *
 * The `.kasal-a2ui` wrapper restores the two preflight bits the renderer needs
 * (box-sizing + default border-style), since Tailwind preflight is disabled
 * globally to protect MUI.
 */
export const A2uiSurface: React.FC<{
  surface: Surface;
  className?: string;
  /** Force a specific palette (e.g. the preview's live restyle, or the 'logs'
   *  palette for run context), overriding both surface.theme and the workspace. */
  palette?: Theme;
  /** When provided, render a corner "expand" control that opens this surface in
   *  the side preview pane. Passed only by the inline chat host — the preview pane
   *  and PDF rasterizer omit it (no nested expand). */
  onExpand?: () => void;
  /** Suppress the surface's OWN download chrome (deck "PowerPoint" / table "CSV").
   *  The preview pane sets this — its top toolbar owns a single PDF/PowerPoint
   *  download — and the PDF rasterizer sets it so buttons aren't baked into the
   *  page. The inline chat and the exported app leave it off (chrome shown). */
  hideDownloads?: boolean;
  /** Host-supplied PDF export. When provided, a deck's in-surface "Download" menu
   *  offers a PDF option (PDF rasterization is host-specific, so the host owns it;
   *  the shared PowerPoint export needs no host help). */
  onDownloadPdf?: () => void;
  /** When provided, render a small "customize colors" control (a compact Look
   *  picker mirroring the preview pane's presets). Called with the restyled
   *  surface so the host can persist it. The inline chat host supplies it; the
   *  preview pane (own RefinePanel) and the PDF rasterizer omit it. */
  onRestyle?: (surface: Surface) => void;
  /** Fit a deck to the available HEIGHT (letterbox, no vertical scroll). The host
   *  must give this surface a bounded-height flex parent (the preview pane does). */
  fit?: boolean;
}> = ({ surface, className, palette, onExpand, hideDownloads, onDownloadPdf, onRestyle, fit }) => {
  const a2uiThemes = useA2uiThemes();
  const [lookOpen, setLookOpen] = useState(false);
  // Prefer a component-level deliverable when the surface's ROOT is a
  // component-based deliverable (Forecast/Graph/Sequence/Album), else fall back
  // to the surfaceKind mapping.
  const rootComponent = surface.components.find((c) => c.id === surface.root)?.component;
  const deliverableKey =
    (rootComponent && ROOT_COMPONENT_TO_DELIVERABLE[rootComponent]) ||
    SURFACE_TO_DELIVERABLE[surface.surfaceKind] ||
    'default';
  // Precedence: explicit prop → per-surface "Look" override → workspace palette.
  const override = palette ?? (surface as ThemedSurface).theme;
  const deckPalette = override ?? a2uiThemes?.[deliverableKey];
  // Token-themed kinds (dashboard/document — where graphs/tables live) must default
  // to the LIGHT DEFAULT_THEME (white background) rather than inheriting the chat
  // root's `--a2-*` tokens, which are DARK when the chat is in dark mode. An
  // unconfigured/disabled workspace yields no palette (useA2uiThemes → null), so
  // without this a graph surface would render dark-on-dark by default. Deck-themed
  // and presentation kinds keep their existing (null → inherit / deck) behavior.
  const isTokenThemed =
    surface.surfaceKind === 'dashboard' || surface.surfaceKind === 'document';
  const tokenPalette =
    override ??
    a2uiThemes?.[deliverableKey] ??
    a2uiThemes?.default ??
    (isTokenThemed ? DEFAULT_THEME : null);
  // Memoized: parents re-render per trace tick, and a fresh theme object /
  // context value each render forces every themed child inside the surface to
  // re-render even when nothing about the surface changed.
  const deckTheme = useMemo(
    () => (deckPalette ? themeToDeck(deckPalette) : getDeckTheme(DEFAULT_DECK_THEME_ID)),
    [deckPalette],
  );
  const tokenStyle = useMemo(
    () => (tokenPalette ? (themeToTokens(tokenPalette) as React.CSSProperties) : undefined),
    [tokenPalette],
  );
  // A quiz is an interactive, multi-question surface — a PDF/PPTX snapshot
  // captures only the current question, so it has nothing meaningful to
  // export. Suppress its Download control (the other kinds keep it).
  const chromeContext = useMemo(
    () => ({
      downloads: !hideDownloads && surface.surfaceKind !== 'quiz',
      onDownloadPdf,
      fit,
    }),
    [hideDownloads, surface.surfaceKind, onDownloadPdf, fit],
  );
  // Paint the surface's OWN themed background + foreground so its content reads
  // correctly instead of compositing over the (possibly opposite-theme) chat page.
  // Two theming systems → two sources:
  //   • deck-themed kinds (presentation/quiz/flashcards/mindmap) use DeckThemeContext
  //     colors (theme.fg, theme.panel, …). Their cards/buttons assume a themed
  //     backdrop, so without painting the deck bg here a light foreground renders
  //     invisibly on the white chat page (e.g. a flashcard's "Still learning" button
  //     or its answer side). The mindmap's own canvas is the same solid bg, so the
  //     wrapper matches it seamlessly.
  //   • token-themed kinds (dashboard/document) use the shadcn `--a2-*` tokens.
  // This mirrors the exported app, which wraps every surface in a solid themed Card.
  // Presentation is intentionally excluded: its slide stage is already themed and
  // its body text is fixed via the `.a2-slide` CSS (color: inherit), so painting
  // the wrapper would only re-tint the area around the slide + its Prev/Next nav.
  const deckThemed =
    surface.surfaceKind === 'quiz' ||
    surface.surfaceKind === 'flashcards' ||
    surface.surfaceKind === 'mindmap';
  const surfaceBgStyle: React.CSSProperties = deckThemed
    ? { backgroundColor: deckTheme.bg, color: deckTheme.fg }
    : tokenStyle && (surface.surfaceKind === 'dashboard' || surface.surfaceKind === 'document')
      ? { backgroundColor: 'hsl(var(--a2-background))', color: 'hsl(var(--a2-foreground))' }
      : {};
  // Apply a preset Look instantly: stamp the picked palette onto the surface
  // (the renderer reads surface.theme as the override) and hand the restyled
  // surface back so the host persists it. Mirrors the preview pane's applyStyle.
  const activeAccent = (override as Theme | undefined)?.accent;
  const restyle = (preset: Theme) => {
    setLookOpen(false);
    onRestyle?.({ ...surface, theme: { ...(surface as ThemedSurface).theme, ...preset } } as Surface);
  };
  // Full screen targets THIS surface element directly (native Fullscreen API) —
  // distinct from "open in preview pane". Toggles so the same button exits.
  const containerRef = useRef<HTMLDivElement>(null);
  const toggleFullscreen = () => {
    const el = containerRef.current;
    if (!el) return;
    if (document.fullscreenElement === el) {
      void document.exitFullscreen?.();
    } else {
      void el.requestFullscreen?.().catch(() => {});
    }
  };
  return (
    <div
      ref={containerRef}
      data-surface-kind={surface.surfaceKind}
      className={className ? `kasal-a2ui ${className}` : 'kasal-a2ui'}
      style={{
        ...tokenStyle,
        position: 'relative',
        // The surface's own themed background + foreground (see `surfaceBgStyle`),
        // so its content never composites over the chat page and reads dark-on-dark.
        ...surfaceBgStyle,
        // fit: become a flex column that fills, so the deck's height chain reaches
        // the SlideDeck (which letterboxes the 16:9 stage to the available height).
        ...(fit ? { display: 'flex', flexDirection: 'column', flex: '1 1 auto', minHeight: 0 } : {}),
      }}
    >
      {(onExpand || onRestyle) && (
        // Top-RIGHT, absolutely positioned to sit on the SAME band as the renderer's
        // own "Download" control (which is the first, top-LEFT in-flow row) — so
        // download + colors + preview + full screen all line up. No background. The
        // download row reserves this vertical space, so the icons never overlap the
        // surface content below.
        <div className="absolute top-1.5 right-2 z-[3] flex items-center gap-2">
          {onRestyle && (
            <div className="relative">
              <button
                type="button"
                aria-label="Customize colors"
                title="Customize colors"
                aria-expanded={lookOpen}
                onClick={(e) => {
                  e.stopPropagation();
                  setLookOpen((o) => !o);
                }}
                className="cursor-pointer border-0 bg-transparent p-0.5 text-gray-500 opacity-70 hover:opacity-100"
              >
                <Palette size={16} />
              </button>
              {lookOpen && (
                <>
                  {/* click-away catcher */}
                  <div
                    className="fixed inset-0 z-[4]"
                    onClick={(e) => {
                      e.stopPropagation();
                      setLookOpen(false);
                    }}
                    aria-hidden="true"
                  />
                  <div
                    role="menu"
                    onClick={(e) => e.stopPropagation()}
                    className="absolute right-0 top-7 z-[5] flex flex-col gap-1.5 rounded-xl p-2.5 shadow-lg"
                    style={{ backgroundColor: 'var(--bg-input)', border: '1px solid var(--border-color)', minWidth: 168 }}
                  >
                    <span className="px-0.5 text-[10px] font-semibold uppercase tracking-wider" style={{ color: 'var(--text-muted)' }}>
                      Colors
                    </span>
                    <div className="flex flex-wrap gap-1.5" style={{ maxWidth: 200 }}>
                      {THEME_PRESETS.map((p) => {
                        const active = activeAccent === p.theme.accent;
                        return (
                          <button
                            key={p.key}
                            type="button"
                            title={p.label}
                            aria-label={p.label}
                            aria-pressed={active}
                            onClick={(e) => {
                              e.stopPropagation();
                              restyle({ ...p.theme });
                            }}
                            className="h-7 w-7 rounded-full transition-transform hover:scale-110"
                            style={{
                              background: `linear-gradient(135deg, ${p.theme.accent} 0%, ${p.theme.accent} 55%, ${p.theme.surface} 55%, ${p.theme.surface} 100%)`,
                              boxShadow: active
                                ? '0 0 0 2px var(--accent), inset 0 0 0 1px rgba(0,0,0,0.18)'
                                : 'inset 0 0 0 1px rgba(0,0,0,0.18)',
                            }}
                          />
                        );
                      })}
                    </div>
                  </div>
                </>
              )}
            </div>
          )}
          {onExpand && (
            <button
              type="button"
              aria-label="Open in preview pane"
              title="Open in preview pane"
              onClick={(e) => {
                e.stopPropagation();
                onExpand();
              }}
              className="cursor-pointer border-0 bg-transparent p-0.5 text-gray-500 opacity-70 hover:opacity-100"
            >
              <PanelRight size={16} />
            </button>
          )}
          {onExpand && (
            <button
              type="button"
              aria-label="Full screen"
              title="Full screen"
              onClick={(e) => {
                e.stopPropagation();
                toggleFullscreen();
              }}
              className="cursor-pointer border-0 bg-transparent p-0.5 text-gray-500 opacity-70 hover:opacity-100"
            >
              <Maximize2 size={16} />
            </button>
          )}
        </div>
      )}
      <DeckThemeContext.Provider value={deckTheme}>
        <SurfaceChromeContext.Provider value={chromeContext}>
          <A2UIRenderer payload={surface} />
        </SurfaceChromeContext.Provider>
      </DeckThemeContext.Provider>
    </div>
  );
};

export default A2uiSurface;
