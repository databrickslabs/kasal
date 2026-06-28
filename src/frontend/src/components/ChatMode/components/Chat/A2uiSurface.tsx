import React from 'react';
import { IconButton, Tooltip } from '@mui/material';
import OpenInFullIcon from '@mui/icons-material/OpenInFull';
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
import type { Theme } from '../../../Configuration/uiConfigShared';

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
  document: 'report',
  conversation: 'default',
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
  /** Fit a deck to the available HEIGHT (letterbox, no vertical scroll). The host
   *  must give this surface a bounded-height flex parent (the preview pane does). */
  fit?: boolean;
}> = ({ surface, className, palette, onExpand, hideDownloads, onDownloadPdf, fit }) => {
  const a2uiThemes = useA2uiThemes();
  const deliverableKey = SURFACE_TO_DELIVERABLE[surface.surfaceKind] || 'default';
  // Precedence: explicit prop → per-surface "Look" override → workspace palette.
  const override = palette ?? (surface as ThemedSurface).theme;
  const deckPalette = override ?? a2uiThemes?.[deliverableKey];
  const tokenPalette =
    override ?? a2uiThemes?.[deliverableKey] ?? a2uiThemes?.default ?? null;
  const deckTheme = deckPalette
    ? themeToDeck(deckPalette)
    : getDeckTheme(DEFAULT_DECK_THEME_ID);
  const tokenStyle = tokenPalette
    ? (themeToTokens(tokenPalette) as React.CSSProperties)
    : undefined;
  return (
    <div
      className={className ? `kasal-a2ui ${className}` : 'kasal-a2ui'}
      style={{
        ...tokenStyle,
        position: 'relative',
        // fit: become a flex column that fills, so the deck's height chain reaches
        // the SlideDeck (which letterboxes the 16:9 stage to the available height).
        ...(fit ? { display: 'flex', flexDirection: 'column', flex: '1 1 auto', minHeight: 0 } : {}),
      }}
    >
      {onExpand && (
        <Tooltip title="Open in side panel">
          <IconButton
            size="small"
            aria-label="Open in side panel"
            onClick={(e) => {
              e.stopPropagation();
              onExpand();
            }}
            sx={{
              // Top-RIGHT to match the Run activity bar's expand control (same
              // icon, same side). Each surface's own controls (deck "PowerPoint" /
              // table "CSV" download, mindmap zoom) live on the LEFT so they never
              // collide with this. No circle/container behind it — just the bare
              // icon (matches the Run activity bar's expand affordance).
              position: 'absolute',
              top: 6,
              right: 6,
              zIndex: 2,
              padding: '2px',
              color: 'text.secondary',
              opacity: 0.7,
              '&:hover': { opacity: 1, bgcolor: 'transparent' },
            }}
          >
            <OpenInFullIcon sx={{ fontSize: 16 }} />
          </IconButton>
        </Tooltip>
      )}
      <DeckThemeContext.Provider value={deckTheme}>
        <SurfaceChromeContext.Provider value={{ downloads: !hideDownloads, onDownloadPdf, fit }}>
          <A2UIRenderer payload={surface} />
        </SurfaceChromeContext.Provider>
      </DeckThemeContext.Provider>
    </div>
  );
};

export default A2uiSurface;
