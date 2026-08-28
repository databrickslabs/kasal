import React, { useEffect, useRef, useState } from 'react';
import { Box, Dialog, DialogContent, IconButton, Tooltip, Typography } from '@mui/material';
import OpenInFullIcon from '@mui/icons-material/OpenInFull';
import CloseIcon from '@mui/icons-material/Close';
import A2uiSurface from '../../ChatMode/components/Chat/A2uiSurface';
// The shared A2UI renderer is styled by the chat-mode CSS, whose Tailwind
// utilities are ALL scoped under the `.kasal-chat-root` class
// (tailwind.config.js `important: '.kasal-chat-root'`). This chat lives outside
// that scope, so without the stylesheet + a scope wrapper every utility in the
// renderer silently no-ops (a SlideDeck's slide collapses to its text height,
// loses padding/centering, and its nav dots render unsized). Import the CSS and
// render each surface inside a `.kasal-chat-root` wrapper — the same recipe as
// `Jobs/ShowResult.tsx`. It's a CLASS (not the chat workspace's id), so it
// coexists cleanly with the chat-mode root.
import '../../ChatMode/chat.css';
import { useThemeStore } from '../../../store/theme';
import type { Surface } from '../../../shared/a2ui';

/** The width an A2UI surface is laid out at before being scaled into the
 *  narrow chat column — the renderer's typography is designed for a wide
 *  preview pane, so we render at a natural width and shrink-to-fit. */
const NATURAL_WIDTH = 860;
const MAX_PREVIEW_HEIGHT = 480;

/**
 * Full-size themed A2UI render: the shared A2uiSurface wrapper re-resolves the
 * workspace UI-Configurator branding (source of truth) and draws through the
 * shared A2UIRenderer — the same one implementation used everywhere + the export.
 * Wrapped in the `.kasal-chat-root` scope (with the current light/dark theme) so
 * the renderer's class-scoped Tailwind utilities and `--a2-*` vars resolve here.
 */
export const UiSurfaceView: React.FC<{ surface: Surface }> = ({ surface }) => {
  const isDarkMode = useThemeStore((s) => s.isDarkMode);
  return (
    <div className="kasal-chat-root" data-theme={isDarkMode ? 'dark' : 'light'}>
      <A2uiSurface surface={surface} />
    </div>
  );
};

/**
 * An A2UI final-result card for the Agent Builder chat: the surface renders as
 * its designed layout (scaled to fit the chat column, clipped at a readable
 * height) instead of raw JSON, with a full-size dialog behind an expand control.
 */
export const UiSurfaceResult: React.FC<{ surface: Surface }> = ({ surface }) => {
  const [dialogOpen, setDialogOpen] = useState(false);
  const outerRef = useRef<HTMLDivElement>(null);
  const innerRef = useRef<HTMLDivElement>(null);
  const [scale, setScale] = useState(1);
  const [scaledHeight, setScaledHeight] = useState<number | null>(null);
  // Presentations are no longer an A2UI kind (they render on the chat HTML
  // path); every surface keeps the click-to-expand + readable-height clip.
  const isDeck = false;

  // Shrink-to-fit: the surface renders at NATURAL_WIDTH and is scaled down to
  // the chat column's width; the wrapper's height tracks the scaled content.
  useEffect(() => {
    const update = () => {
      const w = outerRef.current?.clientWidth ?? 0;
      const h = innerRef.current?.offsetHeight ?? 0;
      const s = w > 0 ? Math.min(1, w / NATURAL_WIDTH) : 1;
      setScale(s);
      setScaledHeight(h > 0 ? h * s : null);
    };
    update();
    if (typeof ResizeObserver === 'undefined') return;
    const ro = new ResizeObserver(update);
    if (outerRef.current) ro.observe(outerRef.current);
    if (innerRef.current) ro.observe(innerRef.current);
    return () => ro.disconnect();
  }, []);

  const clipped = !isDeck && scaledHeight !== null && scaledHeight > MAX_PREVIEW_HEIGHT;
  const previewHeight =
    scaledHeight === null ? 'auto' : isDeck ? scaledHeight : Math.min(scaledHeight, MAX_PREVIEW_HEIGHT);

  return (
    <Box sx={{ width: '100%', minWidth: 0, whiteSpace: 'normal' }}>
      <Box
        sx={{
          borderRadius: 2,
          border: 1,
          borderColor: 'divider',
          overflow: 'hidden',
          bgcolor: 'background.paper',
        }}
      >
        <Box sx={{ display: 'flex', alignItems: 'center', px: 1.5, py: 0.5, borderBottom: 1, borderColor: 'divider' }}>
          <Typography variant="caption" sx={{ color: 'text.secondary', fontWeight: 500 }}>
            Generated UI
          </Typography>
          <Tooltip title="Open full view">
            <IconButton
              size="small"
              aria-label="Open full view"
              onClick={() => setDialogOpen(true)}
              sx={{ ml: 'auto' }}
            >
              <OpenInFullIcon sx={{ fontSize: 14 }} />
            </IconButton>
          </Tooltip>
        </Box>
        <Box
          ref={outerRef}
          onClick={isDeck ? undefined : () => setDialogOpen(true)}
          sx={{
            position: 'relative',
            overflow: 'hidden',
            height: previewHeight,
            cursor: isDeck ? 'default' : 'pointer',
          }}
        >
          <Box ref={innerRef} sx={{ width: NATURAL_WIDTH, transform: `scale(${scale})`, transformOrigin: 'top left' }}>
            <UiSurfaceView surface={surface} />
          </Box>
          {clipped && (
            <Box
              aria-hidden="true"
              sx={{
                position: 'absolute',
                left: 0,
                right: 0,
                bottom: 0,
                height: 56,
                background: (t) =>
                  `linear-gradient(to bottom, transparent, ${t.palette.background.paper})`,
                pointerEvents: 'none',
              }}
            />
          )}
        </Box>
      </Box>
      <Dialog open={dialogOpen} onClose={() => setDialogOpen(false)} fullWidth maxWidth="lg">
        <IconButton
          aria-label="Close full view"
          onClick={() => setDialogOpen(false)}
          sx={{
            position: 'absolute',
            top: 8,
            right: 8,
            zIndex: 1,
            bgcolor: 'background.paper',
            '&:hover': { bgcolor: 'action.hover' },
          }}
          size="small"
        >
          <CloseIcon fontSize="small" />
        </IconButton>
        <DialogContent sx={{ p: 0 }}>
          <UiSurfaceView surface={surface} />
        </DialogContent>
      </Dialog>
    </Box>
  );
};
