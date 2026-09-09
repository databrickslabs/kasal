import { useLayoutEffect, useRef, useState, type ReactNode } from 'react';
import { createPortal } from 'react-dom';
import { Box, Drawer, IconButton, Typography, useTheme } from '@mui/material';
import { X } from 'lucide-react';
import { useCatalogNavigation } from './CatalogNavigation';
import { kasalStageSurface } from '../../../../../theme/kasalSurfaces';

/** Open a detail page inside Catalog, keeping its list and scroll position. */
export default function CatalogActionPane({ open, label, onClose, children }: {
  open: boolean; label: string; onClose: () => void; children: ReactNode;
}) {
  const theme = useTheme();
  const navigation = useCatalogNavigation();
  const openAction = navigation?.openAction;
  const releaseAction = navigation?.releaseAction;
  const [host] = useState(() => document.createElement('div'));
  const closeRef = useRef(onClose);
  closeRef.current = onClose;
  useLayoutEffect(() => {
    if (!open || !openAction) return;
    host.style.cssText = 'height:100%;min-height:0;display:flex;flex-direction:column;';
    openAction({ label, host, onClose: () => closeRef.current() });
    return () => releaseAction?.(host);
  }, [open, openAction, releaseAction, label, host]);

  if (!open) return null;
  const content = <Box role="region" aria-label={label}
    onClick={event => event.stopPropagation()} onKeyDown={event => event.stopPropagation()}
    sx={{ display: 'flex', flexDirection: 'column', flex: 1, height: '100%', minHeight: 0, minWidth: 0, overflow: 'hidden',
      color: 'text.primary',
      '& > .MuiDialogTitle-root': { px: 2.5, pt: 2.5, pb: 2, fontSize: 20, fontWeight: 600, flexShrink: 0 },
      '& > .MuiDialogContent-root': { px: 2.5, py: 2, minHeight: 0, overflowX: 'hidden' },
      '& > .MuiDialogActions-root': { p: 2, gap: 1, flexShrink: 0, flexWrap: 'wrap', borderTop: 1, borderColor: 'divider' },
      '& .MuiButton-root': { textTransform: 'none', borderRadius: 2.5, boxShadow: 'none' },
      '& .MuiOutlinedInput-root': { borderRadius: 2.5 },
    }}>{children}</Box>;
  if (navigation) return createPortal(content, host);

  // Catalog can also be opened outside a builder. Keep those actions in a
  // side pane too, without requiring a canvas or creating a centered dialog.
  return <Drawer anchor="right" variant="persistent" open
    sx={{ zIndex: theme.zIndex.modal + 1 }}
    PaperProps={{ sx: { width: 'min(560px, calc(100vw - 16px))', top: 8, right: 8, bottom: 8,
      height: 'auto', border: 0, borderRadius: 3, ...kasalStageSurface(theme.palette.mode === 'dark') } }}>
    <Box onClick={event => event.stopPropagation()} onKeyDown={event => event.stopPropagation()}
      sx={{ display: 'flex', alignItems: 'center', gap: 1, px: 2, py: 1, borderBottom: 1, borderColor: 'divider' }}>
      <Typography noWrap sx={{ flex: 1, fontSize: 13, fontWeight: 600 }}>{label}</Typography>
      <IconButton size="small" aria-label={`Close ${label}`} onClick={onClose}><X size={16} /></IconButton>
    </Box>
    {content}
  </Drawer>;
}
