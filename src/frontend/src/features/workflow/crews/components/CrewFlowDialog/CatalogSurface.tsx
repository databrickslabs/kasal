import { useEffect, type ReactNode } from 'react';
import { Box, Dialog, DialogTitle, IconButton, useMediaQuery, useTheme } from '@mui/material';
import { X } from 'lucide-react';
import BuilderNodeEditor from '../../../assistant/components/BuilderNodeEditor';
import { kasalStageSurface } from '../../../../../theme/kasalSurfaces';
import { CatalogNavigationContent } from './CatalogNavigation';

/** The same catalog and actions, hosted alongside the canvas in builders. */
export default function CatalogSurface({ open, embedded, onClose, titleId, tab, onEntered, children }: {
  open: boolean; embedded: boolean; onClose: () => void; titleId: string;
  tab?: number; onEntered: () => void; children: ReactNode;
}) {
  const theme = useTheme();
  const compact = useMediaQuery(theme.breakpoints.down('sm'));
  const title = ['Crew catalog', 'Agent catalog', 'Task catalog', 'Flow catalog'][tab ?? -1] || 'Catalog';
  useEffect(() => {
    if (open && embedded) onEntered();
    // Focus only when opening, not on every catalog data update.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open, embedded]);
  if (embedded) return <BuilderNodeEditor open={open} kind="catalog" nodeId={String(tab ?? 'all')} label={title} onClose={onClose}>
    <Box aria-label={title} sx={{ display: 'flex', flexDirection: 'column', height: '100%', minHeight: 0,
      '& .catalog-list > .MuiDialogContent-root': { pt: 2, px: 2 }, '& .MuiGrid-item': { flexBasis: '100%', maxWidth: '100%' } }}>
      <CatalogNavigationContent>{children}</CatalogNavigationContent>
    </Box>
  </BuilderNodeEditor>;
  return <Dialog open={open} onClose={onClose} maxWidth="lg" fullWidth fullScreen={compact}
    aria-labelledby={titleId} TransitionProps={{ onEntered }} PaperProps={{ sx: {
      ...kasalStageSurface(theme.palette.mode === 'dark'), height: compact ? '100%' : '86vh',
      maxHeight: compact ? '100%' : '900px', borderRadius: compact ? 0 : 4, border: 0,
    } }}>
    <DialogTitle id={titleId} sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', px: 3, pt: 3, pb: 2 }}>
      {title}<IconButton aria-label="Close catalog" onClick={onClose}><X size={20} /></IconButton>
    </DialogTitle>
    <CatalogNavigationContent>{children}</CatalogNavigationContent>
  </Dialog>;
}
