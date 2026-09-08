import { Box, Dialog, useMediaQuery, useTheme } from '@mui/material';
import type { ReactNode } from 'react';
import { kasalStageSurface } from '../../../../theme/kasalSurfaces';

export default function CrewOptimizeFrame({ embedded, open, onClose, titleId, children }: {
  embedded?: boolean; open: boolean; onClose: () => void; titleId: string; children: ReactNode;
}) {
  const theme = useTheme();
  const compact = useMediaQuery(theme.breakpoints.down('sm'));
  const sx = {
    ...kasalStageSurface(theme.palette.mode === 'dark'),
    border: 0,
    '& .MuiPaper-outlined': { border: 0, borderRadius: 3, bgcolor: 'action.hover', backgroundImage: 'none' },
    '& .MuiOutlinedInput-root': { borderRadius: 2.5, bgcolor: 'action.hover' },
    '& .MuiOutlinedInput-notchedOutline': { border: 0 },
    '& .MuiOutlinedInput-root.Mui-focused': { boxShadow: '0 0 0 2px var(--text-muted, #8D99A4)' },
    '& .MuiChip-outlined': { border: 0, bgcolor: 'action.hover', color: 'text.secondary' },
    '& .MuiButton-outlined': { border: 0, color: 'text.secondary' },
  };
  if (embedded) return <Box role="region" aria-labelledby={titleId}
    sx={{ ...sx, background: 'transparent', display: 'flex', flexDirection: 'column', height: '100%', minHeight: 0, minWidth: 0,
      '& .MuiDialogContent-root': { overflowX: 'hidden' } }}>{children}</Box>;
  return <Dialog open={open} onClose={onClose} fullWidth maxWidth="lg" fullScreen={compact} aria-labelledby={titleId}
    PaperProps={{ sx: { ...sx, borderRadius: compact ? 0 : 4, maxHeight: compact ? '100%' : '90vh' } }}>{children}</Dialog>;
}
