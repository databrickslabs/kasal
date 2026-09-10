import { useLayoutEffect, useRef, useState, type ReactNode } from 'react';
import { createPortal } from 'react-dom';
import { Box, Dialog, DialogContent, DialogTitle, IconButton } from '@mui/material';
import { X } from 'lucide-react';
import { useBuilderNodeEditorBridge } from '../store/builderNodeEditorBridge';

/** Existing form and callbacks, rendered in a pane when a builder owns one. */
export default function BuilderNodeEditor({ open, kind, nodeId, label, onClose, children }: {
  open: boolean; kind: 'agent' | 'task' | 'connection' | 'catalog' | 'capabilities' | 'model'; nodeId: string; label: string; onClose: () => void; children: ReactNode;
}) {
  const openPane = useBuilderNodeEditorBridge(state => state.open);
  const release = useBuilderNodeEditorBridge(state => state.release);
  const [host] = useState(() => document.createElement('div'));
  const closeRef = useRef(onClose);
  closeRef.current = onClose;
  useLayoutEffect(() => {
    if (!open || !openPane) return;
    const id = `${kind}:${nodeId}` as const;
    host.style.cssText = 'height:100%;min-height:0;display:flex;flex-direction:column;';
    openPane({ id, label, host, onClose: () => closeRef.current() });
    return () => release?.(id);
  }, [open, openPane, release, host, kind, nodeId, label]);
  if (!open) return null;
  const form = kind === 'catalog' ? children : <Box onClick={event => event.stopPropagation()} onKeyDown={event => event.stopPropagation()}
    sx={{ height: '100%', minHeight: 0, minWidth: 0, display: 'flex', flexDirection: 'column', p: 2, boxSizing: 'border-box', overflow: 'auto',
      '& .MuiPaper-root': { backgroundImage: 'none', backgroundColor: 'transparent', boxShadow: 'none' },
      '& > .MuiCard-root': { height: '100%', flex: 1, minHeight: 0, border: 0 },
      '& > .MuiCard-root > .MuiBox-root': { height: 'auto', minHeight: 0 },
      '& > .MuiCard-root > .MuiBox-root:first-of-type, & > .MuiCard-root > .MuiBox-root:last-of-type': { flexShrink: 0 },
      '& > .MuiCard-root > .MuiBox-root:last-of-type': { backgroundColor: 'transparent', borderTop: 0 },
      '&& .MuiInputLabel-root, && .MuiAccordionSummary-root': { backgroundColor: 'transparent' },
      '& .MuiOutlinedInput-root': { borderRadius: 2.5 },
      '& .MuiButton-root': { textTransform: 'none', borderRadius: 2.5 },
      '& .MuiGrid-item': { maxWidth: '100%', flexBasis: '100%' },
    }}>{children}</Box>;
  if (openPane) return createPortal(form, host);
  // Catalog/standalone node consumers without a builder keep their dialog.
  return <Dialog open onClose={onClose} maxWidth="md" fullWidth PaperProps={{ sx: { height: '85vh' } }}>
    {kind === 'task' && <DialogTitle>Edit Task<IconButton aria-label="close" onClick={onClose} sx={{ position: 'absolute', right: 8, top: 8 }}><X size={18} /></IconButton></DialogTitle>}
    <DialogContent sx={{ p: 0, display: 'flex', flexDirection: 'column' }}>{form}</DialogContent>
  </Dialog>;
}
