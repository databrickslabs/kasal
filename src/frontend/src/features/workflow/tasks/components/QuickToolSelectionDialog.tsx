import React, { useEffect, useState } from 'react';
import { Box, Button, CircularProgress, Dialog, DialogActions, DialogContent, DialogTitle, IconButton, Typography } from '@mui/material';
import CloseIcon from '@mui/icons-material/Close';
import CapabilitiesPicker from '../../../tools/components/configuration/CapabilitiesPicker';

export interface QuickToolSelectionDialogProps {
  open: boolean;
  embedded?: boolean;
  onClose: () => void;
  onApply: (tools: string[], mcpServers: string[]) => void;
  currentTools?: string[];
  currentMcpServers?: string[];
  isUpdating?: boolean;
  initialTab?: number;
}

/** Builder side-pane content; standalone callers retain the dialog fallback. */
const QuickToolSelectionDialog: React.FC<QuickToolSelectionDialogProps> = ({
  open, embedded = false, onClose, onApply, currentTools = [], currentMcpServers = [], isUpdating = false,
}) => {
  const [tools, setTools] = useState<string[]>([]);
  const [servers, setServers] = useState<string[]>([]);
  useEffect(() => {
    if (open) { setTools(currentTools.map(String)); setServers([...currentMcpServers]); }
    // Seed once on opening; parent rerenders must not reset an unapplied choice.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open]);
  if (!open) return null;
  const content = <>
    <DialogTitle sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', px: 2 }}>
      <Box><Typography variant="h6">Configure Capabilities</Typography>
        <Typography variant="body2" color="text.secondary">{tools.length} tools, {servers.length} MCP servers</Typography>
      </Box>
      <IconButton aria-label="Close capabilities" onClick={onClose}><CloseIcon /></IconButton>
    </DialogTitle>
    <DialogContent sx={{ px: 1.5, minHeight: 0 }}>
      <CapabilitiesPicker selectedTools={tools} onToolsChange={setTools} selectedMcpServers={servers} onMcpServersChange={setServers} disabled={isUpdating} />
    </DialogContent>
    <DialogActions sx={{ px: 2, py: 1.5 }}>
      <Button onClick={() => { setTools([]); setServers([]); }} disabled={isUpdating} sx={{ mr: 'auto' }}>Clear selection</Button>
      <Button onClick={onClose}>Cancel</Button>
      <Button variant="contained" disabled={isUpdating} onClick={() => { onApply(tools, servers); onClose(); }}>{isUpdating ? <CircularProgress size={20} /> : 'Apply'}</Button>
    </DialogActions>
  </>;
  return embedded ? <Box sx={{ display: 'flex', flexDirection: 'column', flex: 1, minHeight: 0, overflow: 'hidden' }}>{content}</Box>
    : <Dialog open onClose={onClose} maxWidth="sm" fullWidth>{content}</Dialog>;
};
export default QuickToolSelectionDialog;
