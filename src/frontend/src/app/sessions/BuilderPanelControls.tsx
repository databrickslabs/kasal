import React from 'react';
import { Box, Button, IconButton } from '@mui/material';
import { ArrowLeftRight, Maximize2, MessageSquare, X } from 'lucide-react';
import { useUILayoutStore } from '../../store/uiLayout';

/** Compact actions at the top of the conversation pane. */
export default function BuilderPanelControls() {
  const mode = useUILayoutStore(state => state.appMode);
  const open = useUILayoutStore(state => state.executionHistoryVisible);
  const visible = useUILayoutStore(state => state.assistantPanelVisible);
  const flowTab = useUILayoutStore(state => state.flowPanelTab);
  const side = useUILayoutStore(state => state.assistantPanelSide);
  if (mode === 'chat') return null;
  const layout = useUILayoutStore.getState();
  const conversation = open && visible && (mode !== 'flow' || flowTab === 'responses');
  return <Box data-tour="workspace-panel-tabs" sx={{ display: 'flex', justifyContent: 'flex-end', alignItems: 'center', flexShrink: 0, gap: 0.5, px: 1, minHeight: 40 }}>
    {mode === 'flow' && <Button color="inherit" size="small" aria-pressed={open && flowTab === 'crews'}
      onClick={() => layout.setFlowPanelTab('crews')} sx={{ fontSize: 12, borderRadius: 2 }}>Available crews</Button>}
    {!conversation && <IconButton size="small" aria-label="Show conversation" onClick={() => layout.setAssistantPanelVisible(true)}><MessageSquare size={16} /></IconButton>}
    {open && <>
      <IconButton size="small" aria-label={`Move panel to ${side === 'left' ? 'right' : 'left'}`} onClick={() => layout.setAssistantPanelSide(side === 'left' ? 'right' : 'left')}><ArrowLeftRight size={16} /></IconButton>
      {conversation && <IconButton size="small" aria-label="Full screen conversation" onClick={() => window.dispatchEvent(new Event('expandBuilderConversation'))}><Maximize2 size={16} /></IconButton>}
      <IconButton size="small" aria-label="Close panel" onClick={() => layout.setExecutionHistoryVisible(false)}><X size={17} /></IconButton>
    </>}
  </Box>;
}
