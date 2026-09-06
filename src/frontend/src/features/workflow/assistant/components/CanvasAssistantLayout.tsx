import { kasalStageSurface } from '../../../../theme/kasalSurfaces';
import React, { useEffect, useLayoutEffect, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import { Box, Button, Dialog, IconButton, Tooltip, Typography } from '@mui/material';
import { Maximize2, Minimize2, MessageSquare, SquarePen } from 'lucide-react';
import { useUILayoutStore } from '../../../../store/uiLayout';

interface Props {
  composer: React.ReactNode;
  response: React.ReactNode;
  responseKey?: string;
  hasMessages: boolean;
  busy: boolean;
  dark: boolean;
  onNewChat: () => void;
  showEarlier: boolean;
  onToggleEarlier: () => void;
  onHide?: () => void;
}

export function CanvasAssistantLayout({ composer, response, responseKey, hasMessages, busy, dark, onNewChat, showEarlier, onToggleEarlier, onHide }: Props) {
  const visible = useUILayoutStore(state => state.assistantPanelVisible);
  const side = useUILayoutStore(state => state.assistantPanelSide);
  const paneOpen = useUILayoutStore(state => state.executionHistoryVisible);
  const focused = useUILayoutStore(state => state.assistantResponseFocused) && paneOpen;
  const [composerTarget, setComposerTarget] = useState<HTMLElement | null>(null);
  // Keep one composer mounted while moving its DOM between the dock and reader.
  const [composerHost] = useState(() => document.createElement('div'));
  const [fullscreen, setFullscreen] = useState(false);
  const fullscreenTrigger = useRef<HTMLButtonElement>(null);
  const returnToSidebar = () => {
    setFullscreen(false);
    requestAnimationFrame(() => fullscreenTrigger.current?.focus());
  };
  useEffect(() => { if (!visible) setFullscreen(false); }, [visible]);
  const [host, setHost] = useState<HTMLElement | null>(null);
  const dockRef = useRef<HTMLDivElement>(null);
  const restoreRef = useRef<HTMLButtonElement>(null);
  const showResponse = () => useUILayoutStore.getState().setAssistantPanelVisible(true);
  const closeResponse = () => {
    useUILayoutStore.getState().setAssistantPanelVisible(false);
    requestAnimationFrame(() => restoreRef.current?.focus());
  };

  useEffect(() => { if (responseKey || busy) showResponse(); }, [responseKey, busy]);
  useLayoutEffect(() => {
    setHost(document.getElementById('builder-assistant-response-host'));
    setComposerTarget(document.getElementById('builder-assistant-composer-host'));
  }, [visible, side, focused, paneOpen]);
  useEffect(() => {
    if (!dockRef.current) return;
    const measure = () => {
      const height = Math.ceil(dockRef.current?.getBoundingClientRect().height || 0) + 24;
      const state = useUILayoutStore.getState();
      if (state.assistantDockHeight !== height) state.setAssistantDockHeight(height);
    };
    measure();
    const observer = new ResizeObserver(measure);
    observer.observe(dockRef.current);
    return () => observer.disconnect();
  }, []);
  useEffect(() => () => { useUILayoutStore.getState().setAssistantDockHeight(0); }, []);

  const panel = <Box role="region" aria-label="Kasal responses" sx={{ display: 'flex', flexDirection: 'column', height: '100%', minHeight: 0, background: 'transparent', color: 'text.primary', ...(fullscreen || focused ? { flex: 1, height: 'auto', maxWidth: 1000, width: '100%', mx: 'auto', py: { xs: 2, sm: 3 }, boxSizing: 'border-box' } : {}) }}>
    <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', px: 1.5, pb: 1, gap: 1 }}>
      <Typography sx={{ fontSize: 11, color: 'text.secondary' }}>{busy ? 'Working…' : 'Latest response'}</Typography>
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
      {hasMessages && <Button color="inherit" size="small" onClick={onToggleEarlier} sx={{ fontSize: 11, px: 0.5, minWidth: 0, color: 'text.secondary' }}>{showEarlier ? 'Latest only' : 'Conversation'}</Button>}
      {fullscreen
        ? <Button color="inherit" size="small" startIcon={<Minimize2 size={15} />} onClick={returnToSidebar} sx={{ fontSize: 12 }}>Back to canvas</Button>
        : <Tooltip title="Full screen"><IconButton ref={fullscreenTrigger} aria-label="Full screen responses" size="small" onClick={() => setFullscreen(true)}><Maximize2 size={14} /></IconButton></Tooltip>}
      </Box>
    </Box>
    <Box sx={{ flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>{response}</Box>
  </Box>;

  const toolbar = (
      <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'flex-end', gap: 0.25, mb: 0.5, pointerEvents: 'auto', width: 'fit-content', ml: 'auto', bgcolor: 'transparent', borderRadius: 2, px: 0.5 }}>
        {!focused && <Button ref={restoreRef} color="inherit" size="small" startIcon={<MessageSquare size={14} />} onClick={() => visible ? closeResponse() : showResponse()} sx={{ fontSize: 11, px: 1 }}>{visible ? 'Hide response' : 'Responses'}</Button>}
        <Tooltip title="New Chat"><span><IconButton aria-label="New Chat" size="small" disabled={busy} onClick={onNewChat}><SquarePen size={15} /></IconButton></span></Tooltip>
      </Box>
  );
  const focusComposer = <Box>    <Box sx={{ width: '100%', maxWidth: 760, mx: 'auto', px: 1.5, pb: 1.5, flexShrink: 0, boxSizing: 'border-box' }}>
      {toolbar}
      <Box ref={(node: HTMLDivElement | null) => { if (node && !fullscreen && composerHost.parentElement !== node) node.appendChild(composerHost); }} />
    </Box>
  </Box>;

  return <>
    {createPortal(composer, composerHost)}
    {visible && !fullscreen && host && createPortal(panel, host)}
    {focused && !fullscreen && composerTarget && createPortal(focusComposer, composerTarget)}
    <Dialog fullScreen open={fullscreen && visible} onClose={returnToSidebar} PaperProps={{ 'aria-label': 'Full screen responses', sx: { ...kasalStageSurface(dark), backgroundAttachment: 'scroll', backgroundSize: '100% 100%', backgroundPosition: 'center', border: 0, boxShadow: 'none' } }}>
      {fullscreen && <>
        {panel}
        <Box sx={{ width: '100%', maxWidth: 760, mx: 'auto', px: { xs: 2, sm: 3 }, pb: { xs: 2, sm: 3 }, flexShrink: 0, boxSizing: 'border-box' }} ref={(node: HTMLDivElement | null) => { if (node && composerHost.parentElement !== node) node.appendChild(composerHost); }} />
      </>}
    </Dialog>
    <Box ref={dockRef} data-testid="canvas-assistant-dock" sx={{ position: 'absolute', bottom: 0, left: '50%', transform: 'translateX(-50%)', width: 'min(600px, 100%)', pointerEvents: 'none', display: focused ? 'none' : 'block' }}>
      {!focused && toolbar}
      <Box sx={{ pointerEvents: 'auto' }} ref={(node: HTMLDivElement | null) => { if (node && !fullscreen && !focused && composerHost.parentElement !== node) node.appendChild(composerHost); }} />
    </Box>
  </>;
}
