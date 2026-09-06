import { kasalStageSurface } from '../../../../theme/kasalSurfaces';
import React, { useEffect, useLayoutEffect, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import { Box, Button, Dialog, IconButton, Tooltip, Typography } from '@mui/material';
import { Maximize2, Minimize2, MessageSquare, SquarePen } from 'lucide-react';
import { useUILayoutStore } from '../../../../store/uiLayout';
import type { PreviewContent } from '../../../chat/types/preview';
import type { RunStep } from '../../../chat/components/Preview/traceEventStep';
import PreviewPanel from '../../../chat/components/Preview/PreviewPanel';
import { BuilderPreviewContext } from './BuilderPreviewContext';
import '../../../chat/chat.css';

interface Props {
  composer: React.ReactNode;
  response: React.ReactNode;
  responseKey?: string;
  sessionKey?: string;
  hasMessages: boolean;
  busy: boolean;
  dark: boolean;
  onNewChat: () => void;
  onHide?: () => void;
}

export function CanvasAssistantLayout({ composer, response, responseKey, sessionKey, hasMessages, busy, dark, onNewChat, onHide }: Props) {
  const visible = useUILayoutStore(state => state.assistantPanelVisible && (!state.areFlowsVisible || state.flowPanelTab === 'responses'));
  const side = useUILayoutStore(state => state.assistantPanelSide);
  const paneOpen = useUILayoutStore(state => state.executionHistoryVisible);
  const focused = useUILayoutStore(state => state.assistantResponseFocused) && paneOpen;
  const [composerTarget, setComposerTarget] = useState<HTMLElement | null>(null);
  // Keep one composer mounted while moving its DOM between the dock and reader.
  const [composerHost] = useState(() => document.createElement('div'));
  const [fullscreen, setFullscreen] = useState(false);
  const [preview, setPreview] = useState<{ content: PreviewContent; step?: RunStep; sessionKey?: string } | null>(null);
  const activePreview = preview?.sessionKey === sessionKey ? preview : null;
  const [previewHost, setPreviewHost] = useState<HTMLElement | null>(null);
  useEffect(() => { setPreview(null); }, [sessionKey]);
  const openMemory = (jobId: string) => {
    setPreview({ content: { type: 'memory', data: jobId, title: 'Run memory' }, sessionKey });
    useUILayoutStore.getState().setAssistantResponseFocused(true);
    showResponse();
  };
  const openStep = (jobId: string, step: RunStep) => {
    setPreview({ content: { type: 'text', data: step.detail || '', title: 'Run activity', sourceMessageId: jobId }, step, sessionKey });
    useUILayoutStore.getState().setAssistantResponseFocused(true);
    showResponse();
  };
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

  const previousResponse = useRef(responseKey);
  useEffect(() => {
    if (busy || (responseKey && (responseKey !== previousResponse.current || !useUILayoutStore.getState().areFlowsVisible))) showResponse();
    previousResponse.current = responseKey;
  }, [responseKey, busy]);
  useLayoutEffect(() => {
    setHost(document.getElementById('builder-assistant-response-host'));
    setComposerTarget(document.getElementById('builder-assistant-composer-host'));
    setPreviewHost(document.getElementById('builder-assistant-preview-host'));
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

  const panel = <Box role="region" aria-label="Conversation" sx={{ display: 'flex', flexDirection: 'column', height: '100%', minHeight: 0, background: 'transparent', color: 'text.primary', ...(fullscreen || focused ? { flex: 1, height: 'auto', maxWidth: 1000, width: '100%', mx: 'auto', py: { xs: 2, sm: 3 }, boxSizing: 'border-box' } : {}) }}>
    <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', px: 1.5, pb: 1, gap: 1 }}>
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5, ml: 'auto' }}>
      {fullscreen
        ? <Button color="inherit" size="small" startIcon={<Minimize2 size={15} />} onClick={returnToSidebar} sx={{ fontSize: 12 }}>Back to canvas</Button>
        : <Tooltip title="Full screen"><IconButton ref={fullscreenTrigger} aria-label="Full screen conversation" size="small" onClick={() => setFullscreen(true)}><Maximize2 size={14} /></IconButton></Tooltip>}
      </Box>
    </Box>
    <Box sx={{ flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>{response}</Box>
  </Box>;

  const toolbar = (
      <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'flex-end', gap: 0.25, mb: 0.5, pointerEvents: 'auto', width: 'fit-content', ml: 'auto', bgcolor: 'transparent', borderRadius: 2, px: 0.5 }}>
        {!focused && <Button ref={restoreRef} color="inherit" size="small" startIcon={<MessageSquare size={14} />} onClick={() => visible ? closeResponse() : showResponse()} sx={{ fontSize: 11, px: 1 }}>{visible ? 'Hide conversation' : 'Conversation'}</Button>}
        <Tooltip title="New Chat"><span><IconButton aria-label="New Chat" size="small" disabled={busy} onClick={onNewChat}><SquarePen size={15} /></IconButton></span></Tooltip>
      </Box>
  );
  const focusComposer = <Box>    <Box sx={{ width: '100%', maxWidth: 760, mx: 'auto', px: 1.5, pb: 1.5, flexShrink: 0, boxSizing: 'border-box' }}>
      {toolbar}
      <Box ref={(node: HTMLDivElement | null) => { if (node && !fullscreen && composerHost.parentElement !== node) node.appendChild(composerHost); }} />
    </Box>
  </Box>;

  const sidePreview = activePreview && <Box role="region" aria-label={activePreview.content.type === 'memory' ? 'Run memory preview' : 'Run activity preview'} className="kasal-chat-root" data-theme={dark ? 'dark' : 'light'}
    sx={{ display: 'flex', height: '100%', width: '100%', minWidth: 0, minHeight: 0, overflow: 'hidden', pointerEvents: 'auto', color: 'text.primary', '& > aside': { minWidth: 0 } }}>
    <PreviewPanel content={activePreview.content} focusStep={activePreview.step} chatCollapsed={false} onClose={() => setPreview(null)} />
  </Box>;

  return <BuilderPreviewContext.Provider value={{ openMemory, openStep }}>
    {createPortal(composer, composerHost)}
    {visible && !fullscreen && host && createPortal(panel, host)}
    {focused && !fullscreen && composerTarget && createPortal(focusComposer, composerTarget)}
    {visible && focused && !fullscreen && sidePreview && previewHost && createPortal(
      <Box sx={{ position: 'absolute', inset: 0, ...kasalStageSurface(dark), overflow: 'hidden', borderRadius: 3, pointerEvents: 'auto' }}>{sidePreview}</Box>, previewHost)}
    <Dialog fullScreen open={fullscreen && visible} onClose={returnToSidebar} PaperProps={{ 'aria-label': 'Full screen conversation', sx: { ...kasalStageSurface(dark), backgroundAttachment: 'scroll', backgroundSize: '100% 100%', backgroundPosition: 'center', border: 0, boxShadow: 'none' } }}>
      {fullscreen && <Box sx={{ display: 'flex', flexDirection: { xs: 'column', md: side === 'left' ? 'row' : 'row-reverse' }, flex: 1, minHeight: 0 }}>
        <Box sx={{ display: 'flex', flexDirection: 'column', flex: 1, minWidth: 0, minHeight: 0 }}>
        {panel}
        <Box sx={{ width: '100%', maxWidth: 760, mx: 'auto', px: { xs: 2, sm: 3 }, pb: { xs: 2, sm: 3 }, flexShrink: 0, boxSizing: 'border-box' }} ref={(node: HTMLDivElement | null) => { if (node && composerHost.parentElement !== node) node.appendChild(composerHost); }} />
        </Box>
        {sidePreview && <Box sx={{ flex: 1, minWidth: 0, minHeight: 0 }}>{sidePreview}</Box>}
      </Box>}
    </Dialog>
    <Box ref={dockRef} data-testid="canvas-assistant-dock" sx={{ position: 'absolute', bottom: 0, left: '50%', transform: 'translateX(-50%)', width: 'min(600px, 100%)', pointerEvents: 'none', display: focused ? 'none' : 'block' }}>
      {!focused && toolbar}
      <Box sx={{ pointerEvents: 'auto' }} ref={(node: HTMLDivElement | null) => { if (node && !fullscreen && !focused && composerHost.parentElement !== node) node.appendChild(composerHost); }} />
    </Box>
  </BuilderPreviewContext.Provider>;
}
