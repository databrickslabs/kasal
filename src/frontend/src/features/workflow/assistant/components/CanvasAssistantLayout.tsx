import { kasalStageSurface } from '../../../../theme/kasalSurfaces';
import React, { useEffect, useLayoutEffect, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import { Box } from '@mui/material';
import ExpandedBuilderConversation from './ExpandedBuilderConversation';
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
  onHide?: () => void;
}

export function CanvasAssistantLayout({ composer, response, responseKey, sessionKey, hasMessages, busy, dark }: Props) {
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
  useEffect(() => { setPreview(null); setFullscreen(false); }, [sessionKey]);
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
  const openResult = (content: PreviewContent) => {
    setPreview({ content, sessionKey });
    useUILayoutStore.getState().setAssistantResponseFocused(true);
    showResponse();
  };
  useEffect(() => {
    const expand = () => setFullscreen(true);
    const collapse = () => setFullscreen(false);
    window.addEventListener('expandBuilderConversation', expand);
    window.addEventListener('collapseBuilderConversation', collapse);
    return () => {
      window.removeEventListener('expandBuilderConversation', expand);
      window.removeEventListener('collapseBuilderConversation', collapse);
    };
  }, []);
  const returnToSidebar = () => {
    setFullscreen(false);
    requestAnimationFrame(() => document.querySelector<HTMLButtonElement>('[aria-label="Full screen conversation"]')?.focus());
  };
  useEffect(() => { if (!visible) setFullscreen(false); }, [visible]);
  const [host, setHost] = useState<HTMLElement | null>(null);
  const dockRef = useRef<HTMLDivElement>(null);
  const showResponse = () => useUILayoutStore.getState().setAssistantPanelVisible(true);

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

  const panel = <Box role="region" aria-label="Conversation" sx={{ display: 'flex', flexDirection: 'column', height: '100%', minHeight: 0, background: 'transparent', color: 'text.primary', ...(fullscreen || focused ? { flex: 1, height: 'auto', maxWidth: 1000, width: '100%', mx: 'auto', pt: fullscreen ? 2 : 0, pb: 1, boxSizing: 'border-box' } : {}) }}>
    <Box sx={{ flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>{response}</Box>
  </Box>;

  const focusComposer = <Box>    <Box sx={{ width: '100%', maxWidth: 760, mx: 'auto', px: 1.5, pb: 1.5, flexShrink: 0, boxSizing: 'border-box' }}>
      <Box ref={(node: HTMLDivElement | null) => { if (node && !fullscreen && composerHost.parentElement !== node) node.appendChild(composerHost); }} />
    </Box>
  </Box>;

  const sidePreview = activePreview && <Box role="region" aria-label={activePreview.content.type === 'memory' ? 'Run memory preview' : activePreview.content.type === 'ui' ? 'Result preview' : 'Run activity preview'} className="kasal-chat-root" data-theme={dark ? 'dark' : 'light'}
    sx={{ display: 'flex', height: '100%', width: '100%', minWidth: 0, minHeight: 0, overflow: 'hidden', pointerEvents: 'auto', color: 'text.primary', '& > aside': { minWidth: 0 },
      '& button': { border: 0, backgroundColor: 'transparent', color: 'inherit', cursor: 'pointer' } }}>
    <PreviewPanel content={activePreview.content} focusStep={activePreview.step} chatCollapsed={false} onClose={() => setPreview(null)} />
  </Box>;

  return <BuilderPreviewContext.Provider value={{ openMemory, openStep, openResult, previewMessageId: activePreview?.content.sourceMessageId, closePreview: () => setPreview(null) }}>
    {createPortal(composer, composerHost)}
    {visible && !fullscreen && host && createPortal(panel, host)}
    {focused && !fullscreen && composerTarget && createPortal(focusComposer, composerTarget)}
    {visible && focused && !fullscreen && sidePreview && previewHost && createPortal(
      <Box sx={{ position: 'absolute', inset: 0, ...kasalStageSurface(dark), overflow: 'hidden', borderRadius: 3, pointerEvents: 'auto' }}>{sidePreview}</Box>, previewHost)}
    {fullscreen && visible && <ExpandedBuilderConversation dark={dark} landing={!hasMessages && !busy} response={panel} preview={sidePreview} composerHost={composerHost} onClose={returnToSidebar} />}
    <Box ref={dockRef} data-testid="canvas-assistant-dock" sx={{ position: 'absolute', bottom: 0, left: '50%', transform: 'translateX(-50%)', width: 'min(600px, 100%)', pointerEvents: 'none', display: focused ? 'none' : 'block' }}>
      <Box sx={{ pointerEvents: 'auto' }} ref={(node: HTMLDivElement | null) => { if (node && !fullscreen && !focused && composerHost.parentElement !== node) node.appendChild(composerHost); }} />
    </Box>
  </BuilderPreviewContext.Provider>;
}
