import React, { useCallback, useEffect, useLayoutEffect, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import { Box } from '@mui/material';
import ExpandedBuilderConversation from './ExpandedBuilderConversation';
import { useUILayoutStore } from '../../../../store/uiLayout';
import type { PreviewContent } from '../../../chat/types/preview';
import type { RunStep } from '../../../chat/components/Preview/traceEventStep';
import BuilderSidePane, { type BuilderPaneTab, type BuilderPaneId } from './BuilderSidePane';
import { BuilderPreviewContext } from './BuilderPreviewContext';
import { useBuilderNodeEditorBridge, type BuilderNodeEditorEntry } from '../store/builderNodeEditorBridge';
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
  const [views, setViews] = useState<{ sessionKey?: string; tabs: BuilderPaneTab[]; active: BuilderPaneId }>({ sessionKey, tabs: [], active: 'canvas' });
  const tabs = views.sessionKey === sessionKey ? views.tabs : [];
  const activeTab = views.sessionKey === sessionKey ? views.active : 'canvas';
  const activePreview = tabs.find(tab => tab.id === activeTab);
  const [previewHost, setPreviewHost] = useState<HTMLElement | null>(null);
  useEffect(() => { setViews({ sessionKey, tabs: [], active: 'canvas' }); setFullscreen(false); }, [sessionKey]);
  const openTab = useCallback((tab: BuilderPaneTab) => {
    setViews(previous => {
      const current = previous.sessionKey === sessionKey ? previous.tabs : [];
      return { sessionKey, active: tab.id, tabs: current.some(item => item.id === tab.id)
        ? current.map(item => item.id === tab.id ? tab : item) : [...current, tab] };
    });
    useUILayoutStore.getState().setAssistantResponseFocused(true);
    useUILayoutStore.getState().setAssistantPanelVisible(true);
  }, [sessionKey]);
  const releaseTab = useCallback((id: BuilderPaneId) => setViews(previous => ({ ...previous,
    tabs: previous.tabs.filter(tab => tab.id !== id), active: previous.active === id ? 'canvas' : previous.active,
  })), []);
  const closeCanvas = (id: BuilderPaneId) => {
    const tab = tabs.find(item => item.id === id);
    if (tab && 'editor' in tab) tab.editor.onClose();
    releaseTab(id);
  };
  useLayoutEffect(() => {
    const editors = new Map<BuilderNodeEditorEntry['id'], BuilderNodeEditorEntry>();
    const open = (editor: BuilderNodeEditorEntry) => { editors.set(editor.id, editor); openTab({ id: editor.id, editor }); };
    const release = (id: BuilderNodeEditorEntry['id']) => { editors.delete(id); releaseTab(id); };
    useBuilderNodeEditorBridge.setState({ open, release });
    return () => {
      editors.forEach(editor => editor.onClose());
      if (useBuilderNodeEditorBridge.getState().open === open) useBuilderNodeEditorBridge.setState({ open: null, release: null });
    };
  }, [openTab, releaseTab]);
  const selectTab = (id: BuilderPaneId) => {
    setViews(previous => ({ ...previous, active: id }));
    if (id === 'canvas') setFullscreen(false);
  };
  const openMemory = (jobId: string) => openTab({ id: 'memory', content: { type: 'memory', data: jobId, title: 'Run memory' } });
  const openStep = (jobId: string, step: RunStep) => openTab({ id: 'activity', content: { type: 'text', data: step.detail || '', title: 'Run activity', sourceMessageId: jobId }, step });
  const openResult = (content: PreviewContent) => openTab({ id: 'result', content });
  const openSchedule = (executionId: string, defaultName: string, onCreated: (name: string) => void) => openTab({ id: 'schedule', executionId, defaultName, onCreated });
  const openOptimize = (crewId: string, crewName: string) => openTab({ id: 'optimize', crewId, crewName });
  const openApproval = (jobId: string, approvalId: number, onDecision: () => void) => openTab({ id: 'approval', jobId, approvalId, onDecision });
  const openCheckpoints = (jobId: string, onResumed: (newJobId: string) => void) => openTab({ id: 'checkpoints', jobId, onResumed });
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

  // Match the transcript to the composer, including the tutorial clearance on
  // the right. Full screen uses Chat's 768px column with 16px side gutters.
  const columnSx = { width: '100%', maxWidth: fullscreen ? 768 : 760, minWidth: 0, mx: 'auto',
    pl: fullscreen ? 2 : 1.5, pr: fullscreen ? 2 : side === 'right' ? 6.5 : 1.5, boxSizing: 'border-box' as const };
  const panel = <Box role="region" aria-label="Conversation" sx={{ display: 'flex', flexDirection: 'column', height: '100%', minHeight: 0, minWidth: 0, background: 'transparent', color: 'text.primary', overflowWrap: 'anywhere', ...(fullscreen || focused ? {
    ...columnSx, flex: 1, height: 'auto', pt: fullscreen ? 2 : 0, pb: 1,
    '& [data-testid="builder-conversation-scroll"]': { px: 0, boxSizing: 'border-box' },
  } : {}) }}>
    <Box sx={{ flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>{response}</Box>
  </Box>;

  const focusComposer = <Box>    <Box sx={{ ...columnSx, pb: 1.5, flexShrink: 0 }}>
      <Box ref={(node: HTMLDivElement | null) => { if (node && !fullscreen && composerHost.parentElement !== node) node.appendChild(composerHost); }} />
    </Box>
  </Box>;

  const sidePreview = tabs.length > 0 && (!fullscreen || activeTab !== 'canvas') && <BuilderSidePane tabs={tabs} active={activeTab} onSelect={selectTab} onClose={closeCanvas}
    dark={dark} canvasHost={fullscreen ? null : previewHost} />;

  return <BuilderPreviewContext.Provider value={{ openMemory, openStep, openResult, openSchedule, openOptimize, openCheckpoints, openApproval,
    previewMessageId: activePreview && 'content' in activePreview ? activePreview.content.sourceMessageId : undefined,
    closePreview: () => selectTab('canvas') }}>

    {createPortal(composer, composerHost)}
    {visible && !fullscreen && host && createPortal(panel, host)}
    {focused && !fullscreen && composerTarget && createPortal(focusComposer, composerTarget)}
    {visible && focused && !fullscreen && sidePreview && previewHost && createPortal(
      <Box sx={{ position: 'absolute', inset: 0, overflow: 'hidden', pointerEvents: 'none' }}>{sidePreview}</Box>, previewHost)}
    {fullscreen && visible && <ExpandedBuilderConversation dark={dark} landing={!hasMessages && !busy} response={panel} preview={sidePreview} composerHost={composerHost} onClose={returnToSidebar} />}
    <Box ref={dockRef} data-testid="canvas-assistant-dock" sx={{ position: 'absolute', bottom: 0, left: '50%', transform: 'translateX(-50%)', width: 'min(600px, 100%)', pointerEvents: 'none', display: focused ? 'none' : 'block' }}>
      <Box sx={{ pointerEvents: 'auto' }} ref={(node: HTMLDivElement | null) => { if (node && !fullscreen && !focused && composerHost.parentElement !== node) node.appendChild(composerHost); }} />
    </Box>
  </BuilderPreviewContext.Provider>;
}
