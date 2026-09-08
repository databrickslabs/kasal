import { beforeEach, describe, expect, it, vi } from 'vitest';
import { useBuilderCanvasStore } from './builderCanvasStore';
import { useUILayoutStore } from '../../store/uiLayout';
import { usePermissionStore } from '../../store/permissions';
import { useSessionStore } from './sessionStore';
import { useSessionPreferences } from './sessionPreferences';
import { collectSessions } from './sessionIndex';
import { newWorkspaceSession, openWorkspaceSession, switchWorkspaceMode } from './sessionNavigation';

vi.mock('../../features/chat/persistence/sessionApi', () => ({ getSessionMessages: vi.fn(async () => []), initDb: vi.fn() }));
vi.mock('../../features/chat/store/executionStore', () => ({ useExecutionStore: { getState: () => ({ saveSessionState: vi.fn(), resetForSession: vi.fn(), restoreSessionState: vi.fn() }) } }));

beforeEach(() => {
  localStorage.setItem('selectedGroupId', 'group-a');
  useBuilderCanvasStore.setState({ canvases: [], activeCanvasId: null });
  useSessionStore.setState({ sessions: [], currentSessionId: null, messages: [] });
  useUILayoutStore.setState({ appMode: 'chat', areFlowsVisible: false });
  usePermissionStore.setState({ allowAgentBuilder: true, allowFlowBuilder: true });
  useSessionPreferences.setState({ entries: {} });
});

describe('common workspace sessions', () => {
  it('switches modes without relabelling or losing either canvas', () => {
    newWorkspaceSession('crew');
    const crew = useBuilderCanvasStore.getState().getActiveCanvas()!;
    useBuilderCanvasStore.getState().updateCanvasNodes(crew.id, [{ id: 'agent', position: { x: 20, y: 50 }, data: {} }]);
    switchWorkspaceMode('flow');
    const flow = useBuilderCanvasStore.getState().getActiveCanvas()!;
    useBuilderCanvasStore.getState().updateCanvasFlowNodes(flow.id, [{ id: 'crew-node', position: { x: 50, y: 100 }, data: {} }]);
    switchWorkspaceMode('chat'); switchWorkspaceMode('crew');
    expect(useBuilderCanvasStore.getState().getActiveCanvas()?.id).toBe(crew.id);
    expect(useBuilderCanvasStore.getState().getActiveCanvas()?.nodes[0].id).toBe('agent');
    switchWorkspaceMode('flow');
    expect(useBuilderCanvasStore.getState().getActiveCanvas()?.id).toBe(flow.id);
    expect(useBuilderCanvasStore.getState().getActiveCanvas()?.flowNodes[0].id).toBe('crew-node');
    expect(useBuilderCanvasStore.getState().getCanvas(flow.id)?.viewMode).toBe('flow');
  });

  it('new builder sessions have independent conversations and keep the old work', () => {
    newWorkspaceSession('crew');
    const first = useBuilderCanvasStore.getState().getActiveCanvas()!;
    useBuilderCanvasStore.getState().updateCanvasNodes(first.id, [{ id: 'task', position: { x: 0, y: 0 }, data: {} }]);
    newWorkspaceSession('crew');
    const next = useBuilderCanvasStore.getState().getActiveCanvas()!;
    expect(next.chatSessionId).not.toBe(first.chatSessionId);
    expect(next.nodes).toEqual([]);
    expect(useBuilderCanvasStore.getState().getCanvas(first.id)?.nodes).toHaveLength(1);
  });

  it('merges records once and isolates workspaces and restricted modes', () => {
    newWorkspaceSession('crew');
    const tab = useBuilderCanvasStore.getState().getActiveCanvas()!;
    useBuilderCanvasStore.getState().nameSessionFromPrompt(tab.chatSessionId!, 'A real request');
    const listedTab = useBuilderCanvasStore.getState().getActiveCanvas()!;
    const chat = { id: 'chat', title: 'Chat', createdAt: new Date(), updatedAt: new Date(), groupId: 'group-a' };
    const chats = [chat, { ...chat, id: tab.chatSessionId! }, { ...chat, id: 'other', groupId: 'group-b' }];
    expect(collectSessions(chats, [listedTab], 'group-a', { crew: true, flow: true }).map(s => s.mode).sort()).toEqual(['chat', 'crew']);
    expect(collectSessions(chats, [listedTab], 'group-a', { crew: false, flow: false }).map(s => s.id)).toEqual(['chat']);
  });

  it('lists a new session only after its first prompt or a canvas edit', () => {
    newWorkspaceSession('flow');
    const tab = useBuilderCanvasStore.getState().getActiveCanvas()!;
    expect(collectSessions([], [tab], 'group-a', { crew: true, flow: true })).toEqual([]);
    useBuilderCanvasStore.getState().nameSessionFromPrompt(tab.chatSessionId!, 'Research news and make a presentation');
    const titled = useBuilderCanvasStore.getState().getActiveCanvas()!;
    expect(collectSessions([], [titled], 'group-a', { crew: true, flow: true })[0].title).toBe('Research news and make a presentation');
    useBuilderCanvasStore.getState().nameSessionFromPrompt(tab.chatSessionId!, 'A later request');
    expect(useBuilderCanvasStore.getState().getActiveCanvas()?.name).toBe('Research news and make a presentation');
    newWorkspaceSession('crew');
    const id = useBuilderCanvasStore.getState().activeCanvasId!;
    useBuilderCanvasStore.getState().updateCanvasNodes(id, [{ id: 'a', position: { x: 0, y: 0 }, data: {} }]);
    expect(collectSessions([], useBuilderCanvasStore.getState().canvases, 'group-a', { crew: true, flow: true })).toHaveLength(2);
  });

  it('does not navigate an operator to a builder via a saved session', async () => {
    newWorkspaceSession('crew');
    const tab = useBuilderCanvasStore.getState().getActiveCanvas()!;
    switchWorkspaceMode('chat');
    usePermissionStore.setState({ allowAgentBuilder: false, allowFlowBuilder: false });
    await openWorkspaceSession({ id: tab.id, key: `builder:${tab.id}`, title: tab.name, mode: 'crew', updatedAt: 0, running: false });
    expect(useUILayoutStore.getState().appMode).toBe('chat');
    newWorkspaceSession('flow');
    expect(useBuilderCanvasStore.getState().canvases).toHaveLength(1);
  });

  it('does not resurrect an archived session when selecting its mode', () => {
    newWorkspaceSession('flow');
    const first = useBuilderCanvasStore.getState().activeCanvasId!;
    useSessionPreferences.getState().update(`builder:${first}`, { archived: true });
    switchWorkspaceMode('chat'); switchWorkspaceMode('flow');
    expect(useBuilderCanvasStore.getState().activeCanvasId).not.toBe(first);
    expect(useBuilderCanvasStore.getState().getCanvas(first)).toBeTruthy();
  });

  it('a delayed Chat response cannot navigate away from a newer builder selection', async () => {
    const api = await import('../../features/chat/persistence/sessionApi');
    let resolve!: (value: []) => void;
    vi.mocked(api.getSessionMessages).mockImplementationOnce(() => new Promise(done => { resolve = done; }));
    const pending = openWorkspaceSession({ id: 'slow-chat', key: 'chat:slow-chat', title: 'Slow', mode: 'chat', updatedAt: 0, running: false });
    newWorkspaceSession('flow');
    resolve([]); await pending;
    expect(useUILayoutStore.getState().appMode).toBe('flow');
    expect(useSessionStore.getState().currentSessionId).not.toBe('slow-chat');
  });


});

it('opens a new agent session with conversation above the dock even after a hidden flow pane', () => {
  useUILayoutStore.setState({ appMode: 'flow', areFlowsVisible: true, assistantPanelVisible: false, executionHistoryVisible: false, assistantResponseFocused: false });
  newWorkspaceSession('crew');
  expect(useUILayoutStore.getState()).toMatchObject({ appMode: 'crew', assistantPanelVisible: true, executionHistoryVisible: true, assistantResponseFocused: true });
});
