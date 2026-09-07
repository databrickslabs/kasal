import { beforeEach, describe, expect, it, vi } from 'vitest';
import { useTabManagerStore } from '../../store/tabManager';
import { useUILayoutStore } from '../../store/uiLayout';
import { usePermissionStore } from '../../store/permissions';
import { useSessionStore } from '../../features/chat/store/sessionStore';
import { useSessionPreferences } from './sessionPreferences';
import { collectSessions } from './sessionIndex';
import { newWorkspaceSession, openWorkspaceSession, switchWorkspaceMode } from './sessionNavigation';

vi.mock('../../features/chat/persistence/sessionApi', () => ({ getSessionMessages: vi.fn(async () => []), initDb: vi.fn() }));
vi.mock('../../features/chat/store/executionStore', () => ({ useExecutionStore: { getState: () => ({ saveSessionState: vi.fn(), resetForSession: vi.fn(), restoreSessionState: vi.fn() }) } }));

beforeEach(() => {
  localStorage.setItem('selectedGroupId', 'group-a');
  useTabManagerStore.setState({ tabs: [], activeTabId: null });
  useSessionStore.setState({ sessions: [], currentSessionId: null, messages: [] });
  useUILayoutStore.setState({ appMode: 'chat', areFlowsVisible: false });
  usePermissionStore.setState({ allowAgentBuilder: true, allowFlowBuilder: true });
  useSessionPreferences.setState({ entries: {} });
});

describe('common workspace sessions', () => {
  it('switches modes without relabelling or losing either canvas', () => {
    newWorkspaceSession('crew');
    const crew = useTabManagerStore.getState().getActiveTab()!;
    useTabManagerStore.getState().updateTabNodes(crew.id, [{ id: 'agent', position: { x: 20, y: 50 }, data: {} }]);
    switchWorkspaceMode('flow');
    const flow = useTabManagerStore.getState().getActiveTab()!;
    useTabManagerStore.getState().updateTabFlowNodes(flow.id, [{ id: 'crew-node', position: { x: 50, y: 100 }, data: {} }]);
    switchWorkspaceMode('chat'); switchWorkspaceMode('crew');
    expect(useTabManagerStore.getState().getActiveTab()?.id).toBe(crew.id);
    expect(useTabManagerStore.getState().getActiveTab()?.nodes[0].id).toBe('agent');
    switchWorkspaceMode('flow');
    expect(useTabManagerStore.getState().getActiveTab()?.id).toBe(flow.id);
    expect(useTabManagerStore.getState().getActiveTab()?.flowNodes[0].id).toBe('crew-node');
    expect(useTabManagerStore.getState().getTab(flow.id)?.viewMode).toBe('flow');
  });

  it('new builder sessions have independent conversations and keep the old work', () => {
    newWorkspaceSession('crew');
    const first = useTabManagerStore.getState().getActiveTab()!;
    useTabManagerStore.getState().updateTabNodes(first.id, [{ id: 'task', position: { x: 0, y: 0 }, data: {} }]);
    newWorkspaceSession('crew');
    const next = useTabManagerStore.getState().getActiveTab()!;
    expect(next.chatSessionId).not.toBe(first.chatSessionId);
    expect(next.nodes).toEqual([]);
    expect(useTabManagerStore.getState().getTab(first.id)?.nodes).toHaveLength(1);
  });

  it('merges records once and isolates workspaces and restricted modes', () => {
    newWorkspaceSession('crew');
    const tab = useTabManagerStore.getState().getActiveTab()!;
    useTabManagerStore.getState().nameSessionFromPrompt(tab.chatSessionId!, 'A real request');
    const listedTab = useTabManagerStore.getState().getActiveTab()!;
    const chat = { id: 'chat', title: 'Chat', createdAt: new Date(), updatedAt: new Date(), groupId: 'group-a' };
    const chats = [chat, { ...chat, id: tab.chatSessionId! }, { ...chat, id: 'other', groupId: 'group-b' }];
    expect(collectSessions(chats, [listedTab], 'group-a', { crew: true, flow: true }).map(s => s.mode).sort()).toEqual(['chat', 'crew']);
    expect(collectSessions(chats, [listedTab], 'group-a', { crew: false, flow: false }).map(s => s.id)).toEqual(['chat']);
  });

  it('lists a new session only after its first prompt or a canvas edit', () => {
    newWorkspaceSession('flow');
    const tab = useTabManagerStore.getState().getActiveTab()!;
    expect(collectSessions([], [tab], 'group-a', { crew: true, flow: true })).toEqual([]);
    useTabManagerStore.getState().nameSessionFromPrompt(tab.chatSessionId!, 'Research news and make a presentation');
    const titled = useTabManagerStore.getState().getActiveTab()!;
    expect(collectSessions([], [titled], 'group-a', { crew: true, flow: true })[0].title).toBe('Research news and make a presentation');
    useTabManagerStore.getState().nameSessionFromPrompt(tab.chatSessionId!, 'A later request');
    expect(useTabManagerStore.getState().getActiveTab()?.name).toBe('Research news and make a presentation');
    newWorkspaceSession('crew');
    const id = useTabManagerStore.getState().activeTabId!;
    useTabManagerStore.getState().updateTabNodes(id, [{ id: 'a', position: { x: 0, y: 0 }, data: {} }]);
    expect(collectSessions([], useTabManagerStore.getState().tabs, 'group-a', { crew: true, flow: true })).toHaveLength(2);
  });

  it('does not navigate an operator to a builder via a saved session', async () => {
    newWorkspaceSession('crew');
    const tab = useTabManagerStore.getState().getActiveTab()!;
    switchWorkspaceMode('chat');
    usePermissionStore.setState({ allowAgentBuilder: false, allowFlowBuilder: false });
    await openWorkspaceSession({ id: tab.id, key: `builder:${tab.id}`, title: tab.name, mode: 'crew', updatedAt: 0, running: false });
    expect(useUILayoutStore.getState().appMode).toBe('chat');
    newWorkspaceSession('flow');
    expect(useTabManagerStore.getState().tabs).toHaveLength(1);
  });

  it('does not resurrect an archived session when selecting its mode', () => {
    newWorkspaceSession('flow');
    const first = useTabManagerStore.getState().activeTabId!;
    useSessionPreferences.getState().update(`builder:${first}`, { archived: true });
    switchWorkspaceMode('chat'); switchWorkspaceMode('flow');
    expect(useTabManagerStore.getState().activeTabId).not.toBe(first);
    expect(useTabManagerStore.getState().getTab(first)).toBeTruthy();
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

  it('keeps persisted canvases and the selected session across hydration', async () => {
    newWorkspaceSession('flow');
    const id = useTabManagerStore.getState().activeTabId!;
    useTabManagerStore.getState().updateTabFlowNodes(id, [{ id: 'crew', position: { x: 30, y: 80 }, data: {} }]);
    const saved = localStorage.getItem('tab-manager-storage')!;
    useTabManagerStore.setState({ tabs: [], activeTabId: null });
    localStorage.setItem('tab-manager-storage', saved);
    await useTabManagerStore.persist.rehydrate();
    expect(useTabManagerStore.getState().activeTabId).toBe(id);
    expect(useTabManagerStore.getState().getActiveTab()?.flowNodes[0].position).toEqual({ x: 30, y: 80 });
  });
});

it('opens a new agent session with conversation above the dock even after a hidden flow pane', () => {
  useUILayoutStore.setState({ appMode: 'flow', areFlowsVisible: true, assistantPanelVisible: false, executionHistoryVisible: false, assistantResponseFocused: false });
  newWorkspaceSession('crew');
  expect(useUILayoutStore.getState()).toMatchObject({ appMode: 'crew', assistantPanelVisible: true, executionHistoryVisible: true, assistantResponseFocused: true });
});
