import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import SessionSidebar from './SessionSidebar';
import { useBuilderCanvasStore } from './builderCanvasStore';
import { useUILayoutStore } from '../../store/uiLayout';
import { usePermissionStore } from '../../store/permissions';
import { useSessionStore } from './sessionStore';
import { useAppStore } from '../../features/chat/store/appStore';
import { useSessionPreferences } from './sessionPreferences';
import { deleteSession } from '../../features/chat/persistence/sessionApi';
import { useChatMessagesStore } from '../../features/workflow/assistant/store/chatMessagesStore';

vi.mock('./useWorkspaceSessions', () => ({ useWorkspaceSessions: () => ({ groupId: 'g', loadError: false }) }));
vi.mock('../../components/SidebarAccountActions', () => ({ default: () => null }));
vi.mock('../../features/chat/persistence/sessionApi', () => ({ getSessionMessages: vi.fn(async () => []), initDb: vi.fn(), deleteSession: vi.fn() }));
vi.mock('../../features/chat/store/executionStore', () => ({
  useExecutionStore: Object.assign((selector: (state: unknown) => unknown) => selector({ hasActiveExecution: () => false }), {
    getState: () => ({ saveSessionState: vi.fn(), resetForSession: vi.fn(), restoreSessionState: vi.fn(), hasActiveExecution: () => false }),
  }),
}));

beforeEach(() => {
  vi.mocked(deleteSession).mockReset().mockResolvedValue(undefined);
  localStorage.setItem('selectedGroupId', 'g');
  useAppStore.setState({ sidebarOpen: true });
  usePermissionStore.setState({ allowAgentBuilder: true, allowFlowBuilder: true });
  useUILayoutStore.setState({ appMode: 'crew', areFlowsVisible: false });
  useSessionPreferences.setState({ entries: {} });
  useBuilderCanvasStore.setState({ canvases: [], activeCanvasId: null });
  useBuilderCanvasStore.getState().createCanvas('Research crew', 'crew');
  useBuilderCanvasStore.getState().createCanvas('Reporting flow', 'flow');
  useSessionStore.setState({ currentSessionId: 'c', sessions: [{ id: 'c', title: 'A conversation', groupId: 'g', createdAt: new Date(), updatedAt: new Date() }], messages: [] });
});
const mount = () => render(<SessionSidebar onOpenSettings={vi.fn()} />);

describe('shared session sidebar', () => {
  it('opens sessions of every kind from one list and restores the selected mode', async () => {
    mount();
    fireEvent.click(screen.getByTitle('Reporting flow · Flow Builder'));
    expect(useUILayoutStore.getState().appMode).toBe('flow');
    fireEvent.click(screen.getByTitle('A conversation · Chat'));
    await waitFor(() => expect(useUILayoutStore.getState().appMode).toBe('chat'));
    fireEvent.click(screen.getByTitle('Research crew · Agent Builder'));
    expect(useUILayoutStore.getState().appMode).toBe('crew');
  });

  it('searches across modes, renames, pins and restores archived work without deleting it', async () => {
    mount();
    fireEvent.change(screen.getByRole('textbox', { name: 'Search sessions' }), { target: { value: 'Reporting' } });
    expect(screen.queryByTitle('Research crew · Agent Builder')).toBeNull();
    fireEvent.click(screen.getByLabelText('Options for Reporting flow'));
    fireEvent.click(screen.getByRole('menuitem', { name: 'Rename' }));
    fireEvent.change(screen.getByLabelText('Session name'), { target: { value: 'Reporting weekly' } });
    fireEvent.keyDown(screen.getByLabelText('Session name'), { key: 'Enter' });
    expect(screen.getByTitle('Reporting weekly · Flow Builder')).toBeInTheDocument();
    fireEvent.click(screen.getByLabelText('Options for Reporting weekly'));
    fireEvent.click(screen.getByRole('menuitem', { name: 'Pin', exact: true }));
    const id = useBuilderCanvasStore.getState().canvases.find(tab => tab.name === 'Reporting weekly')!.id;
    expect(useSessionPreferences.getState().entries[`builder:${id}`]?.pinned).toBe(true);
    fireEvent.click(screen.getByLabelText('Options for Reporting weekly'));
    fireEvent.click(screen.getByRole('menuitem', { name: 'Archive', exact: true }));
    expect(screen.queryByTitle('Reporting weekly · Flow Builder')).toBeNull();
    expect(useBuilderCanvasStore.getState().getCanvas(id)).toBeTruthy();
    fireEvent.click(screen.getByLabelText('Archived sessions'));
    fireEvent.click(screen.getByLabelText('Options for Reporting weekly'));
    fireEvent.click(screen.getByRole('menuitem', { name: 'Restore session' }));
    fireEvent.click(screen.getByLabelText('Back to sessions'));
    expect(screen.getByTitle('Reporting weekly · Flow Builder')).toBeInTheDocument();
  });

  it('keeps the same sidebar toggle in Chat and excludes restricted builder sessions', () => {
    useUILayoutStore.setState({ appMode: 'chat' });
    usePermissionStore.setState({ allowAgentBuilder: false, allowFlowBuilder: false });
    mount();
    expect(screen.queryByTitle('Research crew · Agent Builder')).toBeNull();
    expect(screen.queryByTitle('Reporting flow · Flow Builder')).toBeNull();
    fireEvent.click(screen.getByLabelText('Hide sessions'));
    expect(screen.queryByRole('textbox', { name: 'Search sessions' })).toBeNull();
    fireEvent.click(screen.getByLabelText('Show sessions'));
    expect(screen.getByTitle('A conversation · Chat')).toBeInTheDocument();
    expect(screen.queryByLabelText('Choose new session mode')).toBeNull();
    fireEvent.click(screen.getByRole('button', { name: 'New session', exact: true }));
    expect(useUILayoutStore.getState().appMode).toBe('chat');
  });

  it.each(['chat', 'crew', 'flow'] as const)('deletes an archived %s session immediately without a confirmation dialog', async mode => {
    const tab = useBuilderCanvasStore.getState().canvases.find(item => item.viewMode === mode);
    const key = tab ? `builder:${tab.id}` : 'chat:c';
    const chatId = tab?.chatSessionId || 'c';
    const title = tab?.name || 'A conversation';
    if (tab) {
      useSessionStore.setState(state => ({ sessions: [...state.sessions, {
        id: chatId, title, groupId: 'g', createdAt: new Date(), updatedAt: new Date(),
      }] }));
      useChatMessagesStore.getState().setMessages(chatId, []);
      sessionStorage.setItem(`kasal-builder-run:g:${chatId}`, 'recovery');
    }
    useSessionPreferences.getState().update(key, { archived: true, pinned: true });
    mount();
    fireEvent.click(screen.getByLabelText('Archived sessions'));
    fireEvent.click(screen.getByLabelText(`Options for ${title}`));
    fireEvent.click(screen.getByRole('menuitem', { name: 'Delete session' }));
    await waitFor(() => expect(deleteSession).toHaveBeenCalledExactlyOnceWith(chatId));
    expect(screen.queryByRole('dialog')).toBeNull();
    await waitFor(() => expect(screen.queryByTitle(`${title} · ${mode === 'chat' ? 'Chat' : mode === 'crew' ? 'Agent Builder' : 'Flow Builder'}`)).toBeNull());
    expect(useSessionStore.getState().sessions.some(item => item.id === chatId)).toBe(false);
    expect(useSessionPreferences.getState().entries[key]).toBeUndefined();
    if (tab) {
      expect(useBuilderCanvasStore.getState().getCanvas(tab.id)).toBeNull();
      expect(useChatMessagesStore.getState().messagesBySession[chatId]).toBeUndefined();
      expect(sessionStorage.getItem(`kasal-builder-run:g:${chatId}`)).toBeNull();
    } else expect(useSessionStore.getState().currentSessionId).toBeNull();
  });

  it('keeps the archived session when deletion fails and allows retry', async () => {
    vi.mocked(deleteSession).mockRejectedValueOnce({ isAxiosError: true, response: { status: 500 } });
    useSessionPreferences.getState().update('chat:c', { archived: true });
    mount();
    fireEvent.click(screen.getByLabelText('Archived sessions'));
    fireEvent.click(screen.getByLabelText('Options for A conversation'));
    fireEvent.click(screen.getByRole('menuitem', { name: 'Delete session' }));
    expect(await screen.findByText('Could not delete this session. Please try again.')).toBeInTheDocument();
    expect(screen.getByTitle('A conversation · Chat')).toBeInTheDocument();
    expect(useSessionPreferences.getState().entries['chat:c']?.archived).toBe(true);
    fireEvent.click(screen.getByLabelText('Options for A conversation'));
    fireEvent.click(screen.getByRole('menuitem', { name: 'Delete session' }));
    await waitFor(() => expect(screen.queryByTitle('A conversation · Chat')).toBeNull());
  });

  it('removes a local canvas when its conversation was never persisted', async () => {
    vi.mocked(deleteSession).mockRejectedValueOnce({ isAxiosError: true, response: { status: 404 } });
    const tab = useBuilderCanvasStore.getState().canvases.find(item => item.viewMode === 'crew')!;
    useSessionPreferences.getState().update(`builder:${tab.id}`, { archived: true });
    mount();
    fireEvent.click(screen.getByLabelText('Archived sessions'));
    fireEvent.click(screen.getByLabelText('Options for Research crew'));
    fireEvent.click(screen.getByRole('menuitem', { name: 'Delete session' }));
    await waitFor(() => expect(screen.queryByTitle('Research crew · Agent Builder')).toBeNull());
    expect(useBuilderCanvasStore.getState().getCanvas(tab.id)).toBeNull();
  });

  it('keeps a running session and explains why it cannot yet be deleted', async () => {
    useSessionStore.setState(state => ({ sessions: state.sessions.map(item => ({ ...item, runningJobId: 'running-job' })) }));
    useSessionPreferences.getState().update('chat:c', { archived: true });
    mount();
    fireEvent.click(screen.getByLabelText('Archived sessions'));
    fireEvent.click(screen.getByLabelText('Options for A conversation'));
    fireEvent.click(screen.getByRole('menuitem', { name: 'Delete session' }));
    expect(await screen.findByText('Stop the running session before deleting it.')).toBeInTheDocument();
    expect(deleteSession).not.toHaveBeenCalled();
  });
});
