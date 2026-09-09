import { act, renderHook, waitFor } from '@testing-library/react';
import { beforeEach, expect, it, vi } from 'vitest';
import { useWorkspaceSessions } from './useWorkspaceSessions';
import { SessionLoadError } from './sessionLoadError';

const m = vi.hoisted(() => ({ init: vi.fn(), reload: vi.fn(), connect: vi.fn(), release: vi.fn(), restore: vi.fn(), canvas: vi.fn() }));
vi.mock('./sessionStore', () => ({ useSessionStore: { getState: () => ({ init: m.init, reloadForGroup: m.reload, sessions: [], currentSessionId: 'chat' }) } }));
vi.mock('./builderSessionLifecycle', () => ({ connectBuilderSessions: m.connect }));
vi.mock('./builderCanvasStore', () => ({ useBuilderCanvasStore: { setState: m.canvas } }));
vi.mock('../../features/chat/store/executionStore', () => ({ useExecutionStore: { getState: () => ({ restoreSessionState: m.restore, resetForSession: vi.fn() }) } }));
vi.mock('../../features/chat/store/appStore', () => ({ useAppStore: { getState: () => ({ init: () => {} }) } }));
vi.mock('../../store/user', () => ({ useUserStore: (select: (s: unknown) => unknown) => select({ currentUser: { email: 'user@example.com' } }) }));
vi.mock('../../store/permissions', () => ({ usePermissionStore: (select: (s: unknown) => unknown) => select({ allowAgentBuilder: true }) }));
vi.mock('../../store/uiLayout', () => ({ useUILayoutStore: {} }));
vi.mock('./sessionNavigation', () => ({ switchWorkspaceMode: vi.fn() }));
beforeEach(() => {
  vi.resetAllMocks(); localStorage.setItem('selectedGroupId', 'team-a');
  m.init.mockResolvedValue(undefined); m.reload.mockResolvedValue(undefined); m.connect.mockResolvedValue(m.release);
});
it('restores chat despite canvas conflict and retries without discarding the warning', async () => {
  m.connect.mockRejectedValueOnce(new Error('This session changed in another browser. Your unsaved canvas is retained locally.'));
  const { result } = renderHook(() => useWorkspaceSessions());
  await waitFor(() => expect(result.current.loading).toBe(false));
  expect(result.current.loadError).toContain('unsaved canvas is retained locally');
  expect(result.current.loadError).toContain('HISTORY-v1/builders');
  expect(m.restore).toHaveBeenCalledWith('chat');
  expect(m.canvas).not.toHaveBeenCalledWith({ hydrated: true });
  act(() => result.current.retry());
  await waitFor(() => expect(result.current.loading).toBe(false));
  expect(result.current.loadError).toBeNull();
  expect(m.connect).toHaveBeenCalledTimes(2);
});
it('reports list HTTP failures without exposing response bodies, then retries', async () => {
  m.init.mockRejectedValueOnce(new SessionLoadError('list', { isAxiosError: true, response: { status: 500, data: { detail: 'secret credentials' } } }));
  const { result } = renderHook(() => useWorkspaceSessions());
  await waitFor(() => expect(result.current.loadError).toContain('HISTORY-v1/list/HTTP-500'));
  expect(result.current.loadError).not.toContain('secret');
  expect(m.connect).not.toHaveBeenCalled();
  act(() => result.current.retry());
  await waitFor(() => expect(result.current.loading).toBe(false));
  expect(result.current.loadError).toBeNull();
});
it('continues builder restore when only selected messages fail', async () => {
  m.init.mockRejectedValueOnce(new SessionLoadError('messages', { isAxiosError: true, response: { status: 404 } }));
  const { result } = renderHook(() => useWorkspaceSessions());
  await waitFor(() => expect(result.current.loading).toBe(false));
  expect(result.current.loadError).toContain('HISTORY-v1/messages/HTTP-404');
  expect(m.connect).toHaveBeenCalledTimes(1);
});
it('ignores stale workspace failure after the new workspace loads', async () => {
  let rejectOld!: (error: Error) => void;
  m.init.mockImplementationOnce(() => new Promise((_, reject) => { rejectOld = reject; }));
  const { result } = renderHook(() => useWorkspaceSessions());
  act(() => { localStorage.setItem('selectedGroupId', 'team-b'); window.dispatchEvent(new Event('group-changed')); });
  await waitFor(() => expect(result.current.loading).toBe(false));
  await act(async () => rejectOld(new Error('old failure')));
  expect(result.current.loadError).toBeNull(); expect(result.current.groupId).toBe('team-b');
});

it('keeps the healthy canvas connection active when one canvas reports a conflict', async () => {
  m.connect.mockImplementationOnce(async (_group, _sessions, _cancelled, onError) => {
    onError(new Error('This session changed in another browser. Your unsaved canvas is retained locally.'));
    return m.release;
  });
  const { result, unmount } = renderHook(() => useWorkspaceSessions());
  await waitFor(() => expect(result.current.loading).toBe(false));
  expect(result.current.loadError).toContain('unsaved canvas is retained locally');
  expect(m.canvas).toHaveBeenCalledWith({ hydrated: true });
  expect(m.restore).toHaveBeenCalledWith('chat');
  expect(m.release).not.toHaveBeenCalled();
  unmount();
  expect(m.release).toHaveBeenCalledTimes(1);
});
