import { beforeEach, expect, it, vi } from 'vitest';
import { collectSessions } from './sessionIndex';
import type { BuilderCanvas } from './builderCanvasStore';
const m = vi.hoisted(() => ({ list: vi.fn() }));
vi.mock('../../features/chat/persistence/sessionApi', () => ({
  listSessions: m.list, initDb: vi.fn(), assignUngroupedSessions: vi.fn(), createSession: vi.fn(),
  deleteSession: vi.fn(), renameSession: vi.fn(), getSessionMessages: vi.fn(), addMessageToSession: vi.fn(), updateMessageInSession: vi.fn(), clearSessionMessages: vi.fn(),
}));
import { useSessionStore } from './sessionStore';
const session = (id: string, mode: 'chat' | 'crew' = 'chat') => ({ id, title: id, mode, groupId: 'g', createdAt: new Date(), updatedAt: new Date() });
beforeEach(() => { vi.resetAllMocks(); localStorage.clear(); localStorage.setItem('selectedGroupId', 'g'); useSessionStore.setState({ sessions: [] }); });
it('ignores an older list response after a newer reload completes', async () => {
  let finishOld!: (rows: ReturnType<typeof session>[]) => void;
  m.list.mockImplementationOnce(() => new Promise(resolve => { finishOld = resolve; }));
  const old = useSessionStore.getState().reloadForGroup();
  m.list.mockResolvedValueOnce([session('newer-list')]);
  await useSessionStore.getState().reloadForGroup();
  expect(useSessionStore.getState().sessions.map(s => s.id)).toEqual(['newer-list']);
  finishOld([session('older-list')]); await old;
  expect(useSessionStore.getState().sessions.map(s => s.id)).toEqual(['newer-list']);
});
it('shows builder metadata immediately and keeps row keys and ordering after hydration', () => {
  const rows = [session('chat'), { ...session('builder', 'crew'), updatedAt: new Date('2020-01-01') }];
  const before = collectSessions(rows, [], 'g', { crew: true, flow: true });
  expect(before.map(s => s.key)).toEqual(['chat:chat', 'builder:builder']);
  expect(before[1].canvasPending).toBe(true);
  const canvas = { id: 'canvas', chatSessionId: 'builder', name: 'builder', group_id: 'g', viewMode: 'crew', lastModified: new Date(), nodes: [], flowNodes: [] } as unknown as BuilderCanvas;
  const after = collectSessions(rows, [canvas], 'g', { crew: true, flow: true });
  expect(after.map(s => s.key)).toEqual(before.map(s => s.key));
  expect(after.map(s => s.updatedAt)).toEqual(before.map(s => s.updatedAt));
  expect(after[1].canvasPending).toBe(false);
  expect(after[1].id).toBe('canvas');
  expect(collectSessions(rows, [], 'g', { crew: false, flow: false }).map(s => s.id)).toEqual(['chat']);
});
it('publishes the latest list without undoing navigation made during the request', async () => {
  let finish!: (rows: ReturnType<typeof session>[]) => void;
  m.list.mockImplementationOnce(() => new Promise(resolve => { finish = resolve; }));
  const request = useSessionStore.getState().reloadForGroup();
  useSessionStore.getState().startNewChat();
  finish([session('saved')]); await request;
  expect(useSessionStore.getState().sessions.map(s => s.id)).toEqual(['saved']);
  expect(useSessionStore.getState().currentSessionId).toBeNull();
});
