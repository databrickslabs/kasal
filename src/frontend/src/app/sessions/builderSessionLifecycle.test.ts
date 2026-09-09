import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { waitFor } from '@testing-library/react';
import { apiClient } from '../../shared/api/client';
import { useBuilderCanvasStore } from './builderCanvasStore';
import { useSessionPreferences } from './sessionPreferences';
import { useSessionStore } from './sessionStore';
import { useUserStore } from '../../store/user';
import { useErrorStore } from '../../store/error';
import { connectBuilderSessions, pauseBuilderSessionWrites } from './builderSessionLifecycle';
import { legacyKey, pendingCanvases, restoreCanvas, snapshot, type CanvasSnapshot } from './builderSessionPersistence';

vi.mock('../../shared/api/client', () => ({ apiClient: { get: vi.fn(), put: vi.fn() } }));
let release = () => {};
const user = 'person@example.com';
const group = 'team-a';
const metadata = (id: string) => ({ id, title: 'Research', mode: 'crew' as const, groupId: group, createdAt: new Date(), updatedAt: new Date() });
const canvas = (id = 'legacy-canvas', session = 'conversation'): CanvasSnapshot => ({
  id, chatSessionId: session, name: 'Research', group_id: group, viewMode: 'crew',
  nodes: [{ id: 'task', type: 'taskNode', data: { label: 'Research' }, position: { x: 30, y: 80 } }],
  edges: [], flowNodes: [], flowEdges: [], isDirty: false, isActive: false,
  executionJobIds: ['old-run'], createdAt: new Date().toISOString(), lastModified: new Date().toISOString(),
});
beforeEach(() => {
  vi.clearAllMocks(); localStorage.clear();
  localStorage.setItem('selectedGroupId', group);
  useUserStore.setState({ currentUser: { id: 'user', username: 'person', email: user } });
  useBuilderCanvasStore.setState({ canvases: [], activeCanvasId: null });
  useSessionStore.setState({ sessions: [] });
  useSessionPreferences.setState({ entries: {} });
  useErrorStore.getState().clearError();
  vi.mocked(apiClient.put).mockResolvedValue({ data: { revision: 1 } });
});
afterEach(() => { release(); release = () => {}; });

it('imports legacy canvases once, preserves conversation and run IDs, and retires the old storage', async () => {
  localStorage.setItem(legacyKey, JSON.stringify({ state: { tabs: [canvas()], activeTabId: 'legacy-canvas' } }));
  vi.mocked(apiClient.get).mockResolvedValue({ data: { state: null, revision: 0 } });
  release = await connectBuilderSessions(group, [], () => false);
  const restored = useBuilderCanvasStore.getState().getActiveCanvas()!;
  expect(restored.id).toBe('legacy-canvas'); expect(restored.chatSessionId).toBe('conversation');
  expect(restored.executionJobIds).toEqual(['old-run']);
  expect(restored.nodes[0].position).toEqual({ x: 30, y: 80 });
  expect(restored.createdAt).toBeInstanceOf(Date);
  expect(apiClient.put).toHaveBeenCalledTimes(1);
  expect(localStorage.getItem(legacyKey)).toBeNull();
  expect(useSessionStore.getState().sessions[0].mode).toBe('crew');
});

it('restores a builder from the server in a browser with no local canvas', async () => {
  vi.mocked(apiClient.get).mockResolvedValue({ data: { state: canvas(), revision: 3 } });
  release = await connectBuilderSessions(group, [metadata('conversation')], () => false);
  expect(useBuilderCanvasStore.getState().getActiveCanvas()?.nodes).toHaveLength(1);
  expect(apiClient.put).not.toHaveBeenCalled();
});

it('saves edits to the originating session after navigating elsewhere', async () => {
  vi.mocked(apiClient.get).mockResolvedValue({ data: { state: canvas(), revision: 3 } });
  vi.mocked(apiClient.put).mockResolvedValue({ data: { revision: 4 } });
  release = await connectBuilderSessions(group, [metadata('conversation')], () => false);
  useBuilderCanvasStore.getState().updateCanvasName('legacy-canvas', 'Updated research');
  useBuilderCanvasStore.getState().createCanvas('Other session', 'flow', { sessionDraft: true });
  await waitFor(() => expect(apiClient.put).toHaveBeenCalledTimes(1));
  expect(apiClient.put).toHaveBeenCalledWith('/chat-history/sessions/conversation/canvas',
    expect.objectContaining({ title: 'Updated research', revision: 3 }), { headers: { group_id: group } });
  await waitFor(() => expect(pendingCanvases(user, group)).toEqual({}));
});

it('keeps the legacy backup when importing fails', async () => {
  localStorage.setItem(legacyKey, JSON.stringify({ state: { tabs: [canvas()] } }));
  vi.mocked(apiClient.get).mockResolvedValue({ data: { state: null, revision: 0 } });
  vi.mocked(apiClient.put).mockRejectedValue(new Error('offline'));
  const onError = vi.fn();
  release = await connectBuilderSessions(group, [], () => false, onError);
  expect(onError).toHaveBeenCalledWith(expect.objectContaining({ message: 'offline' }));
  expect(localStorage.getItem(legacyKey)).toContain('legacy-canvas');
});

it('recovers unsaved edits from the user and teamspace outbox after a refresh', async () => {
  release = await connectBuilderSessions(group, [], () => false);
  vi.mocked(apiClient.put).mockRejectedValueOnce(new Error('offline'));
  const id = useBuilderCanvasStore.getState().createCanvas('Pending research', 'crew');
  await waitFor(() => expect(useErrorStore.getState().showError).toBe(true));
  expect(pendingCanvases(user, group)[id].state.name).toBe('Pending research');
  expect(pendingCanvases('another@example.com', group)).toEqual({});
  release(); useBuilderCanvasStore.setState({ canvases: [], activeCanvasId: null });
  vi.mocked(apiClient.get).mockResolvedValue({ data: { state: null, revision: 0 } });
  release = await connectBuilderSessions(group, [], () => false);
  expect(useBuilderCanvasStore.getState().getCanvas(id)?.name).toBe('Pending research');
  expect(pendingCanvases(user, group)).toEqual({});
});

it('preserves a conflicting outbox rather than overwriting another browser', async () => {
  const state = canvas();
  localStorage.setItem(`kasal-session-outbox:${user}:${group}`, JSON.stringify({ conversation: { state, revision: 1 } }));
  vi.mocked(apiClient.get).mockResolvedValue({ data: { state: { ...state, name: 'Changed elsewhere' }, revision: 2 } });
  const onError = vi.fn();
  release = await connectBuilderSessions(group, [metadata('conversation')], () => false, onError);
  expect(onError).toHaveBeenCalledWith(expect.objectContaining({ message: expect.stringContaining('another browser') }));
  expect(apiClient.put).not.toHaveBeenCalled();
  expect(pendingCanvases(user, group).conversation.state.name).toBe('Research');
});

it('cancels queued saves before deleting a session so it cannot be recreated', async () => {
  vi.mocked(apiClient.get).mockResolvedValue({ data: { state: canvas(), revision: 2 } });
  release = await connectBuilderSessions(group, [metadata('conversation')], () => false);
  useBuilderCanvasStore.getState().updateCanvasName('legacy-canvas', 'About to delete');
  await pauseBuilderSessionWrites('conversation');
  useBuilderCanvasStore.getState().closeCanvas('legacy-canvas');
  await new Promise(resolve => setTimeout(resolve, 350));
  expect(apiClient.put).not.toHaveBeenCalled();
  expect(pendingCanvases(user, group)).toEqual({});
});

it('does not restore data after the requesting teamspace has changed', async () => {
  let changed = false;
  vi.mocked(apiClient.get).mockImplementation(async () => { changed = true; return { data: { state: canvas(), revision: 1 } }; });
  release = await connectBuilderSessions(group, [metadata('conversation')], () => changed);
  expect(useBuilderCanvasStore.getState().canvases).toEqual([]);
});

it('serializes canvas callbacks safely without modifying the live nodes', () => {
  const stored = canvas();
  const node = { ...stored.nodes[0], data: { onClick: () => {}, label: 'Task' } };
  const state = snapshot({ ...stored, nodes: [node], createdAt: new Date(), lastModified: new Date(), lastSavedAt: undefined, lastExecutionTime: undefined });
  expect(state.nodes[0].data).toEqual({ label: 'Task' });
  expect(node.data.onClick).toBeTypeOf('function');
});

it('isolates a conflicting canvas and preserves metadata ordering while healthy sessions save', async () => {
  const conflict = canvas();
  const healthy = canvas('healthy', 'healthy');
  const rows = [metadata('conversation'), { ...metadata('healthy'), updatedAt: new Date('2020-01-01') }];
  useSessionStore.setState({ sessions: rows });
  useBuilderCanvasStore.setState({ canvases: [restoreCanvas(conflict, group, 'conversation')], activeCanvasId: conflict.id });
  localStorage.setItem(`kasal-session-outbox:${user}:${group}`, JSON.stringify({ conversation: { state: conflict, revision: 1 } }));
  vi.mocked(apiClient.get).mockResolvedValueOnce({ data: { state: { ...conflict, name: 'Other browser' }, revision: 2 } })
    .mockResolvedValueOnce({ data: { state: healthy, revision: 3 } });
  const onError = vi.fn();
  release = await connectBuilderSessions(group, rows, () => false, onError);
  expect(onError).toHaveBeenCalledTimes(1);
  expect(useBuilderCanvasStore.getState().getActiveCanvas()?.id).toBe('healthy');
  expect(useBuilderCanvasStore.getState().unavailableSessionIds).toEqual(['conversation']);
  expect(useSessionStore.getState().sessions).toEqual(rows);
  useBuilderCanvasStore.getState().updateCanvasName(conflict.id, 'Still local');
  useBuilderCanvasStore.getState().updateCanvasName('healthy', 'Healthy edit');
  await waitFor(() => expect(apiClient.put).toHaveBeenCalledTimes(1));
  expect(vi.mocked(apiClient.put).mock.calls[0][0]).toContain('/healthy/canvas');
  expect(pendingCanvases(user, group).conversation).toEqual({ state: conflict, revision: 1 });
});

it('retains legacy archive and pin preferences under the stable conversation key', async () => {
  useSessionPreferences.getState().update('builder:legacy-canvas', { archived: true, pinned: true });
  vi.mocked(apiClient.get).mockResolvedValue({ data: { state: canvas(), revision: 3 } });
  release = await connectBuilderSessions(group, [metadata('conversation')], () => false);
  expect(useSessionPreferences.getState().entries['builder:conversation']).toEqual({ archived: true, pinned: true });
});
