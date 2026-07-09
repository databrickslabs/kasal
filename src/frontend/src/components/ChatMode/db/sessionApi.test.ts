/**
 * Tests for the server-side chat session adapter (sessionApi).
 *
 * Sessions/messages persist through /chat-history instead of IndexedDB;
 * these tests verify the wire mapping both ways, UTC timestamp handling,
 * and the one-time IndexedDB -> server migration.
 */
import { describe, it, expect, vi, beforeEach, Mock } from 'vitest';

const mockGet = vi.fn();
const mockPost = vi.fn();
const mockPut = vi.fn();
const mockDelete = vi.fn();

vi.mock('../api/client', () => ({
  getClient: () => ({ get: mockGet, post: mockPost, put: mockPut, delete: mockDelete }),
}));

vi.mock('./sessionDb', () => ({
  initDb: vi.fn(async () => undefined),
  listSessions: vi.fn(async () => []),
  getSessionMessages: vi.fn(async () => []),
}));

import * as api from './sessionApi';
import * as localDb from './sessionDb';

beforeEach(() => {
  vi.clearAllMocks();
  localStorage.clear();
});

describe('sessionApi - adapter contract', () => {
  it('initDb kicks off the background migration and swallows its failures', async () => {
    (localDb.listSessions as Mock).mockRejectedValueOnce(new Error('idb broken'));
    await api.initDb();
    await new Promise((r) => setTimeout(r, 0)); // let the background catch run
    expect(localDb.initDb).toHaveBeenCalled();
  });

  it('assignUngroupedSessions is a server-side no-op', async () => {
    await expect(api.assignUngroupedSessions('g1')).resolves.toBeUndefined();
  });

  it('clearSessionMessages deletes and recreates the session row', async () => {
    mockDelete.mockResolvedValue({ data: {} });
    mockPost.mockResolvedValue({ data: {} });

    await api.clearSessionMessages('s1');

    expect(mockDelete).toHaveBeenCalledWith('/chat-history/sessions/s1');
    expect(mockPost).toHaveBeenCalledWith('/chat-history/sessions', { id: 's1', title: 'New Chat' });
  });

  it('clearSessionMessages tolerates delete and recreate failures', async () => {
    mockDelete.mockRejectedValue(new Error('gone'));
    mockPost.mockRejectedValue(new Error('conflict'));
    await expect(api.clearSessionMessages('s1')).resolves.toBeUndefined();
  });
});

describe('sessionApi - sessions', () => {
  it('creates a session via the API and maps timestamps as UTC', async () => {
    mockPost.mockResolvedValue({
      data: {
        id: 's1', title: 'My Chat', user_id: 'u@x.com', group_id: 'g1',
        created_at: '2026-06-11T07:00:00', updated_at: '2026-06-11T07:00:00',
      },
    });

    const session = await api.createSession('My Chat');

    expect(mockPost).toHaveBeenCalledWith('/chat-history/sessions', { title: 'My Chat' });
    expect(session.id).toBe('s1');
    expect(session.groupId).toBe('g1');
    // Naive backend timestamp parsed as UTC, not local time
    expect(session.createdAt.toISOString()).toBe('2026-06-11T07:00:00.000Z');
  });

  it('lists named sessions', async () => {
    mockGet.mockResolvedValue({
      data: [{
        id: 's1', title: 'A', user_id: 'u', group_id: null,
        created_at: '2026-06-11T07:00:00Z', updated_at: '2026-06-11T08:00:00Z',
      }],
    });

    const sessions = await api.listSessions();

    expect(mockGet).toHaveBeenCalledWith('/chat-history/sessions/named');
    expect(sessions).toHaveLength(1);
    expect(sessions[0].title).toBe('A');
  });

  it('tolerates a session-list response without data', async () => {
    mockGet.mockResolvedValue({ data: null });
    expect(await api.listSessions()).toEqual([]);
  });

  it('renames and deletes via the API', async () => {
    mockPut.mockResolvedValue({ data: {} });
    mockDelete.mockResolvedValue({ data: {} });

    await api.renameSession('s1', 'Renamed');
    await api.deleteSession('s1');

    expect(mockPut).toHaveBeenCalledWith('/chat-history/sessions/s1', { title: 'Renamed' });
    expect(mockDelete).toHaveBeenCalledWith('/chat-history/sessions/s1');
  });
});

describe('sessionApi - messages', () => {
  it('maps wire messages to ChatMessage with extras unpacked', async () => {
    mockGet.mockResolvedValue({
      data: {
        messages: [{
          id: 'm1', session_id: 's1', message_type: 'assistant',
          content: 'done', intent: 'generate_crew',
          generation_result: { __chatmode: { resultType: 'crew', resultData: { a: 1 }, attachments: ['f.txt'] } },
          timestamp: '2026-06-11T07:00:00',
        }, {
          id: 'm2', session_id: 's1', message_type: 'trace',
          content: 'trace line', generation_result: null,
          timestamp: '2026-06-11T07:00:01',
        }],
      },
    });

    const messages = await api.getSessionMessages('s1');

    expect(messages[0].role).toBe('assistant');
    expect(messages[0].resultType).toBe('crew');
    expect(messages[0].resultData).toEqual({ a: 1 });
    expect(messages[0].attachments).toEqual(['f.txt']);
    expect(messages[0].isStreaming).toBe(false);
    // Unknown wire types degrade to assistant
    expect(messages[1].role).toBe('assistant');
  });

  it('loads a short session with a single page request', async () => {
    mockGet.mockResolvedValue({
      data: {
        messages: [{
          id: 'm1', session_id: 's1', message_type: 'user',
          content: 'hi', generation_result: null, timestamp: '2026-06-11T07:00:00',
        }],
      },
    });

    const messages = await api.getSessionMessages('s1');

    expect(messages).toHaveLength(1);
    expect(mockGet).toHaveBeenCalledTimes(1);
    expect(mockGet).toHaveBeenCalledWith('/chat-history/sessions/s1/messages', {
      params: { page: 0, per_page: 100 },
    });
  });

  it('pages through sessions longer than 100 messages (regression: a single page-0 fetch dropped the newest turns)', async () => {
    const wireMsg = (i: number) => ({
      id: `m${i}`, session_id: 's1', message_type: 'user',
      content: `msg ${i}`, generation_result: null,
      timestamp: '2026-06-11T07:00:00',
    });
    const fullPage = Array.from({ length: 100 }, (_, i) => wireMsg(i));
    const lastPage = [wireMsg(100), wireMsg(101)];
    mockGet
      .mockResolvedValueOnce({ data: { messages: fullPage } })
      .mockResolvedValueOnce({ data: { messages: lastPage } });

    const messages = await api.getSessionMessages('s1');

    expect(mockGet).toHaveBeenCalledTimes(2);
    expect(mockGet).toHaveBeenNthCalledWith(1, '/chat-history/sessions/s1/messages', {
      params: { page: 0, per_page: 100 },
    });
    expect(mockGet).toHaveBeenNthCalledWith(2, '/chat-history/sessions/s1/messages', {
      params: { page: 1, per_page: 100 },
    });
    expect(messages).toHaveLength(102);
    // The newest turn (beyond page 0) survives the reload.
    expect(messages[101].content).toBe('msg 101');
  });

  it('heals an envelope-clobbered a2ui card (surface-shaped resultData, no resultType)', async () => {
    // Regression (HAR-confirmed): an old partial PUT replaced generation_result
    // with {resultData} only, stripping resultType — the presentation then never
    // rendered as a card again. A surface-shaped resultData must load as 'a2ui'.
    const surface = { surfaceKind: 'presentation', components: [], theme: { accent: '#FF3621' } };
    mockGet.mockResolvedValue({
      data: {
        messages: [{
          id: 'm1', session_id: 's1', message_type: 'assistant', content: '[ui-card]',
          generation_result: { __chatmode: { resultData: surface } },
          timestamp: '2026-06-11T07:00:00',
        }, {
          // NOT surface-shaped → no inference (stays typeless).
          id: 'm2', session_id: 's1', message_type: 'assistant', content: '[ui-card]',
          generation_result: { __chatmode: { resultData: { agents: [] } } },
          timestamp: '2026-06-11T07:00:01',
        }],
      },
    });

    const [healed, other] = await api.getSessionMessages('s1');
    expect(healed.resultType).toBe('a2ui');
    expect(healed.resultData).toEqual(surface);
    expect(other.resultType).toBeUndefined();
  });

  it('maps user and system wire types to their roles', async () => {
    mockGet.mockResolvedValue({
      data: {
        messages: [
          { id: 'm1', session_id: 's1', message_type: 'user', content: 'hi', timestamp: '2026-06-11T07:00:00' },
          { id: 'm2', session_id: 's1', message_type: 'system', content: 'note', timestamp: '2026-06-11T07:00:00' },
        ],
      },
    });

    const [userMsg, systemMsg] = await api.getSessionMessages('s1');
    expect(userMsg.role).toBe('user');
    expect(systemMsg.role).toBe('system');
  });

  it('tolerates a messages response without messages', async () => {
    mockGet.mockResolvedValue({ data: {} });
    expect(await api.getSessionMessages('s1')).toEqual([]);
  });

  it('persists a message with client id and packed extras', async () => {
    mockPost.mockResolvedValue({ data: {} });

    await api.addMessageToSession('s1', {
      id: 'm1', role: 'user', content: 'hello', timestamp: new Date(),
      attachments: ['doc.pdf'],
    });

    expect(mockPost).toHaveBeenCalledWith('/chat-history/messages', {
      id: 'm1', session_id: 's1', message_type: 'user', content: 'hello',
      intent: null,
      generation_result: { __chatmode: { attachments: ['doc.pdf'] } },
    });
  });

  it('round-trips executionId through the __chatmode extras (preview-derive anchor)', async () => {
    // Unpack: an executionId in the wire extras lands on the ChatMessage so the
    // preview pane can later derive the deliverable from that execution.result.
    mockGet.mockResolvedValue({
      data: {
        messages: [{
          id: 'm1', session_id: 's1', message_type: 'assistant', content: 'done',
          generation_result: { __chatmode: { resultType: 'crew', executionId: 'job-42' } },
          timestamp: '2026-06-11T07:00:00',
        }],
      },
    });
    const [msg] = await api.getSessionMessages('s1');
    expect(msg.executionId).toBe('job-42');

    // Pack: a message's executionId is persisted back into the extras so the
    // anchor survives a reload / session switch.
    mockPost.mockResolvedValue({ data: {} });
    await api.addMessageToSession('s1', {
      id: 'm2', role: 'assistant', content: 'result', timestamp: new Date(),
      executionId: 'job-42',
    });
    expect(mockPost).toHaveBeenCalledWith('/chat-history/messages', {
      id: 'm2', session_id: 's1', message_type: 'assistant', content: 'result',
      intent: null,
      generation_result: { __chatmode: { executionId: 'job-42' } },
    });
  });

  it('round-trips usedWorkspaceMemory through the __chatmode extras', async () => {
    // Unpack: a persisted usedWorkspaceMemory flag lands on the ChatMessage so
    // the memory-graph action reflects what the run actually used.
    mockGet.mockResolvedValue({
      data: {
        messages: [{
          id: 'm1', session_id: 's1', message_type: 'assistant', content: 'done',
          generation_result: { __chatmode: { resultType: 'crew_actions', usedWorkspaceMemory: true } },
          timestamp: '2026-06-11T07:00:00',
        }],
      },
    });
    const [msg] = await api.getSessionMessages('s1');
    expect(msg.usedWorkspaceMemory).toBe(true);

    // Pack: the flag is persisted back into the extras (false is preserved too,
    // since it's a meaningful "session-only" value, not just absence).
    mockPost.mockResolvedValue({ data: {} });
    await api.addMessageToSession('s1', {
      id: 'm2', role: 'assistant', content: 'result', timestamp: new Date(),
      usedWorkspaceMemory: false,
    });
    expect(mockPost).toHaveBeenCalledWith('/chat-history/messages', {
      id: 'm2', session_id: 's1', message_type: 'assistant', content: 'result',
      intent: null,
      generation_result: { __chatmode: { usedWorkspaceMemory: false } },
    });
  });

  it('skips empty-content messages (backend requires content)', async () => {
    await api.addMessageToSession('s1', {
      id: 'm1', role: 'assistant', content: '', timestamp: new Date(),
    });
    expect(mockPost).not.toHaveBeenCalled();
  });

  it('updates message content via PUT and skips transient-only updates', async () => {
    mockPut.mockResolvedValue({ data: {} });

    await api.updateMessageInSession('s1', 'm1', { content: 'longer content' });
    expect(mockPut).toHaveBeenCalledWith('/chat-history/messages/m1', { content: 'longer content' });

    mockPut.mockClear();
    await api.updateMessageInSession('s1', 'm1', { isStreaming: false });
    expect(mockPut).not.toHaveBeenCalled();
  });

  it('updates intent and packed extras via PUT', async () => {
    mockPut.mockResolvedValue({ data: {} });

    await api.updateMessageInSession('s1', 'm1', {
      intent: 'generate_crew',
      resultType: 'trace',
      resultData: { a: 1 },
    });

    expect(mockPut).toHaveBeenCalledWith('/chat-history/messages/m1', {
      intent: 'generate_crew',
      generation_result: { __chatmode: { resultType: 'trace', resultData: { a: 1 } } },
    });
  });
});

describe('sessionApi - migration', () => {
  it('pushes local sessions and messages, skipping ones already on the server', async () => {
    (localDb.listSessions as Mock).mockResolvedValue([
      { id: 'local-1', title: 'Old chat', createdAt: new Date(), updatedAt: new Date() },
      { id: 'server-1', title: 'Already there', createdAt: new Date(), updatedAt: new Date() },
    ]);
    (localDb.getSessionMessages as Mock).mockResolvedValue([
      { id: 'm1', role: 'user', content: 'hi', timestamp: new Date() },
    ]);
    // Server already has server-1
    mockGet.mockResolvedValue({
      data: [{ id: 'server-1', title: 'Already there', user_id: 'u', created_at: '2026-06-11T07:00:00', updated_at: '2026-06-11T07:00:00' }],
    });
    mockPost.mockResolvedValue({ data: {} });

    const migrated = await api.migrateLocalSessionsToServer();

    expect(migrated).toBe(1);
    expect(mockPost).toHaveBeenCalledWith('/chat-history/sessions', { id: 'local-1', title: 'Old chat' });
    // Second run is a no-op (id recorded locally)
    mockPost.mockClear();
    expect(await api.migrateLocalSessionsToServer()).toBe(0);
    expect(mockPost).not.toHaveBeenCalled();
  });

  it('returns 0 when the server is unreachable (retries next init)', async () => {
    (localDb.listSessions as Mock).mockResolvedValue([
      { id: 'local-1', title: 'Old chat', createdAt: new Date(), updatedAt: new Date() },
    ]);
    mockGet.mockRejectedValue(new Error('offline'));

    expect(await api.migrateLocalSessionsToServer()).toBe(0);
    expect(mockPost).not.toHaveBeenCalled();
  });

  it('returns 0 when there are no local sessions to migrate', async () => {
    (localDb.listSessions as Mock).mockResolvedValue([]);
    expect(await api.migrateLocalSessionsToServer()).toBe(0);
    expect(mockGet).not.toHaveBeenCalled();
  });

  it('migrates only the current workspace sessions (plus untagged), defaulting empty titles', async () => {
    localStorage.setItem('selectedGroupId', 'g1');
    // A corrupt migrated-ids flag falls back to an empty set instead of throwing.
    localStorage.setItem('kasal-chat-sessions-migrated-ids', 'not-json');
    (localDb.listSessions as Mock).mockResolvedValue([
      { id: 'mine', title: '', groupId: 'g1', createdAt: new Date(), updatedAt: new Date() },
      { id: 'other', title: 'X', groupId: 'g2', createdAt: new Date(), updatedAt: new Date() },
      { id: 'untagged', title: 'U', createdAt: new Date(), updatedAt: new Date() },
    ]);
    (localDb.getSessionMessages as Mock).mockResolvedValue([]);
    mockGet.mockResolvedValue({ data: [] });
    mockPost.mockResolvedValue({ data: {} });

    const migrated = await api.migrateLocalSessionsToServer();

    expect(migrated).toBe(2);
    expect(mockPost).toHaveBeenCalledWith('/chat-history/sessions', { id: 'mine', title: 'New Chat' });
    expect(mockPost).toHaveBeenCalledWith('/chat-history/sessions', { id: 'untagged', title: 'U' });
    expect(mockPost).not.toHaveBeenCalledWith(
      '/chat-history/sessions',
      expect.objectContaining({ id: 'other' }),
    );
  });

  it('keeps another workspace tag when no workspace is selected, and stops on a post failure', async () => {
    (localDb.listSessions as Mock).mockResolvedValue([
      { id: 'l1', title: 'A', groupId: 'g9', createdAt: new Date(), updatedAt: new Date() },
    ]);
    mockGet.mockResolvedValue({ data: [] });
    mockPost.mockRejectedValue(new Error('conflict'));

    // No selectedGroupId → tagged sessions still migrate… but the post fails,
    // so the loop breaks and the session is retried on the next run.
    expect(await api.migrateLocalSessionsToServer()).toBe(0);
    expect(mockPost).toHaveBeenCalledWith('/chat-history/sessions', { id: 'l1', title: 'A' });
  });
});

describe('sessionApi - card message persistence', () => {
  it('persists card-only messages (empty content) with the sentinel', async () => {
    mockPost.mockResolvedValue({ data: {} });

    await api.addMessageToSession('s1', {
      id: 'm-card', role: 'assistant', content: '', timestamp: new Date(),
      resultType: 'generation_complete',
      resultData: { agents: [{ name: 'A' }] },
    });

    expect(mockPost).toHaveBeenCalledWith('/chat-history/messages', expect.objectContaining({
      id: 'm-card',
      content: '[ui-card]',
      generation_result: {
        __chatmode: {
          resultType: 'generation_complete',
          resultData: { agents: [{ name: 'A' }] },
        },
      },
    }));
  });

  it('strips the sentinel back to empty content on load', async () => {
    mockGet.mockResolvedValue({
      data: {
        messages: [{
          id: 'm-card', session_id: 's1', message_type: 'assistant',
          content: '[ui-card]',
          generation_result: { __chatmode: { resultType: 'generation_complete', resultData: { genieSpaceId: 'sp-1', genieRan: true } } },
          timestamp: '2026-06-11T07:00:00',
        }],
      },
    });

    const [msg] = await api.getSessionMessages('s1');
    expect(msg.content).toBe('');
    expect(msg.resultType).toBe('generation_complete');
    // Genie pick + ran-state round-trip inside resultData
    expect((msg.resultData as { genieSpaceId?: string }).genieSpaceId).toBe('sp-1');
    expect((msg.resultData as { genieRan?: boolean }).genieRan).toBe(true);
  });

  it('still skips truly empty messages (no content, no card)', async () => {
    await api.addMessageToSession('s1', {
      id: 'm-empty', role: 'assistant', content: '', timestamp: new Date(),
    });
    expect(mockPost).not.toHaveBeenCalled();
  });
});

describe('sessionApi - preview (server-backed, replaces IndexedDB)', () => {
  it('saves a preview via PUT', async () => {
    mockPut.mockResolvedValue({ data: {} });
    await api.saveSessionPreview('s1', { type: 'ui', data: '{"messages":[]}', title: 'Deck' });
    expect(mockPut).toHaveBeenCalledWith('/chat-history/sessions/s1/preview', {
      type: 'ui', data: '{"messages":[]}', title: 'Deck',
    });
  });

  it('sends a null title when absent', async () => {
    mockPut.mockResolvedValue({ data: {} });
    await api.saveSessionPreview('s1', { type: 'ui', data: 'x' });
    expect(mockPut).toHaveBeenCalledWith('/chat-history/sessions/s1/preview', {
      type: 'ui', data: 'x', title: null,
    });
  });

  it('reads a stored preview (maps wire -> StoredPreview)', async () => {
    mockGet.mockResolvedValue({ data: { type: 'ui', data: '{"a":1}', title: 'T' } });
    const p = await api.getSessionPreview('s1');
    expect(mockGet).toHaveBeenCalledWith('/chat-history/sessions/s1/preview');
    expect(p).toEqual({ sessionId: 's1', type: 'ui', data: '{"a":1}', title: 'T' });
  });

  it('returns undefined when there is no preview (null data)', async () => {
    mockGet.mockResolvedValue({ data: { type: null, data: null, title: null } });
    expect(await api.getSessionPreview('s1')).toBeUndefined();
  });

  it('reads are resilient — undefined on error', async () => {
    mockGet.mockRejectedValue(new Error('boom'));
    expect(await api.getSessionPreview('s1')).toBeUndefined();
  });

  it('writes are best-effort — never throw', async () => {
    mockPut.mockRejectedValue(new Error('down'));
    await expect(api.saveSessionPreview('s1', { type: 'ui', data: 'x' })).resolves.toBeUndefined();
    mockDelete.mockRejectedValue(new Error('down'));
    await expect(api.deleteSessionPreview('s1')).resolves.toBeUndefined();
  });

  it('deletes the preview via DELETE', async () => {
    mockDelete.mockResolvedValue({ data: {} });
    await api.deleteSessionPreview('s1');
    expect(mockDelete).toHaveBeenCalledWith('/chat-history/sessions/s1/preview');
  });
});

describe('sessionApi - running-job marker (server-backed, replaces IndexedDB)', () => {
  it('sets the marker via PUT', async () => {
    mockPut.mockResolvedValue({ data: {} });
    await api.setSessionRunningJob('s1', 'job-1');
    expect(mockPut).toHaveBeenCalledWith('/chat-history/sessions/s1/running-job', { job_id: 'job-1' });
  });

  it('reads the marker', async () => {
    mockGet.mockResolvedValue({ data: { job_id: 'job-9' } });
    expect(await api.getSessionRunningJob('s1')).toBe('job-9');
    expect(mockGet).toHaveBeenCalledWith('/chat-history/sessions/s1/running-job');
  });

  it('returns null when no job, and on error', async () => {
    mockGet.mockResolvedValue({ data: { job_id: null } });
    expect(await api.getSessionRunningJob('s1')).toBeNull();
    mockGet.mockRejectedValue(new Error('boom'));
    expect(await api.getSessionRunningJob('s2')).toBeNull();
  });

  it('clears via DELETE; writes never throw', async () => {
    mockDelete.mockResolvedValue({ data: {} });
    await api.clearSessionRunningJob('s1');
    expect(mockDelete).toHaveBeenCalledWith('/chat-history/sessions/s1/running-job');
    mockPut.mockRejectedValue(new Error('down'));
    await expect(api.setSessionRunningJob('s1', 'j')).resolves.toBeUndefined();
  });
});
