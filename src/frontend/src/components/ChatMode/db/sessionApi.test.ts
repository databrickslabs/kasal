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
