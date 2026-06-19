import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';
import type { ChatSession, ChatMessage } from '../types/chat';

// --- Mock generateId ---
let idCounter = 0;
vi.mock('../utils/markdown', () => ({
  generateId: vi.fn(() => `gen-id-${++idCounter}`),
}));

// --- Mock sessionDb ---
vi.mock('../db/sessionApi', () => ({
  initDb: vi.fn(),
  assignUngroupedSessions: vi.fn(),
  createSession: vi.fn(),
  listSessions: vi.fn(),
  deleteSession: vi.fn(),
  renameSession: vi.fn(),
  getSessionMessages: vi.fn(),
  addMessageToSession: vi.fn(),
  updateMessageInSession: vi.fn(),
  clearSessionMessages: vi.fn(),
}));

import { useSessionStore } from './sessionStore';
import * as db from '../db/sessionApi';

const ACTIVE_SESSION_KEY = 'kasal-chat-active-session';

const makeSession = (id: string, title = 'Title'): ChatSession => ({
  id,
  title,
  createdAt: new Date('2020-01-01'),
  updatedAt: new Date('2020-01-01'),
});

const makeMsg = (id: string, content = 'hello'): ChatMessage => ({
  id,
  role: 'user',
  content,
  timestamp: new Date('2020-01-01'),
});

// flush pending microtasks/promises (for fire-and-forget async work)
const flush = async () => {
  // allow several microtask turns for chained awaits
  for (let i = 0; i < 5; i++) {
    await Promise.resolve();
  }
};

const resetState = () => {
  useSessionStore.setState({
    sessions: [],
    currentSessionId: null,
    messages: [],
    isDbReady: false,
    hydrating: false,
  });
};

beforeEach(() => {
  vi.clearAllMocks();
  idCounter = 0;
  localStorage.clear();
  resetState();
  // sensible defaults
  (db.initDb as ReturnType<typeof vi.fn>).mockResolvedValue(undefined);
  (db.listSessions as ReturnType<typeof vi.fn>).mockResolvedValue([]);
  (db.getSessionMessages as ReturnType<typeof vi.fn>).mockResolvedValue([]);
  (db.addMessageToSession as ReturnType<typeof vi.fn>).mockResolvedValue(undefined);
  (db.updateMessageInSession as ReturnType<typeof vi.fn>).mockResolvedValue(undefined);
  (db.clearSessionMessages as ReturnType<typeof vi.fn>).mockResolvedValue(undefined);
  (db.deleteSession as ReturnType<typeof vi.fn>).mockResolvedValue(undefined);
  (db.renameSession as ReturnType<typeof vi.fn>).mockResolvedValue(undefined);
  (db.createSession as ReturnType<typeof vi.fn>).mockResolvedValue(makeSession('created-1'));
});

afterEach(() => {
  vi.useRealTimers();
});

describe('init', () => {
  it('restores the last-active session on a full reload (refresh)', async () => {
    // A refresh must keep the user in the conversation they left — the session
    // recorded in ACTIVE_SESSION_KEY is reloaded with its messages.
    const sessions = [makeSession('s1'), makeSession('s2')];
    (db.listSessions as ReturnType<typeof vi.fn>).mockResolvedValue(sessions);
    const msgs = [{ id: 'm1', role: 'user', content: 'hi' }];
    (db.getSessionMessages as ReturnType<typeof vi.fn>).mockResolvedValue(msgs);
    localStorage.setItem(ACTIVE_SESSION_KEY, 's2');
    useSessionStore.setState({ hydrating: true }); // seeded from the persisted id

    await useSessionStore.getState().init();

    const state = useSessionStore.getState();
    expect(state.isDbReady).toBe(true);
    expect(state.sessions).toEqual(sessions);
    expect(state.currentSessionId).toBe('s2');
    expect(state.messages).toEqual(msgs);
    expect(db.getSessionMessages).toHaveBeenCalledWith('s2');
    expect(localStorage.getItem(ACTIVE_SESSION_KEY)).toBe('s2');
    // The greeting hold is released once restore completes.
    expect(state.hydrating).toBe(false);
  });

  it('lands on a fresh chat when the active session no longer belongs to the workspace', async () => {
    // Stale id (deleted, or from another group) → do not restore; clear it.
    const sessions = [makeSession('s1'), makeSession('s2')];
    (db.listSessions as ReturnType<typeof vi.fn>).mockResolvedValue(sessions);
    localStorage.setItem(ACTIVE_SESSION_KEY, 'gone');

    await useSessionStore.getState().init();

    const state = useSessionStore.getState();
    expect(state.currentSessionId).toBeNull();
    expect(state.messages).toEqual([]);
    expect(db.getSessionMessages).not.toHaveBeenCalled();
    expect(localStorage.getItem(ACTIVE_SESSION_KEY)).toBeNull();
  });

  it('lands on a fresh chat when sessions exist but none was active', async () => {
    const sessions = [makeSession('recent'), makeSession('older')];
    (db.listSessions as ReturnType<typeof vi.fn>).mockResolvedValue(sessions);

    await useSessionStore.getState().init();

    const state = useSessionStore.getState();
    expect(state.currentSessionId).toBeNull();
    expect(state.messages).toEqual([]);
    expect(state.sessions).toEqual(sessions);
  });

  it('does nothing extra when there are no sessions', async () => {
    (db.listSessions as ReturnType<typeof vi.fn>).mockResolvedValue([]);

    await useSessionStore.getState().init();

    const state = useSessionStore.getState();
    expect(state.isDbReady).toBe(true);
    expect(state.sessions).toEqual([]);
    expect(state.currentSessionId).toBeNull();
    expect(state.messages).toEqual([]);
    expect(db.getSessionMessages).not.toHaveBeenCalled();
  });
});

describe('reloadForGroup (workspace switch)', () => {
  it('lands on a fresh chat and clears the active session even if one is persisted', async () => {
    // Switching workspace must NOT carry over the other group's session — the
    // default (no restore) drops it; history stays in the rail.
    const sessions = [makeSession('s1'), makeSession('s2')];
    (db.listSessions as ReturnType<typeof vi.fn>).mockResolvedValue(sessions);
    localStorage.setItem(ACTIVE_SESSION_KEY, 's2');

    await useSessionStore.getState().reloadForGroup();

    const state = useSessionStore.getState();
    expect(state.currentSessionId).toBeNull();
    expect(state.messages).toEqual([]);
    expect(state.sessions).toEqual(sessions);
    expect(db.getSessionMessages).not.toHaveBeenCalled();
    expect(localStorage.getItem(ACTIVE_SESSION_KEY)).toBeNull();
  });
});

describe('switchSession', () => {
  it('switches to a session and loads its messages', async () => {
    const msgs = [makeMsg('m1'), makeMsg('m2')];
    (db.getSessionMessages as ReturnType<typeof vi.fn>).mockResolvedValue(msgs);

    await useSessionStore.getState().switchSession('target');

    const state = useSessionStore.getState();
    expect(localStorage.getItem(ACTIVE_SESSION_KEY)).toBe('target');
    expect(state.currentSessionId).toBe('target');
    expect(state.messages).toEqual(msgs);
  });
});

describe('createNewSession', () => {
  it('creates a session, persists active id, and refreshes sessions', async () => {
    const newSession = makeSession('new-sess', 'New Chat');
    (db.createSession as ReturnType<typeof vi.fn>).mockResolvedValue(newSession);
    const allSessions = [newSession];
    (db.listSessions as ReturnType<typeof vi.fn>).mockResolvedValue(allSessions);

    const returnedId = await useSessionStore.getState().createNewSession();

    expect(returnedId).toBe('new-sess');
    expect(db.createSession).toHaveBeenCalledWith('New Chat', undefined);
    expect(localStorage.getItem(ACTIVE_SESSION_KEY)).toBe('new-sess');
    const state = useSessionStore.getState();
    expect(state.currentSessionId).toBe('new-sess');
    expect(state.messages).toEqual([]);
    expect(state.sessions).toEqual(allSessions);
  });
});

describe('deleteSession', () => {
  it('deletes a non-current session and only updates sessions list', async () => {
    useSessionStore.setState({ currentSessionId: 'current' });
    const remaining = [makeSession('current'), makeSession('other2')];
    (db.listSessions as ReturnType<typeof vi.fn>).mockResolvedValue(remaining);

    await useSessionStore.getState().deleteSession('some-other');

    expect(db.deleteSession).toHaveBeenCalledWith('some-other');
    const state = useSessionStore.getState();
    expect(state.sessions).toEqual(remaining);
    // current unchanged
    expect(state.currentSessionId).toBe('current');
  });

  it('deletes the current session and switches to the next remaining one', async () => {
    useSessionStore.setState({ currentSessionId: 'current' });
    const remaining = [makeSession('next'), makeSession('another')];
    (db.listSessions as ReturnType<typeof vi.fn>).mockResolvedValue(remaining);
    const msgs = [makeMsg('m1')];
    (db.getSessionMessages as ReturnType<typeof vi.fn>).mockResolvedValue(msgs);

    await useSessionStore.getState().deleteSession('current');

    expect(db.deleteSession).toHaveBeenCalledWith('current');
    expect(localStorage.getItem(ACTIVE_SESSION_KEY)).toBe('next');
    const state = useSessionStore.getState();
    expect(state.sessions).toEqual(remaining);
    expect(state.currentSessionId).toBe('next');
    expect(state.messages).toEqual(msgs);
  });

  it('deletes the last remaining current session and creates a fresh one', async () => {
    useSessionStore.setState({ currentSessionId: 'current' });
    (db.listSessions as ReturnType<typeof vi.fn>).mockResolvedValue([]);
    const created = makeSession('fresh', 'New Chat');
    (db.createSession as ReturnType<typeof vi.fn>).mockResolvedValue(created);

    await useSessionStore.getState().deleteSession('current');

    expect(db.createSession).toHaveBeenCalledWith('New Chat', undefined);
    expect(localStorage.getItem(ACTIVE_SESSION_KEY)).toBe('fresh');
    const state = useSessionStore.getState();
    expect(state.sessions).toEqual([created]);
    expect(state.currentSessionId).toBe('fresh');
    expect(state.messages).toEqual([]);
  });
});

describe('renameSession', () => {
  it('renames and refreshes sessions', async () => {
    const allSessions = [makeSession('s1', 'Renamed')];
    (db.listSessions as ReturnType<typeof vi.fn>).mockResolvedValue(allSessions);

    await useSessionStore.getState().renameSession('s1', 'Renamed');

    expect(db.renameSession).toHaveBeenCalledWith('s1', 'Renamed');
    expect(useSessionStore.getState().sessions).toEqual(allSessions);
  });
});

describe('addMessage', () => {
  it('adds message with generated id and persists to existing session, auto-titling', async () => {
    useSessionStore.setState({ currentSessionId: 'existing' });
    const allSessions = [makeSession('existing', 'Hello there')];
    (db.listSessions as ReturnType<typeof vi.fn>).mockResolvedValue(allSessions);

    const id = useSessionStore.getState().addMessage('user', 'Hello there friend');

    expect(id).toBe('gen-id-1');
    // in-memory message added synchronously
    const msgs = useSessionStore.getState().messages;
    expect(msgs).toHaveLength(1);
    expect(msgs[0]).toMatchObject({ id: 'gen-id-1', role: 'user', content: 'Hello there friend' });

    await flush();

    expect(db.addMessageToSession).toHaveBeenCalledWith(
      'existing',
      expect.objectContaining({ id: 'gen-id-1', sessionId: 'existing' }),
    );
    // auto-titled with first 40 chars
    expect(db.renameSession).toHaveBeenCalledWith('existing', 'Hello there friend');
    expect(useSessionStore.getState().sessions).toEqual(allSessions);
  });

  it('uses extra.id when provided and does not auto-title command messages', async () => {
    useSessionStore.setState({ currentSessionId: 'sess-cmd' });

    const id = useSessionStore.getState().addMessage('user', '/help me', { id: 'fixed-id' });

    expect(id).toBe('fixed-id');
    await flush();

    expect(db.addMessageToSession).toHaveBeenCalledWith(
      'sess-cmd',
      expect.objectContaining({ id: 'fixed-id', sessionId: 'sess-cmd' }),
    );
    // command message starting with '/' must not auto-title
    expect(db.renameSession).not.toHaveBeenCalled();
  });

  it('does not auto-title non-user roles', async () => {
    useSessionStore.setState({ currentSessionId: 'sess-asst' });

    useSessionStore.getState().addMessage('assistant', 'a reply');
    await flush();

    expect(db.addMessageToSession).toHaveBeenCalled();
    expect(db.renameSession).not.toHaveBeenCalled();
  });

  it('uses fallback "New Chat" title when trimmed content is empty', async () => {
    useSessionStore.setState({ currentSessionId: 'sess-empty' });

    useSessionStore.getState().addMessage('user', '   ');
    await flush();

    expect(db.renameSession).toHaveBeenCalledWith('sess-empty', 'New Chat');
  });

  it('does not auto-title twice for the same session', async () => {
    useSessionStore.setState({ currentSessionId: 'sess-twice' });

    useSessionStore.getState().addMessage('user', 'first message');
    await flush();
    expect(db.renameSession).toHaveBeenCalledTimes(1);

    (db.renameSession as ReturnType<typeof vi.fn>).mockClear();
    useSessionStore.getState().addMessage('user', 'second message');
    await flush();
    // already auto-titled -> no second rename
    expect(db.renameSession).not.toHaveBeenCalled();
  });

  it('creates a session on the fly when there is no current session', async () => {
    useSessionStore.setState({ currentSessionId: null });
    const created = makeSession('auto-created');
    (db.createSession as ReturnType<typeof vi.fn>).mockResolvedValue(created);
    const allSessions = [created];
    (db.listSessions as ReturnType<typeof vi.fn>).mockResolvedValue(allSessions);

    useSessionStore.getState().addMessage('user', 'kick off a chat');
    await flush();

    expect(db.createSession).toHaveBeenCalledWith('New Chat', undefined);
    expect(localStorage.getItem(ACTIVE_SESSION_KEY)).toBe('auto-created');
    const state = useSessionStore.getState();
    expect(state.currentSessionId).toBe('auto-created');
    expect(state.sessions).toEqual(allSessions);
    expect(db.addMessageToSession).toHaveBeenCalledWith(
      'auto-created',
      expect.objectContaining({ sessionId: 'auto-created' }),
    );
    expect(db.renameSession).toHaveBeenCalledWith('auto-created', 'kick off a chat');
  });

  it('creates only ONE session when two messages arrive before the create resolves (Bug 1: double session)', async () => {
    // Regression: on a fresh chat the dispatcher fires addMessage twice in the
    // same tick (user prompt + "Thinking..." placeholder). Under network latency
    // both ran ensureAndPersist, both read currentSessionId===null, and both
    // created a session — leaving an orphaned "New Chat". A controllable
    // createSession promise reproduces that latency window deterministically.
    useSessionStore.setState({ currentSessionId: null });
    let resolveCreate!: (s: ChatSession) => void;
    (db.createSession as ReturnType<typeof vi.fn>).mockReturnValue(
      new Promise<ChatSession>((res) => {
        resolveCreate = res;
      }),
    );
    (db.listSessions as ReturnType<typeof vi.fn>).mockResolvedValue([makeSession('the-one')]);

    // Two messages back-to-back in the same tick, create still pending.
    useSessionStore.getState().addMessage('user', 'hello there');
    useSessionStore.getState().addMessage('assistant', 'Thinking...');
    await flush();

    // Both turns must share ONE in-flight create, not spawn two sessions.
    expect(db.createSession).toHaveBeenCalledTimes(1);

    // Resolve the create; both messages land in the same session.
    resolveCreate(makeSession('the-one'));
    await flush();

    expect(db.createSession).toHaveBeenCalledTimes(1);
    const targetedSessions = (db.addMessageToSession as ReturnType<typeof vi.fn>).mock.calls.map(
      (c) => c[0],
    );
    expect(targetedSessions).toEqual(['the-one', 'the-one']);
    expect(useSessionStore.getState().currentSessionId).toBe('the-one');
  });

  it('creates a fresh session again after a previous create settled', async () => {
    // The in-flight guard must release once settled so a later new chat can
    // create its own session (it must not be pinned to the previous one).
    useSessionStore.setState({ currentSessionId: null });
    (db.createSession as ReturnType<typeof vi.fn>).mockResolvedValueOnce(makeSession('first'));
    (db.listSessions as ReturnType<typeof vi.fn>).mockResolvedValue([makeSession('first')]);
    useSessionStore.getState().addMessage('user', 'first chat');
    await flush();
    expect(db.createSession).toHaveBeenCalledTimes(1);

    // Simulate starting a brand-new chat (fresh, no current session).
    useSessionStore.setState({ currentSessionId: null });
    (db.createSession as ReturnType<typeof vi.fn>).mockResolvedValueOnce(makeSession('second'));
    (db.listSessions as ReturnType<typeof vi.fn>).mockResolvedValue([makeSession('second')]);
    useSessionStore.getState().addMessage('user', 'second chat');
    await flush();

    expect(db.createSession).toHaveBeenCalledTimes(2);
    expect(useSessionStore.getState().currentSessionId).toBe('second');
  });
});

describe('addMessageToTargetSession', () => {
  it('updates in-memory state when viewing the target session and persists', () => {
    useSessionStore.setState({ currentSessionId: 'target', messages: [] });

    const id = useSessionStore
      .getState()
      .addMessageToTargetSession('target', 'assistant', 'live update');

    expect(id).toBe('gen-id-1');
    const msgs = useSessionStore.getState().messages;
    expect(msgs).toHaveLength(1);
    expect(msgs[0]).toMatchObject({ content: 'live update', role: 'assistant' });
    expect(db.addMessageToSession).toHaveBeenCalledWith(
      'target',
      expect.objectContaining({ id: 'gen-id-1', sessionId: 'target' }),
    );
  });

  it('does not update in-memory state when not viewing the target, but persists, honoring extra.id', () => {
    useSessionStore.setState({ currentSessionId: 'viewing', messages: [] });

    const id = useSessionStore
      .getState()
      .addMessageToTargetSession('other', 'user', 'bg msg', { id: 'ext-id' });

    expect(id).toBe('ext-id');
    expect(useSessionStore.getState().messages).toHaveLength(0);
    expect(db.addMessageToSession).toHaveBeenCalledWith(
      'other',
      expect.objectContaining({ id: 'ext-id', sessionId: 'other' }),
    );
  });
});

describe('updateMessage', () => {
  it('updates matching message in-memory and persists when a session is active', () => {
    useSessionStore.setState({
      currentSessionId: 'sess',
      messages: [makeMsg('m1', 'old'), makeMsg('m2', 'keep')],
    });

    useSessionStore.getState().updateMessage('m1', { content: 'new' });

    const msgs = useSessionStore.getState().messages;
    expect(msgs[0].content).toBe('new');
    expect(msgs[1].content).toBe('keep');
    expect(db.updateMessageInSession).toHaveBeenCalledWith('sess', 'm1', { content: 'new' });
  });

  it('does not persist when there is no active session', () => {
    useSessionStore.setState({
      currentSessionId: null,
      messages: [makeMsg('m1', 'old')],
    });

    useSessionStore.getState().updateMessage('m1', { content: 'new' });

    expect(useSessionStore.getState().messages[0].content).toBe('new');
    expect(db.updateMessageInSession).not.toHaveBeenCalled();
  });
});

describe('updateMessageInTargetSession', () => {
  it('updates in-memory when viewing the target and always persists', () => {
    useSessionStore.setState({
      currentSessionId: 'target',
      messages: [makeMsg('m1', 'old'), makeMsg('m2', 'keep')],
    });

    useSessionStore
      .getState()
      .updateMessageInTargetSession('target', 'm1', { content: 'updated' });

    const msgs = useSessionStore.getState().messages;
    expect(msgs[0].content).toBe('updated');
    expect(msgs[1].content).toBe('keep');
    expect(db.updateMessageInSession).toHaveBeenCalledWith('target', 'm1', {
      content: 'updated',
    });
  });

  it('does not touch in-memory when not viewing the target but still persists', () => {
    useSessionStore.setState({
      currentSessionId: 'viewing',
      messages: [makeMsg('m1', 'old')],
    });

    useSessionStore
      .getState()
      .updateMessageInTargetSession('other', 'm1', { content: 'updated' });

    expect(useSessionStore.getState().messages[0].content).toBe('old');
    expect(db.updateMessageInSession).toHaveBeenCalledWith('other', 'm1', {
      content: 'updated',
    });
  });
});

describe('appendToMessage', () => {
  it('appends content and persists full content when session active and message found', () => {
    useSessionStore.setState({
      currentSessionId: 'sess',
      messages: [makeMsg('m1', 'Hello'), makeMsg('m2', 'World')],
    });

    useSessionStore.getState().appendToMessage('m1', ' there');

    const msgs = useSessionStore.getState().messages;
    expect(msgs[0].content).toBe('Hello there');
    expect(msgs[1].content).toBe('World');
    expect(db.updateMessageInSession).toHaveBeenCalledWith('sess', 'm1', {
      content: 'Hello there',
    });
  });

  it('does not persist when there is no active session', () => {
    useSessionStore.setState({
      currentSessionId: null,
      messages: [makeMsg('m1', 'Hello')],
    });

    useSessionStore.getState().appendToMessage('m1', ' world');

    expect(useSessionStore.getState().messages[0].content).toBe('Hello world');
    expect(db.updateMessageInSession).not.toHaveBeenCalled();
  });

  it('does not persist when session active but message id not found', () => {
    useSessionStore.setState({
      currentSessionId: 'sess',
      messages: [makeMsg('m1', 'Hello')],
    });

    useSessionStore.getState().appendToMessage('missing', ' world');

    // unchanged content
    expect(useSessionStore.getState().messages[0].content).toBe('Hello');
    expect(db.updateMessageInSession).not.toHaveBeenCalled();
  });
});

describe('clearMessages', () => {
  it('clears in-memory messages and clears the active session in db', () => {
    useSessionStore.setState({
      currentSessionId: 'sess',
      messages: [makeMsg('m1')],
    });

    useSessionStore.getState().clearMessages();

    expect(useSessionStore.getState().messages).toEqual([]);
    expect(db.clearSessionMessages).toHaveBeenCalledWith('sess');
  });

  it('clears in-memory messages without db call when no active session', () => {
    useSessionStore.setState({
      currentSessionId: null,
      messages: [makeMsg('m1')],
    });

    useSessionStore.getState().clearMessages();

    expect(useSessionStore.getState().messages).toEqual([]);
    expect(db.clearSessionMessages).not.toHaveBeenCalled();
  });
});

describe('workspace scoping (per-group sessions)', () => {
  it('createNewSession tags the session with the current group', async () => {
    localStorage.setItem('selectedGroupId', 'group-x');
    (db.createSession as ReturnType<typeof vi.fn>).mockResolvedValue(makeSession('s-x'));
    (db.listSessions as ReturnType<typeof vi.fn>).mockResolvedValue([makeSession('s-x')]);
    await useSessionStore.getState().createNewSession();
    expect(db.createSession).toHaveBeenCalledWith('New Chat', 'group-x');
    expect(db.listSessions).toHaveBeenCalledWith('group-x');
  });

  it('reloadForGroup lists only the current group and lands on a fresh chat', async () => {
    localStorage.setItem('selectedGroupId', 'group-y');
    (db.listSessions as ReturnType<typeof vi.fn>).mockResolvedValue([makeSession('y1'), makeSession('y2')]);
    await useSessionStore.getState().reloadForGroup();
    expect(db.listSessions).toHaveBeenCalledWith('group-y');
    const s = useSessionStore.getState();
    expect(s.sessions.map((x) => x.id)).toEqual(['y1', 'y2']);
    expect(s.currentSessionId).toBeNull();
  });

  it('reloadForGroup yields an empty state when the workspace has no sessions', async () => {
    localStorage.setItem('selectedGroupId', 'group-empty');
    (db.listSessions as ReturnType<typeof vi.fn>).mockResolvedValue([]);
    await useSessionStore.getState().reloadForGroup();
    const s = useSessionStore.getState();
    expect(s.currentSessionId).toBeNull();
    expect(s.messages).toEqual([]);
  });

  it('init adopts ungrouped sessions into the current workspace', async () => {
    localStorage.setItem('selectedGroupId', 'group-z');
    (db.initDb as ReturnType<typeof vi.fn>).mockResolvedValue(undefined);
    (db.assignUngroupedSessions as ReturnType<typeof vi.fn>).mockResolvedValue(undefined);
    (db.listSessions as ReturnType<typeof vi.fn>).mockResolvedValue([]);
    await useSessionStore.getState().init();
    expect(db.assignUngroupedSessions).toHaveBeenCalledWith('group-z');
  });

  it('init tolerates a failure while adopting ungrouped sessions', async () => {
    localStorage.setItem('selectedGroupId', 'group-z');
    (db.initDb as ReturnType<typeof vi.fn>).mockResolvedValue(undefined);
    (db.assignUngroupedSessions as ReturnType<typeof vi.fn>).mockRejectedValue(new Error('migrate fail'));
    (db.listSessions as ReturnType<typeof vi.fn>).mockResolvedValue([]);
    await expect(useSessionStore.getState().init()).resolves.toBeUndefined();
  });

  it('falls back to no group when reading selectedGroupId from localStorage throws', async () => {
    const orig = Storage.prototype.getItem;
    const spy = vi.spyOn(Storage.prototype, 'getItem').mockImplementation(function (this: Storage, key: string) {
      if (key === 'selectedGroupId') throw new Error('storage blocked');
      return orig.call(this, key);
    });
    (db.createSession as ReturnType<typeof vi.fn>).mockResolvedValue(makeSession('s-nogroup'));
    (db.listSessions as ReturnType<typeof vi.fn>).mockResolvedValue([makeSession('s-nogroup')]);
    await useSessionStore.getState().createNewSession();
    expect(db.createSession).toHaveBeenCalledWith('New Chat', undefined);
    spy.mockRestore();
  });
});
