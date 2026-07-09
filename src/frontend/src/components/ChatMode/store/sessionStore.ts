import { create } from 'zustand';
import { ChatMessage, ChatSession } from '../types/chat';
import { generateId } from '../utils/markdown';
// Sessions persist server-side (SQLite locally / Lakebase when active)
// through the /chat-history API instead of browser IndexedDB. The adapter
// keeps sessionDb's exact contract; previews and running-job markers stay
// device-local in sessionDb.
import {
  initDb,
  assignUngroupedSessions as dbAssignUngroupedSessions,
  createSession as dbCreateSession,
  listSessions as dbListSessions,
  deleteSession as dbDeleteSession,
  renameSession as dbRenameSession,
  getSessionMessages,
  addMessageToSession,
  updateMessageInSession,
  clearSessionMessages,
} from '../db/sessionApi';

const ACTIVE_SESSION_KEY = 'kasal-chat-active-session';

/**
 * Complete a partial message update with the persisted-envelope fields from the
 * in-memory message before it is round-tripped to the server.
 *
 * The backend PUT REPLACES `generation_result` wholesale, so a partial update —
 * e.g. a deck restyle sending only `resultData` — used to clobber the
 * `__chatmode` envelope and strip `resultType`/`executionId`. The message then
 * stopped rendering as an A2UI card on reload AND lost its run anchor, so the
 * preview pane could no longer find the restyled copy (the "presentation
 * vanishes after a session switch" bug, confirmed in a deployed-app HAR).
 * When the message isn't in memory (updating a non-viewed session) there is
 * nothing to merge from; the updates go through unchanged.
 */
export function mergePersistedExtras(
  updates: Partial<ChatMessage>,
  existing?: ChatMessage,
): Partial<ChatMessage> {
  if (!existing) return updates;
  return {
    ...updates,
    ...(updates.resultType === undefined && existing.resultType !== undefined
      ? { resultType: existing.resultType }
      : {}),
    ...(updates.resultData === undefined && existing.resultData !== undefined
      ? { resultData: existing.resultData }
      : {}),
    ...(updates.attachments === undefined && existing.attachments !== undefined
      ? { attachments: existing.attachments }
      : {}),
    ...(updates.executionId === undefined && existing.executionId !== undefined
      ? { executionId: existing.executionId }
      : {}),
    ...(updates.usedWorkspaceMemory === undefined && existing.usedWorkspaceMemory !== undefined
      ? { usedWorkspaceMemory: existing.usedWorkspaceMemory }
      : {}),
  };
}

// The selected workspace/group, mirrored to localStorage by the group store.
// Chat sessions are scoped to it so switching workspace shows only that
// workspace's chats.
const currentGroupId = (): string | undefined => {
  try {
    return localStorage.getItem('selectedGroupId') || undefined;
  } catch {
    return undefined;
  }
};

interface SessionState {
  sessions: ChatSession[];
  currentSessionId: string | null;
  messages: ChatMessage[];
  isDbReady: boolean;
  /** True from mount until init() finishes WHEN a session is about to be
   *  restored (an active-session id is persisted). Lets the UI hold the
   *  "new chat" greeting so a refresh doesn't flash it before the restored
   *  conversation loads. False on a genuine fresh start (no session to wait for). */
  hydrating: boolean;
}

interface SessionActions {
  init: () => Promise<void>;
  /** Re-list sessions for the now-current workspace. On a full page reload
   *  (`restoreActiveSession`) the session the user left is restored; on a
   *  workspace switch we land on a fresh chat. */
  reloadForGroup: (restoreActiveSession?: boolean) => Promise<void>;
  switchSession: (sessionId: string) => Promise<void>;
  createNewSession: () => Promise<string>;
  /** Resolve the current session id, lazily creating ONE session if none exists
   *  yet (shared with addMessage's lazy create via `pendingSessionCreate`, so
   *  the first turn never spawns duplicates). Await this BEFORE capturing an
   *  origin session id for a run — otherwise, under remote-Postgres latency,
   *  `currentSessionId` is still null and the run gets registered under an empty
   *  owner, so its completed result never renders (the "first prompt empty" bug). */
  ensureSession: () => Promise<string>;
  /** Reset to a blank chat WITHOUT persisting a session. Matches the lazy-
   *  creation design: the row is created (and auto-titled) on the first
   *  message, so an empty "New Chat" never clutters the Recent rail. */
  startNewChat: () => void;
  deleteSession: (id: string) => Promise<void>;
  renameSession: (id: string, title: string) => Promise<void>;
  addMessage: (
    role: ChatMessage['role'],
    content: string,
    extra?: Partial<ChatMessage>,
  ) => string;
  addMessageToTargetSession: (
    targetSessionId: string,
    role: ChatMessage['role'],
    content: string,
    extra?: Partial<ChatMessage>,
  ) => string;
  updateMessage: (id: string, updates: Partial<ChatMessage>) => void;
  updateMessageInTargetSession: (
    targetSessionId: string,
    id: string,
    updates: Partial<ChatMessage>,
  ) => void;
  appendToMessage: (id: string, additionalContent: string) => void;
  clearMessages: () => void;
}

type SessionStore = SessionState & SessionActions;

// Track which sessions have been auto-titled
const autoTitled = new Set<string>();

// Guards lazy session creation. On a fresh chat the first turn fires addMessage
// more than once in the same tick (the user prompt AND the "Thinking..."
// placeholder — see useDispatcher), each running an async ensureAndPersist that
// reads currentSessionId. That id stays null until a create's network POST
// returns, so without this guard every caller reads null and EACH creates a
// session — leaving an orphaned "New Chat" alongside the real one. The window
// only opens under network latency (Databricks Apps' remote Postgres); local
// dev's sub-millisecond DB hides it. Concurrent callers share this one promise.
let pendingSessionCreate: Promise<string> | null = null;

export const useSessionStore = create<SessionStore>((set, get) => ({
  // --- State ---
  sessions: [],
  currentSessionId: null,
  messages: [],
  isDbReady: false,
  // Seeded synchronously (before the first paint) from the persisted active
  // session: if one exists, init() is about to restore it, so hold the greeting.
  hydrating: typeof localStorage !== 'undefined' && !!localStorage.getItem(ACTIVE_SESSION_KEY),

  // --- Actions ---
  init: async () => {
    try {
      await initDb();
      set({ isDbReady: true });
      // One-time: adopt any pre-workspace-scoping (untagged) sessions into the
      // current workspace so strict filtering doesn't hide them. No-op once done.
      const gid = currentGroupId();
      if (gid) {
        try {
          await dbAssignUngroupedSessions(gid);
        } catch {
          /* non-fatal */
        }
      }
      // A full page load restores the session the user left (see reloadForGroup).
      await get().reloadForGroup(true);
    } finally {
      // Restore is done (or failed) — release the greeting hold either way.
      set({ hydrating: false });
    }
  },

  reloadForGroup: async (restoreActiveSession = false) => {
    // Always re-list this workspace's sessions for the history rail.
    const allSessions = await dbListSessions(currentGroupId());
    const activeId = localStorage.getItem(ACTIVE_SESSION_KEY);
    // On a full page reload (refresh), RESTORE the session the user left — so a
    // refresh keeps you in your conversation instead of bouncing to a new chat.
    // Only restore when that session still belongs to THIS workspace (a stale id
    // from another group / a deleted session falls through to a fresh chat).
    if (
      restoreActiveSession &&
      activeId &&
      allSessions.some((s) => s.id === activeId)
    ) {
      const msgs = await getSessionMessages(activeId);
      set({ sessions: allSessions, currentSessionId: activeId, messages: msgs });
      return;
    }
    // Otherwise (workspace switch, or no/invalid active session) land on a FRESH
    // chat: a session is created lazily on the first message; previous chats stay
    // one click away in the rail.
    localStorage.removeItem(ACTIVE_SESSION_KEY);
    set({ sessions: allSessions, currentSessionId: null, messages: [] });
  },

  switchSession: async (sessionId: string) => {
    localStorage.setItem(ACTIVE_SESSION_KEY, sessionId);
    const msgs = await getSessionMessages(sessionId);
    set({ currentSessionId: sessionId, messages: msgs });
  },

  createNewSession: async () => {
    const session = await dbCreateSession('New Chat', currentGroupId());
    localStorage.setItem(ACTIVE_SESSION_KEY, session.id);
    const allSessions = await dbListSessions(currentGroupId());
    set({
      currentSessionId: session.id,
      messages: [],
      sessions: allSessions,
    });
    return session.id;
  },

  ensureSession: async () => {
    const existing = get().currentSessionId;
    if (existing) return existing;
    // Reuse an in-flight create so callers in the same tick (the user message,
    // the "Thinking..." placeholder, AND the dispatcher's origin-id capture)
    // share ONE session instead of each spawning its own (the double-session
    // bug). Only one create is ever in flight; the guard blocks a second.
    if (!pendingSessionCreate) {
      const create = (async () => {
        const session = await dbCreateSession('New Chat', currentGroupId());
        localStorage.setItem(ACTIVE_SESSION_KEY, session.id);
        const allSessions = await dbListSessions(currentGroupId());
        set({ currentSessionId: session.id, sessions: allSessions });
        return session.id;
      })();
      pendingSessionCreate = create;
      void create.finally(() => {
        pendingSessionCreate = null;
      });
    }
    return pendingSessionCreate;
  },

  // Land on a blank chat WITHOUT persisting a row. The actual session is
  // created lazily (and auto-titled from the first prompt) by addMessage — the
  // same path reloadForGroup uses for a fresh chat. This is what the "New Chat"
  // button uses: eagerly creating here would drop an untitled "New Chat" into
  // the Recent rail next to the button (the duplicate users reported).
  startNewChat: () => {
    localStorage.removeItem(ACTIVE_SESSION_KEY);
    set({ currentSessionId: null, messages: [] });
  },

  deleteSession: async (id: string) => {
    await dbDeleteSession(id);
    const remaining = await dbListSessions(currentGroupId());
    const state = get();

    if (id === state.currentSessionId) {
      if (remaining.length > 0) {
        const next = remaining[0];
        localStorage.setItem(ACTIVE_SESSION_KEY, next.id);
        const msgs = await getSessionMessages(next.id);
        set({ sessions: remaining, currentSessionId: next.id, messages: msgs });
      } else {
        // No sessions left — create a new one
        const session = await dbCreateSession('New Chat', currentGroupId());
        localStorage.setItem(ACTIVE_SESSION_KEY, session.id);
        set({
          sessions: [session],
          currentSessionId: session.id,
          messages: [],
        });
      }
    } else {
      set({ sessions: remaining });
    }
  },

  renameSession: async (id: string, title: string) => {
    await dbRenameSession(id, title);
    const allSessions = await dbListSessions(currentGroupId());
    set({ sessions: allSessions });
  },

  addMessage: (role, content, extra) => {
    const id = extra?.id || generateId();
    const message: ChatMessage = {
      id,
      role,
      content,
      timestamp: new Date(),
      ...extra,
    };
    set((state) => ({ messages: [...state.messages, message] }));

    // Persist to IndexedDB (fire-and-forget)
    const ensureAndPersist = async () => {
      const sessionId = await get().ensureSession();
      await addMessageToSession(sessionId, { ...message, sessionId });

      // Auto-title: first non-command user message becomes the session title
      if (
        role === 'user' &&
        !autoTitled.has(sessionId) &&
        !content.startsWith('/')
      ) {
        autoTitled.add(sessionId);
        const title = content.slice(0, 40).trim() || 'New Chat';
        await dbRenameSession(sessionId, title);
        const allSessions = await dbListSessions(currentGroupId());
        set({ sessions: allSessions });
      }
    };
    ensureAndPersist();

    return id;
  },

  addMessageToTargetSession: (targetSessionId, role, content, extra) => {
    const id = extra?.id || generateId();
    const message: ChatMessage = {
      id,
      role,
      content,
      timestamp: new Date(),
      ...extra,
    };

    // Only update in-memory state if we're viewing that session
    const state = get();
    if (state.currentSessionId === targetSessionId) {
      set((s) => ({ messages: [...s.messages, message] }));
    }

    // Always persist
    addMessageToSession(targetSessionId, {
      ...message,
      sessionId: targetSessionId,
    });

    return id;
  },

  updateMessage: (id, updates) => {
    const existing = get().messages.find((m) => m.id === id);
    set((state) => ({
      messages: state.messages.map((m) =>
        m.id === id ? { ...m, ...updates } : m,
      ),
    }));
    const sessionId = get().currentSessionId;
    if (sessionId) {
      updateMessageInSession(sessionId, id, mergePersistedExtras(updates, existing));
    }
  },

  updateMessageInTargetSession: (targetSessionId, id, updates) => {
    const state = get();
    // Only the viewed session's messages are in memory — merge from them when
    // available so the persisted envelope stays complete (see helper).
    const existing =
      state.currentSessionId === targetSessionId
        ? state.messages.find((m) => m.id === id)
        : undefined;
    if (state.currentSessionId === targetSessionId) {
      set((s) => ({
        messages: s.messages.map((m) =>
          m.id === id ? { ...m, ...updates } : m,
        ),
      }));
    }
    updateMessageInSession(targetSessionId, id, mergePersistedExtras(updates, existing));
  },

  appendToMessage: (id, additionalContent) => {
    set((state) => {
      const updated = state.messages.map((m) =>
        m.id === id ? { ...m, content: m.content + additionalContent } : m,
      );
      // Persist full content
      const sessionId = get().currentSessionId;
      if (sessionId) {
        const msg = updated.find((m) => m.id === id);
        if (msg) {
          updateMessageInSession(sessionId, id, { content: msg.content });
        }
      }
      return { messages: updated };
    });
  },

  clearMessages: () => {
    set({ messages: [] });
    const sessionId = get().currentSessionId;
    if (sessionId) {
      clearSessionMessages(sessionId);
    }
  },
}));
