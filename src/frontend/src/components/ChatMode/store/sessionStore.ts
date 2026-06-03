import { create } from 'zustand';
import { ChatMessage, ChatSession } from '../types/chat';
import { generateId } from '../utils/markdown';
import {
  initDb,
  createSession as dbCreateSession,
  listSessions as dbListSessions,
  deleteSession as dbDeleteSession,
  renameSession as dbRenameSession,
  getSessionMessages,
  addMessageToSession,
  updateMessageInSession,
  clearSessionMessages,
} from '../db/sessionDb';

const ACTIVE_SESSION_KEY = 'kasal-chat-active-session';

interface SessionState {
  sessions: ChatSession[];
  currentSessionId: string | null;
  messages: ChatMessage[];
  isDbReady: boolean;
}

interface SessionActions {
  init: () => Promise<void>;
  switchSession: (sessionId: string) => Promise<void>;
  createNewSession: () => Promise<string>;
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

export const useSessionStore = create<SessionStore>((set, get) => ({
  // --- State ---
  sessions: [],
  currentSessionId: null,
  messages: [],
  isDbReady: false,

  // --- Actions ---
  init: async () => {
    await initDb();
    set({ isDbReady: true });

    const allSessions = await dbListSessions();
    set({ sessions: allSessions });

    // Restore last active session
    const lastId = localStorage.getItem(ACTIVE_SESSION_KEY);
    if (lastId && allSessions.some((s) => s.id === lastId)) {
      const msgs = await getSessionMessages(lastId);
      set({ currentSessionId: lastId, messages: msgs });
    } else if (allSessions.length > 0) {
      const recent = allSessions[0];
      localStorage.setItem(ACTIVE_SESSION_KEY, recent.id);
      const msgs = await getSessionMessages(recent.id);
      set({ currentSessionId: recent.id, messages: msgs });
    }
  },

  switchSession: async (sessionId: string) => {
    localStorage.setItem(ACTIVE_SESSION_KEY, sessionId);
    const msgs = await getSessionMessages(sessionId);
    set({ currentSessionId: sessionId, messages: msgs });
  },

  createNewSession: async () => {
    const session = await dbCreateSession('New Chat');
    localStorage.setItem(ACTIVE_SESSION_KEY, session.id);
    const allSessions = await dbListSessions();
    set({
      currentSessionId: session.id,
      messages: [],
      sessions: allSessions,
    });
    return session.id;
  },

  deleteSession: async (id: string) => {
    await dbDeleteSession(id);
    const remaining = await dbListSessions();
    const state = get();

    if (id === state.currentSessionId) {
      if (remaining.length > 0) {
        const next = remaining[0];
        localStorage.setItem(ACTIVE_SESSION_KEY, next.id);
        const msgs = await getSessionMessages(next.id);
        set({ sessions: remaining, currentSessionId: next.id, messages: msgs });
      } else {
        // No sessions left — create a new one
        const session = await dbCreateSession('New Chat');
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
    const allSessions = await dbListSessions();
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
      let sessionId = get().currentSessionId;
      if (!sessionId) {
        const session = await dbCreateSession('New Chat');
        sessionId = session.id;
        localStorage.setItem(ACTIVE_SESSION_KEY, sessionId);
        const allSessions = await dbListSessions();
        set({ currentSessionId: sessionId, sessions: allSessions });
      }
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
        const allSessions = await dbListSessions();
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
    set((state) => ({
      messages: state.messages.map((m) =>
        m.id === id ? { ...m, ...updates } : m,
      ),
    }));
    const sessionId = get().currentSessionId;
    if (sessionId) {
      updateMessageInSession(sessionId, id, updates);
    }
  },

  updateMessageInTargetSession: (targetSessionId, id, updates) => {
    const state = get();
    if (state.currentSessionId === targetSessionId) {
      set((s) => ({
        messages: s.messages.map((m) =>
          m.id === id ? { ...m, ...updates } : m,
        ),
      }));
    }
    updateMessageInSession(targetSessionId, id, updates);
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
