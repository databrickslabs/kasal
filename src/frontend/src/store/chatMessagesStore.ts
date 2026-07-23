import { create } from 'zustand';
import { subscribeWithSelector } from 'zustand/middleware';
import { ChatMessage } from '../components/Chat/types';

interface ChatMessagesState {
  // Messages organized by session ID
  messagesBySession: Record<string, ChatMessage[]>;

  // Current active session
  currentSessionId: string | null;

  // Actions
  setCurrentSession: (sessionId: string) => void;
  addMessage: (sessionId: string, message: ChatMessage) => void;
  addMessages: (sessionId: string, messages: ChatMessage[]) => void;
  setMessages: (sessionId: string, messages: ChatMessage[]) => void;
  removeMessage: (sessionId: string, messageId: string) => void;
  clearSession: (sessionId: string) => void;
  clearAllSessions: () => void;

  // Getters
  getMessages: (sessionId: string) => ChatMessage[];
  getDeduplicatedMessages: (sessionId: string) => ChatMessage[];
}

// Deduplication by message id, single pass.
//
// This used to also carry a "near-duplicate content within 1s" check that
// rebuilt and substring-scanned a signature map for EVERY message — O(n²)
// with full-content keys, lagging the workflow chat as executions streamed.
// That check compared mismatched key formats (`type:content:ts` .includes
// (`type-content`)), so it could effectively never match — the suite pins
// that behavior ("both kept" for same-content messages within 1s). Dropping
// the dead scan keeps the observable behavior and removes the quadratic cost.
export const deduplicateMessages = (messages: ChatMessage[]): ChatMessage[] => {
  const seenIds = new Set<string>();

  return messages.filter((message) => {
    if (seenIds.has(message.id)) {
      return false;
    }
    seenIds.add(message.id);
    return true;
  });
};

export const useChatMessagesStore = create<ChatMessagesState>()(
  subscribeWithSelector((set, get) => ({
    messagesBySession: {},
    currentSessionId: null,

    setCurrentSession: (sessionId: string) => {
      set({ currentSessionId: sessionId });
    },

    addMessage: (sessionId: string, message: ChatMessage) => {
      set((state) => {
        const existingMessages = state.messagesBySession[sessionId] || [];

        // Check if message already exists to prevent duplicates
        const messageExists = existingMessages.some(m => m.id === message.id);
        if (messageExists) {
          console.log(`[DEBUG] Message ${message.id} already exists, skipping add`);
          return state;
        }

        const updatedMessages = [...existingMessages, message];

        return {
          messagesBySession: {
            ...state.messagesBySession,
            [sessionId]: updatedMessages,
          },
        };
      });
    },

    addMessages: (sessionId: string, messages: ChatMessage[]) => {
      set((state) => {
        const existingMessages = state.messagesBySession[sessionId] || [];
        const existingIds = new Set(existingMessages.map(m => m.id));

        // Only add messages that don't already exist
        const newMessages = messages.filter(m => !existingIds.has(m.id));

        if (newMessages.length === 0) {
          return state; // No new messages to add
        }

        const updatedMessages = [...existingMessages, ...newMessages];

        return {
          messagesBySession: {
            ...state.messagesBySession,
            [sessionId]: updatedMessages,
          },
        };
      });
    },

    setMessages: (sessionId: string, messages: ChatMessage[]) => {
      set((state) => ({
        messagesBySession: {
          ...state.messagesBySession,
          [sessionId]: messages,
        },
      }));
    },

    removeMessage: (sessionId: string, messageId: string) => {
      set((state) => {
        const existingMessages = state.messagesBySession[sessionId] || [];
        const updatedMessages = existingMessages.filter(m => m.id !== messageId);

        return {
          messagesBySession: {
            ...state.messagesBySession,
            [sessionId]: updatedMessages,
          },
        };
      });
    },

    clearSession: (sessionId: string) => {
      set((state) => {
        const newMessagesBySession = { ...state.messagesBySession };
        delete newMessagesBySession[sessionId];

        return {
          messagesBySession: newMessagesBySession,
          currentSessionId: state.currentSessionId === sessionId ? null : state.currentSessionId,
        };
      });
    },

    clearAllSessions: () => {
      set({
        messagesBySession: {},
        currentSessionId: null,
      });
    },

    getMessages: (sessionId: string) => {
      const state = get();
      return state.messagesBySession[sessionId] || [];
    },

    getDeduplicatedMessages: (sessionId: string) => {
      const state = get();
      const messages = state.messagesBySession[sessionId] || [];
      return deduplicateMessages(messages);
    },
  }))
);