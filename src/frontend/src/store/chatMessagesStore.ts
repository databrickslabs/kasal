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

// Advanced deduplication logic
const deduplicateMessages = (messages: ChatMessage[]): ChatMessage[] => {
  const seenIds = new Set<string>();
  const seenMessages = new Map<string, ChatMessage>();

  return messages.filter((message) => {
    // Check for exact ID duplicates
    if (seenIds.has(message.id)) {
      console.log(`[DEBUG] Duplicate message ID detected: ${message.id}`);
      return false;
    }

    // Create a unique signature for content deduplication
    const messageSignature = `${message.type}:${message.content}:${message.timestamp.getTime()}`;
    const contentKey = `${message.type}-${message.content.substring(0, 100)}`;

    // Check for near-duplicate content within a very small time window (1 second)
    const seenMessagesArray = Array.from(seenMessages.entries());
    for (const [existingKey, existingMessage] of seenMessagesArray) {
      const timeDiff = Math.abs(message.timestamp.getTime() - existingMessage.timestamp.getTime());
      const isNearDuplicate = existingKey.includes(contentKey) && timeDiff < 1000;

      if (isNearDuplicate && existingMessage.type === message.type) {
        console.log(`[DEBUG] Near-duplicate message filtered:`, {
          existing: existingMessage.id,
          duplicate: message.id,
          timeDiff,
          content: message.content.substring(0, 50) + '...'
        });
        return false;
      }
    }

    seenIds.add(message.id);
    seenMessages.set(messageSignature, message);
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