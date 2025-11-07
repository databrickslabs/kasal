import { create } from 'zustand';
import { persist } from 'zustand/middleware';

export interface ChatMessage {
  id: string;
  type: 'user' | 'assistant' | 'execution' | 'trace';
  content: string;
  timestamp: Date;
  intent?: string;
  confidence?: number;
  result?: unknown;
  isIntermediate?: boolean;
  eventSource?: string;
  eventContext?: string;
  eventType?: string;
  jobId?: string;
}

export interface ChatSession {
  sessionId: string;
  messages: ChatMessage[];
  sessionName: string;
  createdAt: Date;
  lastMessageAt: Date;
}

interface ChatState {
  // Current session state
  currentSessionId: string | null;
  messages: ChatMessage[];
  isLoading: boolean;
  executingJobId: string | null;
  lastExecutionJobId: string | null;
  
  // Session management
  sessions: Map<string, ChatSession>;
  
  // Actions
  setCurrentSessionId: (sessionId: string) => void;
  addMessage: (message: ChatMessage) => void;
  setMessages: (messages: ChatMessage[]) => void;
  clearMessages: () => void;
  setIsLoading: (isLoading: boolean) => void;
  setExecutingJobId: (jobId: string | null) => void;
  setLastExecutionJobId: (jobId: string | null) => void;
  
  // Session actions
  createSession: (sessionId: string, sessionName?: string) => void;
  loadSession: (sessionId: string) => void;
  deleteSession: (sessionId: string) => void;
  updateSessionName: (sessionId: string, name: string) => void;
  
  // Utility actions
  removeMessage: (messageId: string) => void;
  updateMessage: (messageId: string, updates: Partial<ChatMessage>) => void;
  getSessionMessages: (sessionId: string) => ChatMessage[];
}

const initialState = {
  currentSessionId: null,
  messages: [],
  isLoading: false,
  executingJobId: null,
  lastExecutionJobId: null,
  sessions: new Map(),
};

export const useChatStore = create<ChatState>()(
  persist(
    (set, get) => ({
      ...initialState,
      
      setCurrentSessionId: (sessionId: string) => 
        set({ currentSessionId: sessionId }),
      
      addMessage: (message: ChatMessage) =>
        set((state) => {
          const updatedMessages = [...state.messages, message];
          
          // Update session if we have one
          if (state.currentSessionId) {
            const session = state.sessions.get(state.currentSessionId);
            if (session) {
              const updatedSession = {
                ...session,
                messages: updatedMessages,
                lastMessageAt: new Date(),
              };
              state.sessions.set(state.currentSessionId, updatedSession);
            }
          }
          
          return { 
            messages: updatedMessages,
            sessions: new Map(state.sessions),
          };
        }),
      
      setMessages: (messages: ChatMessage[]) =>
        set((state) => {
          // Update session if we have one
          if (state.currentSessionId) {
            const session = state.sessions.get(state.currentSessionId);
            if (session) {
              const updatedSession = {
                ...session,
                messages,
                lastMessageAt: messages.length > 0 ? new Date() : session.lastMessageAt,
              };
              state.sessions.set(state.currentSessionId, updatedSession);
            }
          }
          
          return { 
            messages,
            sessions: new Map(state.sessions),
          };
        }),
      
      clearMessages: () => 
        set({ messages: [] }),
      
      setIsLoading: (isLoading: boolean) => 
        set({ isLoading }),
      
      setExecutingJobId: (jobId: string | null) => 
        set({ executingJobId: jobId }),
      
      setLastExecutionJobId: (jobId: string | null) => 
        set({ lastExecutionJobId: jobId }),
      
      createSession: (sessionId: string, sessionName?: string) =>
        set((state) => {
          const newSession: ChatSession = {
            sessionId,
            messages: [],
            sessionName: sessionName || 'New Chat',
            createdAt: new Date(),
            lastMessageAt: new Date(),
          };
          
          state.sessions.set(sessionId, newSession);
          
          return {
            currentSessionId: sessionId,
            messages: [],
            sessions: new Map(state.sessions),
          };
        }),
      
      loadSession: (sessionId: string) =>
        set((state) => {
          const session = state.sessions.get(sessionId);
          if (session) {
            return {
              currentSessionId: sessionId,
              messages: session.messages,
            };
          }
          return state;
        }),
      
      deleteSession: (sessionId: string) =>
        set((state) => {
          state.sessions.delete(sessionId);
          
          // If we're deleting the current session, clear it
          if (state.currentSessionId === sessionId) {
            return {
              currentSessionId: null,
              messages: [],
              sessions: new Map(state.sessions),
            };
          }
          
          return {
            sessions: new Map(state.sessions),
          };
        }),
      
      updateSessionName: (sessionId: string, name: string) =>
        set((state) => {
          const session = state.sessions.get(sessionId);
          if (session) {
            const updatedSession = {
              ...session,
              sessionName: name,
            };
            state.sessions.set(sessionId, updatedSession);
          }
          
          return {
            sessions: new Map(state.sessions),
          };
        }),
      
      removeMessage: (messageId: string) =>
        set((state) => ({
          messages: state.messages.filter(msg => msg.id !== messageId),
        })),
      
      updateMessage: (messageId: string, updates: Partial<ChatMessage>) =>
        set((state) => ({
          messages: state.messages.map(msg => 
            msg.id === messageId ? { ...msg, ...updates } : msg
          ),
        })),
      
      getSessionMessages: (sessionId: string) => {
        const state = get();
        const session = state.sessions.get(sessionId);
        return session?.messages || [];
      },
    }),
    {
      name: 'chat-storage',
      partialize: (state) => ({
        // Only persist session metadata, not actual messages
        // Messages should be loaded from backend
        sessions: Array.from(state.sessions.entries()).map(([id, session]) => ({
          id,
          sessionName: session.sessionName,
          createdAt: session.createdAt,
          lastMessageAt: session.lastMessageAt,
        })),
      }),
    }
  )
);