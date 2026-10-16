import { useShallow } from 'zustand/react/shallow';
import { useBuilderCanvasStore } from '../../../../app/sessions/builderCanvasStore';
import { useState, useEffect, useCallback, useRef } from 'react';
import { SaveMessageRequest, ChatSession, ChatMessage as BackendChatMessage } from '../../../../api/chat/ChatHistoryService';
import { ChatHistoryServiceEnhanced as ChatHistoryService } from '../../../../api/chat/ChatHistoryServiceEnhanced';
import { ChatMessage } from '../types/index';
import { v4 as uuidv4 } from 'uuid';
import { useChatMessagesStore, deduplicateMessages } from '../store/chatMessagesStore';

/** The backend caps per_page at 100 and pages oldest-first with OFFSET/LIMIT. */
const HISTORY_PAGE_SIZE = 100;

/**
 * Every message of a session, oldest first. One default page was only the
 * OLDEST 50 rows, so a long builder conversation reopened without its newest
 * turns (the latest run results included). No page cap: pages run oldest
 * first, so any cap would drop exactly the newest messages. The walk ends at
 * the first short (or empty) page, which a finite table always reaches.
 */
async function loadAllMessages(sessionId: string): Promise<BackendChatMessage[]> {
  const all: BackendChatMessage[] = [];
  for (let page = 0; ; page += 1) {
    const response = await ChatHistoryService.getSessionMessages(sessionId, page, HISTORY_PAGE_SIZE);
    const batch = response?.messages || [];
    all.push(...batch);
    const total = response?.total_messages;
    if (batch.length < HISTORY_PAGE_SIZE || (typeof total === 'number' && all.length >= total)) break;
  }
  return all;
}

export const useChatSession = (providedChatSessionId?: string) => {
  const [sessionId, setSessionId] = useState<string>(providedChatSessionId || uuidv4());

  // Use Zustand store for messages
  const { setMessages: setZustandMessages, addMessage } = useChatMessagesStore(useShallow(state => ({
    setMessages: state.setMessages,
    addMessage: state.addMessage,
  })));
  const [chatSessions, setChatSessions] = useState<ChatSession[]>([]);
  const [isLoadingSessions, setIsLoadingSessions] = useState(false);
  const [currentSessionName, setCurrentSessionName] = useState('New Chat');
  const [chatHistoryDisabled, setChatHistoryDisabled] = useState(false);
  
  // Track consecutive failures for grace period
  const consecutiveFailures = useRef(0);
  const FAILURE_THRESHOLD = 3; // Show warning after 3 consecutive failures

  // Guard: when true, loadChatHistory will skip the merge to avoid
  // overwriting in-flight messages that haven't reached the backend yet.
  const sendInProgress = useRef(false);

  // Initialize session on component mount or when providedChatSessionId changes
  useEffect(() => {
    const initializeSession = () => {
      if (providedChatSessionId) {
        setSessionId(providedChatSessionId);
      } else {
        const newSessionId = ChatHistoryService.generateSessionId();
        setSessionId(newSessionId);
      }
    };
    
    initializeSession();
  }, [providedChatSessionId]);

  // Convert backend message to frontend format
  const convertBackendMessage = (msg: BackendChatMessage): ChatMessage => {
    const baseMessage: ChatMessage = {
      id: msg.id,
      backendId: msg.id,
      type: msg.message_type as ChatMessage['type'],
      content: msg.content || '',
      timestamp: new Date(msg.timestamp),
      intent: msg.intent,
      confidence: msg.confidence ? parseFloat(msg.confidence) : undefined,
      result: msg.generation_result,
    };

    // For execution and trace messages, restore additional fields from generation_result
    if ((msg.message_type === 'execution' || msg.message_type === 'trace' || msg.message_type === 'result') && msg.generation_result) {
      const genResult = msg.generation_result as Record<string, unknown> & {
        jobId?: string;
        agentName?: string;
        taskName?: string;
        isIntermediate?: boolean;
      };
      baseMessage.jobId = genResult.jobId;
      baseMessage.eventSource = genResult.agentName;
      baseMessage.eventContext = genResult.taskName;
      baseMessage.isIntermediate = genResult.isIntermediate;
      
      // Remove the additional fields from result to clean it up
      if (genResult.jobId !== undefined || genResult.agentName !== undefined || 
          genResult.taskName !== undefined || genResult.isIntermediate !== undefined) {
        const { jobId, agentName, taskName, isIntermediate, ...cleanResult } = genResult;
        baseMessage.result = Object.keys(cleanResult).length > 0 ? cleanResult : undefined;
      }
    }

    return baseMessage;
  };

  // Load chat history when session is set or changes
  useEffect(() => {
    const loadChatHistory = async () => {
      if (!sessionId) return;

      // Skip if a message send is in progress – the save hasn't reached the
      // backend yet, so the response would lack the user's latest message.
      // Merging now would risk overwriting in-memory-only messages.
      if (sendInProgress.current) {
        console.log('[ChatHistory] Skipping loadChatHistory – send in progress');
        return;
      }

      try {
        try {
          const stored = await loadAllMessages(sessionId);
          if (stored.length > 0) {
            const loadedMessages = stored.map(convertBackendMessage);
            // Merge loaded messages with any that arrived during the async fetch
            // (e.g. real-time trace messages from SSE). This avoids a race condition
            // where setMessages would wipe messages added by addMessage during the fetch.
            const currentMessages = useChatMessagesStore.getState().messagesBySession[sessionId] || [];
            // A message saved from this page keeps its client id in memory and is
            // stored under a server id (its backendId). Without matching on that,
            // switching away and back showed every message of this page twice.
            // The in-memory copy takes the stored row's place, so the order stays
            // the server's and code holding the client id still finds it.
            const savedHere = new Map(currentMessages
              .filter(m => m.backendId && m.backendId !== m.id)
              .map(m => [m.backendId as string, m]));
            const merged = deduplicateMessages([
              ...loadedMessages.map(m => savedHere.get(m.id) ?? m),
              ...currentMessages,
            ]);
            setZustandMessages(sessionId, merged);
          }
          // If no messages from backend, keep whatever is in the store (don't clear)
        } catch (sessionError) {
          // Session loading failed, continue with existing messages
        }
      } catch (error) {
        console.error('Error loading chat history:', error);
      }
    };

    loadChatHistory();
  }, [sessionId, setZustandMessages]);

  // Save message to backend
  const saveMessageToBackend = useCallback(async (message: ChatMessage): Promise<void> => {
    if (sessionId && message.type === 'user') useBuilderCanvasStore.getState().nameSessionFromPrompt(sessionId, message.content || '');
    // A failure streak does NOT stop saving: that switched history off for the
    // rest of the page, so the re-enable branch below could never run and every
    // later turn, in every session, was silently dropped.
    if (!sessionId) return;
    // Skip saving messages with empty content (backend requires min_length=1)
    if (!message.content) return;

    // Signal that a save is in progress so loadChatHistory won't overwrite
    // in-memory messages that haven't reached the backend yet.
    sendInProgress.current = true;

    try {
      let generationResult = message.result;
      if (message.type === 'execution' || message.type === 'trace' || message.type === 'result') {
        generationResult = {
          ...(message.result || {}),
          jobId: message.jobId,
          agentName: message.eventSource,
          taskName: message.eventContext,
          isIntermediate: message.isIntermediate
        };
      }

      const saveRequest: SaveMessageRequest = {
        session_id: sessionId,
        message_type: message.type,
        content: message.content,
        intent: message.intent,
        confidence: message.confidence,
        generation_result: generationResult
      };

      const saved = await ChatHistoryService.saveMessage(saveRequest);
      if (saved?.id) {
        const store = useChatMessagesStore.getState();
        const latest = store.messagesBySession[sessionId]?.find(item => item.id === message.id);
        store.updateMessage(sessionId, message.id, { backendId: saved.id });
        // A surface may finish composing while the original text is saving.
        if (latest && latest.content !== message.content) {
          await ChatHistoryService.updateMessageContent(saved.id, latest.content);
        }
      }
      console.log(`[ChatHistory] Message saved successfully to session ${sessionId}`);
      
      // Reset failure counter on success
      consecutiveFailures.current = 0;
      
      // Re-enable chat history if it was disabled and we're now succeeding
      if (chatHistoryDisabled) {
        setChatHistoryDisabled(false);
        console.log('[ChatHistory] Chat history re-enabled after successful save');
      }
    } catch (error) {
      console.error('[ChatHistory] Error saving message to backend:', error);

      // Increment failure counter
      consecutiveFailures.current += 1;
      console.log(`[ChatHistory] Consecutive failures: ${consecutiveFailures.current}/${FAILURE_THRESHOLD}`);

      // Only show warning after multiple consecutive failures
      if (consecutiveFailures.current >= FAILURE_THRESHOLD && !chatHistoryDisabled) {
        setChatHistoryDisabled(true);

        const errorMessage: ChatMessage = {
          id: `error-${Date.now()}`,
          type: 'assistant',
          content: '⚠️ Chat history could not be saved due to service issues. Recent messages may be missing after a reload; saving resumes when the service recovers.',
          timestamp: new Date(),
        };

        // Add error message to Zustand store
        addMessage(sessionId, errorMessage);
      }
    } finally {
      sendInProgress.current = false;
    }
  }, [sessionId, chatHistoryDisabled, FAILURE_THRESHOLD, addMessage]);

  // Load user's chat sessions
  const loadChatSessions = async () => {
    setIsLoadingSessions(true);
    try {
      const response = await ChatHistoryService.getGroupSessions();
      setChatSessions(response.sessions || []);
    } catch (error) {
      console.error('Error loading chat sessions:', error);
      const errorMessage: ChatMessage = {
        id: `error-${Date.now()}`,
        type: 'assistant',
        content: '❌ Failed to load chat history. Please try again.',
        timestamp: new Date(),
      };
      addMessage(sessionId, errorMessage);
    } finally {
      setIsLoadingSessions(false);
    }
  };

  // Load messages from a specific session
  const loadSessionMessages = async (selectedSessionId: string) => {
    setIsLoadingSessions(true);
    try {
      const stored = await loadAllMessages(selectedSessionId);

      const sessionJobNames = JSON.parse(localStorage.getItem('chatSessionJobNames') || '{}');
      const jobName = sessionJobNames[selectedSessionId];
      if (jobName) {
        setCurrentSessionName(jobName);
      } else {
        setCurrentSessionName('New Chat');
      }
      
      const loadedMessages = stored.map(convertBackendMessage);
      setZustandMessages(selectedSessionId, loadedMessages);
      setSessionId(selectedSessionId);
      setCurrentSessionName(`Session from ${new Date(stored[0]?.timestamp || Date.now()).toLocaleDateString()}`);
    } catch (error) {
      console.error('Error loading session messages:', error);
      const errorMessage: ChatMessage = {
        id: `error-${Date.now()}`,
        type: 'assistant',
        content: '❌ Failed to load session messages. Please try again.',
        timestamp: new Date(),
      };
      addMessage(selectedSessionId, errorMessage);
    } finally {
      setIsLoadingSessions(false);
    }
  };

  // Create a new chat session
  const startNewChat = () => {
    const newSessionId = ChatHistoryService.generateSessionId();
    setSessionId(newSessionId);
    setZustandMessages(newSessionId, []); // Clear messages in Zustand store
    setCurrentSessionName('New Chat');
  };

  return {
    sessionId,
    setSessionId,
    chatSessions,
    setChatSessions,
    isLoadingSessions,
    currentSessionName,
    setCurrentSessionName,
    saveMessageToBackend,
    loadChatSessions,
    loadSessionMessages,
    startNewChat,
  };
};