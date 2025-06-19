import { useState, useEffect, useCallback, useRef } from 'react';
import { SaveMessageRequest, ChatSession, ChatMessage as BackendChatMessage } from '../../../api/ChatHistoryService';
import { ChatHistoryServiceEnhanced as ChatHistoryService } from '../../../api/ChatHistoryServiceEnhanced';
import { ChatMessage } from '../types';
import { v4 as uuidv4 } from 'uuid';

export const useChatSession = (providedChatSessionId?: string) => {
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [sessionId, setSessionId] = useState<string>(providedChatSessionId || uuidv4());
  const [chatSessions, setChatSessions] = useState<ChatSession[]>([]);
  const [isLoadingSessions, setIsLoadingSessions] = useState(false);
  const [currentSessionName, setCurrentSessionName] = useState('New Chat');
  const [chatHistoryDisabled, setChatHistoryDisabled] = useState(false);
  
  // Track consecutive failures for grace period
  const consecutiveFailures = useRef(0);
  const FAILURE_THRESHOLD = 3; // Show warning after 3 consecutive failures

  // Initialize session on component mount or when providedChatSessionId changes
  useEffect(() => {
    const initializeSession = () => {
      if (providedChatSessionId) {
        console.log(`[WorkflowChat] Using provided chat session ID: ${providedChatSessionId}`);
        setSessionId(providedChatSessionId);
      } else {
        const newSessionId = ChatHistoryService.generateSessionId();
        console.log(`[WorkflowChat] Generated new chat session ID: ${newSessionId}`);
        setSessionId(newSessionId);
      }
    };
    
    initializeSession();
  }, [providedChatSessionId]);

  // Convert backend message to frontend format
  const convertBackendMessage = (msg: BackendChatMessage): ChatMessage => {
    const baseMessage: ChatMessage = {
      id: msg.id,
      type: msg.message_type as 'user' | 'assistant' | 'execution' | 'trace',
      content: msg.content,
      timestamp: new Date(msg.timestamp),
      intent: msg.intent,
      confidence: msg.confidence ? parseFloat(msg.confidence) : undefined,
      result: msg.generation_result,
    };

    // For execution and trace messages, restore additional fields from generation_result
    if ((msg.message_type === 'execution' || msg.message_type === 'trace') && msg.generation_result) {
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
      
      try {
        console.log(`[WorkflowChat] Loading chat history for session: ${sessionId}`);
        setMessages([]);
        
        try {
          const response = await ChatHistoryService.getSessionMessages(sessionId);
          if (response.messages && response.messages.length > 0) {
            const loadedMessages = response.messages.map(convertBackendMessage);
            setMessages(loadedMessages);
            console.log(`[WorkflowChat] Loaded ${loadedMessages.length} messages for session: ${sessionId}`);
          } else {
            console.log(`[WorkflowChat] Initialized new/empty chat session: ${sessionId}`);
          }
        } catch (sessionError) {
          console.log(`[WorkflowChat] Starting new chat session: ${sessionId}`);
        }
      } catch (error) {
        console.error('Error loading chat history:', error);
      }
    };

    loadChatHistory();
  }, [sessionId]);

  // Save message to backend
  const saveMessageToBackend = useCallback(async (message: ChatMessage): Promise<void> => {
    if (!sessionId || chatHistoryDisabled) return;
    
    try {
      let generationResult = message.result;
      if (message.type === 'execution' || message.type === 'trace') {
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

      await ChatHistoryService.saveMessage(saveRequest);
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
          content: '⚠️ Chat history is temporarily disabled due to service issues. Your messages will not be saved.',
          timestamp: new Date(),
        };
        
        setMessages(prev => {
          const hasErrorMessage = prev.some(msg => msg.content.includes('Chat history is temporarily disabled'));
          if (!hasErrorMessage) {
            return [...prev, errorMessage];
          }
          return prev;
        });
      }
    }
  }, [sessionId, chatHistoryDisabled, FAILURE_THRESHOLD]);

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
      setMessages(prev => [...prev, errorMessage]);
    } finally {
      setIsLoadingSessions(false);
    }
  };

  // Load messages from a specific session
  const loadSessionMessages = async (selectedSessionId: string) => {
    setIsLoadingSessions(true);
    try {
      const response = await ChatHistoryService.getSessionMessages(selectedSessionId);
      
      const sessionJobNames = JSON.parse(localStorage.getItem('chatSessionJobNames') || '{}');
      const jobName = sessionJobNames[selectedSessionId];
      if (jobName) {
        setCurrentSessionName(jobName);
      } else {
        setCurrentSessionName('New Chat');
      }
      
      const loadedMessages = response.messages.map(convertBackendMessage);
      setMessages(loadedMessages);
      setSessionId(selectedSessionId);
      setCurrentSessionName(`Session from ${new Date(response.messages[0]?.timestamp || Date.now()).toLocaleDateString()}`);
    } catch (error) {
      console.error('Error loading session messages:', error);
      const errorMessage: ChatMessage = {
        id: `error-${Date.now()}`,
        type: 'assistant',
        content: '❌ Failed to load session messages. Please try again.',
        timestamp: new Date(),
      };
      setMessages(prev => [...prev, errorMessage]);
    } finally {
      setIsLoadingSessions(false);
    }
  };

  // Create a new chat session
  const startNewChat = () => {
    const newSessionId = ChatHistoryService.generateSessionId();
    setSessionId(newSessionId);
    setMessages([]);
    setCurrentSessionName('New Chat');
  };

  return {
    messages,
    setMessages,
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