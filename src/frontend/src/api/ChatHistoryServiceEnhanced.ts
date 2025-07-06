import axios, { AxiosError } from 'axios';
import { config } from '../config/api/ApiConfig';
import {
  ChatMessage,
  SaveMessageRequest,
  ChatHistoryListResponse,
  ChatSessionListResponse
} from './ChatHistoryService';

// Create a dedicated axios instance for chat history with longer timeout
const chatHistoryClient = axios.create({
  baseURL: config.apiUrl,
  headers: {
    'Content-Type': 'application/json',
  },
  timeout: 30000, // 30 seconds timeout
});

// Add request interceptor for authentication
chatHistoryClient.interceptors.request.use(
  (config) => {
    const token = localStorage.getItem('token');
    if (token) {
      config.headers.Authorization = `Bearer ${token}`;
    }
    
    // For local development: add mock tenant headers
    const mockUserEmail = localStorage.getItem('mockUserEmail');
    if (mockUserEmail && process.env.NODE_ENV === 'development') {
      config.headers['X-Forwarded-Email'] = mockUserEmail;
      config.headers['X-Forwarded-Access-Token'] = 'mock-token-for-dev';
      console.log(`[DEV] Using mock user: ${mockUserEmail}`);
    }
    
    return config;
  },
  (error) => {
    return Promise.reject(error);
  }
);

// Add response interceptor for error handling
chatHistoryClient.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error.response) {
      console.error('[ChatHistory] API Error Response:', {
        status: error.response.status,
        data: error.response.data,
        headers: error.response.headers,
      });
    } else if (error.request) {
      console.error('[ChatHistory] API No Response:', error.request);
    } else {
      console.error('[ChatHistory] API Request Error:', error.message);
    }
    return Promise.reject(error);
  }
);

// Retry configuration
const MAX_RETRIES = 3;
const RETRY_DELAY_BASE = 1000; // 1 second base delay

// Helper function to determine if error is retryable
const isRetryableError = (error: AxiosError): boolean => {
  if (!error.response) {
    // Network errors are retryable
    return true;
  }
  
  const status = error.response.status;
  // Retry on 5xx errors and specific 4xx errors
  return status >= 500 || status === 408 || status === 429;
};

// Helper function to calculate retry delay with exponential backoff
const getRetryDelay = (attempt: number): number => {
  return RETRY_DELAY_BASE * Math.pow(2, attempt - 1) + Math.random() * 1000;
};

// Retry wrapper function
async function withRetry<T>(
  operation: () => Promise<T>,
  operationName: string
): Promise<T> {
  let lastError: Error | undefined;
  
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      return await operation();
    } catch (error) {
      lastError = error as Error;
      
      if (attempt === MAX_RETRIES || !isRetryableError(error as AxiosError)) {
        console.error(`[ChatHistory] ${operationName} failed after ${attempt} attempts:`, error);
        throw error;
      }
      
      const delay = getRetryDelay(attempt);
      console.warn(`[ChatHistory] ${operationName} failed (attempt ${attempt}/${MAX_RETRIES}), retrying in ${delay}ms...`);
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }
  
  throw lastError;
}

export class ChatHistoryServiceEnhanced {
  /**
   * Save a chat message to the backend with retry logic
   */
  static async saveMessage(messageData: SaveMessageRequest): Promise<ChatMessage> {
    return withRetry(
      async () => {
        const response = await chatHistoryClient.post<ChatMessage>('/chat-history/messages', messageData);
        return response.data;
      },
      'saveMessage'
    );
  }

  /**
   * Get messages for a specific chat session with retry logic
   */
  static async getSessionMessages(
    sessionId: string, 
    page = 0, 
    perPage = 50
  ): Promise<ChatHistoryListResponse> {
    return withRetry(
      async () => {
        const response = await chatHistoryClient.get<ChatHistoryListResponse>(
          `/chat-history/sessions/${sessionId}/messages`,
          {
            params: { page, per_page: perPage }
          }
        );
        return response.data;
      },
      'getSessionMessages'
    );
  }

  /**
   * Get user's chat sessions (latest message from each session) with retry logic
   */
  static async getUserSessions(
    page = 0, 
    perPage = 20
  ): Promise<ChatMessage[]> {
    return withRetry(
      async () => {
        const response = await chatHistoryClient.get<ChatMessage[]>(
          '/chat-history/users/sessions',
          {
            params: { page, per_page: perPage }
          }
        );
        return response.data;
      },
      'getUserSessions'
    );
  }

  /**
   * Get group chat sessions with optional user filtering and retry logic
   */
  static async getGroupSessions(
    page = 0, 
    perPage = 20,
    userId?: string
  ): Promise<ChatSessionListResponse> {
    return withRetry(
      async () => {
        const params: Record<string, string | number> = { page, per_page: perPage };
        if (userId) {
          params.user_id = userId;
        }

        const response = await chatHistoryClient.get<ChatSessionListResponse>('/chat-history/sessions', {
          params
        });
        return response.data;
      },
      'getGroupSessions'
    );
  }

  /**
   * Delete a complete chat session with retry logic
   */
  static async deleteSession(sessionId: string): Promise<void> {
    return withRetry(
      async () => {
        await chatHistoryClient.delete(`/chat-history/sessions/${sessionId}`);
      },
      'deleteSession'
    );
  }

  /**
   * Create a new chat session with retry logic
   */
  static async createNewSession(): Promise<{ session_id: string }> {
    return withRetry(
      async () => {
        const response = await chatHistoryClient.post<{ session_id: string }>('/chat-history/sessions/new');
        return response.data;
      },
      'createNewSession'
    );
  }

  /**
   * Generate a new session ID locally (UUID)
   */
  static generateSessionId(): string {
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
      const r = Math.random() * 16 | 0;
      const v = c === 'x' ? r : (r & 0x3 | 0x8);
      return v.toString(16);
    });
  }
}