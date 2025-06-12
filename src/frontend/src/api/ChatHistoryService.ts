import ApiService from './ApiService';

export interface ChatMessage {
  id: string;
  session_id: string;
  user_id: string;
  message_type: 'user' | 'assistant';
  content: string;
  timestamp: string;
  intent?: string;
  confidence?: string;
  generation_result?: unknown;
  group_id?: string;
  group_email?: string;
}

export interface SaveMessageRequest {
  session_id: string;
  message_type: 'user' | 'assistant';
  content: string;
  intent?: string;
  confidence?: number;
  generation_result?: unknown;
}

export interface ChatSession {
  session_id: string;
  user_id: string;
  latest_timestamp: string;
  message_count?: number;
}

export interface ChatHistoryListResponse {
  messages: ChatMessage[];
  total_messages: number;
  page: number;
  per_page: number;
  session_id: string;
}

export interface ChatSessionListResponse {
  sessions: ChatSession[];
  total_sessions: number;
  page: number;
  per_page: number;
}

class ChatHistoryService {
  private baseUrl: string;

  constructor() {
    this.baseUrl = '/chat-history';
  }

  /**
   * Save a chat message to the backend
   */
  async saveMessage(messageData: SaveMessageRequest): Promise<ChatMessage> {
    const response = await ApiService.post(`${this.baseUrl}/messages`, messageData);
    return response.data;
  }

  /**
   * Get messages for a specific chat session
   */
  async getSessionMessages(
    sessionId: string, 
    page = 0, 
    perPage = 50
  ): Promise<ChatHistoryListResponse> {
    const response = await ApiService.get(
      `${this.baseUrl}/sessions/${sessionId}/messages`,
      {
        params: { page, per_page: perPage }
      }
    );
    return response.data;
  }

  /**
   * Get user's chat sessions (latest message from each session)
   */
  async getUserSessions(
    page = 0, 
    perPage = 20
  ): Promise<ChatMessage[]> {
    const response = await ApiService.get(
      `${this.baseUrl}/users/sessions`,
      {
        params: { page, per_page: perPage }
      }
    );
    return response.data;
  }

  /**
   * Get group chat sessions with optional user filtering
   */
  async getGroupSessions(
    page = 0, 
    perPage = 20,
    userId?: string
  ): Promise<ChatSessionListResponse> {
    const params: Record<string, string | number> = { page, per_page: perPage };
    if (userId) {
      params.user_id = userId;
    }

    const response = await ApiService.get(`${this.baseUrl}/sessions`, {
      params
    });
    return response.data;
  }

  /**
   * Delete a complete chat session
   */
  async deleteSession(sessionId: string): Promise<void> {
    await ApiService.delete(`${this.baseUrl}/sessions/${sessionId}`);
  }

  /**
   * Create a new chat session
   */
  async createNewSession(): Promise<{ session_id: string }> {
    const response = await ApiService.post(`${this.baseUrl}/sessions/new`);
    return response.data;
  }

  /**
   * Generate a new session ID locally (UUID)
   */
  generateSessionId(): string {
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
      const r = Math.random() * 16 | 0;
      const v = c === 'x' ? r : (r & 0x3 | 0x8);
      return v.toString(16);
    });
  }
}

export default new ChatHistoryService();