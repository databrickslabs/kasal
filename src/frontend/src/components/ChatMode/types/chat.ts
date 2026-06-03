import { IntentType } from './dispatcher';

export interface ChatMessage {
  id: string;
  sessionId?: string;
  role: 'user' | 'assistant' | 'system';
  content: string;
  timestamp: Date;
  intent?: IntentType;
  resultType?: string;
  resultData?: unknown;
  isStreaming?: boolean;
}

export interface ChatSession {
  id: string;
  title: string;
  createdAt: Date;
  updatedAt: Date;
}

export interface AppConfig {
  apiUrl: string;
  email: string;
  groupId: string;
  accessToken: string;
}
