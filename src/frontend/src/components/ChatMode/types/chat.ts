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
  /** Names of knowledge files attached to a user message (shown as chips). */
  attachments?: string[];
}

export interface ChatSession {
  id: string;
  title: string;
  createdAt: Date;
  updatedAt: Date;
  /** Workspace (group) this session belongs to — chat sessions are per-workspace. */
  groupId?: string;
  /** In-flight crew job for refresh reconnect (set while running). */
  runningJobId?: string;
}

export interface AppConfig {
  apiUrl: string;
  email: string;
  groupId: string;
  accessToken: string;
}
