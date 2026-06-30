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
  /**
   * The execution (job) id of the run this message anchors. The deliverable
   * shown in the preview pane is derived on demand from that execution's stored
   * result (findUiSurface), so the output survives navigating away — no separate
   * per-session preview copy is needed. Persisted in the __chatmode extras.
   */
  executionId?: string;
  /** Whether the run that produced this message used workspace memory. */
  usedWorkspaceMemory?: boolean;
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
