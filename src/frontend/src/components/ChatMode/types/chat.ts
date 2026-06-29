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
  /**
   * Whether THIS run actually used workspace memory (the "Workspace memory"
   * mode was on when it ran). Captured per-run at dispatch — NOT the live
   * toggle — so the "Memory graph" action only shows for runs that wrote
   * workspace memory. A later toggle to workspace memory must not retroactively
   * reveal the graph on a run that ran in session-only mode. Persisted in the
   * __chatmode extras.
   */
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
