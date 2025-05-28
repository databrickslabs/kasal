import { UploadedFileInfo } from '../api/UploadService';

export interface KnowledgeSource {
  type: string;
  source: string;
  metadata?: Record<string, unknown>;
  fileInfo?: UploadedFileInfo;
}

export interface StepCallback {
  (step: {
    agent: string;
    message: string;
    timestamp: string;
  }): void;
}

export interface Tool {
  id?: string;
  title: string;
  description: string;
  icon?: string;
  enabled?: boolean;
}

export interface KnowledgeSourcesSectionProps {
  knowledgeSources: KnowledgeSource[];
  onChange: (sources: KnowledgeSource[]) => void;
}

export interface SavedAgentsProps {
  refreshTrigger: number;
}

/**
 * Embedding configuration for agent memory
 */
export interface EmbedderConfig {
  /** Embedding provider (e.g., "openai", "ollama", "google", etc.) */
  provider: string;
  /** Configuration specific to the provider */
  config: {
    /** Model name to use for embeddings */
    model: string;
    [key: string]: unknown;
  };
}

export interface Agent {
  id?: string;
  name: string;
  role: string;
  goal: string;
  backstory: string;
  llm: string;
  tools: string[];
  function_calling_llm?: string;
  max_iter: number;
  max_rpm?: number;
  max_execution_time?: number;
  /** 
   * Enable agent memory (short-term, long-term, and entity memory)
   * When enabled, the agent can remember past interactions and context
   */
  memory?: boolean;
  verbose: boolean;
  allow_delegation: boolean;
  step_callback?: StepCallback;
  cache: boolean;
  system_template?: string;
  prompt_template?: string;
  response_template?: string;
  allow_code_execution: boolean;
  code_execution_mode: 'safe' | 'unsafe';
  max_retry_limit?: number;
  use_system_prompt?: boolean;
  respect_context_window?: boolean;
  reasoning?: boolean;
  max_reasoning_attempts?: number;
  max_tokens?: number;
  max_context_window_size?: number;
  /**
   * Configuration for embedding models used by memory systems
   * Used for short-term and entity memory with RAG
   */
  embedder_config?: EmbedderConfig;
  knowledge_sources?: KnowledgeSource[];
  created_at?: string;
}

export interface AgentGenerationDialogProps {
  open: boolean;
  onClose: () => void;
  onAgentGenerated: (agent: Agent) => void;
  selectedModel?: string;
  tools?: Tool[];
  selectedTools?: string[];
  onToolsChange?: (selectedTools: string[]) => void;
}

export interface AgentFormProps {
  initialData?: Partial<Agent> | null;
  onCancel: () => void;
  onAgentSaved?: (agent?: Agent) => void;
  onSubmit?: (agent: Agent) => Promise<void>;
  isEdit?: boolean;
  tools: Tool[];
}

export interface NotificationState {
  open: boolean;
  message: string;
  severity: 'success' | 'error' | 'info' | 'warning';
}

export interface AgentDialogProps {
  open: boolean;
  onClose: () => void;
  onAgentSelect: (agents: Agent[]) => void;
  agents: Agent[];
  onShowAgentForm: () => void;
  fetchAgents: () => Promise<void>;
  showErrorMessage: (message: string, severity?: 'error' | 'warning' | 'info' | 'success') => void;
} 