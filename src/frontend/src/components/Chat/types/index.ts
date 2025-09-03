import { Node, Edge } from 'reactflow';
import { Agent } from '../../../types/agent';
import { Task } from '../../../types/task';

export interface GeneratedAgent {
  name: string;
  role: string;
  goal: string;
  backstory: string;
  tools?: string[];
  advanced_config?: {
    llm?: string;
    [key: string]: unknown;
  };
}

export interface GeneratedTask {
  name: string;
  description: string;
  expected_output: string;
  tools?: string[];
  advanced_config?: {
    human_input?: boolean;
    async_execution?: boolean;
    [key: string]: unknown;
  };
}

export interface GeneratedCrew {
  agents?: Agent[];
  tasks?: Task[];
}

export interface ChatMessage {
  id: string;
  type: 'user' | 'assistant' | 'execution' | 'trace' | 'result';
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

export interface ModelConfig {
  name: string;
  temperature?: number;
  context_window?: number;
  max_output_tokens?: number;
  enabled: boolean;
  provider?: string;
}

export interface WorkflowChatProps {
  onNodesGenerated?: (nodes: Node[], edges: Edge[]) => void;
  onLoadingStateChange?: (isLoading: boolean) => void;
  selectedModel?: string;
  selectedTools?: string[];
  isVisible?: boolean;
  setSelectedModel?: (model: string) => void;
  nodes?: Node[];
  edges?: Edge[];
  onExecuteCrew?: () => void;
  onToggleCollapse?: () => void;
  chatSessionId?: string;
  onOpenLogs?: (jobId: string) => void;
}