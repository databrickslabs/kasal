import type { Node, Edge, MarkerType } from 'reactflow';
import type { Agent as FormAgent, KnowledgeSource } from './agent';

export interface NodeData {
  id?: string;
  label: string;
  role?: string;
  goal?: string;
  backstory?: string;
  tools?: string[];
  description?: string;
  expected_output?: string;
  name?: string;
  context?: string[];
  agent_id?: string;
  advanced_config?: {
    async_execution: boolean;
    output_json: string | null;
    output_pydantic: string | null;
    output_file: string | null;
    human_input: boolean;
    markdown: boolean;
    retry_on_fail: boolean;
    max_retries: number;
    timeout: string | number | null;
    priority: number;
    error_handling: 'default' | 'ignore' | 'retry' | 'fail';
    cache_response: boolean;
    cache_ttl: number;
  };
}

export interface CustomNode extends Node<NodeData> {
  data: NodeData;
}

export interface CustomEdge extends Omit<Edge, 'id'> {
  id?: string;
  label?: string;
  animated?: boolean;
  style?: {
    stroke?: string;
  };
  markerEnd?: {
    type: MarkerType;
    color: string;
  };
}

export interface TaskNodeData {
  label?: string;
  name?: string;
  taskId?: string;
  tools?: string[];
  tool_configs?: Record<string, unknown>;  // User-specific tool configuration overrides
  context?: string[];
  async_execution?: boolean;
  config?: {
    cache_response?: boolean;
    cache_ttl?: number;
    retry_on_fail?: boolean;
    max_retries?: number;
    timeout?: number | null;
    priority?: number;
    error_handling?: string;
    output_file?: string | null;
    output_json?: string | null;
    output_pydantic?: string | null;
    callback?: string | null;
    human_input?: boolean;
    markdown?: boolean;
    condition?: string;
    guardrail?: string;
  };
  description?: string;
  expected_output?: string;
}

export interface AgentNodeData {
  label?: string;
  name?: string;
  agentId?: string | number;
  role?: string;
  goal?: string;
  backstory?: string;
  llm?: string;
  function_calling_llm?: string;
  max_iter?: number;
  max_rpm?: number;
  max_execution_time?: number;
  memory?: boolean;
  verbose?: boolean;
  allow_delegation?: boolean;
  cache?: boolean;
  system_template?: string;
  prompt_template?: string;
  response_template?: string;
  allow_code_execution?: boolean;
  code_execution_mode?: 'safe' | 'unsafe';
  max_retry_limit?: number;
  use_system_prompt?: boolean;
  respect_context_window?: boolean;
  reasoning?: boolean;
  max_reasoning_attempts?: number;
  embedder_config?: Record<string, unknown>;
  knowledge_sources?: KnowledgeSource[];
  tools?: string[];
  tool_configs?: Record<string, unknown>;  // User-specific tool configuration overrides
  /** Injects current date into agent's context for time-sensitive tasks */
  inject_date?: boolean;
  /** Custom date format string (e.g., '%B %d, %Y') */
  date_format?: string;
  onEdit?: (agent: FormAgent) => void;
}

export interface TaskNode {
  id: string;
  type: string;
  position: { x: number; y: number };
  data: TaskNodeData;
}

export interface AgentNode {
  id: string;
  type: string;
  position: { x: number; y: number };
  data: AgentNodeData;
}
