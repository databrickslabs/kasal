/* eslint-disable @typescript-eslint/no-unused-vars */
import type { Node, Edge, MarkerType, Viewport } from 'reactflow';
import type { Agent as FormAgent } from './agent';
import type { KnowledgeSource } from '../api/AgentService';

export type CodeExecutionMode = 'safe' | 'unsafe';

export interface Agent {
  id: string;
  name: string;
  role: string;
  goal: string;
  backstory: string;
  tools?: string[];
  allow_code_execution: boolean;
  code_execution_mode: CodeExecutionMode;
  max_rpm?: number;
  max_execution_time?: number;
  knowledge_sources?: Array<KnowledgeSource>;
}

export interface Task {
  id: string;
  name: string;
  description: string;
  expected_output: string;
  agent_id?: string;
  tools?: string[];
}

export interface ConfigRun {
  id: string;
  job_id: string;
  status: 'pending' | 'running' | 'completed' | 'failed';
  created_at: string;
  completed_at?: string;
  inputs?: {
    agents?: Agent[];
    tasks?: Task[];
    agents_yaml?: Record<string, AgentYaml>;
    tasks_yaml?: Record<string, TaskYaml>;
  };
}

export interface AgentYaml {
  role: string;
  goal: string;
  backstory: string;
  tools?: string[] | number[];
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
  code_execution_mode?: 'safe' | 'dangerous' | 'none';
  max_retry_limit?: number;
  use_system_prompt?: boolean;
  respect_context_window?: boolean;
  reasoning?: boolean;
  max_reasoning_attempts?: number;
  embedder_config?: any;
  knowledge_sources?: any[];
  max_context_window_size?: number;
  max_tokens?: number;
}

export interface TaskYaml {
  description: string;
  expected_output: string;
  tools: string[];
  context: string[];
  agent: string | null;
  async_execution: boolean;
  markdown: boolean;
  output_file: string | null;
  output_json: string | null;
  output_pydantic: string | null;
  human_input: boolean;
  retry_on_fail: boolean;
  max_retries: number;
  timeout: number | null;
  priority: number;
  error_handling: 'default' | 'ignore' | 'retry' | 'fail';
  cache_response: boolean;
  cache_ttl: number;
  callback: string | null;
  condition?: string;
  guardrail?: string;
}

export interface JobRequest {
  agents_yaml: Record<string, AgentYaml>;
  tasks_yaml: Record<string, TaskYaml>;
  inputs: Record<string, unknown>;
}

export interface EditingNode {
  id: string;
  type: 'agent' | 'task';
  data: Agent | Task;
}

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

export interface NodeActionsContextType {
  handleAgentEdit: (nodeId: string) => void;
  handleTaskEdit: (nodeId: string) => void;
  handleDeleteNode: (nodeId: string) => void;
}

export interface NodeComponentProps {
  data: NodeData;
  id: string;
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
  onEdit?: (agent: FormAgent) => void;
}

export interface AgentNodeProps {
  data: AgentNodeData;
  id: string;
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

export interface FlowNodeManagerProps {
  nodes: Node[];
  setNodes: React.Dispatch<React.SetStateAction<Node[]>>;
  viewport?: Viewport;
}

export interface FlowControlsProps {
  onClearCanvas: () => void;
  onGenerateConnections: () => void;
  isHorizontal?: boolean;
  isLeftToRight?: boolean;
  isDarkMode?: boolean;
  onToggleTheme: () => void;
} 