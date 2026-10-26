import type { Node, Edge } from 'reactflow';
import type { EffortSettings } from './effort';
import type { TaskNode, AgentNode } from './canvas';

// Reasoning controls. Defined here (a leaf types module) so both the crew wire
// types and the execution store can share it without an import cycle. The backend
// maps `reasoning_effort` onto the model's native reasoning/thinking budget.
export interface ReasoningConfig {
  execution_effort?: EffortSettings;
  reasoning_effort?: 'low' | 'medium' | 'high';
}

export interface CrewExecutionConfig {
  process?: 'sequential' | 'hierarchical' | 'parallel';
  reasoning?: boolean;
  reasoning_llm?: string;
  reasoning_config?: ReasoningConfig;  // Model reasoning/thinking budget
  manager_llm?: string;
  tool_configs?: Record<string, any>;
  memory?: boolean;
  verbose?: boolean;
  max_rpm?: number;
}

export interface CrewResponse extends CrewExecutionConfig {
  id: string;
  name: string;
  agent_ids: string[];
  task_ids: string[];
  nodes?: Node[];
  edges?: Edge[];
  tasks?: TaskNode[];
  agents?: AgentNode[];
  created_at: string;
  updated_at: string;
}

export interface CrewCreate extends CrewExecutionConfig {
  name: string;
  agent_ids: string[];
  task_ids: string[];
  nodes: Node[];
  edges: Edge[];
}

export interface Crew extends CrewExecutionConfig {
  id: string;
  name: string;
  agent_ids: string[];
  task_ids: string[];
  nodes: Node[];
  edges: Edge[];
  created_at: string;
  updated_at: string;
}

export interface CrewSaveData extends CrewExecutionConfig {
  name: string;
  nodes: Node[];
  edges: Edge[];
  agent_ids?: string[];
  task_ids?: string[];
}
