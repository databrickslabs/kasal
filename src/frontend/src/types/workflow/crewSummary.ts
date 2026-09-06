import type { KnowledgeSource } from './agent';
import type { AgentYaml, TaskYaml } from './crewPayload';

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

export interface EditingNode {
  id: string;
  type: 'agent' | 'task';
  data: Agent | Task;
}
