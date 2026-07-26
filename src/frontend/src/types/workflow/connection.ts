export interface ConnectionTask {
  name: string;
  description: string;
  expected_output: string;
  agent_id: string;
  tools: string[];
  type?: string;
  priority?: string;
  complexity?: string;
  required_skills?: string[];
  context: {
    type: string;
    priority: string;
    complexity?: string;
    required_skills?: string[];
    metadata: Record<string, unknown>;
  };
  async_execution: boolean;
  human_input: boolean;
  markdown: boolean;
  config?: {
    cache_response: boolean;
    cache_ttl: number;
    retry_on_fail: boolean;
    max_retries: number;
    timeout: number | null;
    priority: number;
    error_handling: 'default' | 'ignore' | 'retry' | 'fail';
    output_file: string | null;
    output_json: string | null;
    output_pydantic: string | null;
    callback: string | null;
    human_input: boolean;
    guardrail: string | null;
    markdown: boolean;
  };
}

export interface ConnectionAgent {
  name: string;
  role: string;
  goal: string;
  backstory?: string;
  tools?: string[];
}

export interface TaskAssignment {
  task_name: string;
  reasoning: string;
}

export interface AgentAssignment {
  agent_name: string;
  tasks: TaskAssignment[];
}

export interface TaskDependency {
  task_name: string;
  depends_on: string[];
  reasoning: string;
}

export interface ConnectionRequest {
  agents: ConnectionAgent[];
  tasks: ConnectionTask[];
  model: string;
}

export interface ConnectionAssignment {
  agent_name: string;
  tasks: Array<{
    task_name: string;
    reasoning: string;
  }>;
}

export interface ConnectionDependency {
  task_name: string;
  depends_on: string[];
  reasoning: string;
}

export interface ConnectionResponse {
  assignments: ConnectionAssignment[];
  dependencies: ConnectionDependency[];
} 