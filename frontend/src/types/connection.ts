export interface ConnectionTask {
  name: string;
  description?: string;
  expected_output?: string;
  human_input?: boolean;
  tools?: string[];
  type?: string;
  priority?: string;
  complexity?: string;
  required_skills?: string[];
  context?: {
    type?: string;
    priority?: string;
    complexity?: string;
    required_skills?: string[];
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
  required_before: string[];
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
  required_before: string[];
  reasoning: string;
}

export interface ConnectionResponse {
  assignments: ConnectionAssignment[];
  dependencies: ConnectionDependency[];
} 