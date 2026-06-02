export type IntentType =
  | 'generate_agent'
  | 'generate_task'
  | 'generate_crew'
  | 'generate_plan'
  | 'execute_crew'
  | 'configure_crew'
  | 'conversation'
  | 'catalog_list'
  | 'catalog_load'
  | 'catalog_save'
  | 'catalog_schedule'
  | 'catalog_help'
  | 'flow_list'
  | 'flow_load'
  | 'flow_save'
  | 'execute_flow'
  | 'catalog_delete'
  | 'flow_delete'
  | 'unknown';

export interface DispatcherRequest {
  message: string;
  model?: string;
  tools?: string[];
}

export interface DispatcherResponse {
  intent: IntentType;
  confidence: number;
  extracted_info: Record<string, unknown>;
  suggested_prompt?: string;
}

export interface DispatchResult {
  dispatcher: DispatcherResponse;
  generation_result: unknown;
  service_called: string | null;
}

export interface GeneratedAgent {
  id?: string;
  name: string;
  role: string;
  goal: string;
  backstory: string;
  tools?: string[];
  llm?: string;
}

export interface GeneratedTask {
  id?: string;
  name: string;
  description: string;
  expected_output: string;
  tools?: string[];
  agent_id?: string;
}

export interface GeneratedCrew {
  agents: GeneratedAgent[];
  tasks: GeneratedTask[];
}

export interface StreamingGenerationResult {
  generation_id: string;
  type: 'streaming';
}

export interface ConfigureCrewResult {
  type: 'configure_crew';
  config_type: 'llm' | 'maxr' | 'tools' | 'general';
  message: string;
  actions: {
    open_llm_dialog: boolean;
    open_maxr_dialog: boolean;
    open_tools_dialog: boolean;
  };
  extracted_info: Record<string, unknown>;
}

export interface CatalogListResult {
  type: 'catalog_list';
  plans: Array<{
    id: string;
    name: string;
    agent_count?: number;
    task_count?: number;
    created_at?: string;
    updated_at?: string;
  }>;
  message: string;
}

export interface CatalogLoadResult {
  type: 'catalog_load';
  plan: {
    id: string;
    name: string;
    nodes: unknown[];
    edges: unknown[];
    process?: string;
    planning?: boolean;
    planning_llm?: string;
    memory?: boolean;
    verbose?: boolean;
    max_rpm?: number;
  } | null;
  message: string;
}

export interface CatalogSaveResult {
  type: 'catalog_save';
  action: 'open_save_dialog';
  suggested_name?: string;
  message: string;
}

export interface CatalogScheduleResult {
  type: 'catalog_schedule';
  action: 'open_schedule_dialog';
  message: string;
}

export interface FlowListResult {
  type: 'flow_list';
  flows: Array<{
    id: string;
    name: string;
    node_count?: number;
    created_at?: string;
    updated_at?: string;
  }>;
  message: string;
}

export interface FlowLoadResult {
  type: 'flow_load';
  flow: {
    id: string;
    name: string;
    nodes: unknown[];
    edges: unknown[];
    flow_config?: Record<string, unknown>;
  } | null;
  message: string;
}

export interface FlowSaveResult {
  type: 'flow_save';
  action: 'open_save_flow_dialog';
  suggested_name?: string;
  message: string;
}

export interface CatalogDeleteResult {
  type: 'catalog_delete';
  message: string;
}

export interface FlowDeleteResult {
  type: 'flow_delete';
  message: string;
}

export interface ExecuteCrewResult {
  plan?: CatalogLoadResult['plan'];
  message: string;
}

export interface ExecuteFlowResult {
  flow?: FlowLoadResult['flow'];
  message: string;
}

export interface ModelConfigResponse {
  id: number;
  key: string;
  name: string;
  provider: string | null;
  temperature: number | null;
  context_window: number | null;
  max_output_tokens: number | null;
  extended_thinking: boolean;
  enabled: boolean;
  created_at: string;
  updated_at: string;
}
