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
  // The user's CLEAN message (before the intent-steering prefix is added to
  // `message`). The backend grounds the generated crew's run with this so the
  // crew answers the real request, not "create a crew plan with…".
  original_prompt?: string;
  // When true (ChatMode only), the backend runs the generated crew immediately.
  // The crew canvas omits this (defaults false): it renders the plan and the
  // user runs it via Play — sending it true here would double-run the crew.
  auto_execute?: boolean;
  // ChatMode run settings — carried to the backend so a generated crew is
  // auto-executed with the chat's own memory scope + attached data sources,
  // without a frontend round-trip. AgentBuilder doesn't send these.
  session_id?: string;
  memory_workspace_scope?: boolean;
  disable_memory?: boolean;
  mcp_servers?: string[];
  agentbricks_endpoints?: string[];
}

/** ChatMode run settings gathered from the execution store at dispatch time. */
export interface DispatchRunSettings {
  auto_execute?: boolean;
  session_id?: string;
  memory_workspace_scope?: boolean;
  disable_memory?: boolean;
  mcp_servers?: string[];
  agentbricks_endpoints?: string[];
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

/** The agents/tasks a crew generation produced (the generation_complete event). */
export interface GenerationCompleteData {
  agents: Record<string, unknown>[];
  tasks: Record<string, unknown>[];
  /**
   * The chat prompt that triggered this generation. Generated task descriptions
   * are often generic mission statements; the executed config appends this so
   * the run answers the user's ACTUAL request instead of asking for a question.
   */
  user_request?: string;
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
