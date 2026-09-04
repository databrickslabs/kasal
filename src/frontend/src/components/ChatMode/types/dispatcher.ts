import type { ImageRef } from './chat';
import { PublicationInputSchema } from '../../../types/workflow/publication';

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
  /**
   * Route this prompt to an ALREADY PUBLISHED crew or flow instead of building
   * one. Its generation_result is an ordinary `execute_crew` / `execute_flow`
   * — deliberately, so everything downstream is unchanged — or a
   * `catalog_no_match`.
   */
  | 'catalog_route'
  | 'unknown';

export interface DispatcherRequest {
  message: string;
  model?: string;
  tools?: string[];
  // The user's CLEAN message (before the intent-steering prefix is added to
  // `message`). The backend grounds the generated crew's run with this so the
  // crew answers the real request, not "create a crew plan with…".
  original_prompt?: string;
  // True for ChatMode prompts. ChatMode always builds a crew; the backend
  // collapses "create a task"/"create an agent" intents to generate_crew.
  // Entity creation is only for the AgentBuilder / crew canvas (omits this).
  chat_mode?: boolean;
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
  /** Paths of files attached this turn — scopes the knowledge search tool to them. */
  knowledge_file_paths?: string[];
  /** Images attached this turn — the run tells the model how to place them in HTML. */
  image_assets?: ImageRef[];
  /** Skill names picked in the "+" menu — attached to every agent of the run. */
  skills?: string[];
  /** Answer mode: 'chat' = single light agent, 'research' = crew + medium reasoning effort, 'deep' = crew + high reasoning effort. */
  chat_mode_type?: 'chat' | 'research' | 'deep';
  /**
   * True when the user picked "Use existing": run something already published
   * rather than building something new.
   *
   * Its own field, NOT a fourth `chat_mode_type`. They are different axes —
   * `chat_mode_type` says what SHAPE to build, this says whether to build at
   * all. The catalogue only stores crews, so reuse could never honour 'chat',
   * and a fourth answer mode would be a value that silently invalidates its own
   * neighbours.
   */
  prefer_existing?: boolean;
  /** False for one turn after the user leaves a held conversation. */
  allow_continuation?: boolean;
}

/** ChatMode run settings gathered from the execution store at dispatch time. */
export interface DispatchRunSettings {
  auto_execute?: boolean;
  session_id?: string;
  memory_workspace_scope?: boolean;
  disable_memory?: boolean;
  mcp_servers?: string[];
  agentbricks_endpoints?: string[];
  knowledge_file_paths?: string[];
  image_assets?: ImageRef[];
  skills?: string[];
  chat_mode_type?: 'chat' | 'research' | 'deep';
  /** @see DispatcherRequest.prefer_existing — the SOURCE axis, not the shape. */
  prefer_existing?: boolean;
  /** False for one turn after the user leaves a held conversation. */
  allow_continuation?: boolean;
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

/**
 * What "Use existing" routing adds to an execute result.
 *
 * Both keys are about asking the user as little as possible: `extracted_inputs`
 * is what the router could bind from the sentence they already typed, and
 * `input_schema` is the authority on what is actually required. Without the
 * schema the consumer falls back to treating every detected `{placeholder}` as
 * required — correct, but it interrogates the user for cosmetic ones too.
 */
export interface RoutedResultFields {
  /** Values bound from the prompt. Only ever things the user actually said. */
  extracted_inputs?: Record<string, string>;
  input_schema?: PublicationInputSchema | null;
  /** The published capability's external name, for the log and the UI. */
  capability?: string;
  /** Whether this capability holds a conversation across turns. */
  conversational?: boolean;
  /** Whether THIS turn was routed to it because it was already holding one. */
  continued?: boolean;
  routed_from?: string;
  /**
   * The sentence that selected this capability, sent on to the run as
   * `user_request`. Memory recall queries on it: a saved crew's task
   * description is byte-identical on every run, so without it recall matches
   * the crew's own history instead of this run's subject.
   */
  request?: string;
  /**
   * The earlier answer this run works FROM, when the router pointed at one.
   * "Turn this into a deck" is useless if the deck crew starts from nothing —
   * it re-does the gathering, and against a polluted memory pool it re-gathers
   * the wrong subject.
   */
  referenced_answer?: string | null;
}

export interface ExecuteCrewResult extends RoutedResultFields {
  plan?: CatalogLoadResult['plan'];
  message: string;
}

export interface ExecuteFlowResult extends RoutedResultFields {
  flow?: FlowLoadResult['flow'];
  message: string;
}

/**
 * "Use existing" found nothing to run.
 *
 * Deliberately NOT a silent fall-through to generation: the user asked to run
 * something they already have, so building a crew instead would run work they
 * did not ask for and bill a full crew run for it. `build_instead` is the
 * one-click offer that keeps the choice theirs.
 */
/**
 * What a routed run knows that a click-to-run does not, as the run handler
 * receives it.
 *
 * Every field is optional and absent for `/run` and click-to-run: those paths
 * have no router and no publication, so the handler keeps its
 * detect-every-placeholder fallback for them.
 */
export interface RoutedRunFields {
  extractedInputs?: Record<string, string>;
  inputSchema?: PublicationInputSchema | null;
  capability?: string;
  /** @see RoutedResultFields.request */
  request?: string;
  /** @see RoutedResultFields.referenced_answer */
  referencedAnswer?: string | null;
}

export interface CatalogNoMatchResult {
  type: 'catalog_no_match';
  /** 'nothing_published' | 'no_match' | 'unresolved' — they read differently. */
  reason: string;
  message: string;
  build_instead: boolean;
  /**
   * The router declined mid-conversation, so this turn should be ANSWERED
   * rather than left as a dead end — it is a question about what is already on
   * screen, not a request for new work. The build offer stays beside the
   * answer; declining to run a crew is not the same as having nothing to say.
   */
  answer_here?: boolean;
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
  /**
   * Whether this model accepts a native reasoning-effort budget. Derived
   * server-side from the same allow-list the engine uses, so the UI cannot
   * offer an answer mode whose reasoning the engine will silently drop.
   * Optional because a cached/older response may not carry it — treat
   * `undefined` as "unknown", not as "unsupported".
   */
  supports_reasoning_effort?: boolean;
  created_at: string;
  updated_at: string;
}
