import type { EffortSettings } from './effort';
export interface AgentYaml {
  execution_effort?: EffortSettings | null;
  role: string;
  goal: string;
  backstory: string;
  tools?: string[] | number[];
  /** Agent Skills, BY NAME — the identity the format uses. */
  skills?: string[];
  tool_configs?: Record<string, any>;  // User-specific tool configuration overrides
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
  /** Per-agent LLM overrides from the Agent form; absent = inherit the model. */
  temperature?: number;
  thinking_budget_tokens?: number;
  reasoning_effort?: string;
  /** Injects current date into agent's context for time-sensitive tasks */
  inject_date?: boolean;
  /** Custom date format string (e.g., '%B %d, %Y') */
  date_format?: string;
}

export interface TaskYaml {
  id?: string;  // Optional frontend task ID for tracking
  description: string;
  expected_output: string;
  tools: string[];
  tool_configs?: Record<string, any>;  // User-specific tool configuration overrides
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
