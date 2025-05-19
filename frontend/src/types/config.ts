export interface AgentConfig {
  role: string;
  goal: string;
  backstory: string;
  tools: string[];
  advanced_config?: {
    llm: string;
    function_calling_llm: string;
    max_iter: string;
    max_rpm: string;
    max_execution_time: string;
    verbose: boolean;
    allow_delegation: boolean;
    cache: boolean;
    system_template: string;
    prompt_template: string;
    response_template: string;
    allow_code_execution: boolean;
    code_execution_mode: string;
    max_retry_limit: string;
    use_system_prompt: boolean;
    respect_context_window: boolean;
  };
}

export interface TaskConfig {
  description: string;
  expected_output: string;
  agent: string | null;
  context: string[];
  tools: string[];
}

export interface AdvancedConfigField {
  name: string;
  label: string;
  type: 'multiselect' | 'boolean' | 'text' | 'number' | 'select' | 'json';
  helperText: string;
  multiline?: boolean;
  options?: Array<{
    value: string;
    label: string;
    description?: string;
    icon?: string;
  }>;
  defaultValue: any;
} 