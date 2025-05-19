export interface ShowTraceProps {
  open: boolean;
  onClose: () => void;
  runId: string;
}

export interface Trace {
  id: string;
  agent_name: string;
  task_name: string;
  task_id?: string;
  created_at: string;
  output: string | Record<string, unknown>;
}

export interface TaskDetails {
  description: string;
  expected_output: string;
  agent: string;
  tools: string[];
  context: string[];
  async_execution: boolean;
  output_file: string | null;
  output_json: string | null;
  output_pydantic: string | null;
  human_input: boolean;
  retry_on_fail: boolean;
  max_retries: number;
  timeout: number | null;
  priority: number;
  error_handling: string;
  cache_response: boolean;
  cache_ttl: number;
  callback: string | null;
  output_parser: string | null;
  create_directory: boolean;
  config: Record<string, unknown>;
  name?: string;
} 