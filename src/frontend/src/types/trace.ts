import { Run } from '../api/ExecutionHistoryService';

export interface ShowTraceProps {
  open: boolean;
  onClose: () => void;
  runId: string;
  run?: Run;
  onViewResult?: (run: Run) => void;
  onShowLogs?: (jobId: string) => void;
}

export interface Trace {
  id: number;
  run_id?: number;
  job_id?: string;
  event_source: string;
  event_context: string;
  event_type: string;
  output: any;
  trace_metadata?: any;
  created_at: string;
  group_id?: string;
  group_email?: string;
  // Frontend-only fields from extra_data (legacy, use trace_metadata instead)
  task_id?: string;
  extra_data?: Record<string, unknown>;
  // OTel span hierarchy fields
  span_id?: string;
  trace_id?: string;
  parent_span_id?: string;
  // OTel-native fields
  span_name?: string;
  status_code?: string;
  duration_ms?: number;
}

// Shared trace processing interfaces (used by useTraceData hook and TraceTimelineContent)
export interface TraceEvent {
  type: string;
  description: string;
  timestamp: Date;
  duration?: number;
  output?: string | Record<string, unknown>;
  extraData?: Record<string, unknown>;
}

export interface GroupedTrace {
  agent: string;
  startTime: Date;
  endTime: Date;
  duration: number;
  agentEvents: TraceEvent[];
  tasks: {
    taskName: string;
    taskId?: string;
    startTime: Date;
    endTime: Date;
    duration: number;
    events: TraceEvent[];
  }[];
}

export interface ProcessedTraces {
  globalStart?: Date;
  globalEnd?: Date;
  totalDuration?: number;
  agents: GroupedTrace[];
  globalEvents: {
    start: Trace[];
    end: Trace[];
  };
  crewPlanningEvents: TraceEvent[];
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