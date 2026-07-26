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
  /** Additive wall-time slice: own timestamp → next visible row (last row →
   *  task end). Row durations sum to the task span by construction. */
  duration?: number;
  /** Intrinsic measured op time (memory query/save ms, MCP call ms). Detail
   *  only — shown in the event's output dialog, never in the duration column. */
  intrinsicMs?: number;
  output?: string | Record<string, unknown>;
  extraData?: Record<string, unknown>;
}

export interface GroupedTrace {
  agent: string;
  startTime: Date;
  endTime: Date;
  duration: number;
  tasks: {
    taskName: string;
    taskId?: string;
    startTime: Date;
    endTime: Date;
    duration: number;
    events: TraceEvent[];
    /** True for the synthetic "Unassigned" bucket of a task-less run (e.g. the
     *  single light/chat agent, which has no crew task). The UI uses this to
     *  avoid framing the activity as a crew task. */
    unassigned?: boolean;
  }[];
}

export interface RunConfigAgent {
  key: string;
  id: string;
  role: string;
  goal: string;
  backstory: string;
  verbose?: boolean;
  max_iter?: number;
  max_rpm?: number;
  delegation_enabled?: boolean;
  tools_names?: string[];
}

export interface RunConfigTask {
  id: string;
  description: string;
  expected_output: string;
  agent_role: string;
  agent_key: string;
  async_execution?: boolean;
  human_input?: boolean;
  tools_names?: string[];
  context?: string[] | null;
}

export interface RunConfig {
  crew_key?: string;
  crew_id?: string;
  crew_agents: RunConfigAgent[];
  crew_tasks: RunConfigTask[];
  crew_inputs?: Record<string, unknown>;
}

/**
 * One crew execution within a run, with the agent groups that belong to it.
 *
 * A flow run drives one crew per node, so the timeline is a spine of these:
 * FLOW STARTED > (CREW STARTED > agents > CREW COMPLETED) x N > FLOW COMPLETED.
 * Membership comes from the OTel span DAG (span_id/parent_span_id), which the
 * backend derives from the engine event bus's parent_event_id — not from
 * timestamp order.
 *
 * `agentIdxs` indexes into `ProcessedTraces.agents` rather than embedding the
 * groups, so agent identity (and therefore expand/collapse state) is unchanged.
 */
export interface CrewSection {
  crewName?: string;
  start?: Trace;
  end?: Trace;
  agentIdxs: number[];
}

/**
 * The timeline as a flat, ordered render stream derived from `crewSections`.
 *
 * Flat rather than a nested tree so the renderer stays a single map: depth is
 * carried by `nested` (agents inside a crew are indented under its banner).
 */
export type TimelineItem =
  | { kind: 'crew-start'; trace: Trace; crewName?: string }
  | { kind: 'crew-end'; trace: Trace }
  | { kind: 'agent'; agentIdx: number; nested: boolean };

export interface ProcessedTraces {
  globalStart?: Date;
  globalEnd?: Date;
  totalDuration?: number;
  agents: GroupedTrace[];
  globalEvents: {
    start: Trace[];
    end: Trace[];
  };
  /**
   * Crew spine for the timeline. Always present: a light/chat run with no crew
   * events yields a single headerless section holding every agent group.
   */
  crewSections: CrewSection[];
  /** `crewSections` flattened into render order. */
  timelineItems: TimelineItem[];
  runConfig?: RunConfig;
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