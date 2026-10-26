import { getClient } from './client';
import { Execution, ExecutionConfig } from '../types/execution';

export async function createExecution(
  config: ExecutionConfig
): Promise<Execution> {
  const response = await getClient().post<Execution>('/executions', config);
  return response.data;
}

export async function listExecutions(limit = 20): Promise<Execution[]> {
  const response = await getClient().get<Execution[]>('/executions', {
    params: { limit },
  });
  return response.data;
}

export async function getExecutionStatus(id: string): Promise<Execution> {
  const response = await getClient().get<Execution>(
    `/executions/${id}/status`
  );
  return response.data;
}

/**
 * Fetch a single execution INCLUDING its full `result` (the crew output that
 * holds the rendered deliverable). Unlike `getExecutionStatus` (/status, which
 * omits the result), this hits /executions/{id}. Used to derive the chat
 * preview on demand from the stored result instead of a separate preview copy.
 */
export async function getExecution(id: string): Promise<Execution> {
  const response = await getClient().get<Execution>(`/executions/${id}`);
  return response.data;
}

export async function stopExecution(id: string): Promise<void> {
  await getClient().post(`/executions/${id}/stop`, {
    stop_type: 'graceful',
    reason: 'Stopped by user',
    preserve_partial_results: true,
  });
}

/** A raw execution trace row (the durable per-step record persisted by the
 *  backend). Shape mirrors what the SSE `onTrace` data carries, so it can run
 *  through the SAME buildTraceEntry mapping. */
export interface ExecutionTrace {
  id: number;
  event_type?: string;
  event_source?: string;
  output?: unknown;
  trace_metadata?: unknown;
  created_at?: string;
  [key: string]: unknown;
}

const TRACE_PAGE_SIZE = 500;
/** Runaway-loop guard; mirrors the backend's own hard cap on `limit`. */
const TRACE_FETCH_MAX = 15000;

/**
 * Fetch ALL persisted traces for a finished run by its job id. The run activity
 * (the "thinking" stream) can always be rebuilt from these — so a refresh
 * restores the full tool context even if the live per-message copy was lost.
 *
 * Pages through explicitly: a bare GET uses the backend default limit of 100,
 * which silently truncated the restored activity timeline for long runs.
 */
export async function getJobTraces(jobId: string): Promise<ExecutionTrace[]> {
  const all: ExecutionTrace[] = [];
  for (let offset = 0; offset < TRACE_FETCH_MAX; offset += TRACE_PAGE_SIZE) {
    const response = await getClient().get<{ traces: ExecutionTrace[] }>(
      `/traces/job/${jobId}`,
      { params: { limit: TRACE_PAGE_SIZE, offset } },
    );
    const page = response.data?.traces || [];
    all.push(...page);
    if (page.length < TRACE_PAGE_SIZE) break;
  }
  return all;
}
