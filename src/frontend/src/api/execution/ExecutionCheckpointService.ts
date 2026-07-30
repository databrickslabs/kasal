import { apiClient } from '../../config/api/ApiConfig';

/**
 * One completed unit of work inside a checkpoint.
 *
 * A unit is a TASK for a crew execution and a CREW for a flow execution. The
 * shape is identical either way, which is what lets a single component serve
 * both instead of the flow-only surface this replaces.
 */
export interface CheckpointUnit {
  key: string;
  name: string | null;
  agent: string | null;
  output_preview: string | null;
  /**
   * The stored output was capped. Surfaced rather than hidden: a resume with
   * reduced fidelity should be visible before someone relies on it.
   */
  truncated: boolean;
  completed_at: string | null;
}

export interface CheckpointUnitDetail extends CheckpointUnit {
  output_raw: string;
  output_json: Record<string, unknown> | null;
}

export interface ExecutionCheckpoint {
  job_id: string;
  execution_id: number;
  kind: 'crew' | 'flow' | null;
  version: number | null;
  status: string | null;
  execution_status: string | null;
  run_name: string | null;
  created_at: string | null;
  unit_count: number | null;
  completed_count: number;
  truncated: boolean;
  /** Migrated from a pre-unification payload; fidelity is not guaranteed. */
  derived: boolean;
  resumable: boolean;
  blocked_reason: string | null;
  units: CheckpointUnit[];
  flow_uuid?: string | null;
  checkpoint_method?: string | null;
}

export interface ResumeResponse {
  execution_id: string;
  status: string;
  run_name: string;
}

/**
 * The unified checkpoint API, on /executions.
 *
 * Checkpoints hang off an EXECUTION rather than off a flow, which is why crews
 * can use them at all — the older /flows/{id}/checkpoints endpoints are now
 * deprecated aliases over the same service.
 */
export class ExecutionCheckpointService {
  /**
   * Get an execution's checkpoint, or null when it has none.
   *
   * A 404 is an ordinary answer here ("this run has nothing to resume"), not an
   * error worth surfacing, so it resolves to null rather than throwing.
   */
  static async getCheckpoint(jobId: string): Promise<ExecutionCheckpoint | null> {
    try {
      const response = await apiClient.get<ExecutionCheckpoint>(
        `/executions/${jobId}/checkpoints`,
      );
      return response.data;
    } catch (error: unknown) {
      const status = (error as { response?: { status?: number } })?.response?.status;
      if (status === 404) return null;
      throw error;
    }
  }

  /** Get one unit with its full stored output. */
  static async getUnit(jobId: string, unitKey: string): Promise<CheckpointUnitDetail> {
    const response = await apiClient.get<CheckpointUnitDetail>(
      `/executions/${jobId}/checkpoints/${encodeURIComponent(unitKey)}`,
    );
    return response.data;
  }

  /**
   * Resume from a checkpoint.
   *
   * Creates a NEW execution linked to this one; the original stays failed. Pass
   * `fromUnit` to rewind further back than the crash point and redo work.
   */
  static async resume(jobId: string, fromUnit?: string): Promise<ResumeResponse> {
    const response = await apiClient.post<ResumeResponse>(
      `/executions/${jobId}/resume`,
      fromUnit ? { from_unit: fromUnit } : {},
    );
    return response.data;
  }

  /** Expire a checkpoint. The recorded units are kept for inspection. */
  static async expire(jobId: string): Promise<void> {
    await apiClient.delete(`/executions/${jobId}/checkpoints`);
  }
}

export default ExecutionCheckpointService;
