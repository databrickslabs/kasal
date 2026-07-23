import apiClient from '../config/api/ApiConfig';

/*
 * Prompt optimization (GEPA via managed MLflow). A run mines training
 * examples from the LLM interaction log (or takes them inline), searches
 * for a better seeded template, and proposes it for explicit
 * review-and-apply as a group-scoped template override.
 */

export interface PromptOptimizationStart {
  run_id: string;
  status: string;
  dataset_size: number;
}

export interface PromptOptimizationRun {
  run_id: string;
  template_name: string;
  status: 'pending' | 'running' | 'completed' | 'failed' | 'cancelled';
  dataset_size: number;
  model?: string | null;
  initial_score?: number | null;
  final_score?: number | null;
  baseline_template?: string | null;
  optimized_template?: string | null;
  error?: string | null;
  applied: boolean;
  created_at?: string | null;
  kind?: 'template' | 'crew' | null;
  crew_id?: string | null;
  baseline_fields?: Record<string, string> | null;
  optimized_fields?: Record<string, string> | null;
  executions_used?: number | null;
  execution_cap?: number | null;
}

export interface LLMJudge {
  /** Display name (crew prefix stripped). */
  name: string;
  /** Registry name (crew-scoped judges carry a crew prefix). */
  full_name?: string;
  /** Crew id prefix when the judge is assigned to a crew; null = shared library. */
  crew_id?: string | null;
  model?: string | null;
  instructions?: string;
}

export interface CrewEval {
  trace_id: string;
  timestamp_ms?: number | null;
  deliverable: string;
  assessment_count: number;
}

export interface CrewOptimizationRequest {
  crew_id: string;
  model?: string;
  judge_model?: string;
  reflection_model?: string;
  guidance?: string;
  max_metric_calls?: number;
  execution_timeout_seconds?: number;
}

export interface StartOptimizationRequest {
  template_name: string;
  model?: string;
  judge_model?: string;
  reflection_model?: string;
  examples?: string[];
  lookback_days?: number;
  max_examples?: number;
  max_metric_calls?: number;
}

export class PromptOptimizationService {
  static async startOptimization(
    request: StartOptimizationRequest,
  ): Promise<PromptOptimizationStart> {
    const response = await apiClient.post<PromptOptimizationStart>(
      '/prompt-optimization/optimize',
      request,
      { headers: { 'Content-Type': 'application/json' } },
    );
    return response.data;
  }

  /** GEPA over a saved crew — every evaluation EXECUTES the crew for real. */
  static async startCrewOptimization(
    request: CrewOptimizationRequest,
  ): Promise<PromptOptimizationStart> {
    const response = await apiClient.post<PromptOptimizationStart>(
      '/prompt-optimization/optimize-crew',
      request,
      { headers: { 'Content-Type': 'application/json' } },
    );
    return response.data;
  }

  static async listRuns(): Promise<PromptOptimizationRun[]> {
    const response = await apiClient.get<{ runs: PromptOptimizationRun[] }>(
      '/prompt-optimization/runs',
    );
    return response.data?.runs || [];
  }

  static async getRun(runId: string): Promise<PromptOptimizationRun> {
    const response = await apiClient.get<PromptOptimizationRun>(
      `/prompt-optimization/runs/${runId}`,
    );
    return response.data;
  }

  /** Optimization-evaluation answers for a crew (local MLflow traces). */
  static async listCrewEvals(crewId: string): Promise<CrewEval[]> {
    const response = await apiClient.get<{ evals: CrewEval[] }>(
      `/prompt-optimization/crew-evals/${crewId}`,
    );
    return response.data?.evals || [];
  }

  /** Grade an evaluation answer (Feedback) and/or state what it SHOULD have
   *  contained (Expectation) — both stored as MLflow assessments. */
  static async addEvalFeedback(
    traceId: string,
    value?: number,
    comment?: string,
    expectation?: string,
  ): Promise<boolean> {
    const response = await apiClient.post<{ ok: boolean }>(
      `/prompt-optimization/crew-evals/${traceId}/feedback`,
      {
        value: value ?? undefined,
        comment: comment || undefined,
        expectation: expectation || undefined,
      },
      { headers: { 'Content-Type': 'application/json' } },
    );
    return Boolean(response.data?.ok);
  }

  /** LLM judges registered on the local MLflow experiment. */
  static async listJudges(): Promise<LLMJudge[]> {
    const response = await apiClient.get<{ judges: LLMJudge[] }>(
      '/prompt-optimization/judges',
    );
    return response.data?.judges || [];
  }

  /** Create + register an LLM judge (plain-language criteria). With crewId
   *  the judge is assigned to that crew; without, it joins the shared library. */
  static async createJudge(
    name: string,
    instructions: string,
    model?: string,
    crewId?: string,
  ): Promise<LLMJudge> {
    const response = await apiClient.post<LLMJudge>(
      '/prompt-optimization/judges',
      { name, instructions, model: model || undefined, crew_id: crewId || undefined },
      { headers: { 'Content-Type': 'application/json' } },
    );
    return response.data;
  }

  /** Assign a shared library judge to a crew (registers a crew-scoped copy). */
  static async assignJudge(name: string, crewId: string): Promise<LLMJudge> {
    const response = await apiClient.post<LLMJudge>(
      `/prompt-optimization/judges/${encodeURIComponent(name)}/assign`,
      { crew_id: crewId },
      { headers: { 'Content-Type': 'application/json' } },
    );
    return response.data;
  }

  static async deleteJudge(name: string): Promise<boolean> {
    const response = await apiClient.delete<{ ok: boolean }>(
      `/prompt-optimization/judges/${encodeURIComponent(name)}`,
    );
    return Boolean(response.data?.ok);
  }

  /** Request a running optimization to stop (honored before the next crew execution). */
  static async cancelRun(runId: string): Promise<boolean> {
    const response = await apiClient.post<{ cancelling: boolean }>(
      `/prompt-optimization/runs/${runId}/cancel`,
    );
    return Boolean(response.data?.cancelling);
  }

  static async applyRun(runId: string): Promise<boolean> {
    const response = await apiClient.post<{ applied: boolean }>(
      `/prompt-optimization/runs/${runId}/apply`,
    );
    return Boolean(response.data?.applied);
  }
}
