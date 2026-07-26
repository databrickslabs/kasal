import { apiClient as API } from '../../config/api/ApiConfig';

/**
 * Workflow recipes — completed crews kept so the next equivalent request can
 * reuse them instead of paying an LLM to derive the same crew again.
 *
 * The one thing this client exists for is CURATION. Mining runs on its own and
 * fills the library automatically, but a mined recipe only says a crew
 * FINISHED, never that its output was right — so nothing is ever reused until a
 * human marks it good. Until then the library is inert. That judgement can only
 * honestly be made while looking at a run's actual result, which is why the
 * control lives on the run list rather than in a settings page.
 */

export type RecipeCuration = 'good' | 'bad' | 'hidden' | null;

export interface RecipeJobEntry {
  recipe_id: number;
  curation: RecipeCuration;
  intent_text: string;
  run_count: number;
  times_reused?: number;
}

export interface RecipeArmStats {
  generations: number;
  linked_runs: number;
  completed: number;
  completion_rate: number | null;
  median_duration_ms: number | null;
  median_error_spans: number | null;
  median_agents: number | null;
  median_tasks: number | null;
}

export interface RecipeEffectiveness {
  window_days: number;
  generations: number;
  with_candidates: number;
  with_blessed_candidates: number;
  coverage_rate: number | null;
  injection_rate: number | null;
  holdout_fraction: number;
  min_similarity: number;
  arms: Record<string, RecipeArmStats>;
  comparable: boolean;
  note: string;
}

export class WorkflowRecipeService {
  /** Which recipe each run was mined into, keyed by job id. */
  static async getByJob(): Promise<Record<string, RecipeJobEntry>> {
    const response = await API.get<Record<string, RecipeJobEntry>>(
      '/workflow-recipes/by-job',
    );
    return response.data || {};
  }

  /** Record (or clear, with null) a human judgement on a recipe. */
  static async curate(
    recipeId: number,
    curation: RecipeCuration,
  ): Promise<void> {
    await API.patch(`/workflow-recipes/${recipeId}/curation`, { curation });
  }

  /** Whether reuse is measurably helping, per arm. */
  static async getEffectiveness(days = 30): Promise<RecipeEffectiveness> {
    const response = await API.get<RecipeEffectiveness>(
      `/workflow-recipes/effectiveness?days=${days}`,
    );
    return response.data;
  }
}

export default WorkflowRecipeService;
