import { ModelConfigResponse } from '../types/dispatcher';

/**
 * What an answer mode actually does, per model.
 *
 * Each mode does TWO things (see crew_generation_service.build_crew_config_from_generated):
 *   chat     -> a single light agent, execution_type="agent"
 *   research -> a full CREW, plus reasoning_effort="medium"
 *   deep     -> a full CREW, plus reasoning_effort="high"
 *
 * Only the second half needs a reasoning-capable model. The engine drops
 * reasoning_effort for models with no native budget, which leaves:
 *   - Research still meaningful on ANY model — a crew instead of one agent is a
 *     real difference in the answer, so it is never disabled.
 *   - Deep Research identical to Research — same crew, same tools, and both
 *     efforts dropped. Offering it promises a difference that cannot occur.
 *
 * ("Deep tools" is a separate, export-only behaviour: FAST_MODE_DISABLED_TOOLS
 * is applied in the exported Databricks app, not by the in-app crew builder.)
 */

export type AnswerModeId = 'chat' | 'research' | 'deep';

/**
 * True when the model is KNOWN to have no reasoning budget.
 *
 * Deliberately not `!supports`: an empty model list (still loading) or a
 * response without the field must not disable a mode — a false "unsupported"
 * is worse than briefly offering a mode that turns out to be a no-op.
 */
export function modelLacksReasoning(
  models: ModelConfigResponse[],
  selectedModel: string,
): boolean {
  if (!selectedModel || models.length === 0) return false;
  const model = models.find((m) => m.key === selectedModel);
  if (!model || model.supports_reasoning_effort === undefined) return false;
  return model.supports_reasoning_effort === false;
}

/** Display name for messages, falling back to the key. */
export function modelDisplayName(
  models: ModelConfigResponse[],
  selectedModel: string,
): string {
  return models.find((m) => m.key === selectedModel)?.name || selectedModel || 'This model';
}

/** The hint under each mode, honest about what the selected model will do. */
export function answerModeHint(mode: AnswerModeId, lacksReasoning: boolean): string {
  switch (mode) {
    case 'chat':
      return 'Quick answer from a single agent';
    case 'research':
      return lacksReasoning ? 'Full multi-agent crew' : 'Full crew with reasoning';
    case 'deep':
      return lacksReasoning
        ? 'Needs a model with a reasoning budget'
        : 'Deep tools with maximum reasoning';
  }
}

/** Deep Research is the only mode a non-reasoning model cannot honour. */
export function isAnswerModeDisabled(mode: AnswerModeId, lacksReasoning: boolean): boolean {
  return mode === 'deep' && lacksReasoning;
}

/** Why the mode is unavailable — names the model rather than saying "unsupported". */
export function answerModeDisabledReason(modelName: string): string {
  return `${modelName} has no reasoning budget, so Deep Research would behave exactly like Research.`;
}
