import { apiClient } from '../../shared/api/client';
import { setServerDefaultModel } from '../../config/defaultModel';
import type { EnabledModelRow, Models } from '../../types/config/models';

interface EnabledModelsResponse {
  models: EnabledModelRow[];
  count: number;
  default_model?: string;
}

/**
 * The workspace's enabled models, straight from the server — no cache.
 *
 * The shared models store (`store/models.ts`) is the only caller that should
 * need this: it de-duplicates and throttles the requests, so every model menu
 * reads one list. Throws on failure, so the store can keep its last good list
 * instead of swapping in a placeholder.
 */
export async function fetchEnabledModelRows(): Promise<EnabledModelRow[]> {
  const response = await apiClient.get<EnabledModelsResponse>('/models/enabled');
  setServerDefaultModel(response.data?.default_model);
  const rows = response.data?.models;
  if (!Array.isArray(rows)) throw new Error('Unexpected /models/enabled response');
  return rows.filter((row) => !!row && !!row.key);
}

/**
 * The same rows keyed by model key, in the `Models` shape the builder surfaces
 * (Agent Builder / Flow Builder assistant, agent form, LLM dialog) render.
 * Every capability field is carried explicitly, for the reason given in
 * `ModelService.convertApiResponseToModels`: a dropped field silently hides the
 * control it gates.
 */
export function toModelRecord(rows: EnabledModelRow[]): Models {
  const record: Models = {};
  for (const row of rows) {
    record[row.key] = {
      name: row.name || row.key,
      provider: row.provider ?? undefined,
      temperature: row.temperature ?? undefined,
      context_window: row.context_window ?? undefined,
      max_output_tokens: row.max_output_tokens ?? undefined,
      extended_thinking: row.extended_thinking,
      supports_reasoning_effort: row.supports_reasoning_effort === true,
      thinking_mode: row.thinking_mode ?? null,
      thinking_budget_tokens: row.thinking_budget_tokens ?? null,
      reasoning_effort: row.reasoning_effort ?? null,
      allowed_efforts: row.allowed_efforts ?? [],
      refused_params: row.refused_params ?? [],
      returns_thinking_text: row.returns_thinking_text === true,
      params: row.params ?? null,
      enabled: row.enabled !== false,
    };
  }
  return record;
}
