/**
 * Which trace rows are LLM calls, and where their readable text lives.
 *
 * Kept out of LlmEventDetails.tsx so that file exports only its component.
 */
import { SelectedTraceEvent } from '../../types/execution/trace';

const LLM_EVENT_TYPES = new Set(['llm', 'llm_request', 'llm_response']);

export const isLlmEvent = (type: string): boolean => LLM_EVENT_TYPES.has(type);

export const asText = (value: unknown): string | undefined => {
  if (typeof value !== 'string') return undefined;
  return value.length > 0 ? value : undefined;
};

export const asCount = (value: unknown): number | undefined => {
  const n = typeof value === 'string' ? Number(value) : value;
  return typeof n === 'number' && Number.isFinite(n) ? n : undefined;
};

/** The text this row is really about: the prompt sent, or the answer returned. */
export const extractLlmText = (event: SelectedTraceEvent): string | undefined => {
  const extra = (event.extraData ?? {}) as Record<string, unknown>;

  if (event.type === 'llm_response') {
    return (
      asText(event.output)
      ?? asText((event.output as Record<string, unknown> | undefined)?.content)
      ?? asText(extra.content)
      ?? asText(extra.value)
    );
  }

  return asText(extra.prompt) ?? asText(extra.task_prompt) ?? asText(event.output);
};
