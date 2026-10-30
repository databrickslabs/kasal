/**
 * Sentinel the backend sends when the model DID reason but the provider withheld
 * the text. Must match `REDACTED_REASONING` in
 * core/llm/transport/response_parsing.py.
 */
export const REDACTED_REASONING = '__kasal_reasoning_redacted__';

/**
 * Whether `text` is the redaction placeholder rather than real reasoning.
 *
 * Tolerates the sentinel REPEATED. It is a per-call flag, but a streaming
 * response reports it on every delta, and the backend used to append each one —
 * so traces already recorded say `__kasal_reasoning_redacted__` six times over
 * and an equality check let that leak to the user as literal text. The backend no
 * longer accumulates it; this keeps existing traces readable.
 */
export const isRedactedReasoning = (text: string): boolean => {
  const trimmed = text.trim();
  return trimmed.length > 0 && trimmed.split(REDACTED_REASONING).join('') === '';
};

/**
 * Seeded Databricks models that actually return reasoning TEXT, newest probe
 * first by volume. Every one was verified against the live workspace on
 * 2026-08-05 by asking a step-by-step question and reading the response:
 *
 *   gemini-3-1-flash-lite  2,226 chars    inkling         309 chars
 *   gemini-3-5-flash       2,104 chars    kimi-k2-7-code  137 chars
 *   gemini-3-1-pro         1,648 chars
 *
 * The Gemini three need `reasoning_effort` on the request (see
 * utils/model_config._REASONING_EFFORT_SUBSTRINGS); inkling and kimi return
 * `reasoning_content` unprompted.
 *
 * Deliberately NOT exhaustive-by-inference: this is a list of models observed
 * returning text, not a guess from model family. gpt-5* accepts a reasoning
 * budget and still returns nothing, which is why naming families would mislead.
 */
export const REASONING_VISIBLE_MODELS = [
  'databricks-gemini-3-1-pro',
  'databricks-gemini-3-5-flash',
  'databricks-gemini-3-1-flash-lite',
  'databricks-inkling',
  'databricks-kimi-k2-7-code',
] as const;

/** The reasoning text, or '' when this row carries none. */
export function reasoningText(value: unknown): string {
  if (typeof value === 'string') return value.trim();
  // Defensive: a provider could hand back a list of summary parts.
  if (Array.isArray(value)) {
    return value
      .map(part =>
        typeof part === 'string'
          ? part
          : typeof part === 'object' && part !== null
            ? String((part as Record<string, unknown>).text ?? '')
            : '',
      )
      .join('')
      .trim();
  }
  return '';
}
