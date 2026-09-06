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

