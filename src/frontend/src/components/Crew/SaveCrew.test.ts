/**
 * Tests for SaveCrew llm_guardrail resolution logic.
 *
 * The guardrail resolution pattern:
 * - config.llm_guardrail !== undefined → use config value (user's explicit choice)
 * - otherwise → null (not configured = disabled)
 * - top-level node.data.llm_guardrail is the LLM suggestion, NOT the user's active choice
 */
import { describe, it, expect } from 'vitest';

/**
 * Replicate the guardrail resolution logic from SaveCrew.tsx
 * to test it in isolation.
 */
function resolveGuardrail(
  existingConfig: Record<string, unknown>,
  nodeData: Record<string, unknown>
): { llmGuardrail: unknown; topLevelGuardrail: unknown; configGuardrail: unknown } {
  // Get llm_guardrail: ONLY use config value (user's explicit choice)
  const llmGuardrail = existingConfig.llm_guardrail !== undefined
    ? existingConfig.llm_guardrail
    : null;

  // Top-level preserves the LLM suggestion (for UI toggle)
  const topLevelGuardrail = nodeData?.llm_guardrail ?? null;

  // Config gets the user's active choice
  const configGuardrail = llmGuardrail;

  return { llmGuardrail, topLevelGuardrail, configGuardrail };
}

describe('SaveCrew - llm_guardrail resolution', () => {
  it('should use config value when user explicitly set a guardrail', () => {
    const guardrailObj = { model: 'gpt-4', prompt: 'Validate output' };
    const result = resolveGuardrail(
      { llm_guardrail: guardrailObj },
      { llm_guardrail: { model: 'suggestion', prompt: 'LLM suggestion' } }
    );
    expect(result.llmGuardrail).toEqual(guardrailObj);
    expect(result.configGuardrail).toEqual(guardrailObj);
  });

  it('should use null when user explicitly disabled guardrail (set to null in config)', () => {
    const result = resolveGuardrail(
      { llm_guardrail: null },
      { llm_guardrail: { model: 'suggestion', prompt: 'LLM suggestion' } }
    );
    expect(result.llmGuardrail).toBeNull();
    expect(result.configGuardrail).toBeNull();
  });

  it('should NOT fall back to top-level LLM suggestion when config has no guardrail', () => {
    const result = resolveGuardrail(
      {},  // No llm_guardrail key at all
      { llm_guardrail: { model: 'suggestion', prompt: 'LLM suggestion' } }
    );
    // Key assertion: should be null, NOT the LLM suggestion
    expect(result.llmGuardrail).toBeNull();
    expect(result.configGuardrail).toBeNull();
  });

  it('should preserve LLM suggestion at top level for UI toggle', () => {
    const suggestion = { model: 'suggestion', prompt: 'LLM suggestion' };
    const result = resolveGuardrail(
      {},
      { llm_guardrail: suggestion }
    );
    expect(result.topLevelGuardrail).toEqual(suggestion);
  });

  it('should handle missing top-level suggestion gracefully', () => {
    const result = resolveGuardrail(
      { llm_guardrail: null },
      {}
    );
    expect(result.llmGuardrail).toBeNull();
    expect(result.topLevelGuardrail).toBeNull();
  });

  it('should handle both config and node data being empty', () => {
    const result = resolveGuardrail({}, {});
    expect(result.llmGuardrail).toBeNull();
    expect(result.topLevelGuardrail).toBeNull();
    expect(result.configGuardrail).toBeNull();
  });

  it('should use false when config explicitly sets guardrail to false', () => {
    const result = resolveGuardrail(
      { llm_guardrail: false },
      { llm_guardrail: { model: 'suggestion' } }
    );
    expect(result.llmGuardrail).toBe(false);
    expect(result.configGuardrail).toBe(false);
  });
});
