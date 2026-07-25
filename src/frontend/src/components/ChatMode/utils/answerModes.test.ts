import { describe, it, expect } from 'vitest';
import {
  answerModeDisabledReason,
  answerModeHint,
  isAnswerModeDisabled,
  modelDisplayName,
  modelLacksReasoning,
} from './answerModes';
import { ModelConfigResponse } from '../types/dispatcher';

const model = (
  key: string,
  supports_reasoning_effort: boolean | undefined,
): ModelConfigResponse => ({
  id: 1,
  key,
  name: key,
  provider: 'openai',
  temperature: 1,
  context_window: 128000,
  max_output_tokens: 32000,
  extended_thinking: false,
  enabled: true,
  supports_reasoning_effort,
  created_at: '',
  updated_at: '',
});

const REASONING = model('gpt-5.6-terra', true);
const INSTRUCT = model('Qwen3-Coder-30B-A3B-Instruct', false);
const UNKNOWN = model('legacy-model', undefined);

describe('modelLacksReasoning', () => {
  it('is true only when the model is KNOWN to have no budget', () => {
    expect(modelLacksReasoning([INSTRUCT], INSTRUCT.key)).toBe(true);
    expect(modelLacksReasoning([REASONING], REASONING.key)).toBe(false);
  });

  // A false "unsupported" would disable a working mode, which is worse than
  // briefly offering one that turns out to be a no-op.
  it('treats an unloaded list as unknown, not unsupported', () => {
    expect(modelLacksReasoning([], 'anything')).toBe(false);
  });

  it('treats a missing field as unknown, not unsupported', () => {
    expect(modelLacksReasoning([UNKNOWN], UNKNOWN.key)).toBe(false);
  });

  it('treats an unselected or unrecognised model as unknown', () => {
    expect(modelLacksReasoning([INSTRUCT], '')).toBe(false);
    expect(modelLacksReasoning([INSTRUCT], 'some-other-key')).toBe(false);
  });
});

describe('isAnswerModeDisabled', () => {
  it('disables Deep Research only, and only without a reasoning budget', () => {
    expect(isAnswerModeDisabled('deep', true)).toBe(true);
    expect(isAnswerModeDisabled('deep', false)).toBe(false);
  });

  // Research still builds a full crew instead of one agent — a real difference
  // on any model. Hiding it would remove a working feature.
  it('never disables Chat or Research', () => {
    expect(isAnswerModeDisabled('chat', true)).toBe(false);
    expect(isAnswerModeDisabled('research', true)).toBe(false);
  });
});

describe('answerModeHint', () => {
  it('drops the reasoning claim when the model has no budget', () => {
    expect(answerModeHint('research', true)).toBe('Full multi-agent crew');
    expect(answerModeHint('research', false)).toBe('Full crew with reasoning');
  });

  it('says what Deep Research needs rather than what it would do', () => {
    expect(answerModeHint('deep', true)).toBe('Needs a model with a reasoning budget');
    expect(answerModeHint('deep', false)).toBe('Deep tools with maximum reasoning');
  });

  it('leaves Chat unchanged — it never claimed reasoning', () => {
    expect(answerModeHint('chat', true)).toBe(answerModeHint('chat', false));
  });
});

describe('answerModeDisabledReason', () => {
  it('names the model and says what would happen instead', () => {
    const reason = answerModeDisabledReason('Qwen3-Coder-30B-A3B-Instruct');
    expect(reason).toContain('Qwen3-Coder-30B-A3B-Instruct');
    expect(reason).toContain('Research');
  });
});

describe('modelDisplayName', () => {
  it('prefers the display name, falls back to the key, then a generic label', () => {
    expect(modelDisplayName([REASONING], REASONING.key)).toBe('gpt-5.6-terra');
    expect(modelDisplayName([], 'raw-key')).toBe('raw-key');
    expect(modelDisplayName([], '')).toBe('This model');
  });
});
