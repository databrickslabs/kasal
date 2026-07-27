import { describe, it, expect } from 'vitest';
import { isLlmEvent, extractLlmText } from './llmEventText';

describe('isLlmEvent', () => {
  it('covers the three LLM row types and nothing else', () => {
    expect(['llm', 'llm_request', 'llm_response'].every(isLlmEvent)).toBe(true);
    expect(isLlmEvent('tool')).toBe(false);
    expect(isLlmEvent('memory_write')).toBe(false);
  });
});

describe('extractLlmText', () => {
  it('reads the prompt off a request row', () => {
    const text = extractLlmText({
      type: 'llm',
      description: 'LLM Request',
      output: { duration_ms: 0.07 },
      extraData: { model: 'some-model', prompt: 'Answer the question.\n\nWhy?' },
    });
    // The prompt itself — not the JSON envelope the generic view dumped.
    expect(text).toBe('Answer the question.\n\nWhy?');
  });

  it('falls back to task_prompt when the row has no prompt field', () => {
    expect(extractLlmText({
      type: 'llm_request',
      description: 'LLM Request',
      extraData: { task_prompt: 'the task prompt' },
    })).toBe('the task prompt');
  });

  it('reads the answer off a response row', () => {
    expect(extractLlmText({
      type: 'llm_response',
      description: 'LLM Response',
      output: 'the answer',
    })).toBe('the answer');
  });

  it('unwraps a response still shaped as the raw row output', () => {
    expect(extractLlmText({
      type: 'llm_response',
      description: 'LLM Response',
      output: { content: 'the answer' },
    })).toBe('the answer');
  });

  it('returns undefined when the row carries no text', () => {
    expect(extractLlmText({ type: 'llm', description: 'LLM Request' })).toBeUndefined();
  });
});
