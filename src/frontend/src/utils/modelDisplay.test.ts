import { formatModelLabel, buildModelLabels } from './modelDisplay';

describe('formatModelLabel', () => {
  it('keeps the version AND the variant for the GPT-5 family', () => {
    // The bug: a catch-all `includes('gpt-5') -> 'GPT-5'` erased both.
    expect(formatModelLabel('gpt-5.6-sol')).toBe('GPT-5.6 Sol');
    expect(formatModelLabel('gpt-5.6-terra')).toBe('GPT-5.6 Terra');
    expect(formatModelLabel('gpt-5.6-luna')).toBe('GPT-5.6 Luna');
    expect(formatModelLabel('gpt-5-nano')).toBe('GPT-5 Nano');
    expect(formatModelLabel('gpt-5-mini')).toBe('GPT-5 Mini');
    expect(formatModelLabel('gpt-5')).toBe('GPT-5');
  });

  it('strips the databricks- prefix', () => {
    expect(formatModelLabel('databricks-gpt-5-nano')).toBe('GPT-5 Nano');
  });

  it('keeps hand-written labels for irregular ids', () => {
    expect(formatModelLabel('databricks-llama-4-maverick')).toBe('Llama 4');
    expect(formatModelLabel('gpt-oss-120b')).toBe('GPT OSS 120B');
  });

  it('passes through names that need no prettifying', () => {
    expect(formatModelLabel('kimi-k2.7-code-highspeed')).toBe('kimi-k2.7-code-highspeed');
    expect(formatModelLabel('Qwen3-Coder-30B-A3B-Instruct')).toBe('Qwen3-Coder-30B-A3B-Instruct');
  });

  it('never returns undefined for empty input', () => {
    expect(formatModelLabel('')).toBe('');
  });
});

describe('buildModelLabels', () => {
  it('produces a distinct label per model for the real enabled set', () => {
    const models = {
      'gpt-5-nano': { name: 'gpt-5-nano' },
      'gpt-5.6-sol': { name: 'gpt-5.6-sol' },
      'gpt-5.6-terra': { name: 'gpt-5.6-terra' },
      'gpt-5.6-luna': { name: 'gpt-5.6-luna' },
      'kimi-k2.7-code': { name: 'kimi-k2.7-code' },
      'kimi-k2.7-code-highspeed': { name: 'kimi-k2.7-code-highspeed' },
    };
    const labels = buildModelLabels(models);
    const values = Object.values(labels);
    expect(new Set(values).size).toBe(values.length);
    expect(labels['gpt-5.6-sol']).toBe('GPT-5.6 Sol');
  });

  it('falls back to raw names when two models would share a label', () => {
    // Both hit the same hand-written special case.
    const models = {
      a: { name: 'databricks-llama-4-maverick' },
      b: { name: 'databricks-llama-4-maverick-v2' },
    };
    const labels = buildModelLabels(models);
    expect(labels.a).toBe('databricks-llama-4-maverick');
    expect(labels.b).toBe('databricks-llama-4-maverick-v2');
    expect(labels.a).not.toBe(labels.b);
  });

  it('falls back to the key when a model has no name', () => {
    const labels = buildModelLabels({ 'some-key': {} });
    expect(labels['some-key']).toBe('some-key');
  });
});
