import { beforeEach, describe, expect, it, vi } from 'vitest';
import { fetchEnabledModelRows, toModelRecord } from './EnabledModelsService';
import { apiClient } from '../../shared/api/client';
import { getDefaultModel } from '../../config/defaultModel';

vi.mock('../../shared/api/client', () => ({ apiClient: { get: vi.fn() } }));
const get = apiClient.get as unknown as ReturnType<typeof vi.fn>;

describe('fetchEnabledModelRows', () => {
  beforeEach(() => vi.clearAllMocks());

  it('returns the rows and records the server default', async () => {
    get.mockResolvedValue({ data: { models: [{ key: 'm', name: 'M' }, { name: 'no key' }], default_model: 'srv-default' } });
    expect((await fetchEnabledModelRows()).map((r) => r.key)).toEqual(['m']);
    expect(getDefaultModel()).toBe('srv-default');
  });

  it('throws on an unexpected payload, so the store keeps its last good list', async () => {
    get.mockResolvedValue({ data: { detail: 'nope' } });
    await expect(fetchEnabledModelRows()).rejects.toThrow('Unexpected');
  });
});

describe('toModelRecord', () => {
  it('keys the rows and carries every capability field', () => {
    const record = toModelRecord([{
      id: 1, key: 'm', name: 'M', provider: null, temperature: null, context_window: 1000,
      max_output_tokens: null, extended_thinking: true, enabled: true, created_at: '', updated_at: '',
      thinking_mode: 'adaptive', allowed_efforts: ['low'], refused_params: ['temperature'],
      supports_reasoning_effort: true, returns_thinking_text: true,
    }]);
    expect(record.m).toMatchObject({
      name: 'M', provider: undefined, context_window: 1000, thinking_mode: 'adaptive',
      allowed_efforts: ['low'], refused_params: ['temperature'], supports_reasoning_effort: true,
      returns_thinking_text: true, enabled: true,
    });
  });
});
