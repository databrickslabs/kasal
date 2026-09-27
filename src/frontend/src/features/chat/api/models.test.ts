import { describe, it, expect, vi, beforeEach } from 'vitest';
import { fetchEnabledModels } from './models';
import { apiClient } from '../../../shared/api/client';

vi.mock('../../../shared/api/client', () => ({
  apiClient: { get: vi.fn() },
}));

const get = apiClient.get as unknown as ReturnType<typeof vi.fn>;

describe('fetchEnabledModels', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('returns response.data.models from /models/enabled', async () => {
    const models = [
      { id: 1, key: 'm1', name: 'Model One' },
      { id: 2, key: 'm2', name: 'Model Two' },
    ];
    get.mockResolvedValue({ data: { models, count: models.length } });

    const result = await fetchEnabledModels();

    expect(get).toHaveBeenCalledWith('/models/enabled');
    expect(result).toEqual(models);
  });

  it('returns an empty array when no models are present', async () => {
    get.mockResolvedValue({ data: { models: [], count: 0 } });

    const result = await fetchEnabledModels();

    expect(result).toEqual([]);
  });

  it('propagates errors from the client', async () => {
    get.mockRejectedValue(new Error('network down'));

    await expect(fetchEnabledModels()).rejects.toThrow('network down');
  });
});
