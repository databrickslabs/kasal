import { describe, it, expect, vi, beforeEach } from 'vitest';
import { fetchEnabledModels } from './models';
import { getClient } from './client';

vi.mock('./client', () => ({
  getClient: vi.fn(),
}));

describe('fetchEnabledModels', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('returns response.data.models from /models/enabled', async () => {
    const models = [
      { id: 'm1', name: 'Model One' },
      { id: 'm2', name: 'Model Two' },
    ];
    const get = vi.fn().mockResolvedValue({ data: { models, count: models.length } });
    (getClient as unknown as ReturnType<typeof vi.fn>).mockReturnValue({ get });

    const result = await fetchEnabledModels();

    expect(getClient).toHaveBeenCalledTimes(1);
    expect(get).toHaveBeenCalledWith('/models/enabled');
    expect(result).toBe(models);
  });

  it('returns an empty array when no models are present', async () => {
    const get = vi.fn().mockResolvedValue({ data: { models: [], count: 0 } });
    (getClient as unknown as ReturnType<typeof vi.fn>).mockReturnValue({ get });

    const result = await fetchEnabledModels();

    expect(result).toEqual([]);
  });

  it('propagates errors from the client', async () => {
    const error = new Error('network down');
    const get = vi.fn().mockRejectedValue(error);
    (getClient as unknown as ReturnType<typeof vi.fn>).mockReturnValue({ get });

    await expect(fetchEnabledModels()).rejects.toThrow('network down');
  });
});
