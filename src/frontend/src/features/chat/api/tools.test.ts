import { describe, it, expect, vi, beforeEach } from 'vitest';
import { fetchEnabledTools, ToolInfo } from './tools';
import { getClient } from './client';

vi.mock('./client', () => ({
  getClient: vi.fn(),
}));

describe('fetchEnabledTools', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('returns the tools array from the client response', async () => {
    const tools: ToolInfo[] = [
      { id: 1, title: 'Search', description: 'Search the web', enabled: true },
      { id: 2, title: 'Calc', description: 'Do math', enabled: false },
    ];
    const get = vi.fn().mockResolvedValue({ data: { tools, count: tools.length } });
    (getClient as unknown as ReturnType<typeof vi.fn>).mockReturnValue({ get });

    const result = await fetchEnabledTools();

    expect(getClient).toHaveBeenCalledTimes(1);
    expect(get).toHaveBeenCalledWith('/tools/enabled');
    expect(result).toEqual(tools);
  });

  it('returns an empty array when no tools are enabled', async () => {
    const get = vi.fn().mockResolvedValue({ data: { tools: [], count: 0 } });
    (getClient as unknown as ReturnType<typeof vi.fn>).mockReturnValue({ get });

    const result = await fetchEnabledTools();

    expect(result).toEqual([]);
  });

  it('propagates errors from the client', async () => {
    const get = vi.fn().mockRejectedValue(new Error('network down'));
    (getClient as unknown as ReturnType<typeof vi.fn>).mockReturnValue({ get });

    await expect(fetchEnabledTools()).rejects.toThrow('network down');
  });
});
