import { describe, it, expect, vi, beforeEach } from 'vitest';
import { listKasalMcpServers } from './mcp';
import { getClient } from './client';

vi.mock('./client', () => ({
  getClient: vi.fn(),
}));

const mockedGetClient = vi.mocked(getClient);

const client = {
  get: vi.fn(),
  post: vi.fn(),
  patch: vi.fn(),
  put: vi.fn(),
};

beforeEach(() => {
  vi.clearAllMocks();
  mockedGetClient.mockReturnValue(client as never);
});

describe('listKasalMcpServers', () => {
  it('returns the configured servers', async () => {
    client.get.mockResolvedValue({ data: { servers: [{ id: 1, name: 'A', enabled: true }] } });
    expect(await listKasalMcpServers()).toEqual([{ id: 1, name: 'A', enabled: true }]);
    expect(client.get).toHaveBeenCalledWith('/mcp/servers');
  });

  it('tolerates a response without servers', async () => {
    client.get.mockResolvedValue({ data: {} });
    expect(await listKasalMcpServers()).toEqual([]);
  });
});
