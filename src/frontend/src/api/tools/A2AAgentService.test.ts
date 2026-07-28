import { describe, it, expect, vi, beforeEach } from 'vitest';

vi.mock('../../config/api/ApiConfig', () => ({
  apiClient: {
    get: vi.fn(),
    post: vi.fn(),
    put: vi.fn(),
    patch: vi.fn(),
    delete: vi.fn(),
  },
}));

import { apiClient } from '../../config/api/ApiConfig';
import { A2AAgentService } from './A2AAgentService';

const client = apiClient as unknown as {
  get: ReturnType<typeof vi.fn>;
  post: ReturnType<typeof vi.fn>;
  put: ReturnType<typeof vi.fn>;
  patch: ReturnType<typeof vi.fn>;
  delete: ReturnType<typeof vi.fn>;
};

beforeEach(() => {
  vi.clearAllMocks();
});

describe('A2AAgentService', () => {
  it('unwraps the list envelope', async () => {
    client.get.mockResolvedValue({ data: { agents: [{ id: 1 }], count: 1 } });
    await expect(A2AAgentService.list()).resolves.toEqual([{ id: 1 }]);
    expect(client.get).toHaveBeenCalledWith('/a2a-agents');
  });

  it('survives a response with no agents field', async () => {
    // A defensive default rather than a crash in the config page.
    client.get.mockResolvedValue({ data: {} });
    await expect(A2AAgentService.list()).resolves.toEqual([]);
  });

  it('sends an empty api_key when the caller is clearing a stored one', async () => {
    // '' clears; omitted leaves it alone. Collapsing the two would make a
    // stored credential impossible to remove.
    client.put.mockResolvedValue({ data: { id: 1 } });
    await A2AAgentService.update(1, { api_key: '' });
    expect(client.put).toHaveBeenCalledWith('/a2a-agents/1', { api_key: '' });
  });

  it('omits api_key entirely when it is not being changed', async () => {
    client.put.mockResolvedValue({ data: { id: 1 } });
    await A2AAgentService.update(1, { enabled: false });
    expect(client.put.mock.calls[0][1]).not.toHaveProperty('api_key');
  });

  it('returns the test result rather than throwing on an unreachable remote', async () => {
    // The endpoint answers 200 with connected:false — that is a result the
    // operator asked for, not a failed request.
    client.post.mockResolvedValue({
      data: { connected: false, message: 'Could not resolve host', skills: [] },
    });
    await expect(A2AAgentService.test(3)).resolves.toMatchObject({
      connected: false,
    });
    expect(client.post).toHaveBeenCalledWith('/a2a-agents/3/test');
  });

  it('deletes by id', async () => {
    client.delete.mockResolvedValue({ data: null });
    await A2AAgentService.remove(7);
    expect(client.delete).toHaveBeenCalledWith('/a2a-agents/7');
  });
});

describe('A2AAgentService — global vs workspace', () => {
  it('reads the Kasal-admin catalogue from /base', async () => {
    client.get.mockResolvedValue({ data: { agents: [], count: 0 } });
    await A2AAgentService.listBase();
    expect(client.get).toHaveBeenCalledWith('/a2a-agents/base');
  });

  it('withdraws an agent globally through its own endpoint', async () => {
    // Distinct from the workspace toggle: this one cascades everywhere, so it
    // must never be reachable by the workspace path.
    client.patch = vi.fn().mockResolvedValue({ data: { id: 1 } });
    await A2AAgentService.setGlobalAvailability(1, false);
    expect(client.patch).toHaveBeenCalledWith('/a2a-agents/1/global-availability', {
      enabled: false,
    });
  });

  it('opts a workspace in without touching the global row', async () => {
    client.patch = vi.fn().mockResolvedValue({ data: { id: 9, group_id: 'acme' } });
    const saved = await A2AAgentService.setWorkspaceEnabled(1, true);
    expect(client.patch).toHaveBeenCalledWith('/a2a-agents/1/workspace-enabled', {
      enabled: true,
    });
    // The response is the workspace's own copy — a DIFFERENT id from the base,
    // which is why the list reconciles by name.
    expect(saved.id).toBe(9);
  });
});
