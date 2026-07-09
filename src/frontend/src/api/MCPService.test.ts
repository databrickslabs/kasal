import { describe, it, expect, vi, beforeEach } from 'vitest';

// Mock the shared axios client before importing the service.
vi.mock('../config/api/ApiConfig', () => ({
  apiClient: {
    get: vi.fn(),
    post: vi.fn(),
    put: vi.fn(),
    patch: vi.fn(),
    delete: vi.fn(),
  },
}));

import { apiClient } from '../config/api/ApiConfig';
import { MCPService, databricksMcpServerName } from './MCPService';

const client = apiClient as unknown as {
  get: ReturnType<typeof vi.fn>;
  post: ReturnType<typeof vi.fn>;
  put: ReturnType<typeof vi.fn>;
  patch: ReturnType<typeof vi.fn>;
  delete: ReturnType<typeof vi.fn>;
};

const service = MCPService.getInstance();

const genieOption = {
  id: 'genie:s1',
  kind: 'genie',
  name: 'Sales Space',
  description: 'sales',
  server_url: 'https://ws.example.com/api/2.0/mcp/genie/s1',
};

const aiSearchOption = {
  id: 'ai-search:main.gold.docs_idx',
  kind: 'ai-search',
  name: 'main.gold.docs_idx',
  description: 'Endpoint: ep1',
  server_url: 'https://ws.example.com/api/2.0/mcp/ai-search/main/gold/docs_idx',
};

beforeEach(() => {
  vi.clearAllMocks();
});

describe('databricksMcpServerName', () => {
  it('prefixes Genie and AI Search instances and lowercases every name', () => {
    expect(databricksMcpServerName(genieOption)).toBe('databricks genie: sales space');
    expect(databricksMcpServerName(aiSearchOption)).toBe(
      'databricks ai search: main.gold.docs_idx',
    );
    expect(
      databricksMcpServerName({ kind: 'sql', name: 'Databricks SQL' }),
    ).toBe('databricks sql');
  });
});

describe('getDatabricksCatalog', () => {
  it('returns the grouped external + managed catalog', async () => {
    const catalog = {
      workspace_url: 'https://ws.example.com',
      external: [
        {
          id: 'external:jira',
          kind: 'external',
          name: 'jira',
          description: 'Jira MCP',
          server_url: 'https://ws.example.com/api/2.0/mcp/external/jira',
        },
      ],
      managed: [
        { id: 'sql', kind: 'sql', name: 'Databricks SQL', server_url: 'u', expandable: false },
        { id: 'genie', kind: 'genie', name: 'Genie', expandable: true },
      ],
    };
    client.get.mockResolvedValue({ data: catalog });
    expect(await service.getDatabricksCatalog()).toEqual(catalog);
    expect(client.get).toHaveBeenCalledWith('/mcp/databricks/available');
  });

  it('fills defaults when the response is partial', async () => {
    client.get.mockResolvedValue({ data: {} });
    expect(await service.getDatabricksCatalog()).toEqual({
      workspace_url: '',
      external: [],
      managed: [],
    });
  });
});

describe('listGenieSpaces', () => {
  it('passes search and page token and returns options with the next token', async () => {
    client.get.mockResolvedValue({ data: { options: [genieOption], next_page_token: 'tok-2' } });

    const result = await service.listGenieSpaces('sales', 'tok-1');

    expect(result).toEqual({ options: [genieOption], next_page_token: 'tok-2' });
    expect(client.get).toHaveBeenCalledWith('/mcp/databricks/genie-spaces', {
      params: { search: 'sales', page_token: 'tok-1' },
    });
  });

  it('omits empty params and tolerates a bare response', async () => {
    client.get.mockResolvedValue({ data: {} });

    const result = await service.listGenieSpaces();

    expect(result).toEqual({ options: [], next_page_token: null });
    expect(client.get).toHaveBeenCalledWith('/mcp/databricks/genie-spaces', { params: {} });
  });
});

describe('listAiSearchIndexes', () => {
  it('returns the indexes', async () => {
    client.get.mockResolvedValue({ data: { options: [aiSearchOption] } });
    expect(await service.listAiSearchIndexes()).toEqual([aiSearchOption]);
    expect(client.get).toHaveBeenCalledWith('/mcp/databricks/ai-search-indexes');
  });

  it('tolerates a response without options', async () => {
    client.get.mockResolvedValue({ data: {} });
    expect(await service.listAiSearchIndexes()).toEqual([]);
  });
});

describe('ensureDatabricksServer', () => {
  it('reuses an exact lowercase registration without any writes', async () => {
    client.get.mockResolvedValue({
      data: { servers: [{ id: 7, name: 'databricks genie: sales space', enabled: true }] },
    });

    const name = await service.ensureDatabricksServer(genieOption);

    expect(name).toBe('databricks genie: sales space');
    expect(client.post).not.toHaveBeenCalled();
    expect(client.patch).not.toHaveBeenCalled();
    expect(client.put).not.toHaveBeenCalled();
  });

  it('renames legacy mixed-case registrations to the lowercase name', async () => {
    client.get.mockResolvedValue({
      data: { servers: [{ id: 7, name: 'Databricks Genie: Sales Space', enabled: true }] },
    });
    client.put.mockResolvedValue({ data: {} });

    const name = await service.ensureDatabricksServer(genieOption);

    expect(name).toBe('databricks genie: sales space');
    expect(client.put).toHaveBeenCalledWith('/mcp/servers/7', {
      name: 'databricks genie: sales space',
    });
    expect(client.post).not.toHaveBeenCalled();
  });

  it('falls back to the stored name when the rename is not permitted', async () => {
    client.get.mockResolvedValue({
      data: { servers: [{ id: 7, name: 'Databricks Genie: Sales Space', enabled: true }] },
    });
    client.put.mockRejectedValue({ response: { status: 403 } });

    const name = await service.ensureDatabricksServer(genieOption);

    // Crews resolve by the REGISTERED name, so the stored casing wins.
    expect(name).toBe('Databricks Genie: Sales Space');
  });

  it('re-enables an existing registration matched by URL and normalizes its name', async () => {
    client.get.mockResolvedValue({
      data: {
        servers: [{ id: 9, name: 'Old name', enabled: false, server_url: genieOption.server_url }],
      },
    });
    client.patch.mockResolvedValue({ data: { message: '', enabled: true } });
    client.put.mockResolvedValue({ data: {} });

    const name = await service.ensureDatabricksServer(genieOption);

    expect(name).toBe('databricks genie: sales space');
    // Workspace scope re-enables via the per-workspace endpoint.
    expect(client.patch).toHaveBeenCalledWith('/mcp/servers/9/workspace-enabled', { enabled: true });
    expect(client.put).toHaveBeenCalledWith('/mcp/servers/9', {
      name: 'databricks genie: sales space',
    });
    expect(client.post).not.toHaveBeenCalled();
  });

  it('registers a new workspace-scoped server (streamable + databricks_obo, lowercase name)', async () => {
    client.get.mockResolvedValue({ data: { servers: [] } });
    client.post.mockResolvedValue({ data: {} });

    const name = await service.ensureDatabricksServer(genieOption);

    expect(name).toBe('databricks genie: sales space');
    expect(client.get).toHaveBeenCalledWith('/mcp/servers'); // workspace scope reads effective list
    expect(client.post).toHaveBeenCalledWith(
      '/mcp/servers',
      expect.objectContaining({
        name: 'databricks genie: sales space',
        server_url: genieOption.server_url,
        server_type: 'streamable',
        // Managed MCP runs per-user OBO (commit 89253b874) — the SPN auth type
        // was retired for these registrations.
        auth_type: 'databricks_obo',
        enabled: true,
      }),
    );
  });

  it("scope='global' matches against base servers and registers a global server", async () => {
    client.get.mockResolvedValue({ data: { servers: [] } });
    client.post.mockResolvedValue({ data: {} });

    const name = await service.ensureDatabricksServer(genieOption, 'global');

    expect(name).toBe('databricks genie: sales space');
    expect(client.get).toHaveBeenCalledWith('/mcp/servers/base'); // global scope reads base list
    expect(client.post).toHaveBeenCalledWith(
      '/mcp/servers/global',
      expect.objectContaining({ name: 'databricks genie: sales space', enabled: true }),
    );
  });
});

describe('getBaseServers', () => {
  it('reads the base/global catalog', async () => {
    client.get.mockResolvedValue({ data: { servers: [{ id: 1, name: 'g', enabled: true }], count: 1 } });
    const res = await service.getBaseServers();
    expect(res.servers).toHaveLength(1);
    expect(client.get).toHaveBeenCalledWith('/mcp/servers/base');
  });
});

describe('createGlobalServer', () => {
  it('posts to the global endpoint', async () => {
    client.post.mockResolvedValue({ data: { id: 9, name: 'g' } });
    await service.createGlobalServer({ name: 'g' } as never);
    expect(client.post).toHaveBeenCalledWith('/mcp/servers/global', { name: 'g' });
  });
});

describe('setGlobalAvailability', () => {
  it('patches the global-availability endpoint with the enabled flag', async () => {
    client.patch.mockResolvedValue({ data: {} });
    await service.setGlobalAvailability('5', false);
    expect(client.patch).toHaveBeenCalledWith('/mcp/servers/5/global-availability', { enabled: false });
  });
});

describe('setWorkspaceEnabled', () => {
  it('patches the workspace-enabled endpoint with the enabled flag', async () => {
    client.patch.mockResolvedValue({ data: {} });
    await service.setWorkspaceEnabled('5', false);
    expect(client.patch).toHaveBeenCalledWith('/mcp/servers/5/workspace-enabled', { enabled: false });
  });
});
