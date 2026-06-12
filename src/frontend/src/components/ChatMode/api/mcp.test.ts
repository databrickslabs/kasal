import { describe, it, expect, vi, beforeEach } from 'vitest';
import {
  databricksMcpServerName,
  ensureDatabricksMcpServer,
  getDatabricksMcpCatalog,
  listAiSearchMcpIndexes,
  listGenieMcpSpaces,
  listKasalMcpServers,
} from './mcp';
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

describe('getDatabricksMcpCatalog', () => {
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
        {
          id: 'sql',
          kind: 'sql',
          name: 'Databricks SQL',
          server_url: 'https://ws.example.com/api/2.0/mcp/sql',
          expandable: false,
        },
        { id: 'genie', kind: 'genie', name: 'Genie', expandable: true },
      ],
    };
    client.get.mockResolvedValue({ data: catalog });
    expect(await getDatabricksMcpCatalog()).toEqual(catalog);
    expect(client.get).toHaveBeenCalledWith('/mcp/databricks/available');
  });

  it('fills defaults when the response is partial', async () => {
    client.get.mockResolvedValue({ data: {} });
    expect(await getDatabricksMcpCatalog()).toEqual({
      workspace_url: '',
      external: [],
      managed: [],
    });
  });
});

describe('listGenieMcpSpaces', () => {
  it('passes search and page token and returns options with the next token', async () => {
    client.get.mockResolvedValue({
      data: { options: [genieOption], next_page_token: 'tok-2' },
    });

    const result = await listGenieMcpSpaces('sales', 'tok-1');

    expect(result).toEqual({ options: [genieOption], next_page_token: 'tok-2' });
    expect(client.get).toHaveBeenCalledWith('/mcp/databricks/genie-spaces', {
      params: { search: 'sales', page_token: 'tok-1' },
    });
  });

  it('omits empty params and tolerates a bare response', async () => {
    client.get.mockResolvedValue({ data: {} });

    const result = await listGenieMcpSpaces();

    expect(result).toEqual({ options: [], next_page_token: null });
    expect(client.get).toHaveBeenCalledWith('/mcp/databricks/genie-spaces', { params: {} });
  });
});

describe('listAiSearchMcpIndexes', () => {
  it('returns the indexes', async () => {
    client.get.mockResolvedValue({ data: { options: [aiSearchOption] } });
    expect(await listAiSearchMcpIndexes()).toEqual([aiSearchOption]);
    expect(client.get).toHaveBeenCalledWith('/mcp/databricks/ai-search-indexes');
  });

  it('tolerates a response without options', async () => {
    client.get.mockResolvedValue({ data: {} });
    expect(await listAiSearchMcpIndexes()).toEqual([]);
  });
});

describe('databricksMcpServerName', () => {
  it('prefixes Genie and AI Search instances and lowercases every name', () => {
    expect(databricksMcpServerName(genieOption)).toBe('databricks genie: sales space');
    expect(databricksMcpServerName(aiSearchOption)).toBe(
      'databricks ai search: main.gold.docs_idx',
    );
    expect(
      databricksMcpServerName({ id: 'sql', kind: 'sql', name: 'Databricks SQL', server_url: 'u' }),
    ).toBe('databricks sql');
  });
});

describe('ensureDatabricksMcpServer', () => {
  it('reuses an exact lowercase registration without any writes', async () => {
    client.get.mockResolvedValue({
      data: { servers: [{ id: 7, name: 'databricks genie: sales space', enabled: true }] },
    });

    const name = await ensureDatabricksMcpServer(genieOption);

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

    const name = await ensureDatabricksMcpServer(genieOption);

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

    const name = await ensureDatabricksMcpServer(genieOption);

    // Crews resolve by the REGISTERED name, so the stored casing wins.
    expect(name).toBe('Databricks Genie: Sales Space');
  });

  it('re-enables an existing registration matched by URL and normalizes its name', async () => {
    client.get.mockResolvedValue({
      data: {
        servers: [
          { id: 9, name: 'Old name', enabled: false, server_url: genieOption.server_url },
        ],
      },
    });
    client.patch.mockResolvedValue({ data: {} });
    client.put.mockResolvedValue({ data: {} });

    const name = await ensureDatabricksMcpServer(genieOption);

    expect(name).toBe('databricks genie: sales space');
    expect(client.patch).toHaveBeenCalledWith('/mcp/servers/9/toggle-enabled');
    expect(client.put).toHaveBeenCalledWith('/mcp/servers/9', {
      name: 'databricks genie: sales space',
    });
    expect(client.post).not.toHaveBeenCalled();
  });

  it('registers a new Kasal server (streamable + databricks_spn, lowercase name)', async () => {
    client.get.mockResolvedValue({ data: { servers: [] } });
    client.post.mockResolvedValue({ data: {} });

    const name = await ensureDatabricksMcpServer(genieOption);

    expect(name).toBe('databricks genie: sales space');
    expect(client.post).toHaveBeenCalledWith('/mcp/servers', {
      name: 'databricks genie: sales space',
      server_url: genieOption.server_url,
      server_type: 'streamable',
      auth_type: 'databricks_spn',
      enabled: true,
    });
  });
});
