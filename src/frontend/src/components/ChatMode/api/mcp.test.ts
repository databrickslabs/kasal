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
  it('prefixes Genie and AI Search instances and keeps other kinds as-is', () => {
    expect(databricksMcpServerName(genieOption)).toBe('Databricks Genie: Sales Space');
    expect(databricksMcpServerName(aiSearchOption)).toBe(
      'Databricks AI Search: main.gold.docs_idx',
    );
    expect(
      databricksMcpServerName({ id: 'sql', kind: 'sql', name: 'Databricks SQL', server_url: 'u' }),
    ).toBe('Databricks SQL');
  });
});

describe('ensureDatabricksMcpServer', () => {
  it('reuses an existing enabled registration by name (no writes)', async () => {
    client.get.mockResolvedValue({
      data: { servers: [{ id: 7, name: 'Databricks Genie: Sales Space', enabled: true }] },
    });

    const name = await ensureDatabricksMcpServer(genieOption);

    expect(name).toBe('Databricks Genie: Sales Space');
    expect(client.post).not.toHaveBeenCalled();
    expect(client.patch).not.toHaveBeenCalled();
  });

  it('re-enables an existing registration matched by URL', async () => {
    client.get.mockResolvedValue({
      data: {
        servers: [
          { id: 9, name: 'Old name', enabled: false, server_url: genieOption.server_url },
        ],
      },
    });
    client.patch.mockResolvedValue({ data: {} });

    const name = await ensureDatabricksMcpServer(genieOption);

    expect(name).toBe('Old name');
    expect(client.patch).toHaveBeenCalledWith('/mcp/servers/9/toggle-enabled');
    expect(client.post).not.toHaveBeenCalled();
  });

  it('registers a new Kasal server (streamable + databricks_spn) when none exists', async () => {
    client.get.mockResolvedValue({ data: { servers: [] } });
    client.post.mockResolvedValue({ data: {} });

    const name = await ensureDatabricksMcpServer(genieOption);

    expect(name).toBe('Databricks Genie: Sales Space');
    expect(client.post).toHaveBeenCalledWith('/mcp/servers', {
      name: 'Databricks Genie: Sales Space',
      server_url: genieOption.server_url,
      server_type: 'streamable',
      auth_type: 'databricks_spn',
      enabled: true,
    });
  });
});
