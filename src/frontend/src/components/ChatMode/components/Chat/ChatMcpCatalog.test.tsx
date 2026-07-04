import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import ChatMcpCatalog from './ChatMcpCatalog';

const api = {
  getDatabricksCatalog: vi.fn(),
  listGenieSpaces: vi.fn(),
  listAiSearchIndexes: vi.fn(),
  ensureDatabricksServer: vi.fn(),
};
vi.mock('../../../../api/MCPService', () => ({
  MCPService: { getInstance: () => api },
  databricksMcpServerName: (o: { name: string }) => o.name.toLowerCase(),
}));

beforeEach(() => {
  vi.clearAllMocks();
  api.getDatabricksCatalog.mockResolvedValue({
    workspace_url: 'https://ws',
    external: [{ id: 'ext1', kind: 'external', name: 'my-uc-mcp', server_url: 'https://ws/mcp/ext', description: 'UC connection' }],
    managed: [
      { id: 'sql', kind: 'databricks-sql', name: 'Databricks SQL', expandable: false, server_url: 'https://ws/mcp/sql' },
      { id: 'gen', kind: 'genie', name: 'Genie spaces', expandable: true },
      { id: 'ais', kind: 'ai-search', name: 'AI Search indexes', expandable: true },
    ],
  });
  api.listGenieSpaces.mockResolvedValue({
    options: [{ id: 'g1', kind: 'genie', name: 'Sales Genie', server_url: 'https://ws/mcp/genie/g1' }],
    next_page_token: null,
  });
  api.listAiSearchIndexes.mockResolvedValue([
    { id: 'i1', kind: 'ai-search', name: 'docs_index', server_url: 'https://ws/mcp/ais/i1' },
  ]);
  api.ensureDatabricksServer.mockResolvedValue('my-uc-mcp');
});

describe('ChatMcpCatalog', () => {
  it('lists external options and managed entries (leaves + expandable)', async () => {
    render(<ChatMcpCatalog scope="global" onRegistered={vi.fn()} />);
    expect(await screen.findByText('my-uc-mcp')).toBeInTheDocument();
    expect(screen.getByText('Databricks SQL')).toBeInTheDocument();
    expect(screen.getByText('Genie spaces')).toBeInTheDocument();
    expect(screen.getByText('AI Search indexes')).toBeInTheDocument();
  });

  it('registers a picked option at the given scope and reloads the parent', async () => {
    const onRegistered = vi.fn();
    render(<ChatMcpCatalog scope="global" onRegistered={onRegistered} />);
    await screen.findByText('my-uc-mcp');
    // First "Add" is the external option.
    fireEvent.click(screen.getAllByRole('button', { name: 'Add' })[0]);
    await waitFor(() =>
      expect(api.ensureDatabricksServer).toHaveBeenCalledWith(
        expect.objectContaining({ name: 'my-uc-mcp' }),
        'global',
      ),
    );
    expect(onRegistered).toHaveBeenCalled();
    // The row flips to "Added".
    expect(await screen.findByText('Added')).toBeInTheDocument();
  });

  it('drills into Genie spaces and registers one', async () => {
    render(<ChatMcpCatalog scope="workspace" onRegistered={vi.fn()} />);
    fireEvent.click(await screen.findByText('Genie spaces'));
    expect(await screen.findByText('Sales Genie')).toBeInTheDocument();
    expect(api.listGenieSpaces).toHaveBeenCalled();
    fireEvent.click(screen.getByRole('button', { name: 'Add' }));
    await waitFor(() =>
      expect(api.ensureDatabricksServer).toHaveBeenCalledWith(
        expect.objectContaining({ name: 'Sales Genie' }),
        'workspace',
      ),
    );
    // Back returns to the catalog.
    fireEvent.click(screen.getByText('Back'));
    expect(await screen.findByText('my-uc-mcp')).toBeInTheDocument();
  });

  it('drills into AI Search indexes', async () => {
    render(<ChatMcpCatalog scope="global" onRegistered={vi.fn()} />);
    fireEvent.click(await screen.findByText('AI Search indexes'));
    expect(await screen.findByText('docs_index')).toBeInTheDocument();
    expect(api.listAiSearchIndexes).toHaveBeenCalled();
  });

  it('shows an empty state when the workspace exposes no Databricks MCPs', async () => {
    api.getDatabricksCatalog.mockResolvedValue({ workspace_url: '', external: [], managed: [] });
    render(<ChatMcpCatalog scope="global" onRegistered={vi.fn()} />);
    expect(await screen.findByText(/No Databricks MCP servers found/i)).toBeInTheDocument();
  });
});
