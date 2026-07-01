import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor, act } from '@testing-library/react';
import '@testing-library/jest-dom';
import DatabricksMcpCatalog from './DatabricksMcpCatalog';

vi.mock('react-i18next', () => ({
  useTranslation: () => ({
    t: (key: string, options?: { defaultValue?: string }) => options?.defaultValue || key,
  }),
}));

const mockService = {
  getDatabricksCatalog: vi.fn(),
  listGenieSpaces: vi.fn(),
  listAiSearchIndexes: vi.fn(),
  setGlobalAvailability: vi.fn(),
  setWorkspaceEnabled: vi.fn(),
  ensureDatabricksServer: vi.fn(),
};

vi.mock('../../../api/MCPService', () => ({
  MCPService: { getInstance: () => mockService },
  databricksMcpServerName: (o: { kind: string; name: string }) => {
    if (o.kind === 'genie') return `databricks genie: ${o.name}`.toLowerCase();
    if (o.kind === 'ai-search') return `databricks ai search: ${o.name}`.toLowerCase();
    return o.name.toLowerCase();
  },
}));

const EXTERNAL = {
  id: 'external:jira',
  kind: 'external',
  name: 'jira',
  description: 'Jira MCP',
  server_url: 'https://ws/api/2.0/mcp/external/jira',
};
const SQL_LEAF = {
  id: 'sql',
  kind: 'sql',
  name: 'Databricks SQL',
  description: null,
  server_url: 'https://ws/api/2.0/mcp/sql',
  expandable: false,
};
const GENIE_TYPE = { id: 'genie', kind: 'genie', name: 'Genie', expandable: true };
const AI_SEARCH_TYPE = { id: 'ai-search', kind: 'ai-search', name: 'AI Search', expandable: true };

const CATALOG = {
  workspace_url: 'https://ws',
  external: [EXTERNAL],
  managed: [SQL_LEAF, GENIE_TYPE, AI_SEARCH_TYPE],
};

const GENIE_SPACE = {
  id: 'genie:s1',
  kind: 'genie',
  name: 'Sales Space',
  description: 'sales data',
  server_url: 'https://ws/api/2.0/mcp/genie/s1',
};

// jira is already registered + enabled; everything else is not registered.
const REGISTERED = [
  { id: '1', name: 'jira', enabled: true, server_url: EXTERNAL.server_url },
];

beforeEach(() => {
  vi.clearAllMocks();
  mockService.getDatabricksCatalog.mockResolvedValue(CATALOG);
  mockService.listGenieSpaces.mockResolvedValue({ options: [GENIE_SPACE], next_page_token: null });
  mockService.listAiSearchIndexes.mockResolvedValue([]);
  mockService.setGlobalAvailability.mockResolvedValue({});
  mockService.setWorkspaceEnabled.mockResolvedValue({});
  mockService.ensureDatabricksServer.mockResolvedValue('databricks sql');
});

const renderCatalog = async (
  scope: 'workspace' | 'global' = 'global',
  registered = REGISTERED,
  onChanged = vi.fn(),
) => {
  await act(async () => {
    render(
      <DatabricksMcpCatalog registeredServers={registered} onChanged={onChanged} scope={scope} />,
    );
  });
  await screen.findByText('jira');
  return { onChanged };
};

describe('DatabricksMcpCatalog', () => {
  it('lists external + managed entries with toggles reflecting the registered state', async () => {
    await renderCatalog('global');

    expect(mockService.getDatabricksCatalog).toHaveBeenCalled();
    expect(screen.getByText('jira')).toBeInTheDocument();
    expect(screen.getByText('Databricks SQL')).toBeInTheDocument();
    expect(screen.getByText('Genie')).toBeInTheDocument();
    expect(screen.getByText('AI Search')).toBeInTheDocument();

    // jira is registered+enabled → ON; Databricks SQL is not registered → OFF.
    expect(screen.getByLabelText('Enable jira')).toBeChecked();
    expect(screen.getByLabelText('Enable Databricks SQL')).not.toBeChecked();
  });

  it("global scope: enabling a new entry registers it globally", async () => {
    const { onChanged } = await renderCatalog('global');

    fireEvent.click(screen.getByLabelText('Enable Databricks SQL'));

    await waitFor(() =>
      expect(mockService.ensureDatabricksServer).toHaveBeenCalledWith(
        expect.objectContaining({ id: 'sql', kind: 'sql', server_url: SQL_LEAF.server_url }),
        'global',
      ),
    );
    expect(onChanged).toHaveBeenCalled();
  });

  it('flips the toggle optimistically without waiting for a parent reload', async () => {
    // onChanged is a no-op mock (it does NOT update registeredServers), mirroring
    // the silent parent refresh. The row must still switch to ON immediately —
    // no dialog reload needed for the toggle to reflect the change.
    await renderCatalog('global');
    const sql = screen.getByLabelText('Enable Databricks SQL');
    expect(sql).not.toBeChecked();

    fireEvent.click(sql);

    await waitFor(() => expect(mockService.ensureDatabricksServer).toHaveBeenCalled());
    await waitFor(() =>
      expect(screen.getByLabelText('Enable Databricks SQL')).toBeChecked(),
    );
  });

  it('global scope: disabling a registered entry flips its global availability', async () => {
    const { onChanged } = await renderCatalog('global');

    fireEvent.click(screen.getByLabelText('Enable jira'));

    await waitFor(() => expect(mockService.setGlobalAvailability).toHaveBeenCalledWith('1', false));
    expect(onChanged).toHaveBeenCalled();
    expect(mockService.setWorkspaceEnabled).not.toHaveBeenCalled();
  });

  it('workspace scope: toggles route through the per-workspace endpoints', async () => {
    await renderCatalog('workspace');

    // Disable a registered (enabled) entry → per-workspace override.
    fireEvent.click(screen.getByLabelText('Enable jira'));
    await waitFor(() => expect(mockService.setWorkspaceEnabled).toHaveBeenCalledWith('1', false));
    expect(mockService.setGlobalAvailability).not.toHaveBeenCalled();

    // Enable a not-registered entry → register workspace-scoped.
    fireEvent.click(screen.getByLabelText('Enable Databricks SQL'));
    await waitFor(() =>
      expect(mockService.ensureDatabricksServer).toHaveBeenCalledWith(
        expect.objectContaining({ id: 'sql' }),
        'workspace',
      ),
    );
  });

  it('drills into Genie spaces on demand and can enable one', async () => {
    await renderCatalog('global');
    expect(mockService.listGenieSpaces).not.toHaveBeenCalled();

    fireEvent.click(screen.getByText('Genie'));
    await waitFor(() => expect(screen.getByText('Sales Space')).toBeInTheDocument());
    expect(mockService.listGenieSpaces).toHaveBeenCalled();

    fireEvent.click(screen.getByLabelText('Enable Sales Space'));
    await waitFor(() =>
      expect(mockService.ensureDatabricksServer).toHaveBeenCalledWith(
        expect.objectContaining({ id: 'genie:s1', kind: 'genie' }),
        'global',
      ),
    );
  });

  it('shows an empty state when the workspace exposes no Databricks MCPs', async () => {
    mockService.getDatabricksCatalog.mockResolvedValue({
      workspace_url: '',
      external: [],
      managed: [],
    });
    await act(async () => {
      render(<DatabricksMcpCatalog registeredServers={[]} onChanged={vi.fn()} scope="global" />);
    });
    await waitFor(() =>
      expect(
        screen.getByText('No Databricks MCP servers are available in this workspace.'),
      ).toBeInTheDocument(),
    );
  });

  it('surfaces a load error', async () => {
    mockService.getDatabricksCatalog.mockRejectedValue(new Error('forbidden'));
    await act(async () => {
      render(<DatabricksMcpCatalog registeredServers={[]} onChanged={vi.fn()} scope="global" />);
    });
    await waitFor(() => expect(screen.getByText('forbidden')).toBeInTheDocument());
  });
});
