import { vi, beforeEach, describe, it, expect } from 'vitest';
import React from 'react';
import { render, screen, fireEvent, waitFor, act } from '@testing-library/react';
import '@testing-library/jest-dom';
import MCPConfiguration from './MCPConfiguration';

// Mock react-i18next
vi.mock('react-i18next', () => ({
  useTranslation: () => ({
    t: (key: string, options?: { defaultValue?: string }) => options?.defaultValue || key,
    i18n: { changeLanguage: vi.fn().mockResolvedValue(undefined) },
  }),
}));

// Mock the MCP service (covers both the page and the embedded Databricks catalog)
const mockMCPService = {
  getGlobalSettings: vi.fn(),
  updateGlobalSettings: vi.fn(),
  getMcpServers: vi.fn(),
  getBaseServers: vi.fn(),
  createMcpServer: vi.fn(),
  createGlobalServer: vi.fn(),
  updateMcpServer: vi.fn(),
  deleteMcpServer: vi.fn(),
  setGlobalAvailability: vi.fn(),
  setWorkspaceEnabled: vi.fn(),
  testConnection: vi.fn(),
  getDatabricksCatalog: vi.fn(),
  listGenieSpaces: vi.fn(),
  listAiSearchIndexes: vi.fn(),
  ensureDatabricksServer: vi.fn(),
};

vi.mock('../../../api/MCPService', () => ({
  MCPService: { getInstance: () => mockMCPService },
  databricksMcpServerName: (o: { kind: string; name: string }) => o.name.toLowerCase(),
}));

describe('MCPConfiguration', () => {
  // Workspace-mode servers: a globally-inherited one (no group_id) the workspace
  // can toggle, and a workspace override row.
  const workspaceServers = [
    {
      id: '1',
      name: 'Global Server',
      server_type: 'streamable',
      server_url: 'https://global.example.com/mcp',
      auth_type: 'databricks_spn',
      enabled: true,
      group_id: null,
      timeout_seconds: 30,
      max_retries: 3,
      rate_limit: 60,
    },
  ];

  const baseServers = [
    {
      id: '10',
      name: 'Base Server',
      server_type: 'streamable',
      server_url: 'https://base.example.com/mcp',
      auth_type: 'databricks_spn',
      enabled: true,
      group_id: null,
      timeout_seconds: 30,
      max_retries: 3,
      rate_limit: 60,
    },
  ];

  beforeEach(() => {
    vi.clearAllMocks();
    mockMCPService.getGlobalSettings.mockResolvedValue({ global_enabled: true });
    mockMCPService.getMcpServers.mockResolvedValue({ servers: workspaceServers });
    mockMCPService.getBaseServers.mockResolvedValue({ servers: baseServers });
    mockMCPService.getDatabricksCatalog.mockResolvedValue({
      workspace_url: '',
      external: [],
      managed: [],
    });
    mockMCPService.setGlobalAvailability.mockResolvedValue({});
    mockMCPService.setWorkspaceEnabled.mockResolvedValue({});
  });

  // =========================================================================
  // Workspace mode (default): consume + toggle the globally-enabled set
  // =========================================================================

  it('renders the workspace view from the effective server list', async () => {
    await act(async () => {
      render(<MCPConfiguration mode="workspace" />);
    });

    expect(screen.getByText('MCP Server Configuration')).toBeInTheDocument();
    await waitFor(() => expect(screen.getByText('Global Server')).toBeInTheDocument());
    expect(mockMCPService.getMcpServers).toHaveBeenCalled();
  });

  it('marks inherited globals with a Global chip and offers no Add/edit in workspace mode', async () => {
    await act(async () => {
      render(<MCPConfiguration mode="workspace" />);
    });
    await screen.findByText('Global Server');

    expect(screen.getByText('Global')).toBeInTheDocument();
    // Workspace admins do not register/edit servers — that's MCP (Global).
    expect(screen.queryByText('Add Server')).not.toBeInTheDocument();
  });

  it('toggling a server in workspace mode sets the per-workspace state', async () => {
    await act(async () => {
      render(<MCPConfiguration mode="workspace" />);
    });
    await screen.findByText('Global Server');

    fireEvent.click(screen.getByRole('checkbox'));
    await waitFor(() =>
      expect(mockMCPService.setWorkspaceEnabled).toHaveBeenCalledWith('1', false),
    );
    expect(mockMCPService.setGlobalAvailability).not.toHaveBeenCalled();
  });

  it('shows the workspace empty state when nothing is globally available', async () => {
    mockMCPService.getMcpServers.mockResolvedValue({ servers: [] });
    await act(async () => {
      render(<MCPConfiguration mode="workspace" />);
    });
    await waitFor(() =>
      expect(
        screen.getByText('No MCP servers have been made available globally yet.'),
      ).toBeInTheDocument(),
    );
  });

  // =========================================================================
  // System (global) mode: register + manage the global catalog
  // =========================================================================

  it('renders the global view from the base server list (catalog is lazy)', async () => {
    await act(async () => {
      render(<MCPConfiguration mode="system" />);
    });

    expect(screen.getByText('Global MCP Servers')).toBeInTheDocument();
    await waitFor(() => expect(screen.getByText('Base Server')).toBeInTheDocument());
    expect(mockMCPService.getBaseServers).toHaveBeenCalled();
    expect(screen.getByText('Add Server')).toBeInTheDocument();
    // The Databricks catalog is NOT rendered inline — it loads only on demand.
    expect(mockMCPService.getDatabricksCatalog).not.toHaveBeenCalled();
    expect(screen.queryByText('Databricks MCP Catalog')).not.toBeInTheDocument();
  });

  it('opens the Databricks catalog picker lazily from the Add menu', async () => {
    await act(async () => {
      render(<MCPConfiguration mode="system" />);
    });
    const addButton = await screen.findByText('Add Server');

    await act(async () => {
      fireEvent.click(addButton);
    });
    // The Add menu offers both entry modes; the catalog has not loaded yet.
    expect(screen.getByText('Manual entry')).toBeInTheDocument();
    expect(screen.getByText('Databricks catalog')).toBeInTheDocument();
    expect(mockMCPService.getDatabricksCatalog).not.toHaveBeenCalled();

    await act(async () => {
      fireEvent.click(screen.getByText('Databricks catalog'));
    });
    // The dialog mounts the catalog → it fetches on demand.
    await waitFor(() => expect(mockMCPService.getDatabricksCatalog).toHaveBeenCalled());
    expect(screen.getByText('Add from Databricks Catalog')).toBeInTheDocument();
  });

  it('toggling a server in system mode sets its global availability', async () => {
    await act(async () => {
      render(<MCPConfiguration mode="system" />);
    });
    await screen.findByText('Base Server');

    fireEvent.click(screen.getByRole('checkbox'));
    await waitFor(() =>
      expect(mockMCPService.setGlobalAvailability).toHaveBeenCalledWith('10', false),
    );
    expect(mockMCPService.setWorkspaceEnabled).not.toHaveBeenCalled();
  });

  it('opens the manual add-server dialog from the Add menu', async () => {
    await act(async () => {
      render(<MCPConfiguration mode="system" />);
    });

    const addButton = await screen.findByText('Add Server');
    await act(async () => {
      fireEvent.click(addButton);
    });
    await act(async () => {
      fireEvent.click(screen.getByText('Manual entry'));
    });

    expect(screen.getByRole('dialog')).toBeInTheDocument();
    expect(screen.getByText('Add MCP Server')).toBeInTheDocument();
  });

  it('shows authentication type options in the manual add dialog', async () => {
    await act(async () => {
      render(<MCPConfiguration mode="system" />);
    });

    const addButton = await screen.findByText('Add Server');
    await act(async () => {
      fireEvent.click(addButton);
    });
    await act(async () => {
      fireEvent.click(screen.getByText('Manual entry'));
    });

    const authSelect = screen.getByLabelText('Authentication Type');
    fireEvent.mouseDown(authSelect);

    await waitFor(() => {
      expect(screen.getByRole('option', { name: 'API Key' })).toBeInTheDocument();
      expect(screen.getByRole('option', { name: 'Apps SPN' })).toBeInTheDocument();
    });
  });

  it('does not show model mapping toggle', async () => {
    await act(async () => {
      render(<MCPConfiguration mode="system" />);
    });
    await waitFor(() => {
      expect(screen.queryByText(/enable model mapping/i)).not.toBeInTheDocument();
    });
  });

  // =========================================================================
  // Loading / error states (workspace mode)
  // =========================================================================

  describe('loading and error states', () => {
    it('shows full-page loading message while fetching servers', async () => {
      let resolveFetch!: (value: unknown) => void;
      mockMCPService.getMcpServers.mockReturnValue(
        new Promise(resolve => { resolveFetch = resolve; })
      );

      await act(async () => {
        render(<MCPConfiguration mode="workspace" />);
      });

      expect(screen.getByText('Loading MCP configuration...')).toBeInTheDocument();
      expect(screen.getByRole('progressbar')).toBeInTheDocument();
      expect(screen.queryByText('MCP Server Configuration')).not.toBeInTheDocument();

      await act(async () => {
        resolveFetch({ servers: [] });
      });
    });

    it('shows full-page error with retry button when server fetch fails', async () => {
      mockMCPService.getMcpServers.mockRejectedValue(new Error('Network error'));

      await act(async () => {
        render(<MCPConfiguration mode="workspace" />);
      });

      await waitFor(() => {
        expect(screen.getByText('Network error')).toBeInTheDocument();
      });
      expect(screen.getByText('Retry')).toBeInTheDocument();
      expect(screen.queryByText('MCP Server Configuration')).not.toBeInTheDocument();
    });

    it('retries loading when Retry button is clicked', async () => {
      mockMCPService.getMcpServers.mockRejectedValueOnce(new Error('Network error'));

      await act(async () => {
        render(<MCPConfiguration mode="workspace" />);
      });
      await waitFor(() => expect(screen.getByText('Network error')).toBeInTheDocument());

      mockMCPService.getMcpServers.mockResolvedValue({ servers: workspaceServers });
      await act(async () => {
        fireEvent.click(screen.getByText('Retry'));
      });

      await waitFor(() => {
        expect(screen.getByText('MCP Server Configuration')).toBeInTheDocument();
        expect(screen.getByText('Global Server')).toBeInTheDocument();
      });
    });
  });
});
