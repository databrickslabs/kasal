import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import ChatMcpDialog from './ChatMcpDialog';
import { usePermissionStore } from '../../../../store/permissions';

const api = {
  getBaseServers: vi.fn(),
  getMcpServers: vi.fn(),
  setGlobalAvailability: vi.fn(),
  setWorkspaceEnabled: vi.fn(),
  createGlobalServer: vi.fn(),
  deleteMcpServer: vi.fn(),
  getDatabricksCatalog: vi.fn(),
  listGenieSpaces: vi.fn(),
  listAiSearchIndexes: vi.fn(),
  ensureDatabricksServer: vi.fn(),
};
vi.mock('../../../../api/MCPService', () => ({
  MCPService: { getInstance: () => api },
  databricksMcpServerName: (o: { name: string }) => o.name.toLowerCase(),
}));

// Same name in both lists → merged into ONE row with two toggles.
const BASE_NEMO = { id: 'g1', name: 'nemo', enabled: true, server_type: 'streamable', server_url: 'https://x/mcp' };
const WS_NEMO = { id: 'w1', name: 'nemo', enabled: false, server_type: 'streamable', server_url: 'https://x/mcp' };
// Globally registered but not available → no workspace-effective row.
const BASE_ORPHAN = { id: 'g2', name: 'orphan', enabled: false, server_type: 'sse', server_url: 'https://y/mcp' };

beforeEach(() => {
  vi.clearAllMocks();
  api.getBaseServers.mockResolvedValue({ servers: [BASE_NEMO, BASE_ORPHAN], count: 2 });
  api.getMcpServers.mockResolvedValue({ servers: [WS_NEMO], count: 1 });
  api.setGlobalAvailability.mockResolvedValue(BASE_NEMO);
  api.setWorkspaceEnabled.mockResolvedValue(WS_NEMO);
  api.createGlobalServer.mockResolvedValue(BASE_NEMO);
  api.getDatabricksCatalog.mockResolvedValue({
    workspace_url: 'https://ws',
    external: [{ id: 'ext1', kind: 'external', name: 'my-uc-mcp', server_url: 'https://ws/mcp/ext' }],
    managed: [{ id: 'gen', kind: 'genie', name: 'Genie spaces', expandable: true }],
  });
  api.listGenieSpaces.mockResolvedValue({ options: [], next_page_token: null });
  api.listAiSearchIndexes.mockResolvedValue([]);
  api.ensureDatabricksServer.mockResolvedValue('my-uc-mcp');
  usePermissionStore.setState({ isSystemAdmin: true });
});

describe('ChatMcpDialog', () => {
  it('renders nothing when closed', () => {
    const { container } = render(<ChatMcpDialog open={false} onClose={vi.fn()} />);
    expect(container).toBeEmptyDOMElement();
  });

  it('system admin: one merged list (no tabs) with a Global AND a Workspace toggle per server', async () => {
    render(<ChatMcpDialog open onClose={vi.fn()} />);
    // Both catalogs are loaded and merged — no tab switching.
    await waitFor(() => expect(api.getBaseServers).toHaveBeenCalled());
    expect(api.getMcpServers).toHaveBeenCalled();
    expect(screen.queryByRole('button', { name: 'This workspace' })).toBeNull(); // no tab

    expect(await screen.findByText('nemo')).toBeInTheDocument();
    expect(screen.getByRole('switch', { name: 'Global availability: nemo' })).toBeInTheDocument();
    expect(screen.getByRole('switch', { name: 'Enabled for this workspace: nemo' })).toBeInTheDocument();
  });

  it('the Global toggle flips availability; the Workspace toggle flips per-workspace enable', async () => {
    render(<ChatMcpDialog open onClose={vi.fn()} />);
    await screen.findByText('nemo');

    fireEvent.click(screen.getByRole('switch', { name: 'Global availability: nemo' }));
    await waitFor(() => expect(api.setGlobalAvailability).toHaveBeenCalledWith('g1', false)); // was true

    fireEvent.click(screen.getByRole('switch', { name: 'Enabled for this workspace: nemo' }));
    await waitFor(() => expect(api.setWorkspaceEnabled).toHaveBeenCalledWith('w1', true)); // was false
  });

  it('disables the Workspace toggle for a server not yet globally available', async () => {
    render(<ChatMcpDialog open onClose={vi.fn()} />);
    await screen.findByText('orphan');
    // orphan has a Global toggle but no workspace-effective row → Workspace toggle disabled.
    expect(screen.getByRole('switch', { name: 'Global availability: orphan' })).toBeEnabled();
    expect(screen.getByRole('switch', { name: 'Enabled for this workspace: orphan' })).toBeDisabled();
  });

  it('registers a new server from the manual add form', async () => {
    render(<ChatMcpDialog open onClose={vi.fn()} />);
    await screen.findByText('nemo');
    fireEvent.click(screen.getByText('Add server'));
    // Add defaults to the Databricks catalog tab — switch to Manual.
    fireEvent.click(screen.getByText('Manual'));
    fireEvent.change(screen.getByPlaceholderText(/Name/i), { target: { value: 'my-mcp' } });
    fireEvent.change(screen.getByPlaceholderText(/Server URL/i), { target: { value: 'https://z/mcp' } });
    fireEvent.click(screen.getByText('Register server'));
    await waitFor(() =>
      expect(api.createGlobalServer).toHaveBeenCalledWith(
        expect.objectContaining({ name: 'my-mcp', server_url: 'https://z/mcp', server_type: 'streamable' }),
      ),
    );
  });

  it('lists the Databricks catalog and registers a picked option', async () => {
    render(<ChatMcpDialog open onClose={vi.fn()} />);
    await screen.findByText('nemo');
    fireEvent.click(screen.getByText('Add server')); // opens on the Databricks tab
    await waitFor(() => expect(api.getDatabricksCatalog).toHaveBeenCalled());
    // An external UC-connection MCP + an expandable Genie drill row appear.
    expect(await screen.findByText('my-uc-mcp')).toBeInTheDocument();
    expect(screen.getByText('Genie spaces')).toBeInTheDocument();
    // Register the external option.
    fireEvent.click(screen.getByRole('button', { name: 'Add' }));
    await waitFor(() =>
      expect(api.ensureDatabricksServer).toHaveBeenCalledWith(
        expect.objectContaining({ name: 'my-uc-mcp' }),
        'global',
      ),
    );
  });

  it('non-admin sees only the single workspace toggle — no Global toggle, no add', async () => {
    usePermissionStore.setState({ isSystemAdmin: false });
    render(<ChatMcpDialog open onClose={vi.fn()} />);
    await waitFor(() => expect(api.getMcpServers).toHaveBeenCalled());
    expect(api.getBaseServers).not.toHaveBeenCalled();
    expect(await screen.findByText('nemo')).toBeInTheDocument();
    const toggle = screen.getByRole('switch', { name: 'Enabled for this workspace: nemo' });
    expect(toggle).toBeInTheDocument();
    expect(screen.queryByRole('switch', { name: 'Global availability: nemo' })).toBeNull();
    expect(screen.queryByText('Add server')).toBeNull();
    // The single workspace toggle flips per-workspace enablement (was false → true).
    fireEvent.click(toggle);
    await waitFor(() => expect(api.setWorkspaceEnabled).toHaveBeenCalledWith('w1', true));
  });

  it('supports the full manual form (type + api key) and closing the add panel', async () => {
    render(<ChatMcpDialog open onClose={vi.fn()} />);
    await screen.findByText('nemo');
    fireEvent.click(screen.getByText('Add server'));
    fireEvent.click(screen.getByText('Manual'));
    fireEvent.change(screen.getByPlaceholderText(/Server URL/i), { target: { value: 'https://z/mcp' } });
    fireEvent.change(screen.getByDisplayValue('Streamable'), { target: { value: 'sse' } });
    fireEvent.change(screen.getByPlaceholderText(/API key/i), { target: { value: 'secret' } });
    // Close the add panel via the X.
    fireEvent.click(screen.getByLabelText('Done adding'));
    expect(screen.queryByPlaceholderText(/Server URL/i)).toBeNull();
  });

  it('closes when the backdrop is clicked', () => {
    const onClose = vi.fn();
    render(<ChatMcpDialog open onClose={onClose} />);
    fireEvent.mouseDown(screen.getByRole('dialog').parentElement!);
    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it('deletes a global server after confirmation', async () => {
    const confirmSpy = vi.spyOn(window, 'confirm').mockReturnValue(true);
    api.deleteMcpServer.mockResolvedValue(true);
    render(<ChatMcpDialog open onClose={vi.fn()} />);
    await screen.findByText('nemo');
    fireEvent.click(screen.getByRole('button', { name: 'Delete nemo' }));
    await waitFor(() => expect(api.deleteMcpServer).toHaveBeenCalledWith('g1'));
    confirmSpy.mockRestore();
  });

  it('does not delete when the confirmation is declined', async () => {
    const confirmSpy = vi.spyOn(window, 'confirm').mockReturnValue(false);
    render(<ChatMcpDialog open onClose={vi.fn()} />);
    await screen.findByText('nemo');
    fireEvent.click(screen.getByRole('button', { name: 'Delete nemo' }));
    expect(api.deleteMcpServer).not.toHaveBeenCalled();
    confirmSpy.mockRestore();
  });

  it('surfaces an error when a toggle fails', async () => {
    api.setGlobalAvailability.mockRejectedValue(new Error('nope'));
    render(<ChatMcpDialog open onClose={vi.fn()} />);
    await screen.findByText('nemo');
    fireEvent.click(screen.getByRole('switch', { name: 'Global availability: nemo' }));
    expect(await screen.findByText('nope')).toBeInTheDocument();
  });

  it('surfaces an error when the catalogs fail to load', async () => {
    api.getBaseServers.mockRejectedValue(new Error('load failed'));
    render(<ChatMcpDialog open onClose={vi.fn()} />);
    expect(await screen.findByText('load failed')).toBeInTheDocument();
  });

  it('surfaces an error when manual registration fails', async () => {
    api.createGlobalServer.mockRejectedValue(new Error('bad url'));
    render(<ChatMcpDialog open onClose={vi.fn()} />);
    await screen.findByText('nemo');
    fireEvent.click(screen.getByText('Add server'));
    fireEvent.click(screen.getByText('Manual'));
    fireEvent.change(screen.getByPlaceholderText(/Name/i), { target: { value: 'x' } });
    fireEvent.change(screen.getByPlaceholderText(/Server URL/i), { target: { value: 'https://z/mcp' } });
    fireEvent.click(screen.getByText('Register server'));
    expect(await screen.findByText('bad url')).toBeInTheDocument();
  });
});
