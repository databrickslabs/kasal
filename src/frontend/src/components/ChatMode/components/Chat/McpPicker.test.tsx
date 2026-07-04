import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor, act } from '@testing-library/react';
import McpPicker from './McpPicker';
import { useExecutionStore } from '../../store/executionStore';
import { useAppStore } from '../../store/appStore';
import { usePermissionStore } from '../../../../store/permissions';

// The picker now lists ONLY the workspace's configured MCP servers (the
// group-scoped allow-list). Browsing/registering the Databricks catalog moved
// to Configuration → MCP, so the picker no longer touches the catalog API.
const listKasalMcpServers = vi.fn();
vi.mock('../../api/mcp', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../../api/mcp')>()),
  listKasalMcpServers: (...a: unknown[]) => listKasalMcpServers(...a),
}));

// Agent Bricks endpoints are fetched lazily when the popover opens.
const getAgentBricksEndpoints = vi.fn();
vi.mock('../../../../api/AgentBricksService', () => ({
  AgentBricksService: {
    getEndpoints: (...a: unknown[]) => getAgentBricksEndpoints(...a),
  },
}));

const AGENT_BRICKS_ENDPOINTS = [
  { id: 'ab1', name: 'agents_sales_bot', display_name: 'Sales Bot', state: 'READY' },
  { id: 'ab2', name: 'agents_support_bot', state: 'READY' }, // no display_name → name shown
];

const KASAL_SERVERS = [
  { id: 1, name: 'My MCP', enabled: true, server_url: 'https://x/mcp' },
  { id: 2, name: 'Disabled MCP', enabled: false },
];

beforeEach(() => {
  vi.clearAllMocks();
  useExecutionStore.setState({ selectedMcpServers: [], selectedAgentBricksEndpoints: [] });
  // The "Agents" section is gated on AgentBricksTool being in the tool catalog —
  // default it OFF so existing MCP-only tests never see the section, then opt
  // specific tests in via useAppStore.setState below.
  useAppStore.setState({ toolNameMap: {} });
  getAgentBricksEndpoints.mockResolvedValue({ endpoints: AGENT_BRICKS_ENDPOINTS, total_count: 2, filtered: false });
  listKasalMcpServers.mockResolvedValue(KASAL_SERVERS);
  // Default: no admin privilege → the "Connect a tool" footer stays hidden.
  usePermissionStore.setState({ userRole: null, isSystemAdmin: false, isPersonalWorkspaceManager: false });
});

/** Grant/deny the workspace-admin privilege the "Connect a tool" footer keys off. */
const setAdmin = (admin: boolean) => {
  localStorage.setItem('selectedGroupId', 'group_acme'); // non-personal workspace
  usePermissionStore.setState({
    userRole: admin ? 'admin' : 'operator',
    isSystemAdmin: false,
    isPersonalWorkspaceManager: false,
  });
};

const openPicker = async () => {
  fireEvent.click(screen.getByLabelText('MCP servers'));
  await waitFor(() => expect(screen.getByText('My MCP')).toBeInTheDocument());
};

describe('McpPicker', () => {
  it('is closed by default and opens into the configured server list', async () => {
    render(<McpPicker />);
    expect(screen.queryByRole('menu')).toBeNull();

    await openPicker();
    expect(screen.getByRole('menu', { name: 'MCP picker' })).toBeInTheDocument();
    expect(screen.getByText('MCP')).toBeInTheDocument();
    expect(screen.getByText('My MCP')).toBeInTheDocument();
    expect(screen.getByText('Disabled MCP')).toBeInTheDocument();
  });

  it('does NOT browse the Databricks catalog (that lives in Configuration → MCP)', async () => {
    render(<McpPicker />);
    await openPicker();

    // None of the old catalog browse affordances appear in the picker anymore.
    expect(screen.queryByText('Genie')).toBeNull();
    expect(screen.queryByText('AI Search')).toBeNull();
    expect(screen.queryByText('Databricks SQL')).toBeNull();
    expect(screen.queryByText(/Unity Catalog Functions/)).toBeNull();
    expect(screen.queryByLabelText('Search Genie spaces')).toBeNull();
    // Only /mcp/servers is consulted — there is no catalog round-trip.
    expect(listKasalMcpServers).toHaveBeenCalled();
  });

  it('toggles a configured server selection and shows the count badge', async () => {
    render(<McpPicker />);
    await openPicker();

    fireEvent.click(screen.getByText('My MCP'));
    expect(useExecutionStore.getState().selectedMcpServers).toEqual(['My MCP']);
    expect(screen.getByText('1')).toBeInTheDocument(); // badge

    fireEvent.click(screen.getByText('My MCP'));
    expect(useExecutionStore.getState().selectedMcpServers).toEqual([]);
  });

  it('marks selected servers as checked (aria-checked)', async () => {
    useExecutionStore.setState({ selectedMcpServers: ['My MCP'] });
    render(<McpPicker />);
    await openPicker();

    const item = screen.getByRole('menuitemcheckbox', { name: /My MCP/ });
    expect(item).toHaveAttribute('aria-checked', 'true');
  });

  it('disables servers that are disabled in Kasal (unless already selected)', async () => {
    render(<McpPicker />);
    await openPicker();
    expect(screen.getByRole('menuitemcheckbox', { name: /Disabled MCP/ })).toBeDisabled();

    // …but a selected disabled server stays clickable so it can be deselected.
    useExecutionStore.setState({ selectedMcpServers: ['Disabled MCP'] });
    await waitFor(() =>
      expect(screen.getByRole('menuitemcheckbox', { name: /Disabled MCP/ })).not.toBeDisabled(),
    );
  });

  it('filters the configured list from the top search box', async () => {
    listKasalMcpServers.mockResolvedValue([
      { id: 1, name: 'My MCP', enabled: true },
      { id: 2, name: 'Jira MCP', enabled: true },
    ]);
    render(<McpPicker />);
    await openPicker();

    const search = screen.getByLabelText('Search MCP servers');
    fireEvent.change(search, { target: { value: 'jira' } });
    expect(screen.getByText('Jira MCP')).toBeInTheDocument();
    expect(screen.queryByText('My MCP')).toBeNull();

    // Clearing the filter restores everything.
    fireEvent.change(search, { target: { value: '' } });
    expect(screen.getByText('My MCP')).toBeInTheDocument();
    expect(screen.getByText('Jira MCP')).toBeInTheDocument();
  });

  it('shows a no-matches state when the search finds nothing', async () => {
    render(<McpPicker />);
    await openPicker();
    fireEvent.change(screen.getByLabelText('Search MCP servers'), { target: { value: 'zzz' } });
    expect(screen.getByText('No matching MCP servers')).toBeInTheDocument();
  });

  it('shows an empty state when no servers are configured', async () => {
    listKasalMcpServers.mockResolvedValue([]);
    render(<McpPicker />);
    fireEvent.click(screen.getByLabelText('MCP servers'));
    await waitFor(() =>
      expect(screen.getByText('No MCP servers available')).toBeInTheDocument(),
    );
  });

  it('prunes stale selected servers no longer in the configured list (phantom badge)', async () => {
    // Selection persisted from before the servers were cleared in Configuration.
    useExecutionStore.setState({ selectedMcpServers: ['Ghost MCP', 'My MCP'] });
    listKasalMcpServers.mockResolvedValue([{ id: 1, name: 'My MCP', enabled: true }]);

    render(<McpPicker />);
    // Badge reflects the stale count until the list loads.
    expect(screen.getByText('2')).toBeInTheDocument();

    await openPicker();
    // "Ghost MCP" is gone from the workspace → pruned; only the real one remains.
    await waitFor(() =>
      expect(useExecutionStore.getState().selectedMcpServers).toEqual(['My MCP']),
    );
    expect(screen.getByText('1')).toBeInTheDocument();
  });

  it('shows an error when the configured server list cannot load', async () => {
    listKasalMcpServers.mockRejectedValue(new Error('boom'));
    render(<McpPicker />);
    fireEvent.click(screen.getByLabelText('MCP servers'));
    await waitFor(() =>
      expect(screen.getByText('Could not load MCP servers')).toBeInTheDocument(),
    );
  });

  it('warns in plain language about data changes while a Databricks SQL server is selected', async () => {
    // A configured server registered under the canonical "databricks sql" name.
    listKasalMcpServers.mockResolvedValue([
      { id: 1, name: 'My MCP', enabled: true },
      { id: 2, name: 'databricks sql', enabled: true, server_url: 'https://ws/api/2.0/mcp/sql' },
    ]);
    render(<McpPicker />);
    await openPicker();

    // No warning for other selections.
    fireEvent.click(screen.getByText('My MCP'));
    expect(screen.queryByText(/change your data/)).toBeNull();

    fireEvent.click(screen.getByText('databricks sql'));
    await waitFor(() =>
      expect(useExecutionStore.getState().selectedMcpServers).toContain('databricks sql'),
    );
    // Plain-language wording (no jargon like "CRUD").
    expect(screen.getByText(/change your data/)).toBeInTheDocument();
    expect(screen.getByText(/add, update or permanently delete/)).toBeInTheDocument();
    expect(screen.queryByText(/CRUD/)).toBeNull();

    // Deselecting clears the warning.
    fireEvent.click(screen.getByText('databricks sql'));
    expect(useExecutionStore.getState().selectedMcpServers).not.toContain('databricks sql');
    expect(screen.queryByText(/change your data/)).toBeNull();
  });

  it('closes on outside click', async () => {
    render(
      <div>
        <button data-testid="outside">outside</button>
        <McpPicker />
      </div>,
    );
    await openPicker();

    fireEvent.mouseDown(screen.getByTestId('outside'));
    await waitFor(() => expect(screen.queryByRole('menu')).toBeNull());
  });

  it('keeps the menu open for clicks inside it', async () => {
    render(<McpPicker />);
    await openPicker();
    fireEvent.mouseDown(screen.getByText('My MCP'));
    expect(screen.getByRole('menu')).toBeInTheDocument();
  });

  it('respects the disabled prop on the + button', () => {
    render(<McpPicker disabled />);
    expect(screen.getByLabelText('MCP servers')).toBeDisabled();
  });

  describe('"Connect a tool" footer (RBAC)', () => {
    it('shows for a workspace admin and opens MCP config on click', async () => {
      const onOpenMcpConfig = vi.fn();
      setAdmin(true);
      render(<McpPicker onOpenMcpConfig={onOpenMcpConfig} />);
      await openPicker();
      fireEvent.click(screen.getByText('Connect a tool…'));
      expect(onOpenMcpConfig).toHaveBeenCalledTimes(1);
    });

    it('is hidden from non-admin operators', async () => {
      setAdmin(false);
      render(<McpPicker onOpenMcpConfig={vi.fn()} />);
      await openPicker();
      expect(screen.queryByText('Connect a tool…')).toBeNull();
    });

    it('is hidden when no onOpenMcpConfig handler is provided', async () => {
      setAdmin(true);
      render(<McpPicker />);
      await openPicker();
      expect(screen.queryByText('Connect a tool…')).toBeNull();
    });
  });

  it('ignores configured-server results that land after unmount', async () => {
    let resolveKasal: (v: unknown) => void = () => undefined;
    let rejectKasal: (e: unknown) => void = () => undefined;
    listKasalMcpServers
      .mockImplementationOnce(() => new Promise((resolve) => { resolveKasal = resolve; }))
      .mockImplementationOnce(() => new Promise((_, reject) => { rejectKasal = reject; }));

    // Resolution after unmount → the cancelled guard drops the result.
    const first = render(<McpPicker />);
    fireEvent.click(screen.getByLabelText('MCP servers'));
    first.unmount();
    await act(async () => resolveKasal(KASAL_SERVERS));

    // Rejection after unmount → the cancelled guard drops the error too.
    const second = render(<McpPicker />);
    fireEvent.click(screen.getByLabelText('MCP servers'));
    second.unmount();
    await act(async () => rejectKasal(new Error('late failure')));
  });

  describe('placement & neutral styling', () => {
    // The picker flips with the composer: it opens UPWARD by default (docked at
    // the bottom of the screen) and DOWNWARD when the input is centered.
    it('defaults to opening upward (bottom-full)', async () => {
      render(<McpPicker />);
      await openPicker();
      const menu = screen.getByRole('menu', { name: 'MCP picker' });
      expect(menu.className).toContain('bottom-full');
      expect(menu.className).not.toContain('top-full');
    });

    it('opens downward when menuPlacement="down" (top-full)', async () => {
      render(<McpPicker menuPlacement="down" />);
      await openPicker();
      const menu = screen.getByRole('menu', { name: 'MCP picker' });
      expect(menu.className).toContain('top-full');
      expect(menu.className).not.toContain('bottom-full');
    });

    it('renders inset rounded rows so the checkbox no longer touches the edge', async () => {
      render(<McpPicker />);
      await openPicker();
      const row = screen.getByRole('menuitemcheckbox', { name: /My MCP/ });
      expect(row.className).toContain('rounded-lg');
      // !px-2.5 wins over the #kasal-chat-root button { padding:0 } reset so the
      // checkbox keeps its inset gutter.
      expect(row.className).toContain('!px-2.5');
    });

    it('uses a neutral "+" trigger colour, reserving the accent for the count badge', () => {
      useExecutionStore.setState({ selectedMcpServers: ['My MCP'] });
      render(<McpPicker />);
      const trigger = screen.getByLabelText('MCP servers');
      const triggerStyle = trigger.getAttribute('style') || '';
      expect(triggerStyle).toContain('var(--text-secondary)');
      expect(triggerStyle).not.toContain('var(--accent)');
      // …the accent lives on the selection count badge instead.
      const badge = screen.getByText('1');
      expect(badge.getAttribute('style') || '').toContain('var(--accent)');
    });
  });

  describe('Agent Bricks "Agents" section', () => {
    // Opt the workspace into the Agent Bricks feature by registering the tool in
    // the catalog (the only signal the picker uses to show the "Agents" section).
    const enableAgentBricksTool = () =>
      useAppStore.setState({ toolNameMap: { '42': 'AgentBricksTool', '7': 'SerperDevTool' } });

    it('hides the Agents section when AgentBricksTool is not in the tool catalog', async () => {
      // toolNameMap left empty by beforeEach → feature off.
      render(<McpPicker />);
      await openPicker();

      // Give the (gated-off) endpoint effect a chance to run; it must not fire.
      await waitFor(() => expect(screen.getByText('My MCP')).toBeInTheDocument());
      expect(screen.queryByText('Agents')).toBeNull();
      expect(screen.queryByText('Sales Bot')).toBeNull();
      expect(getAgentBricksEndpoints).not.toHaveBeenCalled();
    });

    it('shows the Agents section when the tool is enabled and endpoints exist', async () => {
      enableAgentBricksTool();
      render(<McpPicker />);
      await openPicker();

      expect(await screen.findByText('Agents')).toBeInTheDocument();
      // Endpoints are fetched ready-only when the popover opens.
      expect(getAgentBricksEndpoints).toHaveBeenCalledWith(true);
      // Row label = display_name when present, else the raw endpoint name.
      expect(screen.getByText('Sales Bot')).toBeInTheDocument();
      expect(screen.getByText('agents_support_bot')).toBeInTheDocument();
      // Each row carries an "agent" tag.
      expect(screen.getAllByText('agent')).toHaveLength(2);
    });

    it('stays hidden when the tool is enabled but the workspace has no endpoints', async () => {
      enableAgentBricksTool();
      getAgentBricksEndpoints.mockResolvedValue({ endpoints: [], total_count: 0, filtered: false });
      render(<McpPicker />);
      await openPicker();

      await waitFor(() => expect(getAgentBricksEndpoints).toHaveBeenCalledWith(true));
      expect(screen.queryByText('Agents')).toBeNull();
    });

    it('toggles an endpoint by its NAME (not display_name) and reflects selection', async () => {
      enableAgentBricksTool();
      render(<McpPicker />);
      await openPicker();

      const salesRow = await screen.findByRole('menuitemcheckbox', { name: /Sales Bot/ });
      fireEvent.click(salesRow);
      // The serving-endpoint name is what gets equipped, not the friendly label.
      expect(useExecutionStore.getState().selectedAgentBricksEndpoints).toEqual(['agents_sales_bot']);
      expect(screen.getByRole('menuitemcheckbox', { name: /Sales Bot/ })).toHaveAttribute(
        'aria-checked',
        'true',
      );

      // A second click deselects it.
      fireEvent.click(screen.getByRole('menuitemcheckbox', { name: /Sales Bot/ }));
      expect(useExecutionStore.getState().selectedAgentBricksEndpoints).toEqual([]);
    });

    it('counts MCP servers AND Agent Bricks endpoints in the + badge', async () => {
      enableAgentBricksTool();
      useExecutionStore.setState({
        selectedMcpServers: ['My MCP'],
        selectedAgentBricksEndpoints: ['agents_sales_bot'],
      });
      render(<McpPicker />);
      // Badge reflects 1 MCP server + 1 Agent Bricks endpoint = 2, before opening.
      expect(screen.getByText('2')).toBeInTheDocument();

      await openPicker();
      // Selecting another endpoint bumps the badge to 3.
      fireEvent.click(await screen.findByRole('menuitemcheckbox', { name: /agents_support_bot/ }));
      expect(useExecutionStore.getState().selectedAgentBricksEndpoints).toEqual([
        'agents_sales_bot',
        'agents_support_bot',
      ]);
      expect(screen.getByText('3')).toBeInTheDocument();
    });

    it('filters Agent Bricks rows by display_name or name from the top search box', async () => {
      enableAgentBricksTool();
      render(<McpPicker />);
      await openPicker();
      await screen.findByText('Sales Bot');

      const search = screen.getByLabelText('Search MCP servers');

      // Match on the friendly display_name.
      fireEvent.change(search, { target: { value: 'sales' } });
      expect(screen.getByText('Sales Bot')).toBeInTheDocument();
      expect(screen.queryByText('agents_support_bot')).toBeNull();

      // Match on the raw endpoint name when there is no display_name.
      fireEvent.change(search, { target: { value: 'support' } });
      expect(screen.getByText('agents_support_bot')).toBeInTheDocument();
      expect(screen.queryByText('Sales Bot')).toBeNull();

      // No matches → the section keeps its header but shows the empty state.
      fireEvent.change(search, { target: { value: 'zzz-no-such-agent' } });
      expect(screen.getByText('Agents')).toBeInTheDocument();
      expect(screen.getByText('No matching agents')).toBeInTheDocument();
    });
  });
});
