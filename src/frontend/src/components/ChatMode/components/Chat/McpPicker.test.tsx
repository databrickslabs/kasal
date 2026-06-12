import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor, act } from '@testing-library/react';
import McpPicker from './McpPicker';
import { useExecutionStore } from '../../store/executionStore';

const listKasalMcpServers = vi.fn();
const getDatabricksMcpCatalog = vi.fn();
const listGenieMcpSpaces = vi.fn();
const listAiSearchMcpIndexes = vi.fn();
const ensureDatabricksMcpServer = vi.fn();
vi.mock('../../api/mcp', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../../api/mcp')>()),
  listKasalMcpServers: (...a: unknown[]) => listKasalMcpServers(...a),
  getDatabricksMcpCatalog: (...a: unknown[]) => getDatabricksMcpCatalog(...a),
  listGenieMcpSpaces: (...a: unknown[]) => listGenieMcpSpaces(...a),
  listAiSearchMcpIndexes: (...a: unknown[]) => listAiSearchMcpIndexes(...a),
  ensureDatabricksMcpServer: (...a: unknown[]) => ensureDatabricksMcpServer(...a),
}));

const KASAL_SERVERS = [
  { id: 1, name: 'My MCP', enabled: true, server_url: 'https://x/mcp' },
  { id: 2, name: 'Disabled MCP', enabled: false },
];

const EXTERNAL_OPTION = {
  id: 'external:jira',
  kind: 'external',
  name: 'jira',
  description: 'Jira MCP',
  server_url: 'https://ws/api/2.0/mcp/external/jira',
};
const SQL_TYPE = {
  id: 'sql',
  kind: 'sql',
  name: 'Databricks SQL',
  description: null,
  server_url: 'https://ws/api/2.0/mcp/sql',
  expandable: false,
};
const FUNCTIONS_TYPE = {
  id: 'functions:main.gold',
  kind: 'functions',
  name: 'Unity Catalog Functions (main.gold)',
  description: null,
  server_url: 'https://ws/api/2.0/mcp/functions/main/gold',
  expandable: false,
};
const GENIE_TYPE = {
  id: 'genie',
  kind: 'genie',
  name: 'Genie',
  description: 'Pick a Genie space',
  expandable: true,
};
const AI_SEARCH_TYPE = {
  id: 'ai-search',
  kind: 'ai-search',
  name: 'AI Search',
  description: 'Pick an AI Search index',
  expandable: true,
};
const CATALOG = {
  workspace_url: 'https://ws',
  external: [EXTERNAL_OPTION],
  managed: [SQL_TYPE, FUNCTIONS_TYPE, GENIE_TYPE, AI_SEARCH_TYPE],
};

const GENIE_SPACE = {
  id: 'genie:s1',
  kind: 'genie',
  name: 'Sales Space',
  description: 'sales data',
  server_url: 'https://ws/api/2.0/mcp/genie/s1',
};
const GENIE_SPACE_2 = {
  id: 'genie:s2',
  kind: 'genie',
  name: 'Marketing Space',
  description: null,
  server_url: 'https://ws/api/2.0/mcp/genie/s2',
};
const AI_INDEX = {
  id: 'ai-search:main.gold.docs_idx',
  kind: 'ai-search',
  name: 'main.gold.docs_idx',
  description: 'Endpoint: ep1',
  server_url: 'https://ws/api/2.0/mcp/ai-search/main/gold/docs_idx',
};

beforeEach(() => {
  vi.clearAllMocks();
  useExecutionStore.setState({ selectedMcpServers: [] });
  listKasalMcpServers.mockResolvedValue(KASAL_SERVERS);
  getDatabricksMcpCatalog.mockResolvedValue(CATALOG);
  listGenieMcpSpaces.mockResolvedValue({ options: [GENIE_SPACE], next_page_token: null });
  listAiSearchMcpIndexes.mockResolvedValue([AI_INDEX]);
  // Mirrors the real databricksMcpServerName contract: lowercase names.
  ensureDatabricksMcpServer.mockImplementation(async (o: { kind: string; name: string }) => {
    if (o.kind === 'genie') return `databricks genie: ${o.name}`.toLowerCase();
    if (o.kind === 'ai-search') return `databricks ai search: ${o.name}`.toLowerCase();
    return o.name.toLowerCase();
  });
});

const openPicker = async () => {
  fireEvent.click(screen.getByLabelText('MCP servers'));
  await waitFor(() => expect(screen.getByText('My MCP')).toBeInTheDocument());
};

const expandGenie = async () => {
  await openPicker();
  fireEvent.click(screen.getByText('Genie'));
  await waitFor(() => expect(screen.getByText('Sales Space')).toBeInTheDocument());
};

describe('McpPicker', () => {
  it('is closed by default and opens into ONE flat list under a single MCP header', async () => {
    render(<McpPicker />);
    expect(screen.queryByRole('menu')).toBeNull();

    await openPicker();
    expect(screen.getByRole('menu', { name: 'MCP picker' })).toBeInTheDocument();
    expect(screen.getByText('MCP')).toBeInTheDocument();
    // No section sub-headers — everything sits in the same list.
    expect(screen.queryByText('Kasal')).toBeNull();
    expect(screen.queryByText('Databricks')).toBeNull();
    expect(screen.getByText('Disabled MCP')).toBeInTheDocument();
    expect(screen.getByText('jira')).toBeInTheDocument();
    // Managed leaves are directly selectable…
    expect(screen.getByText('Databricks SQL')).toBeInTheDocument();
    expect(screen.getByText('Unity Catalog Functions (main.gold)')).toBeInTheDocument();
    // …while Genie and AI Search are drill-down types with NO instances yet.
    expect(screen.getByText('Genie')).toBeInTheDocument();
    expect(screen.getByText('AI Search')).toBeInTheDocument();
    expect(listGenieMcpSpaces).not.toHaveBeenCalled();
    expect(listAiSearchMcpIndexes).not.toHaveBeenCalled();
    expect(screen.getByText('external')).toBeInTheDocument(); // kind tag
  });

  it('toggles a Kasal server selection and shows the count badge', async () => {
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

  it('filters the combined list from the top search box', async () => {
    render(<McpPicker />);
    await openPicker();

    const search = screen.getByLabelText('Search MCP servers');
    fireEvent.change(search, { target: { value: 'jira' } });
    expect(screen.getByText('jira')).toBeInTheDocument();
    expect(screen.queryByText('My MCP')).toBeNull();
    expect(screen.queryByText('Databricks SQL')).toBeNull();
    expect(screen.queryByText('Genie')).toBeNull();

    // Managed drill-down types are searchable by name too.
    fireEvent.change(search, { target: { value: 'ai sea' } });
    expect(screen.getByText('AI Search')).toBeInTheDocument();
    expect(screen.queryByText('jira')).toBeNull();

    // Clearing the filter restores everything.
    fireEvent.change(search, { target: { value: '' } });
    expect(screen.getByText('My MCP')).toBeInTheDocument();
    expect(screen.getByText('jira')).toBeInTheDocument();
    expect(screen.getByText('Genie')).toBeInTheDocument();
  });

  it('shows a no-matches state when the search finds nothing', async () => {
    render(<McpPicker />);
    await openPicker();
    fireEvent.change(screen.getByLabelText('Search MCP servers'), { target: { value: 'zzz' } });
    expect(screen.getByText('No matching MCP servers')).toBeInTheDocument();
  });

  it('hides Databricks entries that are already registered as Kasal servers', async () => {
    listKasalMcpServers.mockResolvedValue([
      // Same name as the managed SQL leaf…
      { id: 1, name: 'Databricks SQL', enabled: true, server_url: 'https://other/url' },
      // …and same URL as the external jira connection (different name).
      { id: 2, name: 'Jira (registered)', enabled: true, server_url: EXTERNAL_OPTION.server_url },
    ]);
    render(<McpPicker />);
    fireEvent.click(screen.getByLabelText('MCP servers'));
    await waitFor(() => expect(screen.getByText('Jira (registered)')).toBeInTheDocument());

    // Each server appears exactly once: the Kasal registration wins.
    expect(screen.getAllByText('Databricks SQL')).toHaveLength(1);
    expect(screen.queryByText('jira')).toBeNull();
    expect(screen.getByText('Unity Catalog Functions (main.gold)')).toBeInTheDocument();
  });

  it('shows an empty state when no servers exist anywhere', async () => {
    listKasalMcpServers.mockResolvedValue([]);
    getDatabricksMcpCatalog.mockResolvedValue({ workspace_url: '', external: [], managed: [] });
    render(<McpPicker />);
    fireEvent.click(screen.getByLabelText('MCP servers'));
    await waitFor(() =>
      expect(screen.getByText('No MCP servers available')).toBeInTheDocument(),
    );
  });

  it('shows errors when the Kasal list or the Databricks catalog cannot load', async () => {
    listKasalMcpServers.mockRejectedValue(new Error('boom'));
    const first = render(<McpPicker />);
    fireEvent.click(screen.getByLabelText('MCP servers'));
    await waitFor(() =>
      expect(screen.getByText('Could not load MCP servers')).toBeInTheDocument(),
    );
    // The Databricks portion still renders.
    expect(screen.getByText('jira')).toBeInTheDocument();
    first.unmount();

    listKasalMcpServers.mockResolvedValue(KASAL_SERVERS);
    getDatabricksMcpCatalog.mockRejectedValue(new Error('down'));
    render(<McpPicker />);
    fireEvent.click(screen.getByLabelText('MCP servers'));
    await waitFor(() =>
      expect(screen.getByText('Could not load Databricks MCPs')).toBeInTheDocument(),
    );
    // The Kasal servers still render.
    expect(screen.getByText('My MCP')).toBeInTheDocument();
  });

  it('selecting an external server registers it and selects the returned name', async () => {
    render(<McpPicker />);
    await openPicker();

    fireEvent.click(screen.getByText('jira'));
    await waitFor(() =>
      expect(useExecutionStore.getState().selectedMcpServers).toEqual(['jira']),
    );
    expect(ensureDatabricksMcpServer).toHaveBeenCalledWith(EXTERNAL_OPTION);
    // The list refreshes after a registration may have created a server.
    expect(listKasalMcpServers).toHaveBeenCalledTimes(2);
  });

  it('selecting a managed leaf passes its server_url through', async () => {
    render(<McpPicker />);
    await openPicker();

    fireEvent.click(screen.getByText('Databricks SQL'));
    await waitFor(() =>
      expect(useExecutionStore.getState().selectedMcpServers).toEqual(['databricks sql']),
    );
    expect(ensureDatabricksMcpServer).toHaveBeenCalledWith({
      id: 'sql',
      kind: 'sql',
      name: 'Databricks SQL',
      description: null,
      server_url: 'https://ws/api/2.0/mcp/sql',
    });
  });

  it('warns in plain language about data changes while Databricks SQL is selected', async () => {
    render(<McpPicker />);
    await openPicker();

    // No warning for other selections.
    fireEvent.click(screen.getByText('My MCP'));
    expect(screen.queryByText(/change your data/)).toBeNull();

    fireEvent.click(screen.getByText('Databricks SQL'));
    await waitFor(() =>
      expect(useExecutionStore.getState().selectedMcpServers).toContain('databricks sql'),
    );
    // Plain-language wording (no jargon like "CRUD").
    expect(screen.getByText(/change your data/)).toBeInTheDocument();
    expect(screen.getByText(/add, update or permanently delete/)).toBeInTheDocument();
    expect(screen.queryByText(/CRUD/)).toBeNull();

    // Deselecting clears the warning.
    fireEvent.click(screen.getByText('Databricks SQL'));
    expect(useExecutionStore.getState().selectedMcpServers).not.toContain('databricks sql');
    expect(screen.queryByText(/change your data/)).toBeNull();
  });

  it('tolerates a managed leaf without a server_url', async () => {
    getDatabricksMcpCatalog.mockResolvedValue({
      workspace_url: 'https://ws',
      external: [],
      managed: [{ ...SQL_TYPE, server_url: undefined }],
    });
    render(<McpPicker />);
    await openPicker();
    await waitFor(() => expect(screen.getByText('Databricks SQL')).toBeInTheDocument());

    fireEvent.click(screen.getByText('Databricks SQL'));
    await waitFor(() => expect(ensureDatabricksMcpServer).toHaveBeenCalled());
    expect(ensureDatabricksMcpServer.mock.calls[0][0]).toMatchObject({ server_url: '' });
  });

  it('deselecting a Databricks option skips the registration round-trip', async () => {
    useExecutionStore.setState({ selectedMcpServers: ['jira'] });
    render(<McpPicker />);
    await openPicker();

    fireEvent.click(screen.getByText('jira'));
    expect(useExecutionStore.getState().selectedMcpServers).toEqual([]);
    expect(ensureDatabricksMcpServer).not.toHaveBeenCalled();
  });

  it('shows the admin message when registration is forbidden (403)', async () => {
    ensureDatabricksMcpServer.mockRejectedValue({ response: { status: 403 } });
    render(<McpPicker />);
    await openPicker();

    fireEvent.click(screen.getByText('jira'));
    await waitFor(() =>
      expect(
        screen.getByText('Only admins can register Databricks MCP servers'),
      ).toBeInTheDocument(),
    );
    expect(useExecutionStore.getState().selectedMcpServers).toEqual([]);
  });

  it('shows a generic message for non-403 and non-HTTP registration failures', async () => {
    ensureDatabricksMcpServer.mockRejectedValue({ response: { status: 500 } });
    const first = render(<McpPicker />);
    await openPicker();
    fireEvent.click(screen.getByText('jira'));
    await waitFor(() =>
      expect(
        screen.getByText('Could not register the Databricks MCP server'),
      ).toBeInTheDocument(),
    );
    first.unmount();

    ensureDatabricksMcpServer.mockRejectedValue(new Error('network'));
    render(<McpPicker />);
    await openPicker();
    fireEvent.click(screen.getByText('jira'));
    await waitFor(() =>
      expect(
        screen.getByText('Could not register the Databricks MCP server'),
      ).toBeInTheDocument(),
    );
  });

  it('drills into Genie spaces, selects one, and collapses on a second click', async () => {
    render(<McpPicker />);
    await expandGenie();
    expect(listGenieMcpSpaces).toHaveBeenCalledWith(undefined);
    expect(screen.getByLabelText('Search Genie spaces')).toBeInTheDocument();

    fireEvent.click(screen.getByText('Sales Space'));
    await waitFor(() =>
      expect(useExecutionStore.getState().selectedMcpServers).toEqual([
        'databricks genie: sales space',
      ]),
    );
    expect(ensureDatabricksMcpServer).toHaveBeenCalledWith(GENIE_SPACE);

    // Collapse the drill-down.
    fireEvent.click(screen.getByText('Genie'));
    expect(screen.queryByText('Sales Space')).toBeNull();
  });

  it('searches Genie spaces with a debounce', async () => {
    render(<McpPicker />);
    await expandGenie();

    listGenieMcpSpaces.mockResolvedValue({ options: [GENIE_SPACE_2], next_page_token: null });
    fireEvent.change(screen.getByLabelText('Search Genie spaces'), {
      target: { value: 'marketing' },
    });

    await waitFor(() => expect(screen.getByText('Marketing Space')).toBeInTheDocument());
    expect(listGenieMcpSpaces).toHaveBeenLastCalledWith('marketing');
    expect(screen.queryByText('Sales Space')).toBeNull();
  });

  it('pages Genie spaces through Load more', async () => {
    listGenieMcpSpaces
      .mockResolvedValueOnce({ options: [GENIE_SPACE], next_page_token: 'tok-2' })
      .mockResolvedValueOnce({ options: [GENIE_SPACE_2], next_page_token: null });
    render(<McpPicker />);
    await expandGenie();

    fireEvent.click(screen.getByText('Load more…'));
    await waitFor(() => expect(screen.getByText('Marketing Space')).toBeInTheDocument());
    expect(listGenieMcpSpaces).toHaveBeenLastCalledWith(undefined, 'tok-2');
    expect(screen.getByText('Sales Space')).toBeInTheDocument(); // appended, not replaced
    expect(screen.queryByText('Load more…')).toBeNull();
  });

  it('surfaces Genie load and Load-more failures', async () => {
    listGenieMcpSpaces.mockRejectedValue(new Error('down'));
    const first = render(<McpPicker />);
    await openPicker();
    fireEvent.click(screen.getByText('Genie'));
    await waitFor(() =>
      expect(screen.getByText('Could not load Genie spaces')).toBeInTheDocument(),
    );
    expect(screen.getByText('No spaces found')).toBeInTheDocument();
    first.unmount();

    listGenieMcpSpaces
      .mockResolvedValueOnce({ options: [GENIE_SPACE], next_page_token: 'tok-2' })
      .mockRejectedValueOnce(new Error('down'));
    render(<McpPicker />);
    await expandGenie();
    fireEvent.click(screen.getByText('Load more…'));
    await waitFor(() =>
      expect(screen.getByText('Could not load more Genie spaces')).toBeInTheDocument(),
    );
    expect(screen.getByText('Sales Space')).toBeInTheDocument(); // kept
  });

  it('drills into AI Search indexes and selects one', async () => {
    render(<McpPicker />);
    await openPicker();
    fireEvent.click(screen.getByText('AI Search'));
    await waitFor(() => expect(screen.getByText('main.gold.docs_idx')).toBeInTheDocument());
    expect(listAiSearchMcpIndexes).toHaveBeenCalledTimes(1);

    fireEvent.click(screen.getByText('main.gold.docs_idx'));
    await waitFor(() =>
      expect(useExecutionStore.getState().selectedMcpServers).toEqual([
        'databricks ai search: main.gold.docs_idx',
      ]),
    );
    expect(ensureDatabricksMcpServer).toHaveBeenCalledWith(AI_INDEX);
  });

  it('surfaces AI Search load failures', async () => {
    listAiSearchMcpIndexes.mockRejectedValue(new Error('down'));
    render(<McpPicker />);
    await openPicker();
    fireEvent.click(screen.getByText('AI Search'));
    await waitFor(() =>
      expect(screen.getByText('Could not load AI Search indexes')).toBeInTheDocument(),
    );
    expect(screen.getByText('No indexes found')).toBeInTheDocument();
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

  it('ignores Kasal results that land after unmount', async () => {
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

  it('ignores Databricks catalogs that land after unmount', async () => {
    let resolveCatalog: (v: unknown) => void = () => undefined;
    let rejectCatalog: (e: unknown) => void = () => undefined;
    getDatabricksMcpCatalog
      .mockImplementationOnce(() => new Promise((resolve) => { resolveCatalog = resolve; }))
      .mockImplementationOnce(() => new Promise((_, reject) => { rejectCatalog = reject; }));

    const first = render(<McpPicker />);
    fireEvent.click(screen.getByLabelText('MCP servers'));
    first.unmount();
    await act(async () => resolveCatalog(CATALOG));

    const second = render(<McpPicker />);
    fireEvent.click(screen.getByLabelText('MCP servers'));
    second.unmount();
    await act(async () => rejectCatalog(new Error('late failure')));
  });

  it('ignores Genie spaces that land after unmount', async () => {
    let resolveSpaces: (v: unknown) => void = () => undefined;
    let rejectSpaces: (e: unknown) => void = () => undefined;
    listGenieMcpSpaces
      .mockImplementationOnce(() => new Promise((resolve) => { resolveSpaces = resolve; }))
      .mockImplementationOnce(() => new Promise((_, reject) => { rejectSpaces = reject; }));

    const first = render(<McpPicker />);
    await openPicker();
    fireEvent.click(screen.getByText('Genie'));
    // Let the (0ms) debounce timer fire so the request actually starts.
    await waitFor(() => expect(listGenieMcpSpaces).toHaveBeenCalledTimes(1));
    first.unmount();
    await act(async () => resolveSpaces({ options: [GENIE_SPACE], next_page_token: null }));

    const second = render(<McpPicker />);
    await openPicker();
    fireEvent.click(screen.getByText('Genie'));
    await waitFor(() => expect(listGenieMcpSpaces).toHaveBeenCalledTimes(2));
    second.unmount();
    await act(async () => rejectSpaces(new Error('late failure')));
  });

  it('ignores AI Search indexes that land after unmount', async () => {
    let resolveIndexes: (v: unknown) => void = () => undefined;
    let rejectIndexes: (e: unknown) => void = () => undefined;
    listAiSearchMcpIndexes
      .mockImplementationOnce(() => new Promise((resolve) => { resolveIndexes = resolve; }))
      .mockImplementationOnce(() => new Promise((_, reject) => { rejectIndexes = reject; }));

    const first = render(<McpPicker />);
    await openPicker();
    fireEvent.click(screen.getByText('AI Search'));
    first.unmount();
    await act(async () => resolveIndexes([AI_INDEX]));

    const second = render(<McpPicker />);
    await openPicker();
    fireEvent.click(screen.getByText('AI Search'));
    second.unmount();
    await act(async () => rejectIndexes(new Error('late failure')));
  });

  it('marks the option busy while the registration is in flight', async () => {
    let release: (v: string) => void = () => undefined;
    ensureDatabricksMcpServer.mockImplementation(
      () => new Promise<string>((resolve) => { release = resolve; }),
    );
    render(<McpPicker />);
    await openPicker();

    fireEvent.click(screen.getByText('jira'));
    await waitFor(() => expect(screen.getByText('…')).toBeInTheDocument());

    await act(async () => release('jira'));
    await waitFor(() => expect(screen.queryByText('…')).toBeNull());
  });
});
