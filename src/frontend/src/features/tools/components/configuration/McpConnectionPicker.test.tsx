import React, { useState } from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { act, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import McpConnectionPicker from './McpConnectionPicker';
import MCPServerSelector from './MCPServerSelector';
import { usePermissionStore } from '../../../../store/permissions';

const api = vi.hoisted(() => ({ getMcpServers: vi.fn(), getBaseServers: vi.fn(), ensureDatabricksServer: vi.fn(),
  getDatabricksCatalog: vi.fn(), enableForWorkspace: vi.fn(), listGenieSpaces: vi.fn(), listAiSearchIndexes: vi.fn(),
  listFunctionSchemas: vi.fn(), listSchemaFunctions: vi.fn(), createGlobalServer: vi.fn(), setGlobalAvailability: vi.fn() }));
vi.mock('../../../../api/tools/MCPService', () => ({ MCPService: { getInstance: () => api }, databricksMcpServerName: (o: { name: string }) => o.name.toLowerCase() }));
const option = { id: 'sql', name: 'Databricks SQL', kind: 'sql', server_url: 'https://ws/mcp/sql' };
const server = { id: 'base-1', name: 'databricks sql', server_url: option.server_url, enabled: true, group_id: null };
const changed = vi.fn();
function Selection({ form = false }: { form?: boolean }) {
  const [names, setNames] = useState<string[]>([]);
  const onChange = (next: string[] | string | null) => { changed(next); setNames(Array.isArray(next) ? next : next ? [next] : []); };
  return form ? <MCPServerSelector value={names} onChange={onChange} /> : <McpConnectionPicker selectedNames={names} onChange={onChange} />;
}
beforeEach(() => {
  vi.resetAllMocks();
  localStorage.setItem('selectedGroupId', 'team-a');
  usePermissionStore.setState({ isSystemAdmin: true, userRole: 'admin' });
  api.getMcpServers.mockResolvedValue({ servers: [] });
  api.getBaseServers.mockResolvedValue({ servers: [server] });
  api.getDatabricksCatalog.mockResolvedValue({ workspace_url: 'https://ws', external: [], managed: [{ ...option, expandable: false },
    { id: 'genie', kind: 'genie', name: 'Genie spaces', expandable: true },
    { id: 'functions', kind: 'functions', name: 'Unity Catalog Functions', expandable: true }] });
  api.ensureDatabricksServer.mockResolvedValue(server.name);
  api.enableForWorkspace.mockResolvedValue({ ...server, id: 'workspace-1', group_id: 'team-a' });
  api.listGenieSpaces.mockResolvedValue({ options: [{ ...option, id: 'genie-1', name: 'Sales Genie' }], next_page_token: 'page-2' });
  api.listFunctionSchemas.mockResolvedValue({ options: [{ ...option, name: 'main.tools' }], catalogs: ['main'], selected_catalog: 'main' });
});
async function browse() { fireEvent.click(await screen.findByRole('button', { name: 'Connect a tool…' })); await screen.findByText('Databricks SQL'); }
async function connectSql() { fireEvent.click(within(screen.getByText('Databricks SQL').closest('div.rounded-lg') as HTMLElement).getByRole('button', { name: /^(Connect|Use)$/ })); }

describe('shared inline MCP connection picker', () => {
  it.each([false, true])('connects and selects without leaving the picker (builder form=%s)', async (form) => {
    render(<Selection form={form} />);
    if (form) fireEvent.click(screen.getByRole('combobox'));
    await browse();
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    await connectSql();
    await waitFor(() => expect(changed).toHaveBeenCalledWith(['databricks sql']));
    expect(api.enableForWorkspace).toHaveBeenCalledWith('base-1', 'team-a');
    expect(await screen.findByRole('button', { name: 'Selected' })).toBeDisabled();
    fireEvent.click(screen.getByRole('button', { name: '‹ Your tools' }));
    expect(await screen.findByRole('checkbox', { name: /databricks sql/ })).toHaveAttribute('aria-checked', 'true');
  });
  it('can use a previously registered server instead of disabling its catalog action', async () => {
    api.getMcpServers.mockResolvedValue({ servers: [{ ...server, enabled: false }] });
    render(<Selection />); await browse();
    expect(screen.getByRole('button', { name: 'Use' })).toBeEnabled();
    await connectSql();
    await waitFor(() => expect(changed).toHaveBeenCalledWith([server.name]));
  });
  it('does not claim selection after failed enablement and permits retry', async () => {
    api.enableForWorkspace.mockRejectedValueOnce(new Error('Enablement denied'));
    render(<Selection />); await browse(); await connectSql();
    expect(await screen.findByRole('alert')).toHaveTextContent('Enablement denied');
    expect(changed).not.toHaveBeenCalled();
    await connectSql();
    await waitFor(() => expect(changed).toHaveBeenCalledWith([server.name]));
  });
  it('reports failed discovery without showing a misleading empty catalog and retries', async () => {
    api.getDatabricksCatalog.mockRejectedValueOnce(new Error('MCP-DISCOVERY-v1: connection unavailable'));
    render(<Selection />);
    fireEvent.click(await screen.findByRole('button', { name: 'Connect a tool…' }));
    expect(await screen.findByRole('alert')).toHaveTextContent('MCP-DISCOVERY-v1');
    expect(screen.queryByText(/No Databricks servers/)).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Retry Databricks discovery' }));
    expect(await screen.findByText('Databricks SQL')).toBeInTheDocument();
  });
  it('pages through Genie spaces and switches categories without a back step', async () => {
    render(<Selection />); await browse();
    fireEvent.click(screen.getByRole('button', { name: 'Genie spaces' }));
    expect(await screen.findByText('Sales Genie')).toBeInTheDocument();
    api.listGenieSpaces.mockResolvedValueOnce({ options: [{ ...option, id: 'genie-2', name: 'Finance Genie' }], next_page_token: null });
    fireEvent.click(screen.getByRole('button', { name: 'Load more Genie spaces' }));
    expect(await screen.findByText('Finance Genie')).toBeInTheDocument();
    expect(screen.getByText('Sales Genie')).toBeInTheDocument();
    expect(api.listGenieSpaces).toHaveBeenLastCalledWith(undefined, 'page-2');
    fireEvent.click(screen.getByRole('button', { name: 'Unity Catalog Functions' }));
    expect(await screen.findByText('main.tools')).toBeInTheDocument();
    expect(api.listFunctionSchemas).toHaveBeenCalledTimes(1);
  });
  it('does not enable a server in a different teamspace after registration finishes', async () => {
    let resolve!: (name: string) => void;
    api.ensureDatabricksServer.mockReturnValue(new Promise<string>((r) => { resolve = r; }));
    render(<Selection />); await browse(); await connectSql();
    localStorage.setItem('selectedGroupId', 'team-b');
    await act(async () => resolve(server.name));
    expect(api.enableForWorkspace).not.toHaveBeenCalled();
    expect(changed).not.toHaveBeenCalled();
  });
  it('does not select in a different editor after unmount', async () => {
    let resolve!: (value: typeof server) => void;
    api.enableForWorkspace.mockReturnValue(new Promise<typeof server>((r) => { resolve = r; }));
    const { unmount } = render(<Selection />); await browse(); await connectSql();
    await waitFor(() => expect(api.enableForWorkspace).toHaveBeenCalled());
    unmount(); await act(async () => resolve(server));
    expect(changed).not.toHaveBeenCalled();
  });
  it('connects in builder composer mode without changing a Chat selection', async () => {
    render(<McpConnectionPicker />); await browse(); await connectSql();
    expect(await screen.findByRole('button', { name: 'Connected' })).toBeDisabled();
    expect(changed).not.toHaveBeenCalled();
  });
  it('lets workspace admins enable existing servers without exposing global registration', async () => {
    usePermissionStore.setState({ isSystemAdmin: false, userRole: 'admin' });
    api.getMcpServers.mockResolvedValue({ servers: [{ ...server, enabled: false }] });
    render(<Selection />);
    fireEvent.click(await screen.findByRole('checkbox', { name: /databricks sql/ }));
    await waitFor(() => expect(changed).toHaveBeenCalledWith([server.name]));
    expect(screen.queryByRole('button', { name: 'Connect a tool…' })).toBeNull();
    expect(api.getDatabricksCatalog).not.toHaveBeenCalled();
  });
  it('members can select enabled servers but cannot configure or enable disabled servers', async () => {
    usePermissionStore.setState({ isSystemAdmin: false, userRole: 'operator', isPersonalWorkspaceManager: false });
    api.getMcpServers.mockResolvedValue({ servers: [server, { ...server, id: 'off', name: 'Disabled', enabled: false }] });
    render(<Selection />);
    fireEvent.click(await screen.findByRole('checkbox', { name: /databricks sql/ }));
    expect(changed).toHaveBeenCalledWith([server.name]);
    expect(screen.queryByText('Disabled')).toBeNull();
    expect(screen.queryByText('Connect a tool…')).toBeNull();
    expect(api.enableForWorkspace).not.toHaveBeenCalled();
  });
});
