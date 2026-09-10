import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import McpPicker from './McpPicker';
import { useExecutionStore } from '../../store/executionStore';
import { useAppStore } from '../../store/appStore';
import { usePermissionStore } from '../../../../store/permissions';

const api = vi.hoisted(() => ({ getMcpServers: vi.fn(), getDatabricksCatalog: vi.fn(), getEndpoints: vi.fn() }));
vi.mock('../../../../api/tools/MCPService', () => ({ MCPService: { getInstance: () => api }, databricksMcpServerName: (o: { name: string }) => o.name.toLowerCase() }));
vi.mock('../../../../api/databricks/AgentBricksService', () => ({ AgentBricksService: { getEndpoints: api.getEndpoints } }));
beforeEach(() => {
  vi.resetAllMocks();
  localStorage.setItem('selectedGroupId', 'team-a');
  usePermissionStore.setState({ isSystemAdmin: true, userRole: 'admin' });
  useExecutionStore.setState({ selectedMcpServers: [], selectedAgentBricksEndpoints: [] });
  useAppStore.setState({ toolNameMap: {} });
  api.getMcpServers.mockResolvedValue({ servers: [{ id: 's1', name: 'Sales', enabled: true }] });
  api.getDatabricksCatalog.mockResolvedValue({ workspace_url: 'https://ws', managed: [], external: [{ id: 'e1', name: 'Example MCP', server_url: 'https://ws/mcp/example' }] });
  api.getEndpoints.mockResolvedValue({ endpoints: [{ id: 'ab1', name: 'sales_bot', display_name: 'Sales Bot' }] });
});
async function open() { fireEvent.click(screen.getByRole('button', { name: 'MCP servers' })); await screen.findByText('Sales'); }

describe('Chat MCP picker', () => {
  it('connects inside the existing picker instead of opening a dialog', async () => {
    render(<McpPicker />); expect(api.getMcpServers).not.toHaveBeenCalled();
    await open();
    fireEvent.click(screen.getByRole('button', { name: 'Connect a tool…' }));
    expect(await screen.findByText('Example MCP')).toBeInTheDocument();
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    expect(screen.getByRole('menu', { name: 'MCP picker' })).toBeInTheDocument();
  });
  it('selects and deselects the actual Chat execution store', async () => {
    render(<McpPicker />); await open();
    const row = screen.getByRole('checkbox', { name: /Sales/ });
    fireEvent.click(row); expect(useExecutionStore.getState().selectedMcpServers).toEqual(['Sales']);
    expect(row).toHaveAttribute('aria-checked', 'true');
    fireEvent.click(row); expect(useExecutionStore.getState().selectedMcpServers).toEqual([]);
  });
  it('prunes stale selections after a successful read, while preserving them on failure', async () => {
    useExecutionStore.setState({ selectedMcpServers: ['Sales', 'Deleted'] });
    const { unmount } = render(<McpPicker variant="inline" />);
    await waitFor(() => expect(useExecutionStore.getState().selectedMcpServers).toEqual(['Sales']));
    unmount();
    api.getMcpServers.mockRejectedValue(new Error('Connection unavailable'));
    render(<McpPicker variant="inline" />);
    expect(await screen.findByRole('alert')).toHaveTextContent('Connection unavailable');
    expect(useExecutionStore.getState().selectedMcpServers).toEqual(['Sales']);
  });
  it('uses the same picker inline without a second trigger', async () => {
    render(<McpPicker variant="inline" />);
    expect(await screen.findByText('Sales')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'MCP servers' })).toBeNull();
  });
  it('still offers Agent Bricks endpoints when their tool is enabled', async () => {
    useAppStore.setState({ toolNameMap: { '1': 'AgentBricksTool' } });
    render(<McpPicker variant="inline" />);
    fireEvent.click(await screen.findByRole('menuitemcheckbox', { name: /Sales Bot/ }));
    expect(useExecutionStore.getState().selectedAgentBricksEndpoints).toEqual(['sales_bot']);
    fireEvent.change(screen.getByRole('textbox', { name: 'Search agents' }), { target: { value: 'not found' } });
    expect(screen.queryByText('Sales Bot')).toBeNull();
  });
  it('does not discover Agent Bricks when their tool is disabled', async () => {
    render(<McpPicker variant="inline" />); await screen.findByText('Sales');
    expect(api.getEndpoints).not.toHaveBeenCalled();
  });
  it('closes on outside click and respects disabled state', async () => {
    const { rerender } = render(<McpPicker />); await open();
    fireEvent.mouseDown(document.body);
    expect(screen.queryByRole('menu')).toBeNull();
    rerender(<McpPicker disabled />); expect(screen.getByRole('button', { name: 'MCP servers' })).toBeDisabled();
  });
});
