import React, { useState } from 'react';
import { act, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, expect, it, vi } from 'vitest';
import CapabilitiesPicker from './CapabilitiesPicker';
import { ToolService } from '../../../../api/tools/ToolService';
import { GroupToolService } from '../../../../api/groups/GroupToolService';
import { A2AAgentService } from '../../../../api/tools/A2AAgentService';
import { usePermissionStore } from '../../../../store/permissions';
vi.mock('../../../../api/tools/ToolService', () => ({ ToolService: { listEnabledTools: vi.fn() } }));
vi.mock('../../../../api/groups/GroupToolService', () => ({ GroupToolService: { listAvailable: vi.fn(), addTool: vi.fn(), setEnabled: vi.fn() } }));
vi.mock('../../../../api/tools/A2AAgentService', () => ({ A2AAgentService: { list: vi.fn(), setWorkspaceEnabled: vi.fn() } }));
vi.mock('../../../../api/tools/MCPService', () => ({ MCPService: { getInstance: () => ({ getMcpServers: async () => ({ servers: [{ id: 'mcp', name: 'postgres', enabled: true, group_id: 'team' }] }) }) } }));
const tool = { id: 4, title: 'Web search', description: 'Search current information', enabled: true };
const added = { id: 5, title: 'Read documents', description: 'Read files', enabled: true };
const agent = { id: 1, name: 'Research assistant', description: 'Research topics', skills: [], enabled: true, group_id: 'team' };
function Picker() {
  const [tools, setTools] = useState<string[]>([]);
  const [mcps, setMcps] = useState<string[]>([]);
  return <CapabilitiesPicker selectedTools={tools} onToolsChange={setTools} selectedMcpServers={mcps} onMcpServersChange={setMcps} />;
}
beforeEach(() => {
  vi.resetAllMocks(); localStorage.setItem('selectedGroupId', 'team');
  usePermissionStore.setState({ userRole: 'admin', isSystemAdmin: false });
  vi.mocked(ToolService.listEnabledTools).mockResolvedValue([tool]);
  vi.mocked(GroupToolService.listAvailable).mockResolvedValue([added]);
  vi.mocked(A2AAgentService.list).mockResolvedValue([agent as never]);
});
it('uses one search across tools, MCP servers and A2A agents and selects both tool kinds', async () => {
  render(<Picker />);
  expect(await screen.findByText('Web search')).toBeVisible();
  expect(await screen.findByText('Research assistant')).toBeVisible();
  expect(screen.getAllByRole('textbox')).toHaveLength(1);
  fireEvent.click(screen.getByRole('checkbox', { name: /Web search/ }));
  fireEvent.click(screen.getByRole('checkbox', { name: /postgres/ }));
  expect(screen.getByRole('checkbox', { name: /Web search/ })).toHaveAttribute('aria-checked', 'true');
  expect(screen.getByRole('checkbox', { name: /postgres/ })).toHaveAttribute('aria-checked', 'true');
  fireEvent.change(screen.getByRole('textbox'), { target: { value: 'postgres' } });
  expect(screen.queryByText('Web search')).not.toBeInTheDocument();
  expect(screen.queryByText('Research assistant')).not.toBeInTheDocument();
  expect(screen.getByText('postgres')).toBeVisible();
});
it('lets a workspace admin add and select a tool through a scoped mapping', async () => {
  render(<Picker />);
  fireEvent.click(await screen.findByRole('button', { name: 'Add tools to teamspace' }));
  await screen.findByText('Read documents');
  vi.mocked(GroupToolService.addTool).mockResolvedValue({ enabled: true } as never);
  vi.mocked(ToolService.listEnabledTools).mockResolvedValue([tool, added]);
  fireEvent.click(screen.getByRole('button', { name: /Read documents/ }));
  await waitFor(() => expect(GroupToolService.addTool).toHaveBeenCalledWith(5, 'team'));
  expect(await screen.findByRole('checkbox', { name: /Read documents/ })).toHaveAttribute('aria-checked', 'true');
});
it('lets a workspace admin enable a registered A2A agent in this teamspace', async () => {
  vi.mocked(A2AAgentService.list).mockResolvedValue([{ ...agent, enabled: true, group_id: null } as never]);
  vi.mocked(A2AAgentService.setWorkspaceEnabled).mockResolvedValue(agent as never);
  render(<Picker />);
  fireEvent.click(await screen.findByRole('button', { name: 'Add agents to teamspace' }));
  fireEvent.click(await screen.findByRole('button', { name: /Research assistant/ }));
  await waitFor(() => expect(A2AAgentService.setWorkspaceEnabled).toHaveBeenCalledWith(1, true, 'team'));
  expect(await screen.findByText('Enabled')).toBeVisible();
});
it('does not expose teamspace additions to members', async () => {
  usePermissionStore.setState({ userRole: 'user', isSystemAdmin: false });
  render(<Picker />); await screen.findByText('Web search');
  expect(screen.queryByRole('button', { name: /Add .* to teamspace/ })).not.toBeInTheDocument();
  expect(GroupToolService.listAvailable).not.toHaveBeenCalled();
});
it('does not select a tool after the user switches teamspace while adding it', async () => {
  let complete!: (value: never) => void;
  vi.mocked(GroupToolService.addTool).mockImplementation(() => new Promise(resolve => { complete = resolve; }));
  render(<Picker />);
  fireEvent.click(await screen.findByRole('button', { name: 'Add tools to teamspace' }));
  fireEvent.click(await screen.findByRole('button', { name: /Read documents/ }));
  localStorage.setItem('selectedGroupId', 'other');
  await act(async () => complete({ enabled: false } as never));
  expect(GroupToolService.setEnabled).not.toHaveBeenCalled();
  expect(screen.queryByRole('checkbox', { name: /Read documents/ })).not.toBeInTheDocument();
});
