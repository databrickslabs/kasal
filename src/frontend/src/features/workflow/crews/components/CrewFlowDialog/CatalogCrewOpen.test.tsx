/**
 * The catalog card opens a crew IN PLACE; "Open as copy" starts a new one.
 *
 * Clicking a card used to re-create every agent and task row from the canvas
 * nodes (losing per-agent LLM overrides and skills, and nulling every task's
 * agent) and then bind the copy to the ORIGINAL crew id, so the next Save
 * repointed the saved crew at the lossy copies. Opening in place keeps the
 * crew's own rows; the copy is an explicit action and binds no crew id.
 */
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import CrewFlowSelectionDialog from './CrewFlowDialog';
import { CrewService } from '../../../../../api/workflow/CrewService';
import { AgentService } from '../../../../../api/workflow/AgentService';
import { TaskService } from '../../../../../api/workflow/TaskService';
import { usePublicationStore } from '../../../../../store/publication';
import { useCrewExecutionStore } from '../../../../../store/crewExecution';
import type { CrewResponse } from '../../../../../types/workflow/crew';

const permissions = vi.hoisted(() => ({ canEdit: true, canDelete: true }));
vi.mock('../../../../../hooks/usePermissions', () => ({ usePermissions: () => permissions }));
vi.mock('../../../../../hooks/global/useMLflowEnabled', () => ({ useMLflowEnabled: () => false }));
vi.mock('../../../../../api/workflow/CrewService', () => ({
  CrewService: { getCrews: vi.fn(), getCrew: vi.fn() },
  CrewFeedbackService: { getSummary: vi.fn(async () => []), getForCrew: vi.fn(async () => []) },
}));
vi.mock('../../../../../api/workflow/AgentService', () => ({
  AgentService: { createAgent: vi.fn(), listAgents: vi.fn(async () => []) },
}));
vi.mock('../../../../../api/workflow/TaskService', () => ({
  TaskService: { createTask: vi.fn(), listTasks: vi.fn(async () => []) },
}));
vi.mock('./CatalogSurface', () => ({ default: ({ children }: { children: React.ReactNode }) => <div>{children}</div> }));
vi.mock('./PublishButton', () => ({ default: () => null }));
vi.mock('../CrewOptimizeDialog', () => ({ default: () => null }));
vi.mock('../../../export/components/ExportCrewDialog', () => ({ default: () => null }));

const position = { x: 0, y: 0 };
const crew = {
  id: 'crew-1',
  name: 'Research crew',
  agent_ids: ['a1', 'a2'],
  task_ids: ['t1', 't2'],
  created_at: '2026-10-01T10:00:00Z',
  updated_at: '2026-10-01T10:00:00Z',
  process: 'sequential',
  nodes: [
    { id: 'agent-a1', type: 'agentNode', position, data: { label: 'Researcher', role: 'researcher', agentId: 'a1' } },
    { id: 'agent-a2', type: 'agentNode', position, data: { label: 'Writer', role: 'writer', agentId: 'a2' } },
    { id: 'task-t1', type: 'taskNode', position, data: { label: 'research', taskId: 't1' } },
    { id: 'task-t2', type: 'taskNode', position, data: { label: 'write', taskId: 't2' } },
  ],
  edges: [
    { id: 'e1', source: 'agent-a1', target: 'task-t1' },
    { id: 'e2', source: 'agent-a2', target: 'task-t2' },
  ],
} as unknown as CrewResponse;

beforeEach(() => {
  vi.clearAllMocks();
  permissions.canEdit = true;
  usePublicationStore.setState({ refresh: vi.fn(async () => undefined) });
  vi.mocked(CrewService.getCrews).mockResolvedValue([crew]);
  vi.mocked(CrewService.getCrew).mockResolvedValue(crew);
  vi.mocked(AgentService.createAgent).mockImplementation(async data => ({ id: `copy-${data.name}` }) as never);
  vi.mocked(TaskService.createTask).mockImplementation(async data => ({ id: `copy-${data.name}` }) as never);
});

function setup() {
  const onCrewSelect = vi.fn();
  const onClose = vi.fn();
  render(<CrewFlowSelectionDialog open embedded initialTab={0} showOnlyTab={0} onClose={onClose} onCrewSelect={onCrewSelect} />);
  return { onCrewSelect, onClose };
}

describe('Opening a catalog crew', () => {
  it('opens the crew in place: its own rows, bound to its id', async () => {
    const loadStarted = vi.fn();
    window.addEventListener('crewLoadStarted', loadStarted);
    const { onCrewSelect, onClose } = setup();

    fireEvent.click(await screen.findByText('Research crew'));

    await waitFor(() => expect(onCrewSelect).toHaveBeenCalledOnce());
    // The nodes still name the saved agent rows a1/a2, whose per-agent
    // overrides (temperature, effort, skills) the run reads.
    expect(onCrewSelect).toHaveBeenCalledWith(crew.nodes, crew.edges, 'Research crew', 'crew-1');
    expect(AgentService.createAgent).not.toHaveBeenCalled();
    expect(TaskService.createTask).not.toHaveBeenCalled();
    expect(onClose).toHaveBeenCalledOnce();
    // The loading handshake that keeps useManagerNode off the incoming crew.
    expect(loadStarted).toHaveBeenCalled();
    await waitFor(() => expect(useCrewExecutionStore.getState().isLoadingCrew).toBe(false));
    window.removeEventListener('crewLoadStarted', loadStarted);
  });

  it('"Open as copy" creates new rows and binds no crew id, so Save creates a new crew', async () => {
    const { onCrewSelect } = setup();

    fireEvent.click(await screen.findByRole('button', { name: 'Open Research crew as copy' }));

    await waitFor(() => expect(onCrewSelect).toHaveBeenCalledOnce());
    expect(AgentService.createAgent).toHaveBeenCalledTimes(2);
    expect(TaskService.createTask).toHaveBeenCalledTimes(2);
    // Each copied task is assigned the copy of the agent its edge comes from.
    expect(TaskService.createTask).toHaveBeenCalledWith(expect.objectContaining({ name: 'write', agent_id: 'copy-Writer' }));

    const [nodes, edges, name, crewId] = onCrewSelect.mock.calls[0];
    expect(crewId).toBeUndefined();
    expect(name).toBe('Research crew (copy)');
    expect(nodes.map((n: { id: string }) => n.id)).toEqual([
      'agent-copy-Researcher', 'agent-copy-Writer', 'task-copy-research', 'task-copy-write',
    ]);
    expect(edges).toEqual([
      expect.objectContaining({ source: 'agent-copy-Researcher', target: 'task-copy-research' }),
      expect.objectContaining({ source: 'agent-copy-Writer', target: 'task-copy-write' }),
    ]);
  });

  it('"Open as copy" gives a task with two agent edges the LAST one, as a canvas run does', async () => {
    vi.mocked(CrewService.getCrew).mockResolvedValue({
      ...crew,
      edges: [...(crew.edges ?? []), { id: 'e3', source: 'agent-a1', target: 'task-t2' }],
    } as CrewResponse);
    const { onCrewSelect } = setup();

    fireEvent.click(await screen.findByRole('button', { name: 'Open Research crew as copy' }));

    await waitFor(() => expect(onCrewSelect).toHaveBeenCalledOnce());
    expect(TaskService.createTask).toHaveBeenCalledWith(expect.objectContaining({ name: 'write', agent_id: 'copy-Researcher' }));
  });

  it('keeps "Open as copy" from operators, who cannot create rows', async () => {
    permissions.canEdit = false;
    setup();
    await screen.findByText('Research crew');
    expect(screen.queryByRole('button', { name: 'Open Research crew as copy' })).toBeNull();
  });
});
