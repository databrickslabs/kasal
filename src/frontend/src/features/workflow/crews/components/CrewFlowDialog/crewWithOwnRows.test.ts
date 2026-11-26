/**
 * An imported crew file carries the agent and task ids of the installation it
 * came from. Saved as it is, the crew opened on the canvas and the first task
 * edit answered "Task not found", because no such rows existed here. The import
 * now creates the rows and saves the crew pointing at them.
 */
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { Edge, Node } from 'reactflow';
import { crewWithOwnRows } from './cloneCatalogCrew';
import { AgentService } from '../../../../../api/workflow/AgentService';
import { TaskService } from '../../../../../api/workflow/TaskService';

vi.mock('../../../../../api/workflow/AgentService', () => ({ AgentService: { createAgent: vi.fn() } }));
vi.mock('../../../../../api/workflow/TaskService', () => ({ TaskService: { createTask: vi.fn() } }));

const position = { x: 0, y: 0 };
const nodes = [
  { id: 'agent-old-a', type: 'agentNode', position, data: { label: 'Extractor', role: 'Extractor', agentId: 'old-a' } },
  {
    id: 'task-old-t',
    type: 'taskNode',
    position,
    data: { label: 'Extract', description: 'Call the tool', taskId: 'old-t', tools: ['90'], tool_configs: { Tool: { catalog: 'main' } } },
  },
] as unknown as Node[];
const edges = [{ id: 'e1', source: 'agent-old-a', target: 'task-old-t' }] as Edge[];

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(AgentService.createAgent).mockResolvedValue({ id: 'new-a' } as never);
  vi.mocked(TaskService.createTask).mockResolvedValue({ id: 'new-t' } as never);
});

describe('crewWithOwnRows', () => {
  it('creates the agent and the task, with the task bound to the new agent', async () => {
    await crewWithOwnRows({ nodes, edges });

    expect(AgentService.createAgent).toHaveBeenCalledTimes(1);
    expect(TaskService.createTask).toHaveBeenCalledWith(
      expect.objectContaining({ name: 'Extract', description: 'Call the tool', tools: ['90'], agent_id: 'new-a' })
    );
  });

  it('returns the crew pointing at the new rows, not at the ids in the file', async () => {
    const crew = await crewWithOwnRows({ nodes, edges });

    expect(crew.agent_ids).toEqual(['new-a']);
    expect(crew.task_ids).toEqual(['new-t']);
    expect(crew.nodes.map(node => node.id)).toEqual(['agent-new-a', 'task-new-t']);
    expect(crew.nodes[1].data.taskId).toBe('new-t');
    expect(crew.edges[0]).toMatchObject({ source: 'agent-new-a', target: 'task-new-t' });
  });

  it('keeps the tool settings on the task node, which the crew save writes to the new row', async () => {
    const crew = await crewWithOwnRows({ nodes, edges });

    expect(crew.nodes[1].data.tool_configs).toEqual({ Tool: { catalog: 'main' } });
  });
});
