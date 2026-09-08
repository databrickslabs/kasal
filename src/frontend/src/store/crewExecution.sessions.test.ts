import { beforeEach, expect, it, vi } from 'vitest';
import type { Node } from 'reactflow';
import { useCrewExecutionStore } from './crewExecution';
import { useBuilderCanvasStore, type BuilderCanvas } from '../app/sessions/builderCanvasStore';
import { jobExecutionService } from '../api/execution/JobExecutionService';

vi.mock('../api/execution/JobExecutionService', () => ({ jobExecutionService: { executeJob: vi.fn().mockResolvedValue({ job_id: 'run' }) } }));
vi.mock('./agent', () => ({ useAgentStore: { getState: () => ({ getAgent: vi.fn() }) } }));
vi.mock('../api/workflow/TaskService', () => ({ TaskService: { getTask: vi.fn() } }));

beforeEach(() => {
  vi.clearAllMocks();
  useBuilderCanvasStore.setState({ activeCanvasId: 'origin', canvases: [
    { id: 'origin', chatSessionId: 'conversation-origin', savedCrewId: 'saved-origin' },
    { id: 'other', chatSessionId: 'conversation-other', savedCrewId: 'saved-other' },
  ].map(tab => ({ ...tab, createdAt: new Date(), lastModified: new Date() })) as BuilderCanvas[] });
});

it('keeps the originating conversation and saved crew when navigation happens during refresh', async () => {
  const nodes: Node[] = ['agentNode', 'taskNode'].map(type => ({ id: type, type, data: {}, position: { x: 0, y: 0 } }));
  const run = useCrewExecutionStore.getState().executeCrew(nodes, []);
  useBuilderCanvasStore.setState({ activeCanvasId: 'other' });
  await run;
  expect(jobExecutionService.executeJob).toHaveBeenCalledWith(nodes, [], expect.any(String), 'crew',
    expect.objectContaining({ session_id: 'conversation-origin' }), expect.any(Boolean), expect.any(Boolean), undefined, 'saved-origin');
});

it('passes the flow conversation to execution', async () => {
  await useCrewExecutionStore.getState().executeFlow([{ id: 'crew', type: 'crewNode', data: {}, position: { x: 0, y: 0 } }], []);
  expect(vi.mocked(jobExecutionService.executeJob).mock.calls[0][4]).toEqual(expect.objectContaining({ session_id: 'conversation-origin' }));
});
