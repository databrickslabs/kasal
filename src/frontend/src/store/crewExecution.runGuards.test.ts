import { beforeEach, expect, it, vi } from 'vitest';
import type { Node } from 'reactflow';
import { useCrewExecutionStore } from './crewExecution';
import { useBuilderCanvasStore, type BuilderCanvas } from '../app/sessions/builderCanvasStore';
import { jobExecutionService } from '../api/execution/JobExecutionService';
import { ToolService } from '../api/tools/ToolService';

vi.mock('../api/execution/JobExecutionService', () => ({ jobExecutionService: { executeJob: vi.fn().mockResolvedValue({ job_id: 'run' }) } }));
vi.mock('./agent', () => ({ useAgentStore: { getState: () => ({ getAgent: vi.fn() }) } }));
vi.mock('../api/workflow/TaskService', () => ({ TaskService: { getTask: vi.fn() } }));
vi.mock('../api/tools/ToolService', () => ({ ToolService: { listEnabledTools: vi.fn().mockResolvedValue([]) } }));

const node = (id: string, type: string, data: Record<string, unknown> = {}): Node => ({ id, type, data, position: { x: 0, y: 0 } });
const crewA = [node('agent', 'agentNode'), node('task', 'taskNode', { description: 'Research {topic}' })];
const crewB = [node('agent', 'agentNode'), node('task', 'taskNode', { description: 'Write a haiku' })];

function showCanvas(nodes: Node[]) {
  useBuilderCanvasStore.setState({ activeCanvasId: 'tab', canvases: [
    { id: 'tab', chatSessionId: 'conversation', nodes, edges: [], flowNodes: [], flowEdges: [], createdAt: new Date(), lastModified: new Date() },
  ] as unknown as BuilderCanvas[] });
}

const inputsOfRun = (index: number) => vi.mocked(jobExecutionService.executeJob).mock.calls[index][4] as Record<string, unknown>;

beforeEach(() => {
  vi.clearAllMocks();
  useCrewExecutionStore.setState({ inputMode: 'dialog', inputVariables: {}, pendingVariableExecution: null, trifectaAcknowledged: false });
});

it('does not carry one run\'s input variables into the next run', async () => {
  showCanvas(crewA);
  await useCrewExecutionStore.getState().handleRunClick('crew');
  expect(useCrewExecutionStore.getState().showInputVariablesDialog).toBe(true);
  await useCrewExecutionStore.getState().executeWithVariables({ topic: 'AI' });
  expect(inputsOfRun(0)).toEqual(expect.objectContaining({ topic: 'AI' }));

  showCanvas(crewB);
  await useCrewExecutionStore.getState().handleRunClick('crew');

  expect(jobExecutionService.executeJob).toHaveBeenCalledTimes(2);
  expect(inputsOfRun(1)).not.toHaveProperty('topic');
});

it('starts one run when Run is clicked twice while the first start is in flight', async () => {
  showCanvas(crewB);
  let releaseTools: (tools: never[]) => void = () => {};
  vi.mocked(ToolService.listEnabledTools).mockReturnValueOnce(new Promise(resolve => { releaseTools = resolve; }));

  const first = useCrewExecutionStore.getState().handleRunClick('crew');
  // The button can show that the run is starting.
  expect(useCrewExecutionStore.getState().isStartingRun).toBe(true);
  const second = useCrewExecutionStore.getState().handleRunClick('crew');
  releaseTools([]);
  await Promise.all([first, second]);

  expect(jobExecutionService.executeJob).toHaveBeenCalledTimes(1);
  expect(useCrewExecutionStore.getState().isStartingRun).toBe(false);

  // Once that start has finished, Run works again.
  await useCrewExecutionStore.getState().handleRunClick('crew');
  expect(jobExecutionService.executeJob).toHaveBeenCalledTimes(2);
});

it('starts nothing when the inputs dialog confirms again', async () => {
  // A crew on the store's own nodes: what a run without a pending dialog fell back to.
  useCrewExecutionStore.setState({ nodes: crewB, edges: [] });
  useCrewExecutionStore.setState({ pendingVariableExecution: { nodes: crewA, edges: [], type: 'crew' } });

  const confirm = useCrewExecutionStore.getState().executeWithVariables;
  await Promise.all([confirm({ topic: 'AI' }), confirm({ topic: 'AI' })]);
  await confirm({ topic: 'AI' });

  expect(jobExecutionService.executeJob).toHaveBeenCalledTimes(1);
  const ranNodes = vi.mocked(jobExecutionService.executeJob).mock.calls[0][0] as Node[];
  expect(ranNodes.find(n => n.type === 'taskNode')!.data.description).toBe('Research {topic}');
});
