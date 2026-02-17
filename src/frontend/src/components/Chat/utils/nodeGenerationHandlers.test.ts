import { describe, it, expect, vi, beforeEach } from 'vitest';
import { Node, Edge } from 'reactflow';
import type {
  PlanReadyData,
  AgentDetailData,
  TaskDetailData,
  EntityErrorData,
  DependenciesResolvedData,
} from '../../../hooks/global/useCrewGenerationSSE';
import {
  createCrewSkeletonHandler,
  updateAgentNodeDetail,
  updateTaskNodeDetail,
  markNodeError,
  addDependencyEdges,
  IndexNodeIdMap,
} from './nodeGenerationHandlers';

// ---------------------------------------------------------------------------
// Mocks
// ---------------------------------------------------------------------------

vi.mock('../../../api/AgentService', () => ({
  AgentService: { createAgent: vi.fn() },
}));

vi.mock('../../../api/TaskService', () => ({
  TaskService: { createTask: vi.fn() },
}));

vi.mock('../../../store/workflow', () => ({
  useWorkflowStore: { getState: vi.fn(() => ({ nodes: [], edges: [] })) },
}));

vi.mock('../../../store/uiLayout', () => ({
  useUILayoutStore: {
    getState: vi.fn(() => ({ layoutOrientation: 'horizontal' })),
  },
}));

vi.mock('../../../config/edgeConfig', () => ({
  EdgeCategory: {
    AGENT_TO_TASK: 'agent-to-task',
    TASK_TO_TASK: 'task-to-task',
  },
  getEdgeStyleConfig: vi.fn(() => ({ stroke: '#999', strokeWidth: 2 })),
}));

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Create a React-style setState mock that captures the updater callback. */
function createSetStateMock<T>() {
  const fn = vi.fn() as ReturnType<typeof vi.fn> & {
    /** Invoke the last updater callback with `currentState` and return the new state. */
    applyWith: (currentState: T) => T;
  };

  fn.applyWith = (currentState: T): T => {
    const lastCall = fn.mock.calls[fn.mock.calls.length - 1];
    const arg = lastCall[0];
    // If the argument is a function, invoke it; otherwise return the value directly
    return typeof arg === 'function' ? (arg as (prev: T) => T)(currentState) : (arg as T);
  };

  return fn;
}

// ---------------------------------------------------------------------------
// createCrewSkeletonHandler
// ---------------------------------------------------------------------------

describe('createCrewSkeletonHandler', () => {
  let setNodes: ReturnType<typeof createSetStateMock<Node[]>>;
  let setEdges: ReturnType<typeof createSetStateMock<Edge[]>>;
  let setLastExecJobId: ReturnType<typeof vi.fn>;
  let setExecJobId: ReturnType<typeof vi.fn>;

  const plan: PlanReadyData = {
    type: 'plan_ready',
    agents: [
      { name: 'Researcher', role: 'Research expert' },
      { name: 'Writer', role: 'Content writer' },
    ],
    tasks: [
      { name: 'Research task', assigned_agent: 'Researcher' },
      { name: 'Write task', assigned_agent: 'Writer', context: ['Research task'] },
    ],
  };

  beforeEach(() => {
    setNodes = createSetStateMock<Node[]>();
    setEdges = createSetStateMock<Edge[]>();
    setLastExecJobId = vi.fn();
    setExecJobId = vi.fn();
  });

  it('should create skeleton agent nodes with loading=true for each agent in the plan', () => {
    const handler = createCrewSkeletonHandler(
      setNodes,
      setEdges,
      setLastExecJobId,
      setExecJobId,
    );
    handler(plan);

    // setNodes is called with the complete array of skeleton nodes
    const nodes: Node[] = setNodes.mock.calls[0][0];
    const agentNodes = nodes.filter((n) => n.type === 'agentNode');

    expect(agentNodes).toHaveLength(2);
    agentNodes.forEach((node) => {
      expect(node.data.loading).toBe(true);
    });
    expect(agentNodes[0].data.label).toBe('Researcher');
    expect(agentNodes[1].data.label).toBe('Writer');
  });

  it('should create skeleton task nodes with loading=true for each task in the plan', () => {
    const handler = createCrewSkeletonHandler(
      setNodes,
      setEdges,
      setLastExecJobId,
      setExecJobId,
    );
    handler(plan);

    const nodes: Node[] = setNodes.mock.calls[0][0];
    const taskNodes = nodes.filter((n) => n.type === 'taskNode');

    expect(taskNodes).toHaveLength(2);
    taskNodes.forEach((node) => {
      expect(node.data.loading).toBe(true);
    });
    expect(taskNodes[0].data.label).toBe('Research task');
    expect(taskNodes[1].data.label).toBe('Write task');
  });

  it('should create agent-to-task edges based on assigned_agent in the plan', () => {
    const handler = createCrewSkeletonHandler(
      setNodes,
      setEdges,
      setLastExecJobId,
      setExecJobId,
    );
    handler(plan);

    const edges: Edge[] = setEdges.mock.calls[0][0];
    // Two agent-to-task edges (one for each task), plus one task-to-task dep edge
    const agentTaskEdges = edges.filter((e) => e.id.startsWith('edge-'));
    expect(agentTaskEdges.length).toBeGreaterThanOrEqual(2);

    // Each agent-task edge should reference an agent-skeleton source and task-skeleton target
    agentTaskEdges.forEach((edge) => {
      expect(edge.source).toContain('agent-skeleton');
      expect(edge.target).toContain('task-skeleton');
    });
  });

  it('should return an IndexNodeIdMap that maps plan indices to skeleton node IDs', () => {
    const handler = createCrewSkeletonHandler(
      setNodes,
      setEdges,
      setLastExecJobId,
      setExecJobId,
    );
    const indexMap = handler(plan);

    expect(indexMap.agents.size).toBe(2);
    expect(indexMap.tasks.size).toBe(2);

    // Agent 0 and 1 should have skeleton IDs
    expect(indexMap.agents.get(0)).toContain('agent-skeleton-0');
    expect(indexMap.agents.get(1)).toContain('agent-skeleton-1');

    // Task 0 and 1 should have skeleton IDs
    expect(indexMap.tasks.get(0)).toContain('task-skeleton-0');
    expect(indexMap.tasks.get(1)).toContain('task-skeleton-1');
  });
});

// ---------------------------------------------------------------------------
// updateAgentNodeDetail
// ---------------------------------------------------------------------------

describe('updateAgentNodeDetail', () => {
  let setNodes: ReturnType<typeof createSetStateMock<Node[]>>;
  let setEdges: ReturnType<typeof createSetStateMock<Edge[]>>;

  const skeletonId = 'agent-skeleton-0-1000';
  const indexMap: IndexNodeIdMap = {
    agents: new Map([[0, skeletonId]]),
    tasks: new Map(),
  };

  const skeletonNode: Node = {
    id: skeletonId,
    type: 'agentNode',
    position: { x: 100, y: 100 },
    data: { label: 'Placeholder', loading: true },
  };

  const detailData: AgentDetailData = {
    type: 'agent_detail',
    index: 0,
    agent: {
      id: 'db-agent-42',
      name: 'Real Agent',
      role: 'Analyst',
      goal: 'Analyze data',
      backstory: 'A seasoned analyst',
      llm: 'gpt-4',
      tools: ['search'],
    },
  };

  beforeEach(() => {
    setNodes = createSetStateMock<Node[]>();
    setEdges = createSetStateMock<Edge[]>();
    // Reset the map for each test since updateAgentNodeDetail mutates it
    indexMap.agents.set(0, skeletonId);
  });

  it('should replace skeleton data with real agent data and set loading=false', () => {
    const handler = updateAgentNodeDetail(setNodes, setEdges, indexMap, 'default-model');
    handler(detailData);

    const updatedNodes = setNodes.applyWith([skeletonNode]);
    const updatedNode = updatedNodes.find((n: Node) => n.id === 'agent-db-agent-42');

    expect(updatedNode).toBeDefined();
    expect(updatedNode!.data.loading).toBe(false);
    expect(updatedNode!.data.label).toBe('Real Agent');
    expect(updatedNode!.data.role).toBe('Analyst');
    expect(updatedNode!.data.goal).toBe('Analyze data');
    expect(updatedNode!.data.agentId).toBe('db-agent-42');
  });

  it('should update the node ID from skeleton to agent-{dbId}', () => {
    const handler = updateAgentNodeDetail(setNodes, setEdges, indexMap, 'default-model');
    handler(detailData);

    const updatedNodes = setNodes.applyWith([skeletonNode]);
    const nodeIds = updatedNodes.map((n: Node) => n.id);

    expect(nodeIds).toContain('agent-db-agent-42');
    expect(nodeIds).not.toContain(skeletonId);
  });

  it('should be a no-op when the index is not present in the map', () => {
    const handler = updateAgentNodeDetail(setNodes, setEdges, indexMap, 'default-model');
    handler({ ...detailData, index: 99 });

    // setNodes should not be called when the skeleton is not found
    expect(setNodes).not.toHaveBeenCalled();
  });
});

// ---------------------------------------------------------------------------
// updateTaskNodeDetail
// ---------------------------------------------------------------------------

describe('updateTaskNodeDetail', () => {
  let setNodes: ReturnType<typeof createSetStateMock<Node[]>>;
  let setEdges: ReturnType<typeof createSetStateMock<Edge[]>>;

  const skeletonId = 'task-skeleton-0-1000';
  const indexMap: IndexNodeIdMap = {
    agents: new Map(),
    tasks: new Map([[0, skeletonId]]),
  };

  const skeletonNode: Node = {
    id: skeletonId,
    type: 'taskNode',
    position: { x: 400, y: 100 },
    data: { label: 'Loading task...', loading: true },
  };

  const detailData: TaskDetailData = {
    type: 'task_detail',
    index: 0,
    task: {
      id: 'db-task-7',
      name: 'Real Task',
      description: 'Detailed task description',
      expected_output: 'A report',
      tools: ['fileReader'],
      context: [],
    },
  };

  beforeEach(() => {
    setNodes = createSetStateMock<Node[]>();
    setEdges = createSetStateMock<Edge[]>();
    indexMap.tasks.set(0, skeletonId);
  });

  it('should replace skeleton data with real task data and set loading=false', () => {
    const handler = updateTaskNodeDetail(setNodes, setEdges, indexMap);
    handler(detailData);

    const updatedNodes = setNodes.applyWith([skeletonNode]);
    const updatedNode = updatedNodes.find((n: Node) => n.id === 'task-db-task-7');

    expect(updatedNode).toBeDefined();
    expect(updatedNode!.data.loading).toBe(false);
    expect(updatedNode!.data.label).toBe('Real Task');
    expect(updatedNode!.data.description).toBe('Detailed task description');
    expect(updatedNode!.data.taskId).toBe('db-task-7');
  });

  it('should update the node ID from skeleton to task-{dbId}', () => {
    const handler = updateTaskNodeDetail(setNodes, setEdges, indexMap);
    handler(detailData);

    const updatedNodes = setNodes.applyWith([skeletonNode]);
    const nodeIds = updatedNodes.map((n: Node) => n.id);

    expect(nodeIds).toContain('task-db-task-7');
    expect(nodeIds).not.toContain(skeletonId);
  });
});

// ---------------------------------------------------------------------------
// markNodeError
// ---------------------------------------------------------------------------

describe('markNodeError', () => {
  let setNodes: ReturnType<typeof createSetStateMock<Node[]>>;

  const skeletonId = 'agent-skeleton-0-1000';
  const indexMap: IndexNodeIdMap = {
    agents: new Map([[0, skeletonId]]),
    tasks: new Map(),
  };

  const skeletonNode: Node = {
    id: skeletonId,
    type: 'agentNode',
    position: { x: 100, y: 100 },
    data: { label: 'Broken Agent', loading: true },
  };

  const errorData: EntityErrorData = {
    type: 'entity_error',
    index: 0,
    entity_type: 'agent',
    name: 'Broken Agent',
    error: 'LLM timeout',
  };

  beforeEach(() => {
    setNodes = createSetStateMock<Node[]>();
    indexMap.agents.set(0, skeletonId);
  });

  it('should set error=true, loading=false, and errorMessage on the target node', () => {
    const handler = markNodeError(setNodes, indexMap);
    handler(errorData);

    const updatedNodes = setNodes.applyWith([skeletonNode]);
    const errorNode = updatedNodes.find((n: Node) => n.id === skeletonId);

    expect(errorNode).toBeDefined();
    expect(errorNode!.data.error).toBe(true);
    expect(errorNode!.data.loading).toBe(false);
    expect(errorNode!.data.errorMessage).toBe('LLM timeout');
  });

  it('should be a no-op when the index is not present in the map', () => {
    const handler = markNodeError(setNodes, indexMap);
    handler({ ...errorData, index: 99 });

    // setNodes should not be called when there is no matching node
    expect(setNodes).not.toHaveBeenCalled();
  });
});

// ---------------------------------------------------------------------------
// addDependencyEdges
// ---------------------------------------------------------------------------

describe('addDependencyEdges', () => {
  let setNodes: ReturnType<typeof createSetStateMock<Node[]>>;
  let setEdges: ReturnType<typeof createSetStateMock<Edge[]>>;

  const existingNodes: Node[] = [
    {
      id: 'task-100',
      type: 'taskNode',
      position: { x: 400, y: 100 },
      data: { label: 'Source Task', taskId: 'dep-1' },
    },
    {
      id: 'task-200',
      type: 'taskNode',
      position: { x: 400, y: 250 },
      data: { label: 'Target Task', taskId: 'target-1' },
    },
  ];

  const depData: DependenciesResolvedData = {
    type: 'dependencies_resolved',
    task_id: 'target-1',
    task_name: 'Target Task',
    context: ['dep-1'],
  };

  beforeEach(() => {
    setNodes = createSetStateMock<Node[]>();
    setEdges = createSetStateMock<Edge[]>();
  });

  it('should add real dependency edges using DB task IDs', () => {
    const handler = addDependencyEdges(setNodes, setEdges);
    handler(depData);

    // addDependencyEdges calls setNodes with a function to find the nodes,
    // then within that function it calls setEdges.
    // We apply the setNodes updater with existing nodes.
    const nodesResult = setNodes.applyWith(existingNodes);
    // The function should return nodes unchanged
    expect(nodesResult).toEqual(existingNodes);

    // setEdges should have been invoked inside the setNodes callback
    expect(setEdges).toHaveBeenCalled();
    const updatedEdges = setEdges.applyWith([]);

    expect(updatedEdges).toHaveLength(1);
    expect(updatedEdges[0].source).toBe('task-100');
    expect(updatedEdges[0].target).toBe('task-200');
    expect(updatedEdges[0].animated).toBe(true);
  });
});
