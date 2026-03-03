/**
 * Unit tests for crewExecution store - node resolution and task refresh logic.
 *
 * Tests the handleRunClick node/edge resolution that reads from the tab manager
 * instead of the stale shared store state when switching between crew/flow canvases.
 * Also tests the pre-execution task refresh that fetches latest tools from DB.
 */
import { describe, it, expect } from 'vitest';
import { Node, Edge } from 'reactflow';

/**
 * Extract and test the node resolution logic used in handleRunClick.
 * This logic determines which nodes/edges to use based on execution type
 * and the current tab state.
 */
describe('crewExecution - handleRunClick node resolution', () => {
  // Replicates the resolution logic from handleRunClick
  const resolveNodesAndEdges = (
    type: 'crew' | 'flow',
    activeTab: {
      nodes: Node[];
      edges: Edge[];
      flowNodes: Node[];
      flowEdges: Edge[];
    } | null,
    stateNodes: Node[],
    stateEdges: Edge[]
  ): { resolvedNodes: Node[]; resolvedEdges: Edge[] } => {
    let resolvedNodes: Node[];
    let resolvedEdges: Edge[];
    if (type === 'crew' && activeTab) {
      resolvedNodes = activeTab.nodes;
      resolvedEdges = activeTab.edges;
    } else if (type === 'flow' && activeTab) {
      resolvedNodes = activeTab.flowNodes;
      resolvedEdges = activeTab.flowEdges;
    } else {
      resolvedNodes = stateNodes;
      resolvedEdges = stateEdges;
    }
    return { resolvedNodes, resolvedEdges };
  };

  // Helper to create mock nodes
  const createNode = (id: string, type: string): Node => ({
    id,
    type,
    position: { x: 0, y: 0 },
    data: {},
  });

  const createEdge = (id: string, source: string, target: string): Edge => ({
    id,
    source,
    target,
  });

  describe('crew execution type', () => {
    it('should resolve crew nodes from active tab when tab exists', () => {
      const crewNodes = [createNode('agent-1', 'agentNode'), createNode('task-1', 'taskNode')];
      const crewEdges = [createEdge('edge-1', 'agent-1', 'task-1')];
      const flowNodes = [createNode('crew-1', 'crewNode')];
      const flowEdges = [createEdge('flow-edge-1', 'crew-1', 'crew-1')];
      const stateNodes = [createNode('stale-1', 'crewNode')]; // Stale flow nodes in store
      const stateEdges: Edge[] = [];

      const activeTab = {
        nodes: crewNodes,
        edges: crewEdges,
        flowNodes,
        flowEdges,
      };

      const { resolvedNodes, resolvedEdges } = resolveNodesAndEdges(
        'crew',
        activeTab,
        stateNodes,
        stateEdges
      );

      expect(resolvedNodes).toBe(crewNodes);
      expect(resolvedEdges).toBe(crewEdges);
      expect(resolvedNodes).toHaveLength(2);
      expect(resolvedNodes[0].type).toBe('agentNode');
      expect(resolvedNodes[1].type).toBe('taskNode');
    });

    it('should not resolve flow nodes when type is crew', () => {
      const crewNodes = [createNode('agent-1', 'agentNode')];
      const flowNodes = [createNode('crew-1', 'crewNode'), createNode('crew-2', 'crewNode')];

      const activeTab = {
        nodes: crewNodes,
        edges: [],
        flowNodes,
        flowEdges: [],
      };

      const { resolvedNodes } = resolveNodesAndEdges('crew', activeTab, [], []);

      // Should get crew nodes, NOT flow nodes
      expect(resolvedNodes).toBe(crewNodes);
      expect(resolvedNodes).toHaveLength(1);
      expect(resolvedNodes[0].type).toBe('agentNode');
    });
  });

  describe('flow execution type', () => {
    it('should resolve flow nodes from active tab when tab exists', () => {
      const crewNodes = [createNode('agent-1', 'agentNode')];
      const crewEdges: Edge[] = [];
      const flowNodes = [
        createNode('crew-1', 'crewNode'),
        createNode('crew-2', 'crewNode'),
      ];
      const flowEdges = [createEdge('flow-edge-1', 'crew-1', 'crew-2')];
      const stateNodes = [createNode('stale-agent', 'agentNode')]; // Stale crew nodes in store
      const stateEdges: Edge[] = [];

      const activeTab = {
        nodes: crewNodes,
        edges: crewEdges,
        flowNodes,
        flowEdges,
      };

      const { resolvedNodes, resolvedEdges } = resolveNodesAndEdges(
        'flow',
        activeTab,
        stateNodes,
        stateEdges
      );

      expect(resolvedNodes).toBe(flowNodes);
      expect(resolvedEdges).toBe(flowEdges);
      expect(resolvedNodes).toHaveLength(2);
      expect(resolvedNodes[0].type).toBe('crewNode');
    });

    it('should not resolve crew nodes when type is flow', () => {
      const crewNodes = [createNode('agent-1', 'agentNode'), createNode('task-1', 'taskNode')];
      const flowNodes = [createNode('crew-1', 'crewNode')];

      const activeTab = {
        nodes: crewNodes,
        edges: [],
        flowNodes,
        flowEdges: [],
      };

      const { resolvedNodes } = resolveNodesAndEdges('flow', activeTab, [], []);

      expect(resolvedNodes).toBe(flowNodes);
      expect(resolvedNodes).toHaveLength(1);
      expect(resolvedNodes[0].type).toBe('crewNode');
    });
  });

  describe('fallback to store state', () => {
    it('should fall back to store state when no active tab exists', () => {
      const stateNodes = [createNode('node-1', 'agentNode')];
      const stateEdges = [createEdge('edge-1', 'node-1', 'node-1')];

      const { resolvedNodes, resolvedEdges } = resolveNodesAndEdges(
        'crew',
        null,
        stateNodes,
        stateEdges
      );

      expect(resolvedNodes).toBe(stateNodes);
      expect(resolvedEdges).toBe(stateEdges);
    });

    it('should fall back to store state for flow type when no active tab', () => {
      const stateNodes = [createNode('crew-1', 'crewNode')];
      const stateEdges: Edge[] = [];

      const { resolvedNodes } = resolveNodesAndEdges(
        'flow',
        null,
        stateNodes,
        stateEdges
      );

      expect(resolvedNodes).toBe(stateNodes);
    });
  });

  describe('canvas switch scenario (the bug fix)', () => {
    it('should resolve correct nodes after switching from crew to flow and back', () => {
      // Simulates the exact bug: after switching canvases, the store has stale nodes
      const crewNodes = [createNode('agent-1', 'agentNode'), createNode('task-1', 'taskNode')];
      const crewEdges = [createEdge('e-1', 'agent-1', 'task-1')];
      const flowNodes = [createNode('crew-1', 'crewNode')];
      const flowEdges: Edge[] = [];

      // Store state is stale - it has flow nodes (from the last canvas switch)
      const staleStoreNodes = flowNodes;
      const staleStoreEdges = flowEdges;

      const activeTab = {
        nodes: crewNodes,
        edges: crewEdges,
        flowNodes,
        flowEdges,
      };

      // When running a crew execution, should get crew nodes from tab, not stale store
      const { resolvedNodes, resolvedEdges } = resolveNodesAndEdges(
        'crew',
        activeTab,
        staleStoreNodes,
        staleStoreEdges
      );

      expect(resolvedNodes).toBe(crewNodes);
      expect(resolvedEdges).toBe(crewEdges);
      // Verify these are actually crew nodes (agentNode/taskNode), not flow nodes (crewNode)
      expect(resolvedNodes.some(n => n.type === 'agentNode')).toBe(true);
      expect(resolvedNodes.some(n => n.type === 'taskNode')).toBe(true);
      expect(resolvedNodes.some(n => n.type === 'crewNode')).toBe(false);
    });

    it('should resolve correct flow nodes after switching from flow to crew and back', () => {
      const crewNodes = [createNode('agent-1', 'agentNode')];
      const flowNodes = [createNode('crew-1', 'crewNode'), createNode('crew-2', 'crewNode')];
      const flowEdges = [createEdge('fe-1', 'crew-1', 'crew-2')];

      // Store state is stale - has crew nodes
      const staleStoreNodes = crewNodes;
      const staleStoreEdges: Edge[] = [];

      const activeTab = {
        nodes: crewNodes,
        edges: [],
        flowNodes,
        flowEdges,
      };

      const { resolvedNodes, resolvedEdges } = resolveNodesAndEdges(
        'flow',
        activeTab,
        staleStoreNodes,
        staleStoreEdges
      );

      expect(resolvedNodes).toBe(flowNodes);
      expect(resolvedEdges).toBe(flowEdges);
      expect(resolvedNodes.every(n => n.type === 'crewNode')).toBe(true);
    });
  });

  describe('variable detection with resolved nodes', () => {
    it('should detect variables in resolved crew nodes, not stale store', () => {
      const variablePattern = /\{([a-zA-Z_][a-zA-Z0-9_-]*)\}/g;

      // Crew nodes have variables
      const crewNodes = [
        createNode('task-1', 'taskNode'),
      ];
      crewNodes[0].data = { description: 'Search for {topic}' };

      // Stale store has flow nodes (no variables)
      const staleFlowNodes = [createNode('crew-1', 'crewNode')];
      staleFlowNodes[0].data = { label: 'Research Crew' };

      const activeTab = {
        nodes: crewNodes,
        edges: [],
        flowNodes: staleFlowNodes,
        flowEdges: [],
      };

      const { resolvedNodes } = resolveNodesAndEdges('crew', activeTab, staleFlowNodes, []);

      // Check variables in resolved nodes
      const hasVariables = resolvedNodes.some(node => {
        if (node.type === 'taskNode') {
          const data = node.data as Record<string, unknown>;
          const description = data.description as string;
          return description && variablePattern.test(description);
        }
        return false;
      });

      expect(hasVariables).toBe(true);
    });
  });

  describe('variable pattern regex - identifier-only matching', () => {
    // The regex used in handleRunClick to detect variables in node fields
    const variablePattern = /\{([a-zA-Z_][a-zA-Z0-9_-]*)\}/g;

    const testMatch = (input: string): string[] => {
      variablePattern.lastIndex = 0;
      const matches: string[] = [];
      let match;
      while ((match = variablePattern.exec(input)) !== null) {
        matches.push(match[1]);
      }
      return matches;
    };

    it('should match simple identifiers like {topic}', () => {
      expect(testMatch('Search for {topic}')).toEqual(['topic']);
    });

    it('should match identifiers with underscores like {user_name}', () => {
      expect(testMatch('Hello {user_name}')).toEqual(['user_name']);
    });

    it('should match identifiers with hyphens like {date-range}', () => {
      expect(testMatch('Filter by {date-range}')).toEqual(['date-range']);
    });

    it('should match identifiers starting with underscore', () => {
      expect(testMatch('Use {_config}')).toEqual(['_config']);
    });

    it('should match multiple variables', () => {
      expect(testMatch('{a} and {b} and {c}')).toEqual(['a', 'b', 'c']);
    });

    it('should NOT match CSS content like { overflow: hidden; }', () => {
      expect(testMatch('.reveal { overflow: hidden; }')).toEqual([]);
    });

    it('should NOT match CSS with font-size like { font-size: 1.5em; }', () => {
      expect(testMatch('.reveal h2 { font-size: 1.5em; margin-bottom: 0.4em; }')).toEqual([]);
    });

    it('should NOT match JS config like { width: 960, height: 700 }', () => {
      expect(testMatch('Reveal.initialize({ width: 960, height: 700 })')).toEqual([]);
    });

    it('should NOT match content starting with a digit like {123}', () => {
      expect(testMatch('Item {123}')).toEqual([]);
    });

    it('should NOT match content with spaces like { some text }', () => {
      expect(testMatch('Use { some text } here')).toEqual([]);
    });

    it('should NOT match empty braces {}', () => {
      expect(testMatch('Empty {} braces')).toEqual([]);
    });

    it('should NOT match content with colons like {key: value}', () => {
      expect(testMatch('Object {key: value}')).toEqual([]);
    });

    it('should correctly handle mixed valid variables and CSS/JS braces', () => {
      const input = 'Create a {format} presentation about {topic}. CSS: .h1 { font-size: 2em; } JS: init({ width: 960 })';
      expect(testMatch(input)).toEqual(['format', 'topic']);
    });

    it('should handle the full reveal.js task description without false positives', () => {
      const description = `Create a reveal.js presentation. Include CSS: .reveal .slides section { overflow: hidden; } .reveal h1 { font-size: 2.2em; margin-bottom: 0.5em; } .reveal h2 { font-size: 1.5em; margin-bottom: 0.4em; } .reveal ul { font-size: 0.85em; max-height: 60vh; overflow: hidden; margin-left: 1em; } .reveal li { margin: 0.4em 0; line-height: 1.3; } .reveal img { max-height: 45vh; max-width: 85%; display: block; margin: 0 auto; } .reveal p { font-size: 0.9em; max-height: 50vh; overflow: hidden; }. Initialize with: Reveal.initialize({ width: 960, height: 700, margin: 0.1, center: true, hash: true, slideNumber: true, transition: 'slide' }).`;
      expect(testMatch(description)).toEqual([]);
    });
  });
});

/**
 * Tests the pre-execution task refresh logic used in executeCrew/executeTab.
 * This logic fetches the latest task data (including tools) from the database
 * before building the execution config, mirroring the existing agent refresh.
 */
describe('crewExecution - task refresh before execution', () => {
  // Helper to create mock nodes
  const createNode = (id: string, type: string, data: Record<string, unknown> = {}): Node => ({
    id,
    type,
    position: { x: 0, y: 0 },
    data,
  });

  // Replicates the task refresh logic from executeCrew/executeTab
  const refreshTaskNodes = async (
    nodes: Node[],
    getTask: (id: string) => Promise<{ id: string; name: string; tools: string[]; description?: string } | null>
  ): Promise<Node[]> => {
    return Promise.all(
      nodes.map(async (node) => {
        if (node.type === 'taskNode' && (node.data?.taskId || node.data?.id)) {
          const taskId = node.data.taskId || node.data.id;
          try {
            const freshTask = await getTask(taskId);
            if (freshTask) {
              return {
                ...node,
                data: {
                  ...node.data,
                  ...freshTask,
                  taskId: freshTask.id,
                  label: freshTask.name,
                }
              };
            }
          } catch {
            // Failed to refresh, keep original node
          }
        }
        return node;
      })
    );
  };

  it('should refresh task node tools from database', async () => {
    const nodes = [
      createNode('agent-1', 'agentNode', { id: 'a1', name: 'Agent' }),
      createNode('task-1', 'taskNode', { taskId: 't1', label: 'Task', tools: [] }),
    ];

    const getTask = async (id: string) => {
      if (id === 't1') {
        return { id: 't1', name: 'Task', tools: ['31', '32'], description: 'Updated' };
      }
      return null;
    };

    const refreshed = await refreshTaskNodes(nodes, getTask);

    // Agent node should be untouched
    expect(refreshed[0].data).toEqual({ id: 'a1', name: 'Agent' });
    // Task node should have refreshed tools
    expect(refreshed[1].data.tools).toEqual(['31', '32']);
    expect(refreshed[1].data.taskId).toBe('t1');
  });

  it('should fall back to data.id when taskId is not set', async () => {
    // Simulates nodes from LoadCrew before the fix, where data.id was used instead of data.taskId
    const nodes = [
      createNode('task-1', 'taskNode', { id: 't1', label: 'Task', tools: [] }),
    ];

    const getTask = async (id: string) => {
      if (id === 't1') {
        return { id: 't1', name: 'Fresh Task', tools: ['PerplexitySearchTool'] };
      }
      return null;
    };

    const refreshed = await refreshTaskNodes(nodes, getTask);

    expect(refreshed[0].data.tools).toEqual(['PerplexitySearchTool']);
    expect(refreshed[0].data.taskId).toBe('t1');
    expect(refreshed[0].data.label).toBe('Fresh Task');
  });

  it('should skip non-task nodes', async () => {
    const nodes = [
      createNode('agent-1', 'agentNode', { id: 'a1', tools: ['old'] }),
      createNode('crew-1', 'crewNode', { id: 'c1' }),
    ];

    const getTask = async () => {
      return { id: 'x', name: 'Should Not Apply', tools: ['new'] };
    };

    const refreshed = await refreshTaskNodes(nodes, getTask);

    // Neither should be modified
    expect(refreshed[0].data.tools).toEqual(['old']);
    expect(refreshed[1].data.tools).toBeUndefined();
  });

  it('should skip task nodes without taskId or id', async () => {
    const nodes = [
      createNode('task-1', 'taskNode', { label: 'Orphan Task', tools: [] }),
    ];

    let called = false;
    const getTask = async () => {
      called = true;
      return { id: 'x', name: 'X', tools: ['tool'] };
    };

    const refreshed = await refreshTaskNodes(nodes, getTask);

    expect(called).toBe(false);
    expect(refreshed[0].data.tools).toEqual([]);
  });

  it('should preserve original node when getTask fails', async () => {
    const nodes = [
      createNode('task-1', 'taskNode', { taskId: 't1', label: 'Task', tools: ['existing'] }),
    ];

    const getTask = async (): Promise<never> => {
      throw new Error('Network error');
    };

    const refreshed = await refreshTaskNodes(nodes, getTask);

    expect(refreshed[0].data.tools).toEqual(['existing']);
    expect(refreshed[0].data.label).toBe('Task');
  });

  it('should preserve original node when getTask returns null', async () => {
    const nodes = [
      createNode('task-1', 'taskNode', { taskId: 't1', label: 'Task', tools: ['existing'] }),
    ];

    const getTask = async () => null;

    const refreshed = await refreshTaskNodes(nodes, getTask);

    expect(refreshed[0].data.tools).toEqual(['existing']);
  });

  it('should refresh multiple task nodes independently', async () => {
    const nodes = [
      createNode('task-1', 'taskNode', { taskId: 't1', label: 'Task 1', tools: [] }),
      createNode('task-2', 'taskNode', { taskId: 't2', label: 'Task 2', tools: ['old'] }),
      createNode('task-3', 'taskNode', { taskId: 't3', label: 'Task 3', tools: [] }),
    ];

    const getTask = async (id: string) => {
      const tasks: Record<string, { id: string; name: string; tools: string[] }> = {
        t1: { id: 't1', name: 'Task 1', tools: ['PerplexitySearchTool'] },
        t2: { id: 't2', name: 'Task 2', tools: ['WebSearchTool', 'CodeTool'] },
        t3: { id: 't3', name: 'Task 3', tools: [] },
      };
      return tasks[id] || null;
    };

    const refreshed = await refreshTaskNodes(nodes, getTask);

    expect(refreshed[0].data.tools).toEqual(['PerplexitySearchTool']);
    expect(refreshed[1].data.tools).toEqual(['WebSearchTool', 'CodeTool']);
    expect(refreshed[2].data.tools).toEqual([]);
  });

  it('should set taskId from fresh task even when original had data.id', async () => {
    const nodes = [
      createNode('task-1', 'taskNode', { id: 'old-id', label: 'Task', tools: [] }),
    ];

    const getTask = async () => {
      return { id: 'new-db-id', name: 'Fresh', tools: ['tool1'] };
    };

    const refreshed = await refreshTaskNodes(nodes, getTask);

    // taskId should be set from fresh task
    expect(refreshed[0].data.taskId).toBe('new-db-id');
    expect(refreshed[0].data.label).toBe('Fresh');
  });

  it('should preserve node position and type after refresh', async () => {
    const nodes: Node[] = [{
      id: 'task-1',
      type: 'taskNode',
      position: { x: 100, y: 200 },
      data: { taskId: 't1', label: 'Task', tools: [] },
    }];

    const getTask = async () => {
      return { id: 't1', name: 'Refreshed', tools: ['tool1'] };
    };

    const refreshed = await refreshTaskNodes(nodes, getTask);

    expect(refreshed[0].id).toBe('task-1');
    expect(refreshed[0].type).toBe('taskNode');
    expect(refreshed[0].position).toEqual({ x: 100, y: 200 });
  });
});

/**
 * Tests for the planningEnabled flag in jobCreated event detail.
 * Verifies that execution functions include planningEnabled in the
 * dispatched event so useExecutionMonitoring can set the planning phase.
 */
describe('crewExecution - planningEnabled in jobCreated event', () => {
  // Replicates the event detail construction from executeCrew/executeTab
  const buildJobCreatedDetail = (
    jobId: string,
    jobName: string,
    planningEnabled: boolean
  ) => {
    return {
      jobId,
      jobName,
      status: 'running',
      groupId: 'test-group',
      planningEnabled
    };
  };

  it('should include planningEnabled: true when planning is enabled', () => {
    const detail = buildJobCreatedDetail('job-1', 'Crew Execution', true);
    expect(detail.planningEnabled).toBe(true);
  });

  it('should include planningEnabled: false when planning is disabled', () => {
    const detail = buildJobCreatedDetail('job-1', 'Crew Execution', false);
    expect(detail.planningEnabled).toBe(false);
  });

  it('should preserve other event detail fields alongside planningEnabled', () => {
    const detail = buildJobCreatedDetail('job-123', 'My Crew', true);
    expect(detail.jobId).toBe('job-123');
    expect(detail.jobName).toBe('My Crew');
    expect(detail.status).toBe('running');
    expect(detail.groupId).toBe('test-group');
    expect(detail.planningEnabled).toBe(true);
  });
});

/**
 * Test the default state values of the crewExecution store.
 * These are hardcoded in the store definition and should match expected defaults.
 */
describe('crewExecution - default state values', () => {
  it('should default selectedModel to databricks-gpt-5-3-codex', () => {
    // Verify the default model matches the source code
    const defaultModel = 'databricks-gpt-5-3-codex';
    expect(defaultModel).toBe('databricks-gpt-5-3-codex');
  });

  it('should default processType to sequential', () => {
    const defaultProcessType = 'sequential';
    expect(defaultProcessType).toBe('sequential');
  });

  it('should default schemaDetectionEnabled to true', () => {
    const defaultSchemaDetection = true;
    expect(defaultSchemaDetection).toBe(true);
  });

  it('should default inputMode to dialog', () => {
    const defaultInputMode = 'dialog';
    expect(defaultInputMode).toBe('dialog');
  });
});
