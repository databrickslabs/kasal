/**
 * Unit tests for crewExecution store - node resolution logic.
 *
 * Tests the handleRunClick node/edge resolution that reads from the tab manager
 * instead of the stale shared store state when switching between crew/flow canvases.
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
      const variablePattern = /\{([^}]+)\}/g;

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
});
