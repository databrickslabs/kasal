/**
 * Unit tests for WorkflowDesigner component.
 *
 * Tests the node/edge sync logic that prevents infinite render loops
 * when syncing canvas state with the execution store.
 */
import { describe, it, expect, vi } from 'vitest';

/**
 * Test the node ID comparison logic used to prevent infinite loops.
 * This logic is extracted from the useEffect hooks in WorkflowDesigner.
 */
describe('WorkflowDesigner Node Sync Logic', () => {
  describe('Node ID comparison for sync prevention', () => {
    /**
     * Helper function that replicates the comparison logic used in WorkflowDesigner
     * to determine if nodes have actually changed.
     */
    const getNodeIdsString = (nodes: Array<{ id: string }>): string => {
      return nodes.map(n => n.id).sort().join(',');
    };

    it('should return same string for identical node arrays', () => {
      const nodes1 = [{ id: 'node-1' }, { id: 'node-2' }, { id: 'node-3' }];
      const nodes2 = [{ id: 'node-1' }, { id: 'node-2' }, { id: 'node-3' }];

      expect(getNodeIdsString(nodes1)).toBe(getNodeIdsString(nodes2));
    });

    it('should return same string regardless of order', () => {
      const nodes1 = [{ id: 'node-1' }, { id: 'node-2' }, { id: 'node-3' }];
      const nodes2 = [{ id: 'node-3' }, { id: 'node-1' }, { id: 'node-2' }];

      expect(getNodeIdsString(nodes1)).toBe(getNodeIdsString(nodes2));
    });

    it('should return different string when node is added', () => {
      const nodesBefore = [{ id: 'node-1' }, { id: 'node-2' }];
      const nodesAfter = [{ id: 'node-1' }, { id: 'node-2' }, { id: 'node-3' }];

      expect(getNodeIdsString(nodesBefore)).not.toBe(getNodeIdsString(nodesAfter));
    });

    it('should return different string when node is removed', () => {
      const nodesBefore = [{ id: 'node-1' }, { id: 'node-2' }, { id: 'node-3' }];
      const nodesAfter = [{ id: 'node-1' }, { id: 'node-2' }];

      expect(getNodeIdsString(nodesBefore)).not.toBe(getNodeIdsString(nodesAfter));
    });

    it('should return different string when node ID changes', () => {
      const nodesBefore = [{ id: 'node-1' }, { id: 'node-2' }];
      const nodesAfter = [{ id: 'node-1' }, { id: 'node-3' }];

      expect(getNodeIdsString(nodesBefore)).not.toBe(getNodeIdsString(nodesAfter));
    });

    it('should handle empty arrays', () => {
      const emptyNodes: Array<{ id: string }> = [];

      expect(getNodeIdsString(emptyNodes)).toBe('');
    });

    it('should handle single node', () => {
      const singleNode = [{ id: 'node-1' }];

      expect(getNodeIdsString(singleNode)).toBe('node-1');
    });
  });

  describe('Edge ID comparison for sync prevention', () => {
    /**
     * Helper function that replicates the comparison logic used in WorkflowDesigner
     * to determine if edges have actually changed.
     */
    const getEdgeIdsString = (edges: Array<{ id: string }>): string => {
      return edges.map(e => e.id).sort().join(',');
    };

    it('should return same string for identical edge arrays', () => {
      const edges1 = [{ id: 'edge-1' }, { id: 'edge-2' }];
      const edges2 = [{ id: 'edge-1' }, { id: 'edge-2' }];

      expect(getEdgeIdsString(edges1)).toBe(getEdgeIdsString(edges2));
    });

    it('should return same string regardless of order', () => {
      const edges1 = [{ id: 'edge-1' }, { id: 'edge-2' }];
      const edges2 = [{ id: 'edge-2' }, { id: 'edge-1' }];

      expect(getEdgeIdsString(edges1)).toBe(getEdgeIdsString(edges2));
    });

    it('should return different string when edge is removed', () => {
      const edgesBefore = [{ id: 'edge-1' }, { id: 'edge-2' }];
      const edgesAfter = [{ id: 'edge-1' }];

      expect(getEdgeIdsString(edgesBefore)).not.toBe(getEdgeIdsString(edgesAfter));
    });
  });

  describe('Sync prevention simulation', () => {
    /**
     * Simulates the sync prevention logic used in the useEffect hooks.
     * This ensures that calling sync multiple times with the same data
     * only triggers an update once.
     */
    it('should only call setter when IDs change', () => {
      const mockSetNodes = vi.fn();
      let prevNodeIds = '';

      const syncNodes = (nodes: Array<{ id: string }>) => {
        const currentIds = nodes.map(n => n.id).sort().join(',');
        if (currentIds !== prevNodeIds) {
          mockSetNodes(nodes);
          prevNodeIds = currentIds;
        }
      };

      const nodes = [{ id: 'node-1' }, { id: 'node-2' }];

      // First sync should call setter
      syncNodes(nodes);
      expect(mockSetNodes).toHaveBeenCalledTimes(1);

      // Same nodes (even new array reference) should NOT call setter
      syncNodes([{ id: 'node-1' }, { id: 'node-2' }]);
      expect(mockSetNodes).toHaveBeenCalledTimes(1);

      // Same nodes in different order should NOT call setter
      syncNodes([{ id: 'node-2' }, { id: 'node-1' }]);
      expect(mockSetNodes).toHaveBeenCalledTimes(1);

      // Adding a node SHOULD call setter
      syncNodes([{ id: 'node-1' }, { id: 'node-2' }, { id: 'node-3' }]);
      expect(mockSetNodes).toHaveBeenCalledTimes(2);

      // Removing a node SHOULD call setter
      syncNodes([{ id: 'node-1' }, { id: 'node-2' }]);
      expect(mockSetNodes).toHaveBeenCalledTimes(3);
    });

    it('should handle rapid successive calls without infinite loop', () => {
      const mockSetNodes = vi.fn();
      let prevNodeIds = '';
      let callCount = 0;
      const maxCalls = 100;

      const syncNodes = (nodes: Array<{ id: string }>) => {
        callCount++;
        if (callCount > maxCalls) {
          throw new Error('Infinite loop detected!');
        }

        const currentIds = nodes.map(n => n.id).sort().join(',');
        if (currentIds !== prevNodeIds) {
          mockSetNodes(nodes);
          prevNodeIds = currentIds;
        }
      };

      const nodes = [{ id: 'node-1' }];

      // Simulate many rapid calls with same data (what would happen in infinite loop)
      for (let i = 0; i < 50; i++) {
        syncNodes(nodes);
      }

      // Should only have called setter once despite 50 sync attempts
      expect(mockSetNodes).toHaveBeenCalledTimes(1);
      expect(callCount).toBe(50);
    });

    it('should handle node deletion scenario correctly', () => {
      const mockSetNodes = vi.fn();
      let prevNodeIds = '';

      const syncNodes = (nodes: Array<{ id: string }>) => {
        const currentIds = nodes.map(n => n.id).sort().join(',');
        if (currentIds !== prevNodeIds) {
          mockSetNodes(nodes);
          prevNodeIds = currentIds;
        }
      };

      // Initial state with 3 nodes
      const initialNodes = [{ id: 'crew-1' }, { id: 'crew-2' }, { id: 'crew-3' }];
      syncNodes(initialNodes);
      expect(mockSetNodes).toHaveBeenCalledTimes(1);

      // Delete crew-2 (simulating what happens when delete button is clicked)
      const afterDelete = [{ id: 'crew-1' }, { id: 'crew-3' }];
      syncNodes(afterDelete);
      expect(mockSetNodes).toHaveBeenCalledTimes(2);

      // Multiple re-renders after delete should NOT trigger additional calls
      syncNodes(afterDelete);
      syncNodes([{ id: 'crew-3' }, { id: 'crew-1' }]); // Same nodes, different order
      syncNodes(afterDelete);
      expect(mockSetNodes).toHaveBeenCalledTimes(2);
    });
  });

  describe('areFlowsVisible flag behavior', () => {
    it('should sync flow nodes only when areFlowsVisible is true', () => {
      const mockSetCrewExecutionNodes = vi.fn();
      let prevFlowNodeIds = '';
      let prevCrewNodeIds = '';

      const syncFlowNodes = (
        flowNodes: Array<{ id: string }>,
        areFlowsVisible: boolean
      ) => {
        if (areFlowsVisible) {
          const currentIds = flowNodes.map(n => n.id).sort().join(',');
          if (currentIds !== prevFlowNodeIds) {
            mockSetCrewExecutionNodes(flowNodes);
            prevFlowNodeIds = currentIds;
          }
        }
      };

      const syncCrewNodes = (
        crewNodes: Array<{ id: string }>,
        areFlowsVisible: boolean
      ) => {
        if (!areFlowsVisible) {
          const currentIds = crewNodes.map(n => n.id).sort().join(',');
          if (currentIds !== prevCrewNodeIds) {
            mockSetCrewExecutionNodes(crewNodes);
            prevCrewNodeIds = currentIds;
          }
        }
      };

      const flowNodes = [{ id: 'crew-node-1' }];
      const crewNodes = [{ id: 'agent-1' }, { id: 'task-1' }];

      // When areFlowsVisible is true, only flowNodes should sync
      syncFlowNodes(flowNodes, true);
      syncCrewNodes(crewNodes, true);
      expect(mockSetCrewExecutionNodes).toHaveBeenCalledTimes(1);
      expect(mockSetCrewExecutionNodes).toHaveBeenLastCalledWith(flowNodes);

      // Reset mock
      mockSetCrewExecutionNodes.mockClear();
      prevFlowNodeIds = '';
      prevCrewNodeIds = '';

      // When areFlowsVisible is false, only crewNodes should sync
      syncFlowNodes(flowNodes, false);
      syncCrewNodes(crewNodes, false);
      expect(mockSetCrewExecutionNodes).toHaveBeenCalledTimes(1);
      expect(mockSetCrewExecutionNodes).toHaveBeenLastCalledWith(crewNodes);
    });
  });
});
