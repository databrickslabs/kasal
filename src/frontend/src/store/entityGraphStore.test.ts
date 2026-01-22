import { describe, it, expect, vi, beforeEach } from 'vitest';
import { act, renderHook } from '@testing-library/react';

// Mock ForceGraph2D
vi.mock('force-graph', () => ({
  default: vi.fn(() => vi.fn(() => ({
    backgroundColor: vi.fn().mockReturnThis(),
    nodeId: vi.fn().mockReturnThis(),
    nodeLabel: vi.fn().mockReturnThis(),
    nodeCanvasObject: vi.fn().mockReturnThis(),
    linkWidth: vi.fn().mockReturnThis(),
    linkColor: vi.fn().mockReturnThis(),
    linkDirectionalArrowLength: vi.fn().mockReturnThis(),
    linkDirectionalArrowRelPos: vi.fn().mockReturnThis(),
    linkCurvature: vi.fn().mockReturnThis(),
    onNodeClick: vi.fn().mockReturnThis(),
    onBackgroundClick: vi.fn().mockReturnThis(),
    d3Force: vi.fn().mockReturnValue({
      strength: vi.fn().mockReturnThis(),
      distance: vi.fn().mockReturnThis(),
      radius: vi.fn().mockReturnValue({ strength: vi.fn() }),
    }),
    graphData: vi.fn(),
    zoomToFit: vi.fn(),
    zoom: vi.fn().mockReturnValue(1),
    width: vi.fn().mockReturnValue(800),
    height: vi.fn().mockReturnValue(600),
    d3ReheatSimulation: vi.fn(),
    _destructor: vi.fn(),
  }))),
}));

// Mock d3-force
vi.mock('d3-force', () => ({
  forceX: vi.fn().mockReturnValue({ strength: vi.fn().mockReturnThis() }),
  forceY: vi.fn().mockReturnValue({ strength: vi.fn().mockReturnThis() }),
}));

// Import after mocks
import useEntityGraphStore from './entityGraphStore';

describe('entityGraphStore', () => {
  beforeEach(() => {
    // Reset store state between tests
    useEntityGraphStore.setState({
      graphInstance: null,
      graphData: { nodes: [], links: [] },
      filteredGraphData: { nodes: [], links: [] },
      loading: false,
      error: null,
      focusedNodeId: null,
      selectedNode: null,
      showInferredNodes: true,
      deduplicateNodes: false,
      showOrphanedNodes: false,
      forceStrength: -300,
      linkDistance: 100,
      linkCurvature: 0,
      centerForce: 0.3,
    });
  });

  describe('initial state', () => {
    it('has correct default values', () => {
      const state = useEntityGraphStore.getState();

      expect(state.selectedNode).toBeNull();
      expect(state.graphData).toEqual({ nodes: [], links: [] });
      expect(state.forceStrength).toBe(-300);
      expect(state.linkDistance).toBe(100);
      expect(state.centerForce).toBe(0.3);
      expect(state.loading).toBe(false);
      expect(state.error).toBeNull();
    });
  });

  describe('setSelectedNode', () => {
    it('updates selected node', () => {
      const { result } = renderHook(() => useEntityGraphStore());

      const testNode = {
        id: 'node-1',
        name: 'Test Node',
        type: 'agent',
        attributes: {},
        color: '#ff0000',
      };

      act(() => {
        result.current.setSelectedNode(testNode);
      });

      expect(result.current.selectedNode).toEqual(testNode);
    });

    it('clears selected node when set to null', () => {
      const { result } = renderHook(() => useEntityGraphStore());

      const testNode = {
        id: 'node-1',
        name: 'Test Node',
        type: 'agent',
        attributes: {},
        color: '#ff0000',
      };

      act(() => {
        result.current.setSelectedNode(testNode);
      });

      act(() => {
        result.current.setSelectedNode(null);
      });

      expect(result.current.selectedNode).toBeNull();
    });
  });

  describe('updateForceParameters', () => {
    it('updates force parameters', () => {
      const { result } = renderHook(() => useEntityGraphStore());

      act(() => {
        result.current.updateForceParameters(-500, 150, 0.5);
      });

      expect(result.current.forceStrength).toBe(-500);
      expect(result.current.linkDistance).toBe(150);
      expect(result.current.centerForce).toBe(0.5);
    });

    it('keeps previous centerForce if not provided', () => {
      const { result } = renderHook(() => useEntityGraphStore());

      act(() => {
        result.current.updateForceParameters(-400, 120);
      });

      expect(result.current.forceStrength).toBe(-400);
      expect(result.current.linkDistance).toBe(120);
      expect(result.current.centerForce).toBe(0.3); // Default value
    });
  });

  describe('setLoading and setError', () => {
    it('updates loading state', () => {
      const { result } = renderHook(() => useEntityGraphStore());

      act(() => {
        result.current.setLoading(true);
      });

      expect(result.current.loading).toBe(true);

      act(() => {
        result.current.setLoading(false);
      });

      expect(result.current.loading).toBe(false);
    });

    it('updates error state', () => {
      const { result } = renderHook(() => useEntityGraphStore());

      act(() => {
        result.current.setError('Test error');
      });

      expect(result.current.error).toBe('Test error');

      act(() => {
        result.current.setError(null);
      });

      expect(result.current.error).toBeNull();
    });
  });

  describe('toggles', () => {
    it('toggles inferred nodes', () => {
      const { result } = renderHook(() => useEntityGraphStore());

      expect(result.current.showInferredNodes).toBe(true);

      act(() => {
        result.current.toggleInferredNodes();
      });

      expect(result.current.showInferredNodes).toBe(false);
    });

    it('toggles deduplication', () => {
      const { result } = renderHook(() => useEntityGraphStore());

      expect(result.current.deduplicateNodes).toBe(false);

      act(() => {
        result.current.toggleDeduplication();
      });

      expect(result.current.deduplicateNodes).toBe(true);
    });

    it('toggles orphaned nodes', () => {
      const { result } = renderHook(() => useEntityGraphStore());

      expect(result.current.showOrphanedNodes).toBe(false);

      act(() => {
        result.current.toggleOrphanedNodes();
      });

      expect(result.current.showOrphanedNodes).toBe(true);
    });
  });

  describe('resetFilters', () => {
    it('resets filter state', () => {
      const { result } = renderHook(() => useEntityGraphStore());

      // Set some filter state
      act(() => {
        result.current.setSelectedNode({
          id: 'node-1',
          name: 'Test',
          type: 'agent',
          attributes: {},
        });
        result.current.setFocusedNode('node-1');
        result.current.toggleInferredNodes(); // false
        result.current.toggleDeduplication(); // true
      });

      // Reset
      act(() => {
        result.current.resetFilters();
      });

      expect(result.current.selectedNode).toBeNull();
      expect(result.current.focusedNodeId).toBeNull();
      expect(result.current.showInferredNodes).toBe(true);
      expect(result.current.deduplicateNodes).toBe(false);
    });
  });

  describe('cleanupGraph', () => {
    it('clears graph instance on cleanup', () => {
      const { result } = renderHook(() => useEntityGraphStore());

      // Set a mock graph instance
      act(() => {
        useEntityGraphStore.setState({
          graphInstance: { _destructor: vi.fn() },
        });
      });

      // Cleanup
      act(() => {
        result.current.cleanupGraph();
      });

      expect(result.current.graphInstance).toBeNull();
    });
  });
});
