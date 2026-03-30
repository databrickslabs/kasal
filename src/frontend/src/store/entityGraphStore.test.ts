import { describe, it, expect, vi, beforeEach } from 'vitest';
import { act, renderHook } from '@testing-library/react';

// Mock Logger
vi.mock('../utils/logger', () => {
  const MockLogger = vi.fn(function (this: Record<string, unknown>) {
    this.debug = vi.fn();
    this.info = vi.fn();
    this.warn = vi.fn();
    this.error = vi.fn();
  });
  return { default: MockLogger };
});

// Mock entity colors
vi.mock('../utils/entityColors', () => ({
  getEntityColor: vi.fn(() => '#999'),
  getRelationshipColor: vi.fn(() => '#78909C'),
  lightenColor: vi.fn((hex: string) => hex),
  darkenColor: vi.fn((hex: string) => hex),
}));

// Create a reusable mock graph instance
function createMockGraphInstance() {
  return {
    backgroundColor: vi.fn().mockReturnThis(),
    nodeId: vi.fn().mockReturnThis(),
    nodeLabel: vi.fn().mockReturnThis(),
    nodeCanvasObject: vi.fn().mockReturnThis(),
    nodePointerAreaPaint: vi.fn().mockReturnThis(),
    linkWidth: vi.fn().mockReturnThis(),
    linkColor: vi.fn().mockReturnThis(),
    linkDirectionalArrowLength: vi.fn().mockReturnThis(),
    linkDirectionalArrowRelPos: vi.fn().mockReturnThis(),
    linkDirectionalArrowColor: vi.fn().mockReturnThis(),
    linkDirectionalParticles: vi.fn().mockReturnThis(),
    linkDirectionalParticleSpeed: vi.fn().mockReturnThis(),
    linkDirectionalParticleWidth: vi.fn().mockReturnThis(),
    linkDirectionalParticleColor: vi.fn().mockReturnThis(),
    linkCanvasObject: vi.fn().mockReturnThis(),
    linkCanvasObjectMode: vi.fn().mockReturnThis(),
    linkCurvature: vi.fn().mockReturnThis(),
    onNodeClick: vi.fn().mockReturnThis(),
    onNodeHover: vi.fn().mockReturnThis(),
    onBackgroundClick: vi.fn().mockReturnThis(),
    centerAt: vi.fn().mockReturnThis(),
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
  };
}

let currentMockInstance: ReturnType<typeof createMockGraphInstance>;

// Mock ForceGraph2D
vi.mock('force-graph', () => ({
  default: vi.fn(() => vi.fn((/* container */) => {
    currentMockInstance = createMockGraphInstance();
    return currentMockInstance;
  })),
}));

// Mock d3-force
vi.mock('d3-force', () => ({
  forceX: vi.fn().mockReturnValue({ strength: vi.fn().mockReturnThis() }),
  forceY: vi.fn().mockReturnValue({ strength: vi.fn().mockReturnThis() }),
}));

// Import after mocks
import useEntityGraphStore from './entityGraphStore';
import ForceGraph2D from 'force-graph';

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
      hoveredNodeId: null,
      highlightedNodeIds: new Set<string>(),
      highlightedLinkKeys: new Set<string>(),
      hiddenEntityTypes: new Set<string>(),
      controlsPanelCollapsed: false,
      legendPanelCollapsed: false,
      isDarkMode: false,
    });
  });

  describe('initial state', () => {
    it('has correct default values', () => {
      const state = useEntityGraphStore.getState();

      expect(state.selectedNode).toBeNull();
      expect(state.graphData).toEqual({ nodes: [], links: [] });
      expect(state.filteredGraphData).toEqual({ nodes: [], links: [] });
      expect(state.forceStrength).toBe(-300);
      expect(state.linkDistance).toBe(100);
      expect(state.centerForce).toBe(0.3);
      expect(state.linkCurvature).toBe(0);
      expect(state.loading).toBe(false);
      expect(state.error).toBeNull();
      expect(state.focusedNodeId).toBeNull();
      expect(state.showInferredNodes).toBe(true);
      expect(state.deduplicateNodes).toBe(false);
      expect(state.showOrphanedNodes).toBe(false);
      expect(state.hoveredNodeId).toBeNull();
      expect(state.highlightedNodeIds.size).toBe(0);
      expect(state.highlightedLinkKeys.size).toBe(0);
      expect(state.hiddenEntityTypes.size).toBe(0);
      expect(state.controlsPanelCollapsed).toBe(false);
      expect(state.legendPanelCollapsed).toBe(false);
      expect(state.isDarkMode).toBe(false);
      expect(state.graphInstance).toBeNull();
    });
  });

  describe('setGraphData', () => {
    it('updates graphData', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      const data = {
        nodes: [{ id: 'a', name: 'A', type: 'person', attributes: {} }],
        links: [],
      };

      act(() => {
        result.current.setGraphData(data);
      });

      expect(result.current.graphData).toEqual(data);
    });
  });

  describe('setFilteredGraphData', () => {
    it('updates filteredGraphData', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      const data = {
        nodes: [{ id: 'a', name: 'A', type: 'person', attributes: {} }],
        links: [],
      };

      act(() => {
        result.current.setFilteredGraphData(data);
      });

      expect(result.current.filteredGraphData).toEqual(data);
    });

    it('calls graphData on instance when graph instance exists and data has nodes', () => {
      const { result } = renderHook(() => useEntityGraphStore());

      const mockInstance = createMockGraphInstance();
      act(() => {
        useEntityGraphStore.setState({ graphInstance: mockInstance });
      });

      const data = {
        nodes: [{ id: 'a', name: 'A', type: 'person', attributes: {} }],
        links: [],
      };

      act(() => {
        result.current.setFilteredGraphData(data);
      });

      expect(mockInstance.graphData).toHaveBeenCalledWith(data);
    });

    it('does not call graphData when no graph instance', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      const data = {
        nodes: [{ id: 'a', name: 'A', type: 'person', attributes: {} }],
        links: [],
      };

      act(() => {
        result.current.setFilteredGraphData(data);
      });

      // No error thrown, just state updated
      expect(result.current.filteredGraphData).toEqual(data);
    });

    it('does not call graphData when data has no nodes', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      const mockInstance = createMockGraphInstance();
      act(() => {
        useEntityGraphStore.setState({ graphInstance: mockInstance });
      });

      act(() => {
        result.current.setFilteredGraphData({ nodes: [], links: [] });
      });

      expect(mockInstance.graphData).not.toHaveBeenCalled();
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
      const testNode = { id: 'node-1', name: 'Test Node', type: 'agent', attributes: {} };

      act(() => {
        result.current.setSelectedNode(testNode);
      });

      act(() => {
        result.current.setSelectedNode(null);
      });

      expect(result.current.selectedNode).toBeNull();
    });
  });

  describe('setFocusedNode', () => {
    it('updates focused node id', () => {
      const { result } = renderHook(() => useEntityGraphStore());

      act(() => {
        result.current.setFocusedNode('node-1');
      });

      expect(result.current.focusedNodeId).toBe('node-1');
    });

    it('clears focused node when set to null', () => {
      const { result } = renderHook(() => useEntityGraphStore());

      act(() => {
        result.current.setFocusedNode('node-1');
      });

      act(() => {
        result.current.setFocusedNode(null);
      });

      expect(result.current.focusedNodeId).toBeNull();
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

  describe('updateForceParameters', () => {
    it('updates force parameters without graph instance', () => {
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
      expect(result.current.centerForce).toBe(0.3);
    });

    it('updates forces on graph instance with centerForce < 0.3', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      const mockInstance = createMockGraphInstance();
      act(() => {
        useEntityGraphStore.setState({ graphInstance: mockInstance });
      });

      act(() => {
        result.current.updateForceParameters(-300, 100, 0.1);
      });

      expect(mockInstance.d3Force).toHaveBeenCalled();
      expect(mockInstance.d3ReheatSimulation).toHaveBeenCalled();
    });

    it('updates forces on graph instance with centerForce 0.3-0.7', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      const mockInstance = createMockGraphInstance();
      act(() => {
        useEntityGraphStore.setState({ graphInstance: mockInstance });
      });

      act(() => {
        result.current.updateForceParameters(-300, 100, 0.5);
      });

      expect(mockInstance.d3Force).toHaveBeenCalled();
      expect(mockInstance.d3ReheatSimulation).toHaveBeenCalled();
    });

    it('updates forces on graph instance with centerForce > 0.7', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      const mockInstance = createMockGraphInstance();
      act(() => {
        useEntityGraphStore.setState({ graphInstance: mockInstance });
      });

      act(() => {
        result.current.updateForceParameters(-300, 100, 0.9);
      });

      expect(mockInstance.d3Force).toHaveBeenCalled();
      expect(mockInstance.d3ReheatSimulation).toHaveBeenCalledWith(1);
    });

    it('reheats simulation without centerForce param', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      const mockInstance = createMockGraphInstance();
      act(() => {
        useEntityGraphStore.setState({ graphInstance: mockInstance });
      });

      act(() => {
        result.current.updateForceParameters(-300, 100);
      });

      expect(mockInstance.d3ReheatSimulation).toHaveBeenCalledWith(0.3);
    });
  });

  describe('setLinkCurvature', () => {
    it('updates curvature state', () => {
      const { result } = renderHook(() => useEntityGraphStore());

      act(() => {
        result.current.setLinkCurvature(0.5);
      });

      expect(result.current.linkCurvature).toBe(0.5);
    });

    it('calls linkCurvature on graph instance', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      const mockInstance = createMockGraphInstance();
      act(() => {
        useEntityGraphStore.setState({ graphInstance: mockInstance });
      });

      act(() => {
        result.current.setLinkCurvature(0.2);
      });

      expect(mockInstance.linkCurvature).toHaveBeenCalledWith(0.2);
    });

    it('works without graph instance', () => {
      const { result } = renderHook(() => useEntityGraphStore());

      act(() => {
        result.current.setLinkCurvature(0.3);
      });

      expect(result.current.linkCurvature).toBe(0.3);
    });
  });

  describe('toggles', () => {
    it('toggles inferred nodes', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      expect(result.current.showInferredNodes).toBe(true);
      act(() => { result.current.toggleInferredNodes(); });
      expect(result.current.showInferredNodes).toBe(false);
      act(() => { result.current.toggleInferredNodes(); });
      expect(result.current.showInferredNodes).toBe(true);
    });

    it('toggles deduplication', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      expect(result.current.deduplicateNodes).toBe(false);
      act(() => { result.current.toggleDeduplication(); });
      expect(result.current.deduplicateNodes).toBe(true);
      act(() => { result.current.toggleDeduplication(); });
      expect(result.current.deduplicateNodes).toBe(false);
    });

    it('toggles orphaned nodes', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      expect(result.current.showOrphanedNodes).toBe(false);
      act(() => { result.current.toggleOrphanedNodes(); });
      expect(result.current.showOrphanedNodes).toBe(true);
      act(() => { result.current.toggleOrphanedNodes(); });
      expect(result.current.showOrphanedNodes).toBe(false);
    });
  });

  describe('resetFilters', () => {
    it('resets filter state including new fields', () => {
      const { result } = renderHook(() => useEntityGraphStore());

      act(() => {
        result.current.setSelectedNode({ id: 'node-1', name: 'Test', type: 'agent', attributes: {} });
        result.current.setFocusedNode('node-1');
        result.current.toggleInferredNodes();
        result.current.toggleDeduplication();
        result.current.toggleEntityTypeVisibility('person');
      });

      act(() => {
        result.current.resetFilters();
      });

      expect(result.current.selectedNode).toBeNull();
      expect(result.current.focusedNodeId).toBeNull();
      expect(result.current.showInferredNodes).toBe(true);
      expect(result.current.deduplicateNodes).toBe(false);
      expect(result.current.hoveredNodeId).toBeNull();
      expect(result.current.highlightedNodeIds.size).toBe(0);
      expect(result.current.highlightedLinkKeys.size).toBe(0);
      expect(result.current.hiddenEntityTypes.size).toBe(0);
    });
  });

  describe('cleanupGraph', () => {
    it('clears graph instance on cleanup', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      const mockInstance = createMockGraphInstance();
      act(() => {
        useEntityGraphStore.setState({ graphInstance: mockInstance });
      });

      act(() => {
        result.current.cleanupGraph();
      });

      expect(result.current.graphInstance).toBeNull();
      expect(mockInstance._destructor).toHaveBeenCalled();
    });

    it('does nothing when no graph instance', () => {
      const { result } = renderHook(() => useEntityGraphStore());

      act(() => {
        result.current.cleanupGraph();
      });

      expect(result.current.graphInstance).toBeNull();
    });

    it('handles destructor errors gracefully', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      const mockInstance = createMockGraphInstance();
      mockInstance._destructor.mockImplementation(() => {
        throw new Error('Destructor error');
      });

      act(() => {
        useEntityGraphStore.setState({ graphInstance: mockInstance });
      });

      act(() => {
        result.current.cleanupGraph();
      });

      expect(result.current.graphInstance).toBeNull();
    });

    it('handles instance without _destructor', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      const mockInstance = { someOtherMethod: vi.fn() };

      act(() => {
        useEntityGraphStore.setState({ graphInstance: mockInstance });
      });

      act(() => {
        result.current.cleanupGraph();
      });

      expect(result.current.graphInstance).toBeNull();
    });
  });

  describe('zoom functions', () => {
    it('zoomToFit calls zoomToFit on instance', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      const mockInstance = createMockGraphInstance();
      act(() => {
        useEntityGraphStore.setState({ graphInstance: mockInstance });
      });

      act(() => {
        result.current.zoomToFit();
      });

      expect(mockInstance.zoomToFit).toHaveBeenCalledWith(400, 50);
    });

    it('zoomToFit does nothing without instance', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      act(() => {
        result.current.zoomToFit();
      });
      // No error thrown
    });

    it('zoomIn multiplies current zoom by 1.2', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      const mockInstance = createMockGraphInstance();
      mockInstance.zoom.mockReturnValue(2);
      act(() => {
        useEntityGraphStore.setState({ graphInstance: mockInstance });
      });

      act(() => {
        result.current.zoomIn();
      });

      expect(mockInstance.zoom).toHaveBeenCalledWith(2.4, 300);
    });

    it('zoomIn does nothing without instance', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      act(() => {
        result.current.zoomIn();
      });
      // No error thrown
    });

    it('zoomOut multiplies current zoom by 0.8', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      const mockInstance = createMockGraphInstance();
      mockInstance.zoom.mockReturnValue(2);
      act(() => {
        useEntityGraphStore.setState({ graphInstance: mockInstance });
      });

      act(() => {
        result.current.zoomOut();
      });

      expect(mockInstance.zoom).toHaveBeenCalledWith(1.6, 300);
    });

    it('zoomOut does nothing without instance', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      act(() => {
        result.current.zoomOut();
      });
      // No error thrown
    });
  });

  describe('setHoveredNode', () => {
    it('sets highlighted nodes and links for hovered node', () => {
      const { result } = renderHook(() => useEntityGraphStore());

      act(() => {
        useEntityGraphStore.setState({
          filteredGraphData: {
            nodes: [
              { id: 'a', name: 'A', type: 'person', attributes: {} },
              { id: 'b', name: 'B', type: 'person', attributes: {} },
              { id: 'c', name: 'C', type: 'person', attributes: {} },
              { id: 'd', name: 'D', type: 'person', attributes: {} },
            ],
            links: [
              { source: 'a', target: 'b', relationship: 'knows' },
              { source: 'a', target: 'c', relationship: 'works_with' },
              { source: 'c', target: 'd', relationship: 'manages' },
            ],
          },
        });
      });

      act(() => {
        result.current.setHoveredNode('a');
      });

      expect(result.current.hoveredNodeId).toBe('a');
      expect(result.current.highlightedNodeIds.has('a')).toBe(true);
      expect(result.current.highlightedNodeIds.has('b')).toBe(true);
      expect(result.current.highlightedNodeIds.has('c')).toBe(true);
      expect(result.current.highlightedNodeIds.has('d')).toBe(false);
      expect(result.current.highlightedLinkKeys.has('a--b')).toBe(true);
      expect(result.current.highlightedLinkKeys.has('a--c')).toBe(true);
      expect(result.current.highlightedLinkKeys.has('c--d')).toBe(false);
    });

    it('handles object source/target in links', () => {
      const { result } = renderHook(() => useEntityGraphStore());

      act(() => {
        useEntityGraphStore.setState({
          filteredGraphData: {
            nodes: [
              { id: 'a', name: 'A', type: 'person', attributes: {} },
              { id: 'b', name: 'B', type: 'person', attributes: {} },
            ],
            links: [
              {
                source: { id: 'a', name: 'A', type: 'person', attributes: {} },
                target: { id: 'b', name: 'B', type: 'person', attributes: {} },
                relationship: 'knows',
              },
            ],
          },
        });
      });

      act(() => {
        result.current.setHoveredNode('a');
      });

      expect(result.current.highlightedNodeIds.has('a')).toBe(true);
      expect(result.current.highlightedNodeIds.has('b')).toBe(true);
      expect(result.current.highlightedLinkKeys.has('a--b')).toBe(true);
    });

    it('clears highlights when set to null', () => {
      const { result } = renderHook(() => useEntityGraphStore());

      act(() => {
        useEntityGraphStore.setState({
          hoveredNodeId: 'a',
          highlightedNodeIds: new Set(['a', 'b']),
          highlightedLinkKeys: new Set(['a--b']),
        });
      });

      act(() => {
        result.current.setHoveredNode(null);
      });

      expect(result.current.hoveredNodeId).toBeNull();
      expect(result.current.highlightedNodeIds.size).toBe(0);
      expect(result.current.highlightedLinkKeys.size).toBe(0);
    });
  });

  describe('toggleEntityTypeVisibility', () => {
    it('adds type to hidden set', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      act(() => { result.current.toggleEntityTypeVisibility('person'); });
      expect(result.current.hiddenEntityTypes.has('person')).toBe(true);
    });

    it('removes type from hidden set on second toggle', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      act(() => { result.current.toggleEntityTypeVisibility('person'); });
      act(() => { result.current.toggleEntityTypeVisibility('person'); });
      expect(result.current.hiddenEntityTypes.has('person')).toBe(false);
    });

    it('handles multiple types independently', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      act(() => {
        result.current.toggleEntityTypeVisibility('person');
        result.current.toggleEntityTypeVisibility('organization');
      });
      expect(result.current.hiddenEntityTypes.has('person')).toBe(true);
      expect(result.current.hiddenEntityTypes.has('organization')).toBe(true);
      act(() => { result.current.toggleEntityTypeVisibility('person'); });
      expect(result.current.hiddenEntityTypes.has('person')).toBe(false);
      expect(result.current.hiddenEntityTypes.has('organization')).toBe(true);
    });
  });

  describe('panel collapse states', () => {
    it('sets controls panel collapsed state', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      expect(result.current.controlsPanelCollapsed).toBe(false);
      act(() => { result.current.setControlsPanelCollapsed(true); });
      expect(result.current.controlsPanelCollapsed).toBe(true);
      act(() => { result.current.setControlsPanelCollapsed(false); });
      expect(result.current.controlsPanelCollapsed).toBe(false);
    });

    it('sets legend panel collapsed state', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      expect(result.current.legendPanelCollapsed).toBe(false);
      act(() => { result.current.setLegendPanelCollapsed(true); });
      expect(result.current.legendPanelCollapsed).toBe(true);
      act(() => { result.current.setLegendPanelCollapsed(false); });
      expect(result.current.legendPanelCollapsed).toBe(false);
    });
  });

  describe('setIsDarkMode', () => {
    it('updates dark mode state', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      expect(result.current.isDarkMode).toBe(false);
      act(() => { result.current.setIsDarkMode(true); });
      expect(result.current.isDarkMode).toBe(true);
    });

    it('calls backgroundColor on graph instance when toggling dark mode', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      const mockInstance = createMockGraphInstance();
      act(() => {
        useEntityGraphStore.setState({ graphInstance: mockInstance });
      });

      act(() => {
        result.current.setIsDarkMode(true);
      });

      expect(mockInstance.backgroundColor).toHaveBeenCalledWith('#1a1a2e');
    });

    it('calls backgroundColor with light color for non-dark mode', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      const mockInstance = createMockGraphInstance();
      act(() => {
        useEntityGraphStore.setState({ graphInstance: mockInstance, isDarkMode: true });
      });

      act(() => {
        result.current.setIsDarkMode(false);
      });

      expect(mockInstance.backgroundColor).toHaveBeenCalledWith('#fafafa');
    });

    it('works without graph instance', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      act(() => {
        result.current.setIsDarkMode(true);
      });
      expect(result.current.isDarkMode).toBe(true);
    });
  });

  describe('centerOnNode', () => {
    it('centers and zooms on node with coordinates', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      const mockInstance = createMockGraphInstance();
      const nodeWithCoords = { id: 'a', name: 'A', type: 'person', attributes: {}, x: 100, y: 200 };

      act(() => {
        useEntityGraphStore.setState({
          graphInstance: mockInstance,
          filteredGraphData: {
            nodes: [nodeWithCoords],
            links: [],
          },
        });
      });

      act(() => {
        result.current.centerOnNode('a');
      });

      expect(mockInstance.centerAt).toHaveBeenCalledWith(100, 200, 600);
      expect(mockInstance.zoom).toHaveBeenCalledWith(1.5, 600);
    });

    it('does nothing when no graph instance', () => {
      const { result } = renderHook(() => useEntityGraphStore());

      act(() => {
        result.current.centerOnNode('a');
      });
      // No error thrown
    });

    it('does nothing when node not found', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      const mockInstance = createMockGraphInstance();

      act(() => {
        useEntityGraphStore.setState({
          graphInstance: mockInstance,
          filteredGraphData: { nodes: [], links: [] },
        });
      });

      act(() => {
        result.current.centerOnNode('nonexistent');
      });

      expect(mockInstance.centerAt).not.toHaveBeenCalled();
    });

    it('does nothing when node has no coordinates', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      const mockInstance = createMockGraphInstance();

      act(() => {
        useEntityGraphStore.setState({
          graphInstance: mockInstance,
          filteredGraphData: {
            nodes: [{ id: 'a', name: 'A', type: 'person', attributes: {} }],
            links: [],
          },
        });
      });

      act(() => {
        result.current.centerOnNode('a');
      });

      expect(mockInstance.centerAt).not.toHaveBeenCalled();
    });
  });

  describe('initializeGraph', () => {
    it('creates force graph and sets graphInstance', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      const container = document.createElement('div');
      Object.defineProperty(container, 'offsetWidth', { value: 800 });
      Object.defineProperty(container, 'offsetHeight', { value: 600 });

      act(() => {
        result.current.initializeGraph(container);
      });

      expect(result.current.graphInstance).not.toBeNull();
    });

    it('cleans up existing graph instance before initializing', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      const existingInstance = createMockGraphInstance();
      act(() => {
        useEntityGraphStore.setState({ graphInstance: existingInstance });
      });

      const container = document.createElement('div');
      Object.defineProperty(container, 'offsetWidth', { value: 800 });
      Object.defineProperty(container, 'offsetHeight', { value: 600 });

      act(() => {
        result.current.initializeGraph(container);
      });

      expect(existingInstance._destructor).toHaveBeenCalled();
    });

    it('sets graph data when filteredGraphData has nodes and calls zoomToFit after timeout', () => {
      vi.useFakeTimers();
      const { result } = renderHook(() => useEntityGraphStore());
      const container = document.createElement('div');
      Object.defineProperty(container, 'offsetWidth', { value: 800 });
      Object.defineProperty(container, 'offsetHeight', { value: 600 });

      act(() => {
        useEntityGraphStore.setState({
          filteredGraphData: {
            nodes: [{ id: 'a', name: 'A', type: 'person', attributes: {} }],
            links: [],
          },
        });
      });

      act(() => {
        result.current.initializeGraph(container);
      });

      expect(currentMockInstance.graphData).toHaveBeenCalled();

      // Advance past the 500ms setTimeout to trigger zoomToFit (line 459)
      act(() => {
        vi.advanceTimersByTime(500);
      });

      expect(currentMockInstance.zoomToFit).toHaveBeenCalledWith(400, 50);
      vi.useRealTimers();
    });

    it('sets empty graph data when no filteredGraphData nodes', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      const container = document.createElement('div');
      Object.defineProperty(container, 'offsetWidth', { value: 800 });
      Object.defineProperty(container, 'offsetHeight', { value: 600 });

      act(() => {
        result.current.initializeGraph(container);
      });

      expect(currentMockInstance.graphData).toHaveBeenCalledWith({ nodes: [], links: [] });
    });

    it('configures d3 forces with low centerForce', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      const container = document.createElement('div');
      Object.defineProperty(container, 'offsetWidth', { value: 800 });
      Object.defineProperty(container, 'offsetHeight', { value: 600 });

      act(() => {
        useEntityGraphStore.setState({ centerForce: 0.1 });
      });

      act(() => {
        result.current.initializeGraph(container);
      });

      // centerForce < 0.3 → d3Force('x', null) and d3Force('y', null) are called
      expect(currentMockInstance.d3Force).toHaveBeenCalledWith('x', null);
      expect(currentMockInstance.d3Force).toHaveBeenCalledWith('y', null);
    });

    it('configures d3 forces with medium centerForce', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      const container = document.createElement('div');
      Object.defineProperty(container, 'offsetWidth', { value: 800 });
      Object.defineProperty(container, 'offsetHeight', { value: 600 });

      act(() => {
        useEntityGraphStore.setState({ centerForce: 0.5 });
      });

      act(() => {
        result.current.initializeGraph(container);
      });

      expect(currentMockInstance.d3Force).toHaveBeenCalledWith('x', expect.anything());
      expect(currentMockInstance.d3Force).toHaveBeenCalledWith('y', expect.anything());
    });

    it('configures d3 forces with high centerForce', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      const container = document.createElement('div');
      Object.defineProperty(container, 'offsetWidth', { value: 800 });
      Object.defineProperty(container, 'offsetHeight', { value: 600 });

      act(() => {
        useEntityGraphStore.setState({ centerForce: 0.9 });
      });

      act(() => {
        result.current.initializeGraph(container);
      });

      expect(currentMockInstance.d3Force).toHaveBeenCalledWith('x', expect.anything());
      expect(currentMockInstance.d3Force).toHaveBeenCalledWith('y', expect.anything());
    });

    it('configures dark mode background', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      const container = document.createElement('div');
      Object.defineProperty(container, 'offsetWidth', { value: 800 });
      Object.defineProperty(container, 'offsetHeight', { value: 600 });

      act(() => {
        useEntityGraphStore.setState({ isDarkMode: true });
      });

      act(() => {
        result.current.initializeGraph(container);
      });

      expect(currentMockInstance.backgroundColor).toHaveBeenCalledWith('#1a1a2e');
    });

    it('invokes onNodeClick callback - single click selects node', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      const container = document.createElement('div');
      Object.defineProperty(container, 'offsetWidth', { value: 800 });
      Object.defineProperty(container, 'offsetHeight', { value: 600 });

      act(() => {
        result.current.initializeGraph(container);
      });

      // Get the onNodeClick callback from the mock
      const onNodeClickCall = currentMockInstance.onNodeClick.mock.calls[0];
      const onNodeClickFn = onNodeClickCall[0];

      // Simulate a single click
      const node = { id: 'n1', name: 'Test', type: 'person', attributes: {}, x: 100, y: 200 };
      act(() => {
        onNodeClickFn(node);
      });

      expect(result.current.selectedNode).toEqual(node);
    });

    it('invokes onNodeClick callback - double click focuses and centers', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      const container = document.createElement('div');
      Object.defineProperty(container, 'offsetWidth', { value: 800 });
      Object.defineProperty(container, 'offsetHeight', { value: 600 });

      act(() => {
        result.current.initializeGraph(container);
      });

      const onNodeClickFn = currentMockInstance.onNodeClick.mock.calls[0][0];

      const node = { id: 'n1', name: 'Test', type: 'person', attributes: {}, x: 100, y: 200 };

      // First click
      act(() => {
        onNodeClickFn(node);
      });

      // Second click within 350ms (same node) triggers double-click
      act(() => {
        onNodeClickFn(node);
      });

      expect(result.current.focusedNodeId).toBe('n1');
      expect(currentMockInstance.centerAt).toHaveBeenCalledWith(100, 200, 500);
    });

    it('invokes onBackgroundClick callback - clears selection', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      const container = document.createElement('div');
      Object.defineProperty(container, 'offsetWidth', { value: 800 });
      Object.defineProperty(container, 'offsetHeight', { value: 600 });

      // Set initial selection
      act(() => {
        result.current.setSelectedNode({ id: 'n1', name: 'Test', type: 'person', attributes: {} });
        result.current.setFocusedNode('n1');
      });

      act(() => {
        result.current.initializeGraph(container);
      });

      const onBackgroundClickFn = currentMockInstance.onBackgroundClick.mock.calls[0][0];

      act(() => {
        onBackgroundClickFn();
      });

      expect(result.current.selectedNode).toBeNull();
      expect(result.current.focusedNodeId).toBeNull();
    });

    it('invokes onNodeHover callback', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      const container = document.createElement('div');
      Object.defineProperty(container, 'offsetWidth', { value: 800 });
      Object.defineProperty(container, 'offsetHeight', { value: 600 });

      act(() => {
        useEntityGraphStore.setState({
          filteredGraphData: {
            nodes: [
              { id: 'a', name: 'A', type: 'person', attributes: {} },
              { id: 'b', name: 'B', type: 'person', attributes: {} },
            ],
            links: [{ source: 'a', target: 'b', relationship: 'knows' }],
          },
        });
      });

      act(() => {
        result.current.initializeGraph(container);
      });

      const onNodeHoverFn = currentMockInstance.onNodeHover.mock.calls[0][0];

      act(() => {
        onNodeHoverFn({ id: 'a', name: 'A', type: 'person', attributes: {} });
      });

      expect(result.current.hoveredNodeId).toBe('a');

      // Hover off
      act(() => {
        onNodeHoverFn(null);
      });

      expect(result.current.hoveredNodeId).toBeNull();
    });

    it('invokes nodeCanvasObject callback for rendering', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      const container = document.createElement('div');
      Object.defineProperty(container, 'offsetWidth', { value: 800 });
      Object.defineProperty(container, 'offsetHeight', { value: 600 });

      act(() => {
        useEntityGraphStore.setState({
          filteredGraphData: {
            nodes: [{ id: 'a', name: 'A', type: 'person', attributes: {} }],
            links: [],
          },
        });
      });

      act(() => {
        result.current.initializeGraph(container);
      });

      const nodeCanvasObjectFn = currentMockInstance.nodeCanvasObject.mock.calls[0][0];

      // Create a mock canvas context
      const ctx = {
        createRadialGradient: vi.fn().mockReturnValue({
          addColorStop: vi.fn(),
        }),
        beginPath: vi.fn(),
        arc: vi.fn(),
        fill: vi.fn(),
        stroke: vi.fn(),
        fillText: vi.fn(),
        measureText: vi.fn().mockReturnValue({ width: 20 }),
        save: vi.fn(),
        restore: vi.fn(),
        moveTo: vi.fn(),
        lineTo: vi.fn(),
        quadraticCurveTo: vi.fn(),
        roundRect: undefined, // Test fallback path
        fillStyle: '',
        strokeStyle: '',
        lineWidth: 0,
        textAlign: '',
        textBaseline: '',
        font: '',
        globalAlpha: 1,
        shadowColor: '',
        shadowBlur: 0,
      };

      const node = { id: 'a', name: 'A', type: 'person', attributes: {}, x: 10, y: 20, color: '#68CCE5' };

      // Test with scale > 0.4 (shows labels, uses fallback for roundRect)
      nodeCanvasObjectFn(node, ctx, 1.0);

      expect(ctx.createRadialGradient).toHaveBeenCalled();
      expect(ctx.arc).toHaveBeenCalled();
      expect(ctx.fill).toHaveBeenCalled();
      expect(ctx.fillText).toHaveBeenCalled();
      // Verify fallback roundRect path was used (moveTo/lineTo/quadraticCurveTo)
      expect(ctx.moveTo).toHaveBeenCalled();
      expect(ctx.quadraticCurveTo).toHaveBeenCalled();
      expect(ctx.globalAlpha).toBe(1);
    });

    it('nodeCanvasObject dims non-highlighted nodes during hover', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      const container = document.createElement('div');
      Object.defineProperty(container, 'offsetWidth', { value: 800 });
      Object.defineProperty(container, 'offsetHeight', { value: 600 });

      act(() => {
        useEntityGraphStore.setState({
          filteredGraphData: {
            nodes: [
              { id: 'a', name: 'A', type: 'person', attributes: {} },
              { id: 'b', name: 'B', type: 'person', attributes: {} },
            ],
            links: [{ source: 'a', target: 'b', relationship: 'knows' }],
          },
          hoveredNodeId: 'a',
          highlightedNodeIds: new Set(['a', 'b']),
          highlightedLinkKeys: new Set(['a--b']),
        });
      });

      act(() => {
        result.current.initializeGraph(container);
      });

      const nodeCanvasObjectFn = currentMockInstance.nodeCanvasObject.mock.calls[0][0];
      const ctx = {
        createRadialGradient: vi.fn().mockReturnValue({ addColorStop: vi.fn() }),
        beginPath: vi.fn(),
        arc: vi.fn(),
        fill: vi.fn(),
        stroke: vi.fn(),
        fillText: vi.fn(),
        measureText: vi.fn().mockReturnValue({ width: 20 }),
        save: vi.fn(),
        restore: vi.fn(),
        roundRect: vi.fn(), // Test with roundRect available
        fillStyle: '',
        strokeStyle: '',
        lineWidth: 0,
        textAlign: '',
        textBaseline: '',
        font: '',
        globalAlpha: 1,
        shadowColor: '',
        shadowBlur: 0,
      };

      // Render a non-highlighted node (c) while hover is active
      const nodeC = { id: 'c', name: 'C', type: 'person', attributes: {}, x: 50, y: 50 };
      nodeCanvasObjectFn(nodeC, ctx, 1.0);

      // globalAlpha should have been set to 0.15 for dimming, then restored to 1
      expect(ctx.globalAlpha).toBe(1);
    });

    it('nodeCanvasObject renders glow ring for selected node', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      const container = document.createElement('div');
      Object.defineProperty(container, 'offsetWidth', { value: 800 });
      Object.defineProperty(container, 'offsetHeight', { value: 600 });

      const selectedNode = { id: 'a', name: 'A', type: 'person', attributes: {}, x: 10, y: 20 };
      act(() => {
        useEntityGraphStore.setState({
          filteredGraphData: { nodes: [selectedNode], links: [] },
          selectedNode: selectedNode,
        });
      });

      act(() => {
        result.current.initializeGraph(container);
      });

      const nodeCanvasObjectFn = currentMockInstance.nodeCanvasObject.mock.calls[0][0];
      const ctx = {
        createRadialGradient: vi.fn().mockReturnValue({ addColorStop: vi.fn() }),
        beginPath: vi.fn(),
        arc: vi.fn(),
        fill: vi.fn(),
        stroke: vi.fn(),
        fillText: vi.fn(),
        measureText: vi.fn().mockReturnValue({ width: 20 }),
        save: vi.fn(),
        restore: vi.fn(),
        roundRect: vi.fn(),
        fillStyle: '',
        strokeStyle: '',
        lineWidth: 0,
        textAlign: '',
        textBaseline: '',
        font: '',
        globalAlpha: 1,
        shadowColor: '',
        shadowBlur: 0,
      };

      nodeCanvasObjectFn(selectedNode, ctx, 1.0);

      // save/restore for glow ring
      expect(ctx.save).toHaveBeenCalled();
      expect(ctx.restore).toHaveBeenCalled();
    });

    it('nodeCanvasObject skips labels when zoom < 0.4', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      const container = document.createElement('div');
      Object.defineProperty(container, 'offsetWidth', { value: 800 });
      Object.defineProperty(container, 'offsetHeight', { value: 600 });

      act(() => {
        useEntityGraphStore.setState({
          filteredGraphData: { nodes: [{ id: 'a', name: 'A', type: 'person', attributes: {} }], links: [] },
        });
      });

      act(() => {
        result.current.initializeGraph(container);
      });

      const nodeCanvasObjectFn = currentMockInstance.nodeCanvasObject.mock.calls[0][0];
      const ctx = {
        createRadialGradient: vi.fn().mockReturnValue({ addColorStop: vi.fn() }),
        beginPath: vi.fn(),
        arc: vi.fn(),
        fill: vi.fn(),
        stroke: vi.fn(),
        fillText: vi.fn(),
        measureText: vi.fn().mockReturnValue({ width: 20 }),
        fillStyle: '',
        strokeStyle: '',
        lineWidth: 0,
        globalAlpha: 1,
      };

      const node = { id: 'a', name: 'A', type: 'person', attributes: {}, x: 10, y: 20 };
      nodeCanvasObjectFn(node, ctx, 0.3); // Scale below 0.4

      // fillText should NOT have been called (no label)
      expect(ctx.fillText).not.toHaveBeenCalled();
    });

    it('invokes nodePointerAreaPaint callback', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      const container = document.createElement('div');
      Object.defineProperty(container, 'offsetWidth', { value: 800 });
      Object.defineProperty(container, 'offsetHeight', { value: 600 });

      act(() => {
        useEntityGraphStore.setState({
          filteredGraphData: { nodes: [{ id: 'a', name: 'A', type: 'person', attributes: {} }], links: [] },
        });
      });

      act(() => {
        result.current.initializeGraph(container);
      });

      const nodePointerAreaPaintFn = currentMockInstance.nodePointerAreaPaint.mock.calls[0][0];
      const ctx = {
        beginPath: vi.fn(),
        arc: vi.fn(),
        fill: vi.fn(),
        fillStyle: '',
      };

      const node = { id: 'a', name: 'A', type: 'person', attributes: {}, x: 10, y: 20 };
      nodePointerAreaPaintFn(node, '#ff0000', ctx);

      expect(ctx.fillStyle).toBe('#ff0000');
      expect(ctx.arc).toHaveBeenCalled();
      expect(ctx.fill).toHaveBeenCalled();
    });

    it('invokes linkWidth callback', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      const container = document.createElement('div');
      Object.defineProperty(container, 'offsetWidth', { value: 800 });
      Object.defineProperty(container, 'offsetHeight', { value: 600 });

      act(() => {
        result.current.initializeGraph(container);
      });

      const linkWidthFn = currentMockInstance.linkWidth.mock.calls[0][0];

      // No hover active → default width
      const link = { source: 'a', target: 'b', relationship: 'knows' };
      expect(linkWidthFn(link)).toBe(1.5);
    });

    it('linkWidth returns 3 for highlighted links during hover', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      const container = document.createElement('div');
      Object.defineProperty(container, 'offsetWidth', { value: 800 });
      Object.defineProperty(container, 'offsetHeight', { value: 600 });

      act(() => {
        useEntityGraphStore.setState({
          hoveredNodeId: 'a',
          highlightedLinkKeys: new Set(['a--b']),
        });
      });

      act(() => {
        result.current.initializeGraph(container);
      });

      const linkWidthFn = currentMockInstance.linkWidth.mock.calls[0][0];
      const highlightedLink = { source: 'a', target: 'b', relationship: 'knows' };
      const nonHighlightedLink = { source: 'c', target: 'd', relationship: 'knows' };

      expect(linkWidthFn(highlightedLink)).toBe(3);
      expect(linkWidthFn(nonHighlightedLink)).toBe(0.5);
    });

    it('invokes linkColor callback', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      const container = document.createElement('div');
      Object.defineProperty(container, 'offsetWidth', { value: 800 });
      Object.defineProperty(container, 'offsetHeight', { value: 600 });

      act(() => {
        result.current.initializeGraph(container);
      });

      const linkColorFn = currentMockInstance.linkColor.mock.calls[0][0];
      const link = { source: 'a', target: 'b', relationship: 'knows' };
      const color = linkColorFn(link);
      expect(color).toContain('#78909C'); // mocked getRelationshipColor returns this
    });

    it('linkColor dims non-highlighted links during hover', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      const container = document.createElement('div');
      Object.defineProperty(container, 'offsetWidth', { value: 800 });
      Object.defineProperty(container, 'offsetHeight', { value: 600 });

      act(() => {
        useEntityGraphStore.setState({
          hoveredNodeId: 'a',
          highlightedLinkKeys: new Set(['a--b']),
        });
      });

      act(() => {
        result.current.initializeGraph(container);
      });

      const linkColorFn = currentMockInstance.linkColor.mock.calls[0][0];
      const highlighted = { source: 'a', target: 'b', relationship: 'knows' };
      const dimmed = { source: 'c', target: 'd', relationship: 'knows' };

      expect(linkColorFn(highlighted)).toBe('#78909C');
      expect(linkColorFn(dimmed)).toBe('#78909C15');
    });

    it('invokes linkDirectionalArrowColor callback', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      const container = document.createElement('div');
      Object.defineProperty(container, 'offsetWidth', { value: 800 });
      Object.defineProperty(container, 'offsetHeight', { value: 600 });

      act(() => {
        result.current.initializeGraph(container);
      });

      const arrowColorFn = currentMockInstance.linkDirectionalArrowColor.mock.calls[0][0];
      const link = { source: 'a', target: 'b', relationship: 'knows' };
      expect(arrowColorFn(link)).toBe('#78909C');
    });

    it('linkDirectionalArrowColor dims during hover for non-highlighted', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      const container = document.createElement('div');
      Object.defineProperty(container, 'offsetWidth', { value: 800 });
      Object.defineProperty(container, 'offsetHeight', { value: 600 });

      act(() => {
        useEntityGraphStore.setState({
          hoveredNodeId: 'a',
          highlightedLinkKeys: new Set(['a--b']),
        });
      });

      act(() => {
        result.current.initializeGraph(container);
      });

      const arrowColorFn = currentMockInstance.linkDirectionalArrowColor.mock.calls[0][0];
      const dimmed = { source: 'c', target: 'd', relationship: 'knows' };
      expect(arrowColorFn(dimmed)).toBe('#78909C15');
    });

    it('invokes linkDirectionalParticleColor callback', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      const container = document.createElement('div');
      Object.defineProperty(container, 'offsetWidth', { value: 800 });
      Object.defineProperty(container, 'offsetHeight', { value: 600 });

      act(() => {
        result.current.initializeGraph(container);
      });

      const particleColorFn = currentMockInstance.linkDirectionalParticleColor.mock.calls[0][0];
      const link = { source: 'a', target: 'b', relationship: 'knows' };
      expect(particleColorFn(link)).toBe('#78909C');
    });

    it('invokes linkCanvasObject callback - skips when scale < 0.8', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      const container = document.createElement('div');
      Object.defineProperty(container, 'offsetWidth', { value: 800 });
      Object.defineProperty(container, 'offsetHeight', { value: 600 });

      act(() => {
        result.current.initializeGraph(container);
      });

      const linkCanvasObjectFn = currentMockInstance.linkCanvasObject.mock.calls[0][0];
      const ctx = { fillText: vi.fn() };
      const link = { source: { x: 0, y: 0 }, target: { x: 100, y: 100 }, relationship: 'knows' };

      linkCanvasObjectFn(link, ctx, 0.5); // Scale below 0.8

      expect(ctx.fillText).not.toHaveBeenCalled();
    });

    it('linkCanvasObject renders label when scale >= 0.8 and no hover', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      const container = document.createElement('div');
      Object.defineProperty(container, 'offsetWidth', { value: 800 });
      Object.defineProperty(container, 'offsetHeight', { value: 600 });

      act(() => {
        result.current.initializeGraph(container);
      });

      const linkCanvasObjectFn = currentMockInstance.linkCanvasObject.mock.calls[0][0];
      const ctx = {
        measureText: vi.fn().mockReturnValue({ width: 30 }),
        beginPath: vi.fn(),
        fill: vi.fn(),
        stroke: vi.fn(),
        fillText: vi.fn(),
        roundRect: vi.fn(),
        font: '',
        textAlign: '',
        textBaseline: '',
        fillStyle: '',
        strokeStyle: '',
        lineWidth: 0,
      };

      const link = {
        source: { id: 'a', x: 10, y: 20 },
        target: { id: 'b', x: 110, y: 120 },
        relationship: 'knows',
      };

      linkCanvasObjectFn(link, ctx, 1.0);

      expect(ctx.fillText).toHaveBeenCalledWith('knows', 60, 70);
    });

    it('linkCanvasObject skips non-highlighted links during hover', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      const container = document.createElement('div');
      Object.defineProperty(container, 'offsetWidth', { value: 800 });
      Object.defineProperty(container, 'offsetHeight', { value: 600 });

      act(() => {
        useEntityGraphStore.setState({
          hoveredNodeId: 'a',
          highlightedLinkKeys: new Set(['a--b']),
        });
      });

      act(() => {
        result.current.initializeGraph(container);
      });

      const linkCanvasObjectFn = currentMockInstance.linkCanvasObject.mock.calls[0][0];
      const ctx = { fillText: vi.fn(), measureText: vi.fn().mockReturnValue({ width: 30 }) };

      const nonHighlightedLink = {
        source: { id: 'c', x: 0, y: 0 },
        target: { id: 'd', x: 100, y: 100 },
        relationship: 'manages',
      };

      linkCanvasObjectFn(nonHighlightedLink, ctx, 1.0);

      expect(ctx.fillText).not.toHaveBeenCalled();
    });

    it('linkCanvasObject skips when source has no x coordinate', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      const container = document.createElement('div');
      Object.defineProperty(container, 'offsetWidth', { value: 800 });
      Object.defineProperty(container, 'offsetHeight', { value: 600 });

      act(() => {
        result.current.initializeGraph(container);
      });

      const linkCanvasObjectFn = currentMockInstance.linkCanvasObject.mock.calls[0][0];
      const ctx = { fillText: vi.fn() };

      const link = {
        source: { id: 'a' }, // No x
        target: { id: 'b', x: 100 },
        relationship: 'knows',
      };

      linkCanvasObjectFn(link, ctx, 1.0);

      expect(ctx.fillText).not.toHaveBeenCalled();
    });

    it('linkCanvasObject uses fallback roundRect when not available', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      const container = document.createElement('div');
      Object.defineProperty(container, 'offsetWidth', { value: 800 });
      Object.defineProperty(container, 'offsetHeight', { value: 600 });

      act(() => {
        result.current.initializeGraph(container);
      });

      const linkCanvasObjectFn = currentMockInstance.linkCanvasObject.mock.calls[0][0];
      const ctx = {
        measureText: vi.fn().mockReturnValue({ width: 30 }),
        beginPath: vi.fn(),
        fill: vi.fn(),
        stroke: vi.fn(),
        fillText: vi.fn(),
        roundRect: undefined, // No roundRect
        moveTo: vi.fn(),
        lineTo: vi.fn(),
        quadraticCurveTo: vi.fn(),
        font: '',
        textAlign: '',
        textBaseline: '',
        fillStyle: '',
        strokeStyle: '',
        lineWidth: 0,
      };

      const link = {
        source: { id: 'a', x: 10, y: 20 },
        target: { id: 'b', x: 110, y: 120 },
        relationship: 'knows',
      };

      linkCanvasObjectFn(link, ctx, 1.0);

      expect(ctx.moveTo).toHaveBeenCalled();
      expect(ctx.quadraticCurveTo).toHaveBeenCalled();
      expect(ctx.fillText).toHaveBeenCalled();
    });

    it('invokes nodeLabel callback', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      const container = document.createElement('div');
      Object.defineProperty(container, 'offsetWidth', { value: 800 });
      Object.defineProperty(container, 'offsetHeight', { value: 600 });

      act(() => {
        result.current.initializeGraph(container);
      });

      const nodeLabelFn = currentMockInstance.nodeLabel.mock.calls[0][0];
      const node = {
        id: 'n1',
        name: 'Alice',
        type: 'person',
        attributes: { role: 'Engineer' },
      };

      const label = nodeLabelFn(node);
      expect(label).toContain('Alice');
      expect(label).toContain('person');
      expect(label).toContain('role');
      expect(label).toContain('Engineer');
    });

    it('nodeLabel handles empty attributes', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      const container = document.createElement('div');
      Object.defineProperty(container, 'offsetWidth', { value: 800 });
      Object.defineProperty(container, 'offsetHeight', { value: 600 });

      act(() => {
        result.current.initializeGraph(container);
      });

      const nodeLabelFn = currentMockInstance.nodeLabel.mock.calls[0][0];
      const node = { id: 'n1', name: 'Alice', type: 'person', attributes: {} };

      const label = nodeLabelFn(node);
      expect(label).toContain('Alice');
      expect(label).toContain('person');
    });

    it('invokes linkCanvasObjectMode callback', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      const container = document.createElement('div');
      Object.defineProperty(container, 'offsetWidth', { value: 800 });
      Object.defineProperty(container, 'offsetHeight', { value: 600 });

      act(() => {
        result.current.initializeGraph(container);
      });

      const linkCanvasObjectModeFn = currentMockInstance.linkCanvasObjectMode.mock.calls[0][0];
      expect(linkCanvasObjectModeFn()).toBe('after');
    });

    it('nodeCanvasObject renders in dark mode', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      const container = document.createElement('div');
      Object.defineProperty(container, 'offsetWidth', { value: 800 });
      Object.defineProperty(container, 'offsetHeight', { value: 600 });

      act(() => {
        useEntityGraphStore.setState({
          isDarkMode: true,
          filteredGraphData: { nodes: [{ id: 'a', name: 'A', type: 'person', attributes: {} }], links: [] },
        });
      });

      act(() => {
        result.current.initializeGraph(container);
      });

      const nodeCanvasObjectFn = currentMockInstance.nodeCanvasObject.mock.calls[0][0];
      const ctx = {
        createRadialGradient: vi.fn().mockReturnValue({ addColorStop: vi.fn() }),
        beginPath: vi.fn(),
        arc: vi.fn(),
        fill: vi.fn(),
        stroke: vi.fn(),
        fillText: vi.fn(),
        measureText: vi.fn().mockReturnValue({ width: 20 }),
        roundRect: vi.fn(),
        fillStyle: '',
        strokeStyle: '',
        lineWidth: 0,
        textAlign: '',
        textBaseline: '',
        font: '',
        globalAlpha: 1,
      };

      const node = { id: 'a', name: 'A', type: 'person', attributes: {}, x: 10, y: 20 };
      nodeCanvasObjectFn(node, ctx, 1.0);

      // Dark mode label text color
      expect(ctx.fillStyle).toBe('#e0e0e0');
    });

    it('sets error when ForceGraph initialization throws', () => {
      const { result } = renderHook(() => useEntityGraphStore());
      const container = document.createElement('div');
      Object.defineProperty(container, 'offsetWidth', { value: 800 });
      Object.defineProperty(container, 'offsetHeight', { value: 600 });

      // Make the ForceGraph factory throw
      const mockedForceGraph = vi.mocked(ForceGraph2D);
      mockedForceGraph.mockImplementationOnce(() => {
        throw new Error('Canvas not supported');
      });

      act(() => {
        result.current.initializeGraph(container);
      });

      expect(result.current.error).toBe('Failed to initialize graph visualization');
    });
  });
});
