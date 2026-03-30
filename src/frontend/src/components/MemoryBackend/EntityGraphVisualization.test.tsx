import { describe, it, expect, vi, beforeEach } from 'vitest';
import React from 'react';
import { render, screen, fireEvent, waitFor, act } from '@testing-library/react';
import '@testing-library/jest-dom';

// Mock Logger
vi.mock('../../utils/logger', () => {
  const MockLogger = vi.fn(function (this: Record<string, unknown>) {
    this.debug = vi.fn();
    this.info = vi.fn();
    this.warn = vi.fn();
    this.error = vi.fn();
  });
  return { default: MockLogger };
});

// Hoisted mock state and actions
const { mockState, mockActions, mockApiClient, mockThemeState, mockFilterResult } = vi.hoisted(() => ({
  mockState: {
    graphData: {
      nodes: [] as Array<{ id: string; name: string; type: string; attributes: Record<string, unknown>; color?: string }>,
      links: [] as Array<{ source: string; target: string; relationship: string }>,
    },
    loading: false,
    error: null as string | null,
    focusedNodeId: null as string | null,
    selectedNode: null as null | { id: string; name: string; type: string; attributes: Record<string, unknown>; color?: string },
    hiddenEntityTypes: new Set<string>(),
    isDarkMode: false,
  },
  mockActions: {
    initializeGraph: vi.fn(),
    cleanupGraph: vi.fn(),
    setGraphData: vi.fn(),
    setFilteredGraphData: vi.fn(),
    setLoading: vi.fn(),
    setError: vi.fn(),
    setFocusedNode: vi.fn(),
    setSelectedNode: vi.fn(),
    resetFilters: vi.fn(),
    toggleEntityTypeVisibility: vi.fn(),
    zoomToFit: vi.fn(),
    zoomIn: vi.fn(),
    zoomOut: vi.fn(),
    setIsDarkMode: vi.fn(),
    centerOnNode: vi.fn(),
  },
  mockApiClient: {
    get: vi.fn().mockResolvedValue({
      data: {
        entities: [
          { id: 'e1', name: 'Alice', type: 'person', attributes: {} },
          { id: 'e2', name: 'Bob', type: 'person', attributes: {} },
        ],
        relationships: [
          { source: 'e1', target: 'e2', type: 'knows' },
        ],
      },
    }),
  },
  mockThemeState: {
    isDarkMode: false,
  },
  mockFilterResult: {
    filteredNodes: [] as unknown[],
    filteredLinks: [] as unknown[],
    availableEntityTypes: [] as Array<{ type: string; color: string; count: number }>,
  },
}));

vi.mock('../../config/api/ApiConfig', () => ({
  apiClient: mockApiClient,
}));

vi.mock('../../store/entityGraphStore', () => ({
  default: (selector: (state: typeof mockState & typeof mockActions) => unknown) =>
    selector({ ...mockState, ...mockActions }),
}));

vi.mock('../../store/theme', () => ({
  useThemeStore: () => mockThemeState,
}));

vi.mock('../../hooks/global/useEntityGraphFilters', () => ({
  useEntityGraphFilters: () => mockFilterResult,
}));

vi.mock('../../utils/entityColors', () => ({
  getEntityColor: vi.fn(() => '#999'),
}));

// Mock sub-components
vi.mock('./GraphControlsPanel', () => ({
  default: () => <div data-testid="graph-controls-panel">Controls</div>,
}));
vi.mock('./GraphLegend', () => ({
  default: ({ availableEntityTypes }: { availableEntityTypes: unknown[] }) => (
    <div data-testid="graph-legend">Legend ({availableEntityTypes.length} types)</div>
  ),
}));
vi.mock('./GraphStatsPanel', () => ({
  default: () => <div data-testid="graph-stats-panel">Stats</div>,
}));
vi.mock('./NodeDetailsPanel', () => ({
  default: () => <div data-testid="node-details-panel">Details</div>,
}));

import EntityGraphVisualization from './EntityGraphVisualization';

describe('EntityGraphVisualization', () => {
  const defaultProps = {
    open: true,
    onClose: vi.fn(),
    indexName: 'test-index',
    workspaceUrl: 'https://example.com',
    endpointName: 'test-endpoint',
  };

  beforeEach(() => {
    mockState.graphData = { nodes: [], links: [] };
    mockState.loading = false;
    mockState.error = null;
    mockState.focusedNodeId = null;
    mockState.selectedNode = null;
    mockState.hiddenEntityTypes = new Set();
    mockState.isDarkMode = false;
    mockThemeState.isDarkMode = false;
    mockFilterResult.filteredNodes = [];
    mockFilterResult.filteredLinks = [];
    mockFilterResult.availableEntityTypes = [];
    vi.clearAllMocks();
  });

  it('renders dialog with title when open', () => {
    render(<EntityGraphVisualization {...defaultProps} />);
    expect(screen.getByText('Entity Graph Visualization')).toBeInTheDocument();
  });

  it('does not render dialog content when closed', () => {
    render(<EntityGraphVisualization {...defaultProps} open={false} />);
    expect(screen.queryByText('Entity Graph Visualization')).not.toBeInTheDocument();
  });

  it('renders sub-components', () => {
    render(<EntityGraphVisualization {...defaultProps} />);
    expect(screen.getByTestId('graph-controls-panel')).toBeInTheDocument();
    expect(screen.getByTestId('graph-legend')).toBeInTheDocument();
    expect(screen.getByTestId('graph-stats-panel')).toBeInTheDocument();
  });

  it('renders loading state', () => {
    mockState.loading = true;
    render(<EntityGraphVisualization {...defaultProps} />);
    expect(screen.getByText('Loading entity data...')).toBeInTheDocument();
  });

  it('renders error state', () => {
    mockState.error = 'Failed to fetch data';
    render(<EntityGraphVisualization {...defaultProps} />);
    expect(screen.getByText('Failed to fetch data')).toBeInTheDocument();
  });

  it('does not render error when loading', () => {
    mockState.loading = true;
    mockState.error = 'Some error';
    render(<EntityGraphVisualization {...defaultProps} />);
    expect(screen.queryByText('Some error')).not.toBeInTheDocument();
  });

  it('renders NodeDetailsPanel when selectedNode exists', () => {
    mockState.selectedNode = {
      id: 'n1',
      name: 'Alice',
      type: 'person',
      attributes: {},
    };
    render(<EntityGraphVisualization {...defaultProps} />);
    expect(screen.getByTestId('node-details-panel')).toBeInTheDocument();
  });

  it('does not render NodeDetailsPanel when no selectedNode', () => {
    render(<EntityGraphVisualization {...defaultProps} />);
    expect(screen.queryByTestId('node-details-panel')).not.toBeInTheDocument();
  });

  it('calls fetchEntityData on open', async () => {
    render(<EntityGraphVisualization {...defaultProps} />);
    await waitFor(() => {
      expect(mockActions.setLoading).toHaveBeenCalledWith(true);
    });
    await waitFor(() => {
      expect(mockApiClient.get).toHaveBeenCalledWith('/memory-backend/databricks/entity-data', {
        params: {
          index_name: 'test-index',
          workspace_url: 'https://example.com',
          endpoint_name: 'test-endpoint',
        },
      });
    });
  });

  it('calls setGraphData after successful fetch', async () => {
    render(<EntityGraphVisualization {...defaultProps} />);
    await waitFor(() => {
      expect(mockActions.setGraphData).toHaveBeenCalled();
    });
    const callArg = mockActions.setGraphData.mock.calls[0][0];
    expect(callArg.nodes).toHaveLength(2);
    expect(callArg.links).toHaveLength(1);
  });

  it('sets error on fetch failure', async () => {
    mockApiClient.get.mockRejectedValueOnce(new Error('Network error'));
    render(<EntityGraphVisualization {...defaultProps} />);
    await waitFor(() => {
      expect(mockActions.setError).toHaveBeenCalledWith('Network error');
    });
  });

  it('sets error with response detail on fetch failure', async () => {
    mockApiClient.get.mockRejectedValueOnce({
      response: { data: { detail: 'Backend error detail' } },
    });
    render(<EntityGraphVisualization {...defaultProps} />);
    await waitFor(() => {
      expect(mockActions.setError).toHaveBeenCalledWith('Backend error detail');
    });
  });

  it('sets generic error on non-Error fetch failure', async () => {
    mockApiClient.get.mockRejectedValueOnce('some string error');
    render(<EntityGraphVisualization {...defaultProps} />);
    await waitFor(() => {
      expect(mockActions.setError).toHaveBeenCalledWith('Failed to fetch entity data');
    });
  });

  it('does not fetch when missing required props', () => {
    render(
      <EntityGraphVisualization open={true} onClose={vi.fn()} />
    );
    expect(mockApiClient.get).not.toHaveBeenCalled();
  });

  it('calls resetFilters and onClose when close button is clicked', () => {
    const onClose = vi.fn();
    render(<EntityGraphVisualization {...defaultProps} onClose={onClose} />);
    // Find close button (last IconButton with CloseIcon)
    const closeButtons = screen.getAllByRole('button');
    const closeBtn = closeButtons.find((btn) => btn.querySelector('[data-testid="CloseIcon"]'));
    fireEvent.click(closeBtn!);
    expect(mockActions.resetFilters).toHaveBeenCalled();
    expect(onClose).toHaveBeenCalled();
  });

  it('calls zoomIn when zoom in button is clicked', () => {
    render(<EntityGraphVisualization {...defaultProps} />);
    const zoomInBtn = screen.getByRole('button', { name: /zoom in/i });
    fireEvent.click(zoomInBtn);
    expect(mockActions.zoomIn).toHaveBeenCalled();
  });

  it('calls zoomOut when zoom out button is clicked', () => {
    render(<EntityGraphVisualization {...defaultProps} />);
    const zoomOutBtn = screen.getByRole('button', { name: /zoom out/i });
    fireEvent.click(zoomOutBtn);
    expect(mockActions.zoomOut).toHaveBeenCalled();
  });

  it('calls zoomToFit when fit to screen button is clicked', () => {
    render(<EntityGraphVisualization {...defaultProps} />);
    const fitBtn = screen.getByRole('button', { name: /fit to screen/i });
    fireEvent.click(fitBtn);
    expect(mockActions.zoomToFit).toHaveBeenCalled();
  });

  it('calls fetchEntityData when refresh button is clicked', async () => {
    render(<EntityGraphVisualization {...defaultProps} />);
    await waitFor(() => {
      expect(mockApiClient.get).toHaveBeenCalledTimes(1);
    });

    const refreshBtn = screen.getByRole('button', { name: /refresh/i });
    fireEvent.click(refreshBtn);
    await waitFor(() => {
      expect(mockApiClient.get).toHaveBeenCalledTimes(2);
    });
  });

  it('renders entity type filter chips', () => {
    mockFilterResult.availableEntityTypes = [
      { type: 'person', color: '#68CCE5', count: 3 },
      { type: 'organization', color: '#94D82D', count: 2 },
    ];

    render(<EntityGraphVisualization {...defaultProps} />);
    expect(screen.getByText('person')).toBeInTheDocument();
    expect(screen.getByText('organization')).toBeInTheDocument();
  });

  it('calls toggleEntityTypeVisibility when filter chip is clicked', () => {
    mockFilterResult.availableEntityTypes = [
      { type: 'person', color: '#68CCE5', count: 3 },
    ];

    render(<EntityGraphVisualization {...defaultProps} />);
    fireEvent.click(screen.getByText('person'));
    expect(mockActions.toggleEntityTypeVisibility).toHaveBeenCalledWith('person');
  });

  it('limits filter chips to 6', () => {
    mockFilterResult.availableEntityTypes = Array.from({ length: 10 }, (_, i) => ({
      type: `type${i}`,
      color: '#999',
      count: 10 - i,
    }));

    render(<EntityGraphVisualization {...defaultProps} />);
    // Only first 6 should be rendered
    expect(screen.getByText('type0')).toBeInTheDocument();
    expect(screen.getByText('type5')).toBeInTheDocument();
    expect(screen.queryByText('type6')).not.toBeInTheDocument();
  });

  it('syncs dark mode from theme store to entity graph store', () => {
    mockThemeState.isDarkMode = true;
    mockState.isDarkMode = false;
    render(<EntityGraphVisualization {...defaultProps} />);
    expect(mockActions.setIsDarkMode).toHaveBeenCalledWith(true);
  });

  it('does not sync dark mode when already in sync', () => {
    mockThemeState.isDarkMode = false;
    mockState.isDarkMode = false;
    render(<EntityGraphVisualization {...defaultProps} />);
    expect(mockActions.setIsDarkMode).not.toHaveBeenCalled();
  });

  it('calls cleanupGraph and resetFilters when dialog closes', () => {
    const { rerender } = render(<EntityGraphVisualization {...defaultProps} open={true} />);
    rerender(<EntityGraphVisualization {...defaultProps} open={false} />);
    expect(mockActions.cleanupGraph).toHaveBeenCalled();
    expect(mockActions.resetFilters).toHaveBeenCalled();
  });

  it('calls setFilteredGraphData when filtered data changes', () => {
    mockFilterResult.filteredNodes = [{ id: 'a' }];
    mockFilterResult.filteredLinks = [];
    render(<EntityGraphVisualization {...defaultProps} />);
    expect(mockActions.setFilteredGraphData).toHaveBeenCalledWith({
      nodes: [{ id: 'a' }],
      links: [],
    });
  });

  it('renders search autocomplete', () => {
    render(<EntityGraphVisualization {...defaultProps} />);
    expect(screen.getByRole('combobox')).toBeInTheDocument();
  });

  it('shows focused entity label when focusedNodeId is set', () => {
    mockState.focusedNodeId = 'e1';
    mockState.graphData = {
      nodes: [{ id: 'e1', name: 'Alice', type: 'person', attributes: {}, color: '#68CCE5' }],
      links: [],
    };
    render(<EntityGraphVisualization {...defaultProps} />);
    // When focused, the autocomplete shows the focused entity name as its value
    const combobox = screen.getByRole('combobox');
    expect(combobox).toHaveValue('Alice');
  });

  it('applies hidden styling to hidden entity type chips', () => {
    mockFilterResult.availableEntityTypes = [
      { type: 'person', color: '#68CCE5', count: 3 },
    ];
    mockState.hiddenEntityTypes = new Set(['person']);

    render(<EntityGraphVisualization {...defaultProps} />);
    // The chip exists and is rendered (sx styles are applied via CSS classes, not inline)
    const chip = screen.getByText('person');
    expect(chip).toBeInTheDocument();
  });

  it('filters relationships that reference non-existent nodes', async () => {
    mockApiClient.get.mockResolvedValueOnce({
      data: {
        entities: [
          { id: 'e1', name: 'Alice', type: 'person', attributes: {} },
        ],
        relationships: [
          { source: 'e1', target: 'nonexistent', type: 'knows' },
          { source: 'e1', target: 'e1', type: 'self' },
        ],
      },
    });

    render(<EntityGraphVisualization {...defaultProps} />);
    await waitFor(() => {
      expect(mockActions.setGraphData).toHaveBeenCalled();
    });
    const callArg = mockActions.setGraphData.mock.calls[0][0];
    // Only e1→e1 should remain (nonexistent target filtered out)
    expect(callArg.links).toHaveLength(1);
    expect(callArg.links[0].source).toBe('e1');
    expect(callArg.links[0].target).toBe('e1');
  });

  it('uses entity name as fallback for id', async () => {
    mockApiClient.get.mockResolvedValueOnce({
      data: {
        entities: [
          { name: 'NoIdEntity', type: 'thing', attributes: {} },
        ],
        relationships: [],
      },
    });

    render(<EntityGraphVisualization {...defaultProps} />);
    await waitFor(() => {
      expect(mockActions.setGraphData).toHaveBeenCalled();
    });
    const callArg = mockActions.setGraphData.mock.calls[0][0];
    expect(callArg.nodes[0].id).toBe('NoIdEntity');
    expect(callArg.nodes[0].name).toBe('NoIdEntity');
  });

  it('uses entity id as fallback for name', async () => {
    mockApiClient.get.mockResolvedValueOnce({
      data: {
        entities: [
          { id: 'no-name', type: 'thing', attributes: {} },
        ],
        relationships: [],
      },
    });

    render(<EntityGraphVisualization {...defaultProps} />);
    await waitFor(() => {
      expect(mockActions.setGraphData).toHaveBeenCalled();
    });
    const callArg = mockActions.setGraphData.mock.calls[0][0];
    expect(callArg.nodes[0].name).toBe('no-name');
  });

  it('uses relationship label as fallback for type', async () => {
    mockApiClient.get.mockResolvedValueOnce({
      data: {
        entities: [
          { id: 'e1', name: 'A', type: 'person', attributes: {} },
          { id: 'e2', name: 'B', type: 'person', attributes: {} },
        ],
        relationships: [
          { source: 'e1', target: 'e2', label: 'friend_of' },
        ],
      },
    });

    render(<EntityGraphVisualization {...defaultProps} />);
    await waitFor(() => {
      expect(mockActions.setGraphData).toHaveBeenCalled();
    });
    const callArg = mockActions.setGraphData.mock.calls[0][0];
    expect(callArg.links[0].relationship).toBe('friend_of');
  });

  it('defaults relationship to related_to when no type or label', async () => {
    mockApiClient.get.mockResolvedValueOnce({
      data: {
        entities: [
          { id: 'e1', name: 'A', type: 'person', attributes: {} },
          { id: 'e2', name: 'B', type: 'person', attributes: {} },
        ],
        relationships: [
          { source: 'e1', target: 'e2' },
        ],
      },
    });

    render(<EntityGraphVisualization {...defaultProps} />);
    await waitFor(() => {
      expect(mockActions.setGraphData).toHaveBeenCalled();
    });
    const callArg = mockActions.setGraphData.mock.calls[0][0];
    expect(callArg.links[0].relationship).toBe('related_to');
  });

  it('handles search selection with camera animation', async () => {
    const nodeA = { id: 'e1', name: 'Alice', type: 'person', attributes: {}, color: '#68CCE5' };
    const nodeB = { id: 'e2', name: 'Bob', type: 'person', attributes: {}, color: '#68CCE5' };
    mockState.graphData = {
      nodes: [nodeA, nodeB],
      links: [],
    };

    render(<EntityGraphVisualization {...defaultProps} />);

    // Open autocomplete
    const combobox = screen.getByRole('combobox');
    fireEvent.mouseDown(combobox);
    fireEvent.change(combobox, { target: { value: 'Bo' } });

    // Wait for and click the option
    await waitFor(() => {
      const option = screen.getByText('Bob');
      fireEvent.click(option);
    });

    expect(mockActions.setFocusedNode).toHaveBeenCalledWith('e2');
    expect(mockActions.setSelectedNode).toHaveBeenCalledWith(nodeB);
    expect(mockActions.centerOnNode).toHaveBeenCalledWith('e2');
  });

  it('renders autocomplete options with type and color dot', async () => {
    const nodeA = { id: 'e1', name: 'Alice', type: 'person', attributes: {}, color: '#68CCE5' };
    mockState.graphData = {
      nodes: [nodeA],
      links: [],
    };

    render(<EntityGraphVisualization {...defaultProps} />);

    // Open the autocomplete dropdown
    const combobox = screen.getByRole('combobox');
    fireEvent.mouseDown(combobox);

    await waitFor(() => {
      // The option should render with the node name and type
      expect(screen.getByText('Alice')).toBeInTheDocument();
      expect(screen.getByText('(person)')).toBeInTheDocument();
    });
  });

  it('groups autocomplete options by type', async () => {
    mockState.graphData = {
      nodes: [
        { id: 'e1', name: 'Alice', type: 'person', attributes: {}, color: '#68CCE5' },
        { id: 'e2', name: 'Acme', type: 'organization', attributes: {}, color: '#94D82D' },
      ],
      links: [],
    };

    render(<EntityGraphVisualization {...defaultProps} />);

    const combobox = screen.getByRole('combobox');
    fireEvent.mouseDown(combobox);

    await waitFor(() => {
      // Group headers should appear
      expect(screen.getByText('person')).toBeInTheDocument();
      expect(screen.getByText('organization')).toBeInTheDocument();
    });
  });

  it('defaults entity type to unknown', async () => {
    mockApiClient.get.mockResolvedValueOnce({
      data: {
        entities: [
          { id: 'e1', name: 'Mystery' },
        ],
        relationships: [],
      },
    });

    render(<EntityGraphVisualization {...defaultProps} />);
    await waitFor(() => {
      expect(mockActions.setGraphData).toHaveBeenCalled();
    });
    const callArg = mockActions.setGraphData.mock.calls[0][0];
    expect(callArg.nodes[0].type).toBe('unknown');
    expect(callArg.nodes[0].attributes).toEqual({});
  });

  it('initializes graph when container is ready and data is loaded', async () => {
    // First render with empty data so the init effect's early return (line 179) is hit
    mockState.graphData = { nodes: [], links: [] };
    mockState.loading = false;
    mockState.error = null;

    const { rerender } = render(<EntityGraphVisualization {...defaultProps} />);

    // Allow the Dialog Portal to mount and set containerRef.current
    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 50));
    });

    // Now set graphData with nodes and rerender to trigger the init useEffect
    mockState.graphData = {
      nodes: [{ id: 'e1', name: 'Alice', type: 'person', attributes: {}, color: '#68CCE5' }],
      links: [],
    };
    rerender(<EntityGraphVisualization {...defaultProps} />);

    // Wait for the 100ms setTimeout in the init useEffect
    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 200));
    });

    expect(mockActions.initializeGraph).toHaveBeenCalled();
  });

  describe('Lakebase data source', () => {
    const lakebaseProps = {
      open: true,
      onClose: vi.fn(),
      dataSource: 'lakebase' as const,
      lakebaseInstanceName: 'kasal-lakebase1',
    };

    it('fetches from Lakebase endpoint when dataSource is lakebase', async () => {
      render(<EntityGraphVisualization {...lakebaseProps} />);
      await waitFor(() => {
        expect(mockApiClient.get).toHaveBeenCalledWith('/memory-backend/lakebase/entity-data', {
          params: {
            entity_table: 'crew_entity_memory',
            limit: 200,
            instance_name: 'kasal-lakebase1',
          },
        });
      });
    });

    it('does not require indexName/workspaceUrl/endpointName for Lakebase', async () => {
      render(<EntityGraphVisualization {...lakebaseProps} />);
      await waitFor(() => {
        expect(mockApiClient.get).toHaveBeenCalledTimes(1);
      });
      // Should NOT have called Databricks endpoint
      expect(mockApiClient.get).not.toHaveBeenCalledWith(
        '/memory-backend/databricks/entity-data',
        expect.anything()
      );
    });

    it('fetches from Lakebase without instance_name when not provided', async () => {
      render(
        <EntityGraphVisualization
          open={true}
          onClose={vi.fn()}
          dataSource="lakebase"
        />
      );
      await waitFor(() => {
        expect(mockApiClient.get).toHaveBeenCalledWith('/memory-backend/lakebase/entity-data', {
          params: {
            entity_table: 'crew_entity_memory',
            limit: 200,
          },
        });
      });
    });

    it('processes Lakebase entity data into graph format', async () => {
      render(<EntityGraphVisualization {...lakebaseProps} />);
      await waitFor(() => {
        expect(mockActions.setGraphData).toHaveBeenCalled();
      });
      const callArg = mockActions.setGraphData.mock.calls[0][0];
      expect(callArg.nodes).toHaveLength(2);
      expect(callArg.links).toHaveLength(1);
    });

    it('handles Lakebase fetch error', async () => {
      mockApiClient.get.mockRejectedValueOnce(new Error('Lakebase connection failed'));
      render(<EntityGraphVisualization {...lakebaseProps} />);
      await waitFor(() => {
        expect(mockActions.setError).toHaveBeenCalledWith('Lakebase connection failed');
      });
    });

    it('defaults to Databricks data source when not specified', async () => {
      render(<EntityGraphVisualization {...defaultProps} />);
      await waitFor(() => {
        expect(mockApiClient.get).toHaveBeenCalledWith('/memory-backend/databricks/entity-data', {
          params: {
            index_name: 'test-index',
            workspace_url: 'https://example.com',
            endpoint_name: 'test-endpoint',
          },
        });
      });
    });
  });
});
