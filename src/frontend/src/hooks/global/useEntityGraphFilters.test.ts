import { describe, it, expect, vi, beforeEach } from 'vitest';
import { renderHook } from '@testing-library/react';

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

// Mock entity colors
vi.mock('../../utils/entityColors', () => ({
  getEntityColor: vi.fn((type: string) => {
    const map: Record<string, string> = {
      person: '#68CCE5',
      organization: '#94D82D',
      unknown: '#C5C5C5',
    };
    return map[type] || '#C5C5C5';
  }),
}));

// Hoisted mock state
const { mockStoreState } = vi.hoisted(() => {
  return {
    mockStoreState: {
      graphData: { nodes: [] as Array<{ id: string; name: string; type: string; attributes: Record<string, unknown> }>, links: [] as Array<{ source: string; target: string; relationship: string }> },
      focusedNodeId: null as string | null,
      deduplicateNodes: false,
      showOrphanedNodes: false,
      hiddenEntityTypes: new Set<string>(),
    },
  };
});

// Mock the zustand store
vi.mock('../../store/entityGraphStore', () => ({
  default: (selector: (state: typeof mockStoreState) => unknown) => selector(mockStoreState),
}));

import { useEntityGraphFilters } from './useEntityGraphFilters';

describe('useEntityGraphFilters', () => {
  beforeEach(() => {
    mockStoreState.graphData = { nodes: [], links: [] };
    mockStoreState.focusedNodeId = null;
    mockStoreState.deduplicateNodes = false;
    mockStoreState.showOrphanedNodes = false;
    mockStoreState.hiddenEntityTypes = new Set<string>();
  });

  describe('availableEntityTypes', () => {
    it('returns empty array when no nodes', () => {
      const { result } = renderHook(() => useEntityGraphFilters());
      expect(result.current.availableEntityTypes).toEqual([]);
    });

    it('computes entity types with counts sorted by count descending', () => {
      mockStoreState.graphData = {
        nodes: [
          { id: '1', name: 'Alice', type: 'person', attributes: {} },
          { id: '2', name: 'Bob', type: 'person', attributes: {} },
          { id: '3', name: 'Acme', type: 'organization', attributes: {} },
          { id: '4', name: 'Charlie', type: 'person', attributes: {} },
        ],
        links: [],
      };

      const { result } = renderHook(() => useEntityGraphFilters());
      expect(result.current.availableEntityTypes).toEqual([
        { type: 'person', color: '#68CCE5', count: 3 },
        { type: 'organization', color: '#94D82D', count: 1 },
      ]);
    });

    it('normalizes type to lowercase', () => {
      mockStoreState.graphData = {
        nodes: [
          { id: '1', name: 'A', type: 'Person', attributes: {} },
          { id: '2', name: 'B', type: 'PERSON', attributes: {} },
        ],
        links: [],
      };

      const { result } = renderHook(() => useEntityGraphFilters());
      expect(result.current.availableEntityTypes).toEqual([
        { type: 'person', color: '#68CCE5', count: 2 },
      ]);
    });
  });

  describe('basic filtering (no filters active)', () => {
    it('returns all nodes and links when no filters active and all connected', () => {
      mockStoreState.graphData = {
        nodes: [
          { id: 'a', name: 'A', type: 'person', attributes: {} },
          { id: 'b', name: 'B', type: 'person', attributes: {} },
        ],
        links: [{ source: 'a', target: 'b', relationship: 'knows' }],
      };

      const { result } = renderHook(() => useEntityGraphFilters());
      expect(result.current.filteredNodes).toHaveLength(2);
      expect(result.current.filteredLinks).toHaveLength(1);
    });
  });

  describe('hidden entity types', () => {
    it('filters out nodes of hidden entity types', () => {
      mockStoreState.graphData = {
        nodes: [
          { id: 'a', name: 'A', type: 'person', attributes: {} },
          { id: 'b', name: 'B', type: 'organization', attributes: {} },
          { id: 'c', name: 'C', type: 'person', attributes: {} },
        ],
        links: [
          { source: 'a', target: 'b', relationship: 'works_at' },
          { source: 'a', target: 'c', relationship: 'knows' },
        ],
      };
      mockStoreState.hiddenEntityTypes = new Set(['organization']);

      const { result } = renderHook(() => useEntityGraphFilters());
      // 'b' is hidden (organization), so link a->b should be removed
      expect(result.current.filteredNodes.map((n) => n.id)).toEqual(['a', 'c']);
      expect(result.current.filteredLinks).toHaveLength(1);
      expect(result.current.filteredLinks[0].relationship).toBe('knows');
    });

    it('removes links connected to hidden node types', () => {
      mockStoreState.graphData = {
        nodes: [
          { id: 'a', name: 'A', type: 'person', attributes: {} },
          { id: 'b', name: 'B', type: 'organization', attributes: {} },
        ],
        links: [{ source: 'a', target: 'b', relationship: 'works_at' }],
      };
      mockStoreState.hiddenEntityTypes = new Set(['organization']);
      // showOrphanedNodes defaults to false, but after hiding 'b', 'a' is now orphaned
      // and connectedNodeIds will be empty set (size=0), so orphan filter won't apply
      mockStoreState.showOrphanedNodes = true;

      const { result } = renderHook(() => useEntityGraphFilters());
      expect(result.current.filteredNodes).toHaveLength(1);
      expect(result.current.filteredLinks).toHaveLength(0);
    });
  });

  describe('orphaned nodes removal', () => {
    it('removes orphaned nodes by default', () => {
      mockStoreState.graphData = {
        nodes: [
          { id: 'a', name: 'A', type: 'person', attributes: {} },
          { id: 'b', name: 'B', type: 'person', attributes: {} },
          { id: 'c', name: 'C', type: 'person', attributes: {} }, // orphaned
        ],
        links: [{ source: 'a', target: 'b', relationship: 'knows' }],
      };

      const { result } = renderHook(() => useEntityGraphFilters());
      expect(result.current.filteredNodes.map((n) => n.id)).toEqual(['a', 'b']);
    });

    it('keeps orphaned nodes when showOrphanedNodes is true', () => {
      mockStoreState.graphData = {
        nodes: [
          { id: 'a', name: 'A', type: 'person', attributes: {} },
          { id: 'b', name: 'B', type: 'person', attributes: {} },
          { id: 'c', name: 'C', type: 'person', attributes: {} }, // orphaned
        ],
        links: [{ source: 'a', target: 'b', relationship: 'knows' }],
      };
      mockStoreState.showOrphanedNodes = true;

      const { result } = renderHook(() => useEntityGraphFilters());
      expect(result.current.filteredNodes).toHaveLength(3);
    });

    it('keeps orphaned nodes when in focused mode', () => {
      mockStoreState.graphData = {
        nodes: [
          { id: 'a', name: 'A', type: 'person', attributes: {} },
          { id: 'b', name: 'B', type: 'person', attributes: {} },
          { id: 'c', name: 'C', type: 'person', attributes: {} },
        ],
        links: [{ source: 'a', target: 'b', relationship: 'knows' }],
      };
      mockStoreState.focusedNodeId = 'a';

      const { result } = renderHook(() => useEntityGraphFilters());
      // Focus filter is applied: only a and its neighbors (b)
      expect(result.current.filteredNodes.map((n) => n.id)).toEqual(['a', 'b']);
    });

    it('does not filter orphaned nodes when connectedNodeIds is empty', () => {
      // All nodes, zero links → connectedNodeIds.size == 0, skip orphan filter
      mockStoreState.graphData = {
        nodes: [
          { id: 'a', name: 'A', type: 'person', attributes: {} },
          { id: 'b', name: 'B', type: 'person', attributes: {} },
        ],
        links: [],
      };

      const { result } = renderHook(() => useEntityGraphFilters());
      expect(result.current.filteredNodes).toHaveLength(2);
    });
  });

  describe('focused node filtering', () => {
    it('filters to focused node and its direct neighbors', () => {
      mockStoreState.graphData = {
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
      };
      mockStoreState.showOrphanedNodes = true; // skip orphan removal
      mockStoreState.focusedNodeId = 'a';

      const { result } = renderHook(() => useEntityGraphFilters());
      const nodeIds = result.current.filteredNodes.map((n) => n.id);
      expect(nodeIds).toContain('a');
      expect(nodeIds).toContain('b');
      expect(nodeIds).toContain('c');
      expect(nodeIds).not.toContain('d');
      // Link c->d should be filtered out since d is not in the focused set
      expect(result.current.filteredLinks).toHaveLength(2);
    });

    it('handles focused node that does not exist', () => {
      mockStoreState.graphData = {
        nodes: [
          { id: 'a', name: 'A', type: 'person', attributes: {} },
          { id: 'b', name: 'B', type: 'person', attributes: {} },
        ],
        links: [{ source: 'a', target: 'b', relationship: 'knows' }],
      };
      mockStoreState.focusedNodeId = 'nonexistent';

      const { result } = renderHook(() => useEntityGraphFilters());
      // focusedNode not found → no filtering applied by focus, but orphan removed
      expect(result.current.filteredNodes).toHaveLength(2);
    });

    it('includes links where focused node is the target', () => {
      mockStoreState.graphData = {
        nodes: [
          { id: 'a', name: 'A', type: 'person', attributes: {} },
          { id: 'b', name: 'B', type: 'person', attributes: {} },
        ],
        links: [{ source: 'b', target: 'a', relationship: 'reports_to' }],
      };
      mockStoreState.showOrphanedNodes = true;
      mockStoreState.focusedNodeId = 'a';

      const { result } = renderHook(() => useEntityGraphFilters());
      expect(result.current.filteredNodes.map((n) => n.id)).toEqual(['a', 'b']);
      expect(result.current.filteredLinks).toHaveLength(1);
    });
  });

  describe('deduplication', () => {
    it('merges nodes with the same name', () => {
      mockStoreState.graphData = {
        nodes: [
          { id: 'a1', name: 'Alice', type: 'person', attributes: {} },
          { id: 'a2', name: 'Alice', type: 'person', attributes: {} },
          { id: 'b1', name: 'Bob', type: 'person', attributes: {} },
        ],
        links: [
          { source: 'a1', target: 'b1', relationship: 'knows' },
          { source: 'a2', target: 'b1', relationship: 'works_with' },
        ],
      };
      mockStoreState.deduplicateNodes = true;
      mockStoreState.showOrphanedNodes = true;

      const { result } = renderHook(() => useEntityGraphFilters());
      // a1 and a2 have same name "Alice" → deduplicated to canonical (a1)
      expect(result.current.filteredNodes).toHaveLength(2);
      // Both links now point from a1 to b1 but with different relationships
      expect(result.current.filteredLinks).toHaveLength(2);
    });

    it('removes self-links after deduplication', () => {
      mockStoreState.graphData = {
        nodes: [
          { id: 'a1', name: 'Alice', type: 'person', attributes: {} },
          { id: 'a2', name: 'Alice', type: 'person', attributes: {} },
        ],
        links: [
          { source: 'a1', target: 'a2', relationship: 'duplicate_of' },
        ],
      };
      mockStoreState.deduplicateNodes = true;
      mockStoreState.showOrphanedNodes = true;

      const { result } = renderHook(() => useEntityGraphFilters());
      expect(result.current.filteredNodes).toHaveLength(1);
      // Self-link (a1→a1) should be removed
      expect(result.current.filteredLinks).toHaveLength(0);
    });

    it('removes duplicate links after deduplication', () => {
      mockStoreState.graphData = {
        nodes: [
          { id: 'a1', name: 'Alice', type: 'person', attributes: {} },
          { id: 'a2', name: 'Alice', type: 'person', attributes: {} },
          { id: 'b1', name: 'Bob', type: 'person', attributes: {} },
        ],
        links: [
          { source: 'a1', target: 'b1', relationship: 'knows' },
          { source: 'a2', target: 'b1', relationship: 'knows' },
        ],
      };
      mockStoreState.deduplicateNodes = true;
      mockStoreState.showOrphanedNodes = true;

      const { result } = renderHook(() => useEntityGraphFilters());
      // Both links become a1→b1 with same relationship, deduped to 1
      expect(result.current.filteredLinks).toHaveLength(1);
    });

    it('does not deduplicate when focusedNodeId is set', () => {
      mockStoreState.graphData = {
        nodes: [
          { id: 'a1', name: 'Alice', type: 'person', attributes: {} },
          { id: 'a2', name: 'Alice', type: 'person', attributes: {} },
          { id: 'b1', name: 'Bob', type: 'person', attributes: {} },
        ],
        links: [
          { source: 'a1', target: 'b1', relationship: 'knows' },
          { source: 'a2', target: 'b1', relationship: 'works_with' },
        ],
      };
      mockStoreState.deduplicateNodes = true;
      mockStoreState.focusedNodeId = 'a1';
      mockStoreState.showOrphanedNodes = true;

      const { result } = renderHook(() => useEntityGraphFilters());
      // Focus takes priority over dedup. Focus on a1 → a1 + b1
      expect(result.current.filteredNodes).toHaveLength(2);
    });
  });

  describe('getNodeId helper (internal)', () => {
    it('handles object nodes in links (source/target as EntityNode)', () => {
      const nodeA = { id: 'a', name: 'A', type: 'person', attributes: {} };
      const nodeB = { id: 'b', name: 'B', type: 'person', attributes: {} };
      const nodeC = { id: 'c', name: 'C', type: 'person', attributes: {} };

      mockStoreState.graphData = {
        nodes: [nodeA, nodeB, nodeC],
        links: [
          // source/target as objects (as force-graph may replace strings with objects)
          { source: nodeA as unknown as string, target: nodeB as unknown as string, relationship: 'knows' },
          { source: 'b', target: 'c', relationship: 'works_with' },
        ],
      };
      mockStoreState.showOrphanedNodes = true;
      mockStoreState.hiddenEntityTypes = new Set(['person']);

      const { result } = renderHook(() => useEntityGraphFilters());
      // All nodes hidden since all are 'person'
      expect(result.current.filteredNodes).toHaveLength(0);
    });
  });

  describe('combined filters', () => {
    it('applies hidden types then orphan removal', () => {
      mockStoreState.graphData = {
        nodes: [
          { id: 'a', name: 'A', type: 'person', attributes: {} },
          { id: 'b', name: 'B', type: 'organization', attributes: {} },
          { id: 'c', name: 'C', type: 'person', attributes: {} },
        ],
        links: [
          { source: 'a', target: 'b', relationship: 'works_at' },
        ],
      };
      // Hide organization → removes b → link a→b removed → a is now orphaned, c is orphaned
      // connectedNodeIds will be empty since no links left, so orphan filter won't apply
      mockStoreState.hiddenEntityTypes = new Set(['organization']);

      const { result } = renderHook(() => useEntityGraphFilters());
      // No links left → connectedNodeIds.size == 0 → orphan filter skipped
      expect(result.current.filteredNodes).toHaveLength(2); // a and c remain
    });
  });

  describe('dedup edge cases', () => {
    it('falls back to original source/target when idMapping has no entry', () => {
      // Links referencing nodes not in the node array → idMapping.get() returns undefined
      // → fallback to originalSource/originalTarget (lines 112-113)
      mockStoreState.graphData = {
        nodes: [
          { id: 'a1', name: 'Alice', type: 'person', attributes: {} },
          { id: 'b1', name: 'Bob', type: 'person', attributes: {} },
        ],
        links: [
          { source: 'nonexistent', target: 'b1', relationship: 'knows' },
          { source: 'a1', target: 'ghost', relationship: 'haunts' },
        ],
      };
      mockStoreState.deduplicateNodes = true;
      mockStoreState.showOrphanedNodes = true;

      const { result } = renderHook(() => useEntityGraphFilters());
      // nonexistent/ghost are not in nodeIds, so links are filtered out
      expect(result.current.filteredLinks).toHaveLength(0);
    });

    it('uses related fallback for link key when relationship is undefined during dedup', () => {
      // Line 125: link.relationship || 'related' fallback in unique links key
      mockStoreState.graphData = {
        nodes: [
          { id: 'a', name: 'Alice', type: 'person', attributes: {} },
          { id: 'b', name: 'Bob', type: 'person', attributes: {} },
        ],
        links: [
          { source: 'a', target: 'b' } as { source: string; target: string; relationship: string },
        ],
      };
      mockStoreState.deduplicateNodes = true;
      mockStoreState.showOrphanedNodes = true;

      const { result } = renderHook(() => useEntityGraphFilters());
      expect(result.current.filteredLinks).toHaveLength(1);
    });
  });
});
