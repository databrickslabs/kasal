import { describe, it, expect, vi, beforeEach } from 'vitest';
import React from 'react';
import { render, screen } from '@testing-library/react';
import '@testing-library/jest-dom';

// Hoisted mock state
const { mockState } = vi.hoisted(() => ({
  mockState: {
    graphData: { nodes: [] as unknown[], links: [] as unknown[] },
    filteredGraphData: { nodes: [] as unknown[], links: [] as unknown[] },
    focusedNodeId: null as string | null,
    deduplicateNodes: false,
    loading: false,
    error: null as string | null,
  },
}));

vi.mock('../../store/entityGraphStore', () => ({
  default: (selector: (state: typeof mockState) => unknown) => selector(mockState),
}));

import GraphStatsPanel from './GraphStatsPanel';

describe('GraphStatsPanel', () => {
  beforeEach(() => {
    mockState.graphData = { nodes: [], links: [] };
    mockState.filteredGraphData = { nodes: [], links: [] };
    mockState.focusedNodeId = null;
    mockState.deduplicateNodes = false;
    mockState.loading = false;
    mockState.error = null;
  });

  it('returns null when loading', () => {
    mockState.loading = true;
    const { container } = render(<GraphStatsPanel />);
    expect(container.firstChild).toBeNull();
  });

  it('returns null when error', () => {
    mockState.error = 'Some error';
    const { container } = render(<GraphStatsPanel />);
    expect(container.firstChild).toBeNull();
  });

  it('renders node and link counts', () => {
    mockState.filteredGraphData = {
      nodes: [{ id: 'a' }, { id: 'b' }],
      links: [{ source: 'a', target: 'b' }],
    };
    mockState.graphData = {
      nodes: [{ id: 'a' }, { id: 'b' }],
      links: [{ source: 'a', target: 'b' }],
    };

    render(<GraphStatsPanel />);
    expect(screen.getByText('Nodes: 2')).toBeInTheDocument();
    expect(screen.getByText('Links: 1')).toBeInTheDocument();
  });

  it('shows total counts when filtered differs from total', () => {
    mockState.graphData = {
      nodes: [{ id: 'a' }, { id: 'b' }, { id: 'c' }],
      links: [{ source: 'a', target: 'b' }, { source: 'b', target: 'c' }],
    };
    mockState.filteredGraphData = {
      nodes: [{ id: 'a' }, { id: 'b' }],
      links: [{ source: 'a', target: 'b' }],
    };

    render(<GraphStatsPanel />);
    expect(screen.getByText(/Nodes: 2/)).toBeInTheDocument();
    expect(screen.getByText(/\/ 3/)).toBeInTheDocument();
    expect(screen.getByText(/Links: 1/)).toBeInTheDocument();
    expect(screen.getByText(/\/ 2/)).toBeInTheDocument();
  });

  it('shows Focused View chip when focusedNodeId is set', () => {
    mockState.focusedNodeId = 'a';
    mockState.filteredGraphData = { nodes: [{ id: 'a' }], links: [] };
    mockState.graphData = { nodes: [{ id: 'a' }], links: [] };

    render(<GraphStatsPanel />);
    expect(screen.getByText('Focused View')).toBeInTheDocument();
  });

  it('does not show Focused View chip when focusedNodeId is null', () => {
    mockState.filteredGraphData = { nodes: [{ id: 'a' }], links: [] };
    mockState.graphData = { nodes: [{ id: 'a' }], links: [] };

    render(<GraphStatsPanel />);
    expect(screen.queryByText('Focused View')).not.toBeInTheDocument();
  });

  it('shows Deduplicated chip when deduplicateNodes is true and not focused', () => {
    mockState.deduplicateNodes = true;
    mockState.filteredGraphData = { nodes: [{ id: 'a' }], links: [] };
    mockState.graphData = { nodes: [{ id: 'a' }], links: [] };

    render(<GraphStatsPanel />);
    expect(screen.getByText('Deduplicated')).toBeInTheDocument();
  });

  it('does not show Deduplicated chip when focusedNodeId is set even if dedup is on', () => {
    mockState.deduplicateNodes = true;
    mockState.focusedNodeId = 'a';
    mockState.filteredGraphData = { nodes: [{ id: 'a' }], links: [] };
    mockState.graphData = { nodes: [{ id: 'a' }], links: [] };

    render(<GraphStatsPanel />);
    expect(screen.queryByText('Deduplicated')).not.toBeInTheDocument();
  });
});
