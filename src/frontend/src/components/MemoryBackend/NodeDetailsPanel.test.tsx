import { describe, it, expect, vi, beforeEach } from 'vitest';
import React from 'react';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';

// Hoisted mock state and functions
const { mockState, mockActions } = vi.hoisted(() => ({
  mockState: {
    selectedNode: null as null | {
      id: string;
      name: string;
      type: string;
      attributes: Record<string, unknown>;
      color?: string;
    },
    focusedNodeId: null as string | null,
    graphData: {
      nodes: [] as Array<{
        id: string;
        name: string;
        type: string;
        attributes: Record<string, unknown>;
        color?: string;
      }>,
      links: [] as Array<{ source: string; target: string; relationship?: string }>,
    },
  },
  mockActions: {
    setSelectedNode: vi.fn(),
    setFocusedNode: vi.fn(),
  },
}));

vi.mock('../../store/entityGraphStore', () => ({
  default: (selector: (state: typeof mockState & typeof mockActions) => unknown) =>
    selector({ ...mockState, ...mockActions }),
}));

vi.mock('../../utils/entityColors', () => ({
  getEntityColor: vi.fn(() => '#999'),
}));

import NodeDetailsPanel from './NodeDetailsPanel';

describe('NodeDetailsPanel', () => {
  beforeEach(() => {
    mockState.selectedNode = null;
    mockState.focusedNodeId = null;
    mockState.graphData = { nodes: [], links: [] };
    vi.clearAllMocks();
  });

  it('returns null when no node is selected', () => {
    const { container } = render(<NodeDetailsPanel />);
    expect(container.firstChild).toBeNull();
  });

  it('renders selected node name and type', () => {
    mockState.selectedNode = {
      id: 'n1',
      name: 'Alice',
      type: 'person',
      attributes: {},
      color: '#68CCE5',
    };
    mockState.graphData = {
      nodes: [mockState.selectedNode],
      links: [],
    };

    render(<NodeDetailsPanel />);
    expect(screen.getByText('Alice')).toBeInTheDocument();
    expect(screen.getByText('person')).toBeInTheDocument();
  });

  it('shows Focused chip when selectedNode is focused', () => {
    mockState.selectedNode = {
      id: 'n1',
      name: 'Alice',
      type: 'person',
      attributes: {},
      color: '#68CCE5',
    };
    mockState.focusedNodeId = 'n1';
    mockState.graphData = { nodes: [mockState.selectedNode], links: [] };

    render(<NodeDetailsPanel />);
    expect(screen.getByText('Focused')).toBeInTheDocument();
  });

  it('does not show Focused chip when node is not focused', () => {
    mockState.selectedNode = {
      id: 'n1',
      name: 'Alice',
      type: 'person',
      attributes: {},
    };
    mockState.graphData = { nodes: [mockState.selectedNode], links: [] };

    render(<NodeDetailsPanel />);
    expect(screen.queryByText('Focused')).not.toBeInTheDocument();
  });

  it('renders connection count', () => {
    mockState.selectedNode = {
      id: 'n1',
      name: 'Alice',
      type: 'person',
      attributes: {},
    };
    mockState.graphData = {
      nodes: [
        mockState.selectedNode,
        { id: 'n2', name: 'Bob', type: 'person', attributes: {} },
        { id: 'n3', name: 'Acme', type: 'organization', attributes: {} },
      ],
      links: [
        { source: 'n1', target: 'n2', relationship: 'knows' },
        { source: 'n1', target: 'n3', relationship: 'works_at' },
      ],
    };

    render(<NodeDetailsPanel />);
    expect(screen.getByText('2 relationships')).toBeInTheDocument();
  });

  it('renders node attributes', () => {
    mockState.selectedNode = {
      id: 'n1',
      name: 'Alice',
      type: 'person',
      attributes: { role: 'Engineer', team: 'Platform' },
    };
    mockState.graphData = { nodes: [mockState.selectedNode], links: [] };

    render(<NodeDetailsPanel />);
    expect(screen.getByText('Attributes')).toBeInTheDocument();
    expect(screen.getByText('role')).toBeInTheDocument();
    expect(screen.getByText('Engineer')).toBeInTheDocument();
    expect(screen.getByText('team')).toBeInTheDocument();
    expect(screen.getByText('Platform')).toBeInTheDocument();
  });

  it('does not show Attributes section when empty', () => {
    mockState.selectedNode = {
      id: 'n1',
      name: 'Alice',
      type: 'person',
      attributes: {},
    };
    mockState.graphData = { nodes: [mockState.selectedNode], links: [] };

    render(<NodeDetailsPanel />);
    expect(screen.queryByText('Attributes')).not.toBeInTheDocument();
  });

  it('renders connected entities', () => {
    const nodeA = { id: 'n1', name: 'Alice', type: 'person', attributes: {}, color: '#68CCE5' };
    const nodeB = { id: 'n2', name: 'Bob', type: 'person', attributes: {}, color: '#68CCE5' };

    mockState.selectedNode = nodeA;
    mockState.graphData = {
      nodes: [nodeA, nodeB],
      links: [{ source: 'n1', target: 'n2', relationship: 'knows' }],
    };

    render(<NodeDetailsPanel />);
    expect(screen.getByText('Connected Entities')).toBeInTheDocument();
    expect(screen.getByText('Bob')).toBeInTheDocument();
  });

  it('shows "No connections found" when no connected entities', () => {
    mockState.selectedNode = {
      id: 'n1',
      name: 'Alice',
      type: 'person',
      attributes: {},
    };
    mockState.graphData = {
      nodes: [mockState.selectedNode],
      links: [],
    };

    render(<NodeDetailsPanel />);
    expect(screen.getByText('No connections found')).toBeInTheDocument();
  });

  it('handles incoming connections correctly', () => {
    const nodeA = { id: 'n1', name: 'Alice', type: 'person', attributes: {} };
    const nodeB = { id: 'n2', name: 'Bob', type: 'person', attributes: {}, color: '#68CCE5' };

    mockState.selectedNode = nodeA;
    mockState.graphData = {
      nodes: [nodeA, nodeB],
      links: [{ source: 'n2', target: 'n1', relationship: 'reports_to' }],
    };

    render(<NodeDetailsPanel />);
    expect(screen.getByText('Bob')).toBeInTheDocument();
    // The arrow direction chip (← reports_to)
    expect(screen.getByText(/reports_to/)).toBeInTheDocument();
  });

  it('calls setSelectedNode and setFocusedNode when close button is clicked', () => {
    mockState.selectedNode = {
      id: 'n1',
      name: 'Alice',
      type: 'person',
      attributes: {},
    };
    mockState.graphData = { nodes: [mockState.selectedNode], links: [] };

    render(<NodeDetailsPanel />);
    // The close button is an IconButton with CloseIcon
    const closeButtons = screen.getAllByRole('button');
    const closeBtn = closeButtons.find((btn) => btn.querySelector('[data-testid="CloseIcon"]'));
    fireEvent.click(closeBtn!);
    expect(mockActions.setSelectedNode).toHaveBeenCalledWith(null);
    expect(mockActions.setFocusedNode).toHaveBeenCalledWith(null);
  });

  it('calls setSelectedNode and setFocusedNode when connected entity is clicked', () => {
    const nodeA = { id: 'n1', name: 'Alice', type: 'person', attributes: {} };
    const nodeB = { id: 'n2', name: 'Bob', type: 'person', attributes: {}, color: '#68CCE5' };

    mockState.selectedNode = nodeA;
    mockState.graphData = {
      nodes: [nodeA, nodeB],
      links: [{ source: 'n1', target: 'n2', relationship: 'knows' }],
    };

    render(<NodeDetailsPanel />);
    fireEvent.click(screen.getByText('Bob'));
    expect(mockActions.setSelectedNode).toHaveBeenCalledWith(nodeB);
    expect(mockActions.setFocusedNode).toHaveBeenCalledWith('n2');
  });

  it('shows Focus on Node button when node is not focused', () => {
    mockState.selectedNode = {
      id: 'n1',
      name: 'Alice',
      type: 'person',
      attributes: {},
    };
    mockState.graphData = { nodes: [mockState.selectedNode], links: [] };

    render(<NodeDetailsPanel />);
    expect(screen.getByText('Focus on Node')).toBeInTheDocument();
  });

  it('shows Show All button when node is focused', () => {
    mockState.selectedNode = {
      id: 'n1',
      name: 'Alice',
      type: 'person',
      attributes: {},
    };
    mockState.focusedNodeId = 'n1';
    mockState.graphData = { nodes: [mockState.selectedNode], links: [] };

    render(<NodeDetailsPanel />);
    expect(screen.getByText('Show All')).toBeInTheDocument();
    expect(screen.queryByText('Focus on Node')).not.toBeInTheDocument();
  });

  it('calls setFocusedNode when Focus on Node button is clicked', () => {
    mockState.selectedNode = {
      id: 'n1',
      name: 'Alice',
      type: 'person',
      attributes: {},
    };
    mockState.graphData = { nodes: [mockState.selectedNode], links: [] };

    render(<NodeDetailsPanel />);
    fireEvent.click(screen.getByText('Focus on Node'));
    expect(mockActions.setFocusedNode).toHaveBeenCalledWith('n1');
  });

  it('calls setFocusedNode(null) when Show All button is clicked', () => {
    mockState.selectedNode = {
      id: 'n1',
      name: 'Alice',
      type: 'person',
      attributes: {},
    };
    mockState.focusedNodeId = 'n1';
    mockState.graphData = { nodes: [mockState.selectedNode], links: [] };

    render(<NodeDetailsPanel />);
    fireEvent.click(screen.getByText('Show All'));
    expect(mockActions.setFocusedNode).toHaveBeenCalledWith(null);
  });

  it('handles object-typed source/target in links', () => {
    const nodeA = { id: 'n1', name: 'Alice', type: 'person', attributes: {} };
    const nodeB = { id: 'n2', name: 'Bob', type: 'person', attributes: {}, color: '#68CCE5' };

    mockState.selectedNode = nodeA;
    mockState.graphData = {
      nodes: [nodeA, nodeB],
      links: [{ source: nodeA as unknown as string, target: nodeB as unknown as string, relationship: 'knows' }],
    };

    render(<NodeDetailsPanel />);
    expect(screen.getByText('Bob')).toBeInTheDocument();
    expect(screen.getByText('1 relationships')).toBeInTheDocument();
  });

  it('groups multiple relationships from same connected node', () => {
    const nodeA = { id: 'n1', name: 'Alice', type: 'person', attributes: {} };
    const nodeB = { id: 'n2', name: 'Bob', type: 'person', attributes: {}, color: '#68CCE5' };

    mockState.selectedNode = nodeA;
    mockState.graphData = {
      nodes: [nodeA, nodeB],
      links: [
        { source: 'n1', target: 'n2', relationship: 'knows' },
        { source: 'n2', target: 'n1', relationship: 'reports_to' },
      ],
    };

    render(<NodeDetailsPanel />);
    // Bob should appear only once in the connected entities list
    const bobElements = screen.getAllByText('Bob');
    expect(bobElements).toHaveLength(1);
    // Both relationships should be shown
    expect(screen.getByText(/knows/)).toBeInTheDocument();
    expect(screen.getByText(/reports_to/)).toBeInTheDocument();
  });

  it('renders 0 relationships when link has no relationship field', () => {
    const nodeA = { id: 'n1', name: 'Alice', type: 'person', attributes: {} };
    const nodeB = { id: 'n2', name: 'Bob', type: 'person', attributes: {}, color: '#68CCE5' };

    mockState.selectedNode = nodeA;
    mockState.graphData = {
      nodes: [nodeA, nodeB],
      links: [{ source: 'n1', target: 'n2' }],
    };

    render(<NodeDetailsPanel />);
    expect(screen.getByText('1 relationships')).toBeInTheDocument();
    // Falls back to 'related_to'
    expect(screen.getByText(/related_to/)).toBeInTheDocument();
  });

  it('handles outgoing link to non-existent target node', () => {
    const nodeA = { id: 'n1', name: 'Alice', type: 'person', attributes: {} };

    mockState.selectedNode = nodeA;
    mockState.graphData = {
      nodes: [nodeA],
      links: [{ source: 'n1', target: 'nonexistent', relationship: 'knows' }],
    };

    render(<NodeDetailsPanel />);
    // Link references non-existent target → targetNode is undefined → skipped
    expect(screen.getByText('1 relationships')).toBeInTheDocument();
    expect(screen.getByText('No connections found')).toBeInTheDocument();
  });

  it('handles incoming link from non-existent source node', () => {
    const nodeA = { id: 'n1', name: 'Alice', type: 'person', attributes: {} };

    mockState.selectedNode = nodeA;
    mockState.graphData = {
      nodes: [nodeA],
      links: [{ source: 'nonexistent', target: 'n1', relationship: 'reports_to' }],
    };

    render(<NodeDetailsPanel />);
    // Link references non-existent source → sourceNode is undefined → skipped
    expect(screen.getByText('1 relationships')).toBeInTheDocument();
    expect(screen.getByText('No connections found')).toBeInTheDocument();
  });

  it('handles incoming link without relationship field', () => {
    const nodeA = { id: 'n1', name: 'Alice', type: 'person', attributes: {} };
    const nodeB = { id: 'n2', name: 'Bob', type: 'person', attributes: {}, color: '#68CCE5' };

    mockState.selectedNode = nodeA;
    mockState.graphData = {
      nodes: [nodeA, nodeB],
      links: [{ source: 'n2', target: 'n1' }],
    };

    render(<NodeDetailsPanel />);
    // Incoming link with no relationship → falls back to 'related_to'
    expect(screen.getByText('Bob')).toBeInTheDocument();
    expect(screen.getByText(/related_to/)).toBeInTheDocument();
  });

  it('handles selectedNode with undefined attributes', () => {
    mockState.selectedNode = {
      id: 'n1',
      name: 'Alice',
      type: 'person',
      attributes: undefined as unknown as Record<string, unknown>,
    };
    mockState.graphData = { nodes: [mockState.selectedNode], links: [] };

    render(<NodeDetailsPanel />);
    expect(screen.queryByText('Attributes')).not.toBeInTheDocument();
  });
});
