import { describe, it, expect, vi, beforeEach } from 'vitest';
import React from 'react';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';

// Hoisted mock state and functions
const { mockState, mockActions } = vi.hoisted(() => ({
  mockState: {
    legendPanelCollapsed: false,
    hiddenEntityTypes: new Set<string>(),
  },
  mockActions: {
    setLegendPanelCollapsed: vi.fn(),
    toggleEntityTypeVisibility: vi.fn(),
  },
}));

vi.mock('../../store/entityGraphStore', () => ({
  default: (selector: (state: typeof mockState & typeof mockActions) => unknown) =>
    selector({ ...mockState, ...mockActions }),
}));

import GraphLegend from './GraphLegend';

const sampleEntityTypes = [
  { type: 'person', color: '#68CCE5', count: 5 },
  { type: 'organization', color: '#94D82D', count: 3 },
  { type: 'system', color: '#FCC940', count: 1 },
];

describe('GraphLegend', () => {
  beforeEach(() => {
    mockState.legendPanelCollapsed = false;
    mockState.hiddenEntityTypes = new Set<string>();
    vi.clearAllMocks();
  });

  it('renders the Entity Types header', () => {
    render(<GraphLegend availableEntityTypes={sampleEntityTypes} />);
    expect(screen.getByText('Entity Types')).toBeInTheDocument();
  });

  it('renders entity types with names and counts', () => {
    render(<GraphLegend availableEntityTypes={sampleEntityTypes} />);
    expect(screen.getByText('person')).toBeInTheDocument();
    expect(screen.getByText('organization')).toBeInTheDocument();
    expect(screen.getByText('system')).toBeInTheDocument();
    expect(screen.getByText('5')).toBeInTheDocument();
    expect(screen.getByText('3')).toBeInTheDocument();
    expect(screen.getByText('1')).toBeInTheDocument();
  });

  it('shows "No entities loaded" when empty', () => {
    render(<GraphLegend availableEntityTypes={[]} />);
    expect(screen.getByText('No entities loaded')).toBeInTheDocument();
  });

  it('calls toggleEntityTypeVisibility when entity type row is clicked', () => {
    render(<GraphLegend availableEntityTypes={sampleEntityTypes} />);
    fireEvent.click(screen.getByText('person'));
    expect(mockActions.toggleEntityTypeVisibility).toHaveBeenCalledWith('person');
  });

  it('applies reduced opacity for hidden entity types', () => {
    mockState.hiddenEntityTypes = new Set(['person']);
    render(<GraphLegend availableEntityTypes={sampleEntityTypes} />);
    const personRow = screen.getByText('person').closest('[class*="MuiBox"]');
    expect(personRow).toHaveStyle({ opacity: '0.4' });
  });

  it('applies line-through text decoration for hidden entity types', () => {
    mockState.hiddenEntityTypes = new Set(['organization']);
    render(<GraphLegend availableEntityTypes={sampleEntityTypes} />);
    const orgText = screen.getByText('organization');
    expect(orgText).toHaveStyle({ textDecoration: 'line-through' });
  });

  it('calls setLegendPanelCollapsed when header is clicked', () => {
    render(<GraphLegend availableEntityTypes={sampleEntityTypes} />);
    const header = screen.getByText('Entity Types').closest('[class*="MuiBox"]')!;
    fireEvent.click(header);
    expect(mockActions.setLegendPanelCollapsed).toHaveBeenCalledWith(true);
  });

  it('toggles collapse state correctly', () => {
    mockState.legendPanelCollapsed = true;
    render(<GraphLegend availableEntityTypes={sampleEntityTypes} />);
    const header = screen.getByText('Entity Types').closest('[class*="MuiBox"]')!;
    fireEvent.click(header);
    expect(mockActions.setLegendPanelCollapsed).toHaveBeenCalledWith(false);
  });
});
