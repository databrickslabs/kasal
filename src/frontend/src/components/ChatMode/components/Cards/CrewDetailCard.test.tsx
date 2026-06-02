import React from 'react';
import { render, screen, fireEvent } from '@testing-library/react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import CrewDetailCard from './CrewDetailCard';
import { CatalogLoadResult } from '../../types/dispatcher';

// Mock the execution store so we can control the `busy` selector value.
let storeState: { isExecuting: boolean; isLoading: boolean };

vi.mock('../../store/executionStore', () => ({
  useExecutionStore: (selector: (s: typeof storeState) => unknown) =>
    selector(storeState),
}));

const makeData = (
  plan: CatalogLoadResult['plan'],
): CatalogLoadResult => ({
  type: 'catalog_load',
  plan,
  message: 'msg',
});

describe('CrewDetailCard', () => {
  beforeEach(() => {
    storeState = { isExecuting: false, isLoading: false };
  });

  it('renders fallback when no plan data is available', () => {
    render(<CrewDetailCard data={makeData(null)} />);
    expect(screen.getByText('No plan data available.')).toBeInTheDocument();
  });

  it('renders crew detail with agents and tasks counted across both node type variants', () => {
    const plan: CatalogLoadResult['plan'] = {
      id: 'p1',
      name: 'My Crew',
      nodes: [
        { type: 'agentNode' },
        { type: 'agent' },
        { type: 'taskNode' },
        { type: 'task' },
        { type: 'other' },
      ],
      edges: [],
      process: 'hierarchical',
      planning: true,
      memory: true,
      max_rpm: 10,
    };

    render(<CrewDetailCard data={makeData(plan)} />);

    expect(screen.getByText('My Crew')).toBeInTheDocument();
    expect(screen.getByText('hierarchical')).toBeInTheDocument();
    // Agents (agentNode + agent = 2) and Tasks (taskNode + task = 2) each show "2"
    const twos = screen.getAllByText('2');
    expect(twos.length).toBe(2);
    // Planning Yes, Memory Yes
    const yesNodes = screen.getAllByText('Yes');
    expect(yesNodes.length).toBe(2);
    expect(screen.getByText('Memory:')).toBeInTheDocument();
    expect(screen.getByText('Max RPM:')).toBeInTheDocument();
    expect(screen.getByText('10')).toBeInTheDocument();
  });

  it('uses default process and No flags, and hides optional fields when undefined', () => {
    const plan: CatalogLoadResult['plan'] = {
      id: 'p2',
      name: 'Bare Crew',
      // nodes undefined to exercise the `?? 0` fallback branches
      nodes: undefined as unknown as unknown[],
      edges: [],
      // process omitted -> 'sequential'
      // planning omitted/falsey -> 'No'
      // memory + max_rpm undefined -> hidden
    };

    render(<CrewDetailCard data={makeData(plan)} />);

    expect(screen.getByText('Bare Crew')).toBeInTheDocument();
    expect(screen.getByText('sequential')).toBeInTheDocument();
    // Two "0" values for agents and tasks
    expect(screen.getAllByText('0').length).toBe(2);
    expect(screen.getByText('No')).toBeInTheDocument();
    expect(screen.queryByText('Memory:')).not.toBeInTheDocument();
    expect(screen.queryByText('Max RPM:')).not.toBeInTheDocument();
  });

  it('renders memory "No" when memory is explicitly false and max_rpm 0', () => {
    const plan: CatalogLoadResult['plan'] = {
      id: 'p3',
      name: 'Crew',
      nodes: [],
      edges: [],
      memory: false,
      max_rpm: 0,
    };

    render(<CrewDetailCard data={makeData(plan)} />);
    expect(screen.getByText('Memory:')).toBeInTheDocument();
    expect(screen.getByText('Max RPM:')).toBeInTheDocument();
    // memory false -> "No", planning false -> "No" : two "No"
    expect(screen.getAllByText('No').length).toBe(2);
  });

  it('does not render the Execute button when onExecute is not provided', () => {
    const plan: CatalogLoadResult['plan'] = {
      id: 'p4',
      name: 'Crew',
      nodes: [],
      edges: [],
    };
    render(<CrewDetailCard data={makeData(plan)} />);
    expect(screen.queryByRole('button')).not.toBeInTheDocument();
  });

  it('renders Execute button and fires onExecute when clicked', () => {
    const onExecute = vi.fn();
    const plan: CatalogLoadResult['plan'] = {
      id: 'p5',
      name: 'Crew',
      nodes: [],
      edges: [],
    };
    render(<CrewDetailCard data={makeData(plan)} onExecute={onExecute} />);

    const btn = screen.getByRole('button', { name: 'Execute' });
    expect(btn).not.toBeDisabled();
    fireEvent.click(btn);
    expect(onExecute).toHaveBeenCalledTimes(1);
  });

  it('shows Starting... and disables button when busy (isExecuting)', () => {
    storeState = { isExecuting: true, isLoading: false };
    const onExecute = vi.fn();
    const plan: CatalogLoadResult['plan'] = {
      id: 'p6',
      name: 'Crew',
      nodes: [],
      edges: [],
    };
    render(<CrewDetailCard data={makeData(plan)} onExecute={onExecute} />);

    const btn = screen.getByRole('button', { name: 'Starting...' });
    expect(btn).toBeDisabled();
  });

  it('shows busy state when isLoading is true', () => {
    storeState = { isExecuting: false, isLoading: true };
    const onExecute = vi.fn();
    const plan: CatalogLoadResult['plan'] = {
      id: 'p7',
      name: 'Crew',
      nodes: [],
      edges: [],
    };
    render(<CrewDetailCard data={makeData(plan)} onExecute={onExecute} />);
    expect(screen.getByRole('button', { name: 'Starting...' })).toBeDisabled();
  });
});
