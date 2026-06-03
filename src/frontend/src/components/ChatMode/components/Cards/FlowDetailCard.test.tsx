import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import FlowDetailCard from './FlowDetailCard';
import { FlowLoadResult } from '../../types/dispatcher';

// Mock the execution store. The component selects `s.isExecuting || s.isLoading`.
const storeState = { isExecuting: false, isLoading: false };
vi.mock('../../store/executionStore', () => ({
  useExecutionStore: (selector: (s: typeof storeState) => unknown) => selector(storeState),
}));

const makeData = (overrides: Partial<FlowLoadResult['flow']> = {}): FlowLoadResult => ({
  type: 'flow_load',
  flow: {
    id: 'f1',
    name: 'My Flow',
    nodes: [{}, {}, {}],
    edges: [],
    ...overrides,
  },
  message: 'loaded',
});

describe('FlowDetailCard', () => {
  beforeEach(() => {
    storeState.isExecuting = false;
    storeState.isLoading = false;
  });

  it('renders the empty state when flow is null', () => {
    const data: FlowLoadResult = { type: 'flow_load', flow: null, message: 'none' };
    render(<FlowDetailCard data={data} />);
    expect(screen.getByText('No flow data available.')).toBeInTheDocument();
  });

  it('renders flow name and node count from nodes array', () => {
    render(<FlowDetailCard data={makeData()} />);
    expect(screen.getByText('My Flow')).toBeInTheDocument();
    // "Nodes:" label and the count "3" are rendered.
    expect(screen.getByText('Nodes:')).toBeInTheDocument();
    expect(screen.getByText('3')).toBeInTheDocument();
  });

  it('falls back to 0 nodes when nodes is undefined', () => {
    const data = makeData();
    // Force the `flow.nodes?.length ?? 0` nullish branch.
    (data.flow as { nodes?: unknown[] }).nodes = undefined;
    render(<FlowDetailCard data={data} />);
    expect(screen.getByText('0')).toBeInTheDocument();
  });

  it('renders the config count when flow_config is present', () => {
    const data = makeData({ flow_config: { a: 1, b: 2 } });
    render(<FlowDetailCard data={data} />);
    expect(screen.getByText('Config:')).toBeInTheDocument();
    expect(screen.getByText('2 settings')).toBeInTheDocument();
  });

  it('does not render config section when flow_config is absent', () => {
    render(<FlowDetailCard data={makeData()} />);
    expect(screen.queryByText('Config:')).not.toBeInTheDocument();
  });

  it('does not render the Execute button without an onExecute handler', () => {
    render(<FlowDetailCard data={makeData()} />);
    expect(screen.queryByRole('button')).not.toBeInTheDocument();
  });

  it('renders an enabled Execute button and calls onExecute on click', () => {
    const onExecute = vi.fn();
    render(<FlowDetailCard data={makeData()} onExecute={onExecute} />);
    const button = screen.getByRole('button', { name: 'Execute' });
    expect(button).not.toBeDisabled();
    fireEvent.click(button);
    expect(onExecute).toHaveBeenCalledTimes(1);
  });

  it('shows "Starting..." and disables the button when isExecuting is true', () => {
    storeState.isExecuting = true;
    const onExecute = vi.fn();
    render(<FlowDetailCard data={makeData()} onExecute={onExecute} />);
    const button = screen.getByRole('button', { name: 'Starting...' });
    expect(button).toBeDisabled();
  });

  it('shows the busy state when isLoading is true', () => {
    storeState.isLoading = true;
    const onExecute = vi.fn();
    render(<FlowDetailCard data={makeData()} onExecute={onExecute} />);
    expect(screen.getByRole('button', { name: 'Starting...' })).toBeDisabled();
  });
});
