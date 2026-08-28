import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, within } from '@testing-library/react';
import MemoryPane from './MemoryPane';
import type { RunMemory } from '../../hooks/useRunMemory';
import { deriveIndex, coOccurrenceEdges, MemoryRecord } from '../../../MemoryBackend/memoryData';

// The pane is presentation; the data hook is mocked per test. The force graph
// is replaced with a probe that records its nodes and forwards pin clicks.
const hookState = vi.hoisted(() => ({ current: {} as Partial<RunMemory> }));
vi.mock('../../hooks/useRunMemory', () => ({
  useRunMemory: () => hookState.current,
}));
vi.mock('../../../MemoryBackend/ConceptForceGraph', () => ({
  ConceptForceGraph: (props: {
    nodes: { id: string; label: string }[];
    onToggleNode: (id: string) => void;
  }) => (
    <div data-testid="force-graph" data-nodes={props.nodes.map((n) => n.id).join(',')}>
      {props.nodes.map((n) => (
        <button key={n.id} onClick={() => props.onToggleNode(n.id)}>
          pin-{n.label}
        </button>
      ))}
    </div>
  ),
}));

const rec = (id: string, categories: string[], content = `content of ${id}`): MemoryRecord => ({
  id,
  content,
  scope: '/g/agent/Assistant/_crew_ff00aa',
  categories,
  importance: 0.5,
  private: false,
  metadata: {},
  created_at: '2026-06-21 13:00:00',
  last_accessed: null,
});

const withRecords = (records: MemoryRecord[], over: Partial<RunMemory> = {}): void => {
  const index = deriveIndex(records);
  hookState.current = {
    loading: false,
    error: null,
    backend: 'Local (SQLite)',
    mode: 'saved',
    setMode: vi.fn(),
    records,
    index,
    edges: coOccurrenceEdges(index),
    refresh: vi.fn(),
    ...over,
  };
};

describe('MemoryPane', () => {
  beforeEach(() => {
    withRecords([rec('a', ['browser', 'mcp-tools']), rec('b', ['browser'])]);
  });

  it('shows the stats strip, backend badge and the graph by default', () => {
    render(<MemoryPane runId="job-1" />);
    const stats = within(screen.getByTestId('memory-stats'));
    expect(stats.getByText('Records')).toBeInTheDocument();
    expect(stats.getByText('Concepts')).toBeInTheDocument();
    expect(stats.getByText('Local (SQLite)')).toBeInTheDocument();
    expect(screen.getByTestId('force-graph')).toHaveAttribute('data-nodes', 'browser,mcp-tools');
  });

  it("hides the backend badge when it only says 'default'", () => {
    withRecords([rec('a', ['browser'])], { backend: 'default' });
    render(<MemoryPane runId="job-1" />);
    expect(screen.queryByText('default')).toBeNull();
  });

  it('switches to the record list view', () => {
    render(<MemoryPane runId="job-1" />);
    fireEvent.click(screen.getByRole('tab', { name: 'Records' }));
    expect(screen.getByText('content of a')).toBeInTheDocument();
    expect(screen.getByText('content of b')).toBeInTheDocument();
    expect(screen.queryByTestId('force-graph')).toBeNull();
  });

  it('pinning a graph concept lists only the records mentioning it', () => {
    render(<MemoryPane runId="job-1" />);
    fireEvent.click(screen.getByText('pin-mcp-tools'));
    expect(screen.getByText('Records mentioning pinned concepts')).toBeInTheDocument();
    expect(screen.getByText('content of a')).toBeInTheDocument();
    expect(screen.queryByText('content of b')).toBeNull();
  });

  it('routes the Saved / Recalled toggle through the hook', () => {
    render(<MemoryPane runId="job-1" />);
    fireEvent.click(screen.getByRole('tab', { name: 'Recalled' }));
    expect(hookState.current.setMode).toHaveBeenCalledWith('recalled');
  });

  it('states plainly when the run recalled nothing', () => {
    withRecords([], { mode: 'recalled' });
    render(<MemoryPane runId="job-1" />);
    expect(screen.getByText('This run recalled nothing from memory.')).toBeInTheDocument();
  });

  it('shows loading and error states', () => {
    hookState.current = { ...hookState.current, loading: true };
    const { unmount } = render(<MemoryPane runId="job-1" />);
    expect(screen.getByText('Loading memory…')).toBeInTheDocument();
    unmount();
    withRecords([], { error: 'boom' });
    render(<MemoryPane runId="job-1" />);
    expect(screen.getByText(/Could not load memory: boom/)).toBeInTheDocument();
  });
});
