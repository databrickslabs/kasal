import { fireEvent, render, screen } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { MemoryOntologyView } from './MemoryOntologyView';

const api = vi.hoisted(() => ({ get: vi.fn(), delete: vi.fn() }));
vi.mock('../../../shared/api/client', () => ({ apiClient: api }));
vi.mock('./ConceptForceGraph', () => ({
  ConceptForceGraph: ({
    nodes,
    onToggleNode,
  }: {
    nodes: { id: string; label: string }[];
    onToggleNode: (id: string) => void;
  }) => (
    <div>
      {nodes.map((node) => (
        <button key={node.id} onClick={() => onToggleNode(node.id)}>
          node {node.label}
        </button>
      ))}
    </div>
  ),
}));

const graph = {
  backend: 'default',
  enabled: true,
  entities: [
    { id: 'atlas', name: 'Atlas', type: 'project' },
    { id: 'dana', name: 'Dana', type: 'person' },
    { id: 'user', name: 'user', type: 'person' },
  ],
  facts: [
    { id: 'old', subject_id: 'atlas', subject: 'Atlas', relation: 'deadline_is', object: '15 June', statement: 'Atlas was due 15 June.', valid_to: '2026-10-01T00:00:00', superseded_by: 'new' },
    { id: 'new', subject_id: 'atlas', subject: 'Atlas', relation: 'deadline_is', object: '30 July', statement: 'Atlas is due 30 July.' },
    { id: 'own', subject_id: 'atlas', subject: 'Atlas', relation: 'owned_by', object: 'Dana', object_id: 'dana', statement: 'Dana owns Atlas.' },
    { id: 'pref', subject_id: 'user', subject: 'user', relation: 'prefers', object: 'tables', statement: 'The user prefers tables.' },
  ],
};

describe('MemoryOntologyView', () => {
  beforeEach(() => {
    api.get.mockReset();
    api.delete.mockReset();
  });

  it('says how to turn graph memory on when it is off', async () => {
    api.get.mockResolvedValue({ data: { backend: 'default', enabled: false, entities: [], facts: [] } });
    render(<MemoryOntologyView />);
    expect(await screen.findByText(/Graph memory is off for this teamspace/)).toBeInTheDocument();
  });

  it('lists current facts and reveals replaced ones on request', async () => {
    api.get.mockResolvedValue({ data: graph });
    render(<MemoryOntologyView />);
    expect(await screen.findByText('Atlas is due 30 July.')).toBeInTheDocument();
    expect(api.get).toHaveBeenCalledWith('/memory-backend/graph', { params: { limit: 2000 } });
    expect(screen.getByText('deadline is')).toBeInTheDocument();
    expect(screen.queryByText('Atlas was due 15 June.')).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole('checkbox', { name: 'Show replaced facts (1)' }));
    expect(screen.getByText('Atlas was due 15 June.')).toBeInTheDocument();
    expect(screen.getByText('replaced')).toBeInTheDocument();
  });

  it('filters the facts to a pinned entity', async () => {
    api.get.mockResolvedValue({ data: graph });
    render(<MemoryOntologyView />);
    fireEvent.click(await screen.findByRole('button', { name: 'node Dana' }));
    expect(screen.getByText('Dana owns Atlas.')).toBeInTheDocument();
    expect(screen.queryByText('The user prefers tables.')).not.toBeInTheDocument();
    expect(screen.getByText('Facts about the pinned entities (1)')).toBeInTheDocument();
  });

  it('forgets a fact on request and reloads', async () => {
    api.get.mockResolvedValue({ data: graph });
    api.delete.mockResolvedValue({ data: { forgotten: true } });
    render(<MemoryOntologyView />);
    fireEvent.click(
      await screen.findByRole('button', { name: 'Forget: The user prefers tables.' }),
    );
    await screen.findByText('Atlas is due 30 July.');
    expect(api.delete).toHaveBeenCalledWith('/memory-backend/graph/facts/pref');
    expect(api.get).toHaveBeenCalledTimes(2);
  });

  it('scoped to a run, shows only what that run recalled', async () => {
    api.get.mockResolvedValue({ data: graph });
    const trace = { recalledIds: new Set(['own']), savedIds: new Set<string>(), savedContents: [] };
    render(<MemoryOntologyView run={{ id: 'run-1', mode: 'recalled', trace }} />);
    expect(await screen.findByText('Dana owns Atlas.')).toBeInTheDocument();
    expect(screen.queryByText('Atlas is due 30 July.')).not.toBeInTheDocument();
  });

  it('says so when a run recalled nothing', async () => {
    api.get.mockResolvedValue({ data: graph });
    const trace = { recalledIds: new Set<string>(), savedIds: new Set<string>(), savedContents: [] };
    render(<MemoryOntologyView run={{ id: 'run-1', mode: 'recalled', trace }} />);
    expect(await screen.findByText('This run recalled no facts from memory.')).toBeInTheDocument();
  });

  it('reports a failed load', async () => {
    api.get.mockRejectedValue(new Error('down'));
    render(<MemoryOntologyView />);
    expect(await screen.findByText('Could not load the memory graph.')).toBeInTheDocument();
  });
});
