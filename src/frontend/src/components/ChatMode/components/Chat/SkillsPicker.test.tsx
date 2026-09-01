import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import SkillsPicker from './SkillsPicker';
import { useExecutionStore } from '../../store/executionStore';

// SkillsPicker lists the workspace's ENABLED skills via SkillService.list().
const list = vi.fn();
vi.mock('../../../../api/tools/SkillService', () => ({
  SkillService: { list: (...a: unknown[]) => list(...a) },
}));

const SKILLS = [
  { id: 1, name: 'databricks-sql', description: 'Write governed SQL', enabled: true },
  { id: 2, name: 'writing-agent-tasks', description: 'Author crisp tasks', enabled: true },
  { id: 3, name: 'disabled-one', description: 'Off', enabled: false },
];

beforeEach(() => {
  vi.clearAllMocks();
  useExecutionStore.setState({ selectedSkills: [] });
  list.mockResolvedValue(SKILLS);
});

describe('SkillsPicker', () => {
  it('lists only enabled skills (inline, always open)', async () => {
    render(<SkillsPicker />);
    await waitFor(() => expect(screen.getByText('databricks-sql')).toBeInTheDocument());
    expect(screen.getByText('writing-agent-tasks')).toBeInTheDocument();
    // A disabled skill can't be attached, so it's omitted.
    expect(screen.queryByText('disabled-one')).toBeNull();
  });

  it('toggles a skill into the store by name', async () => {
    render(<SkillsPicker />);
    await waitFor(() => expect(screen.getByText('databricks-sql')).toBeInTheDocument());

    fireEvent.click(screen.getByText('databricks-sql'));
    expect(useExecutionStore.getState().selectedSkills).toEqual(['databricks-sql']);

    // Clicking again removes it.
    fireEvent.click(screen.getByText('databricks-sql'));
    expect(useExecutionStore.getState().selectedSkills).toEqual([]);
  });

  it('reconciles a stale persisted selection against what is available', async () => {
    // "gone" no longer exists in the workspace; it must be pruned on load.
    useExecutionStore.setState({ selectedSkills: ['databricks-sql', 'gone'] });
    render(<SkillsPicker />);
    await waitFor(() =>
      expect(useExecutionStore.getState().selectedSkills).toEqual(['databricks-sql']),
    );
  });

  it('filters by the search box', async () => {
    render(<SkillsPicker />);
    await waitFor(() => expect(screen.getByText('databricks-sql')).toBeInTheDocument());

    fireEvent.change(screen.getByLabelText('Search skills'), { target: { value: 'writing' } });
    expect(screen.queryByText('databricks-sql')).toBeNull();
    expect(screen.getByText('writing-agent-tasks')).toBeInTheDocument();
  });

  it('shows an empty state when the workspace has no skills', async () => {
    list.mockResolvedValue([]);
    render(<SkillsPicker />);
    await waitFor(() => expect(screen.getByText('No skills available')).toBeInTheDocument());
  });
});
