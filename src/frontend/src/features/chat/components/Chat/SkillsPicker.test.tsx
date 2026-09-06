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
  { id: 1, name: 'databricks-sql', description: 'Write governed SQL', enabled: true, global_enabled: false },
  { id: 2, name: 'writing-agent-tasks', description: 'Author crisp tasks', enabled: true, global_enabled: false },
  { id: 3, name: 'disabled-one', description: 'Off', enabled: false, global_enabled: false },
  { id: 4, name: 'always-on-skill', description: 'For everyone', enabled: true, global_enabled: true },
];

beforeEach(async () => {
  vi.clearAllMocks();
  const { invalidateSkillsCache } = await import('../../utils/skillSelection');
  invalidateSkillsCache();
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
    // "gone" no longer exists, and "always-on-skill" is attached to every agent
    // regardless — both must be pruned on load.
    useExecutionStore.setState({
      selectedSkills: ['databricks-sql', 'gone', 'always-on-skill'],
    });
    render(<SkillsPicker />);
    await waitFor(() =>
      expect(useExecutionStore.getState().selectedSkills).toEqual(['databricks-sql']),
    );
  });

  it('pins globally-enabled skills as Always on, not toggleable', async () => {
    render(<SkillsPicker />);
    await waitFor(() => expect(screen.getByText('always-on-skill')).toBeInTheDocument());
    expect(screen.getByText('Always on')).toBeInTheDocument();
    // Clicking the pinned row selects nothing — the backend attaches it anyway.
    fireEvent.click(screen.getByText('always-on-skill'));
    expect(useExecutionStore.getState().selectedSkills).toEqual([]);
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
