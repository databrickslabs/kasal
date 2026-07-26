/**
 * Per-run recipe curation control.
 *
 * The behaviours worth pinning: it stays out of the way for runs that were
 * never mined, it writes the human judgement that is the ONLY thing that
 * unblocks reuse, and the job→recipe index is fetched once for the whole run
 * list rather than once per row.
 */
import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';

const getByJob = vi.fn();
const curate = vi.fn(async () => undefined);
const getEffectiveness = vi.fn();

vi.mock('../../api/WorkflowRecipeService', () => ({
  WorkflowRecipeService: {
    getByJob: (...a: unknown[]) => getByJob(...a),
    curate: (...a: unknown[]) => curate(...a),
    getEffectiveness: (...a: unknown[]) => getEffectiveness(...a),
  },
}));

import RecipeCurationButton from './RecipeCurationButton';
import { invalidateRecipeIndex, refreshRecipeIndexIfStale } from './recipeIndexCache';

const MINED = {
  'job-1': {
    recipe_id: 42,
    curation: null,
    intent_text: 'Load US and EU',
    run_count: 29,
  },
};

beforeEach(() => {
  vi.clearAllMocks();
  // The index is cached at module scope on purpose (one fetch per run list);
  // clear it so each case starts from a cold read.
  invalidateRecipeIndex();
  getByJob.mockResolvedValue(MINED);
  getEffectiveness.mockResolvedValue({
    window_days: 30,
    generations: 10,
    with_candidates: 6,
    with_blessed_candidates: 2,
    coverage_rate: 0.6,
    injection_rate: 0.2,
    holdout_fraction: 0,
    min_similarity: 0.75,
    arms: {
      exemplar: {
        generations: 2, linked_runs: 2, completed: 2, completion_rate: 1,
        median_duration_ms: 1000, median_error_spans: 0, median_agents: 3, median_tasks: 4,
      },
    },
    comparable: false,
    note: 'n/a',
  });
});

describe('RecipeCurationButton', () => {
  it('renders nothing for a run that was never mined', async () => {
    render(<RecipeCurationButton jobId="not-mined" />);
    await waitFor(() => expect(getByJob).toHaveBeenCalled());
    expect(screen.queryByRole('button')).toBeNull();
  });

  it('offers curation for a mined run and records the judgement', async () => {
    render(<RecipeCurationButton jobId="job-1" />);
    const button = await screen.findByRole('button');
    fireEvent.click(button);

    fireEvent.click(await screen.findByText('Good — reuse this'));
    await waitFor(() => expect(curate).toHaveBeenCalledWith(42, 'good'));
  });

  it('can reject a recipe so it is never suggested again', async () => {
    render(<RecipeCurationButton jobId="job-1" />);
    fireEvent.click(await screen.findByRole('button'));
    fireEvent.click(await screen.findByText('Not good'));
    await waitFor(() => expect(curate).toHaveBeenCalledWith(42, 'bad'));
  });

  it('does not carry the aggregate report — the table header owns that', async () => {
    render(<RecipeCurationButton jobId="job-1" />);
    fireEvent.click(await screen.findByRole('button'));
    expect(screen.queryByText('Is reuse helping?')).toBeNull();
    expect(getEffectiveness).not.toHaveBeenCalled();
  });

  it('fetches the job index once for many rows, not once per row', async () => {
    render(
      <>
        <RecipeCurationButton jobId="job-1" />
        <RecipeCurationButton jobId="job-2" />
        <RecipeCurationButton jobId="job-3" />
      </>,
    );
    await waitFor(() => expect(screen.getAllByRole('button')).toHaveLength(1));
    expect(getByJob).toHaveBeenCalledTimes(1);
  });

});

/**
 * Mining is a background sweep, so a run's recipe appears seconds-to-minutes
 * AFTER the row is already on screen. A cache with no expiry meant the control
 * never showed up for a just-finished run until a full page reload — which
 * reads exactly like the feature being broken.
 */
describe('recipe index staleness', () => {
  it('re-reads the index once it goes stale, so a newly mined run gains its control', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    try {
      // First read: the sweep has not mined this run yet.
      getByJob.mockResolvedValue({});
      render(<RecipeCurationButton jobId="job-1" />);
      await waitFor(() => expect(getByJob).toHaveBeenCalledTimes(1));
      expect(screen.queryByRole('button')).toBeNull();

      // The sweep runs; the run is now mined.
      getByJob.mockResolvedValue(MINED);

      // Within the TTL, a list refresh must NOT refetch — the run list updates
      // on every SSE status change, and one request per transition is a storm.
      refreshRecipeIndexIfStale();
      expect(getByJob).toHaveBeenCalledTimes(1);

      // Past the TTL it refetches, and the control appears without a reload.
      vi.advanceTimersByTime(21_000);
      refreshRecipeIndexIfStale();
      await waitFor(() => expect(getByJob).toHaveBeenCalledTimes(2));
      expect(await screen.findByRole('button')).toBeInTheDocument();
    } finally {
      vi.useRealTimers();
    }
  });
});
