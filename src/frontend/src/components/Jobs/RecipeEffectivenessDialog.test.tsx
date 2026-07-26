/**
 * The measurement report.
 *
 * The behaviour that matters most is the caveat: when there is no control arm,
 * the numbers are NOT a causal claim, and the dialog has to say so. A reader who
 * compares "got exemplars" against "nothing available" is measuring how familiar
 * the request was, not what the exemplars did — and would conclude the feature
 * works no matter what.
 */
import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';

const getEffectiveness = vi.fn();
vi.mock('../../api/workflow/WorkflowRecipeService', () => ({
  WorkflowRecipeService: {
    getEffectiveness: (...a: unknown[]) => getEffectiveness(...a),
  },
}));

import RecipeEffectivenessDialog from './RecipeEffectivenessDialog';

const REPORT = {
  window_days: 30,
  generations: 20,
  with_candidates: 12,
  with_blessed_candidates: 8,
  coverage_rate: 0.6,
  injection_rate: 0.25,
  holdout_fraction: 0.2,
  min_similarity: 0.75,
  arms: {
    exemplar: {
      generations: 5, linked_runs: 4, completed: 4, completion_rate: 1,
      median_duration_ms: 134_000, median_error_spans: 0,
      median_agents: 3, median_tasks: 4,
    },
    control: {
      generations: 3, linked_runs: 2, completed: 1, completion_rate: 0.5,
      median_duration_ms: 182_000, median_error_spans: 2,
      median_agents: 3, median_tasks: 5,
    },
    none_available: {
      generations: 12, linked_runs: 0, completed: 0, completion_rate: null,
      median_duration_ms: null, median_error_spans: null,
      median_agents: null, median_tasks: null,
    },
  },
  comparable: true,
  note: 'exemplar vs control is the only unconfounded comparison',
};

beforeEach(() => {
  vi.clearAllMocks();
  getEffectiveness.mockResolvedValue(REPORT);
});

describe('RecipeEffectivenessDialog', () => {
  it('does not fetch while closed', () => {
    render(<RecipeEffectivenessDialog open={false} onClose={() => undefined} />);
    expect(getEffectiveness).not.toHaveBeenCalled();
  });

  it('shows coverage and every arm', async () => {
    render(<RecipeEffectivenessDialog open onClose={() => undefined} />);

    await waitFor(() => expect(getEffectiveness).toHaveBeenCalled());
    // Twice on purpose: once as the headline injection-rate tile, once as the
    // arm's row in the comparison table.
    expect((await screen.findAllByText('Got exemplars')).length).toBe(2);
    expect(screen.getByText('Withheld (control)')).toBeInTheDocument();
    expect(screen.getByText('Nothing available')).toBeInTheDocument();
    expect(screen.getByText('60%')).toBeInTheDocument(); // coverage
  });

  it('calls a populated control arm a fair comparison', async () => {
    render(<RecipeEffectivenessDialog open onClose={() => undefined} />);
    expect(await screen.findByText(/fair comparison/)).toBeInTheDocument();
  });

  it('warns that no causal claim is available without a control arm', async () => {
    getEffectiveness.mockResolvedValue({ ...REPORT, comparable: false, holdout_fraction: 0 });
    render(<RecipeEffectivenessDialog open onClose={() => undefined} />);

    expect(await screen.findByText(/no causal claim is available/)).toBeInTheDocument();
    expect(screen.getByText(/WORKFLOW_RECIPE_HOLDOUT/)).toBeInTheDocument();
  });

  it('says so plainly when nothing has been generated yet', async () => {
    getEffectiveness.mockResolvedValue({
      ...REPORT, generations: 0, with_candidates: 0, with_blessed_candidates: 0,
      coverage_rate: null, injection_rate: null, comparable: false,
    });
    render(<RecipeEffectivenessDialog open onClose={() => undefined} />);
    expect(await screen.findByText(/No crew generations recorded yet/)).toBeInTheDocument();
  });

  it('surfaces a failed load instead of rendering an empty report', async () => {
    getEffectiveness.mockRejectedValue(new Error('boom'));
    render(<RecipeEffectivenessDialog open onClose={() => undefined} />);
    expect(await screen.findByText(/Could not load the effectiveness report/)).toBeInTheDocument();
  });
});
