/**
 * Tests for PromptOptimization.
 *
 * Covers the fixedTemplate scoping introduced with the Prompts surface: the
 * template picker is hidden and the run list filtered to the target template,
 * plus the start flow's request payload and error surfacing.
 */

import { describe, it, expect, vi, beforeEach, Mock } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import PromptOptimization from './PromptOptimization';
import { PromptOptimizationService } from '../../api/PromptOptimizationService';
import { ModelService } from '../../api/ModelService';

vi.mock('../../api/PromptOptimizationService', () => ({
  PromptOptimizationService: {
    listRuns: vi.fn(),
    startOptimization: vi.fn(),
    applyRun: vi.fn(),
  },
}));

vi.mock('../../api/ModelService', () => {
  const instance = { getEnabledModels: vi.fn() };
  return { ModelService: { getInstance: () => instance } };
});

const listRuns = PromptOptimizationService.listRuns as Mock;
const startOptimization = PromptOptimizationService.startOptimization as Mock;
const getEnabledModels = ModelService.getInstance().getEnabledModels as Mock;

const RUNS = [
  {
    run_id: 'run-intent',
    template_name: 'detect_intent',
    status: 'completed',
    dataset_size: 5,
    applied: false,
    initial_score: 0.5,
    final_score: 0.9,
    baseline_template: 'OLD',
    optimized_template: 'NEW',
  },
  {
    run_id: 'run-agent',
    template_name: 'generate_agent',
    status: 'completed',
    dataset_size: 5,
    applied: false,
  },
];

beforeEach(() => {
  vi.clearAllMocks();
  getEnabledModels.mockResolvedValue({ 'qwen-30b': {} });
  listRuns.mockResolvedValue(RUNS);
});

describe('PromptOptimization', () => {
  it('unscoped: shows the template picker and every run', async () => {
    render(<PromptOptimization />);
    await waitFor(() => expect(listRuns).toHaveBeenCalled());
    expect((await screen.findAllByText(/detect_intent/)).length).toBeGreaterThan(0);
    expect(screen.getAllByText(/generate_agent/).length).toBeGreaterThan(0);
    // The template select is present (labelled options from OPTIMIZABLE_TEMPLATES)
    expect(screen.getAllByRole('combobox').length).toBeGreaterThanOrEqual(2);
  });

  it('fixedTemplate: hides the picker and filters runs to the target', async () => {
    render(<PromptOptimization fixedTemplate="detect_intent" />);
    await waitFor(() => expect(listRuns).toHaveBeenCalled());
    await screen.findByText(/detect_intent/);
    expect(screen.queryByText(/generate_agent/)).not.toBeInTheDocument();
    // Only the model select remains.
    expect(screen.getAllByRole('combobox')).toHaveLength(1);
  });

  it('starts an optimization for the scoped template', async () => {
    startOptimization.mockResolvedValue({
      run_id: 'r-new',
      status: 'pending',
      dataset_size: 7,
    });
    render(<PromptOptimization fixedTemplate="detect_intent" />);
    await waitFor(() => expect(listRuns).toHaveBeenCalled());
    await userEvent.click(screen.getByRole('button', { name: /start|optimize/i }));
    await waitFor(() => expect(startOptimization).toHaveBeenCalled());
    const request = startOptimization.mock.calls[0][0];
    expect(request.template_name).toBe('detect_intent');
    expect(request.max_metric_calls).toBeGreaterThan(0);
  });

  it('surfaces a backend rejection detail on start failure', async () => {
    startOptimization.mockRejectedValue({
      response: { data: { detail: 'Need at least 4 examples' } },
    });
    render(<PromptOptimization fixedTemplate="detect_intent" />);
    await waitFor(() => expect(listRuns).toHaveBeenCalled());
    await userEvent.click(screen.getByRole('button', { name: /start|optimize/i }));
    expect(
      await screen.findByText(/Need at least 4 examples/),
    ).toBeInTheDocument();
  });
});
