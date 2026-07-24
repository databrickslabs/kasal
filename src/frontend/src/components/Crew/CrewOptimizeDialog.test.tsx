/**
 * Tests for CrewOptimizeDialog.
 *
 * Focuses on the logic that has bitten in production:
 * - judge scoping: assigned chips filtered by the crew's registry prefix,
 *   the Assign menu limited to unassigned LIBRARY judges (other crews'
 *   judges and same-name duplicates excluded)
 * - evaluation answers sorted ungraded-first with header counts, rendered
 *   as markdown (not raw text)
 * - run cards exposing the honest-progress chips (executions, variants,
 *   human notes)
 * - the judge lifecycle: create (auto-assign), edit (full registry name),
 *   delete (confirm dialog), and starting a run with the budget as a hard cap
 */

import { describe, it, expect, vi, beforeEach, Mock } from 'vitest';
import { render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import CrewOptimizeDialog from './CrewOptimizeDialog';
import { PromptOptimizationService } from '../../api/PromptOptimizationService';
import { ModelService } from '../../api/ModelService';

// react-markdown 9.x is ESM-only; passthrough mock renders the raw string.
vi.mock('react-markdown', () => ({
  default: ({ children }: { children: string }) => (
    <div data-testid="md">{children}</div>
  ),
}));
vi.mock('remark-gfm', () => ({ default: () => {} }));

vi.mock('react-hot-toast', () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

vi.mock('../../api/PromptOptimizationService', () => ({
  PromptOptimizationService: {
    listRuns: vi.fn(),
    listCrewEvals: vi.fn(),
    listJudges: vi.fn(),
    startCrewOptimization: vi.fn(),
    cancelRun: vi.fn(),
    applyRun: vi.fn(),
    addEvalFeedback: vi.fn(),
    createJudge: vi.fn(),
    assignJudge: vi.fn(),
    updateJudge: vi.fn(),
    deleteJudge: vi.fn(),
  },
}));

vi.mock('../../api/ModelService', () => {
  const instance = { getEnabledModels: vi.fn() };
  return { ModelService: { getInstance: () => instance } };
});

const service = PromptOptimizationService as unknown as Record<string, Mock>;
const getEnabledModels = ModelService.getInstance().getEnabledModels as Mock;

const CREW_ID = '88ab4478-823c-4f12-b1ca-8e74c568995e'; // prefix 88ab4478823c

const JUDGES = [
  {
    name: 'accuracy',
    full_name: 'crew_88ab4478823c__accuracy',
    crew_id: '88ab4478823c',
    instructions: 'Assigned criteria for {{ outputs }}',
  },
  // Library original of the assigned judge — must NOT appear in the menu.
  { name: 'accuracy', full_name: 'accuracy', crew_id: null, instructions: 'lib copy' },
  { name: 'citation', full_name: 'citation', crew_id: null, instructions: 'Cite sources' },
  // Another crew's judge — must appear NOWHERE in this dialog.
  {
    name: 'foreign',
    full_name: 'crew_deadbeef1234__foreign',
    crew_id: 'deadbeef1234',
    instructions: 'other crew',
  },
];

const RUNS = [
  {
    run_id: 'run1',
    template_name: 'crew:Gather apartments',
    kind: 'crew',
    crew_id: CREW_ID,
    status: 'completed',
    dataset_size: 1,
    applied: false,
    initial_score: 0.57,
    final_score: 0.97,
    executions_used: 10,
    execution_cap: 10,
    candidates_tried: 10,
    human_feedback_count: 13,
    baseline_fields: { 'task.t1.description': 'old' },
    optimized_fields: { 'task.t1.description': 'new german-only' },
    created_at: '2026-07-24T08:00:00+00:00',
  },
];

const EVALS = [
  {
    trace_id: 'graded-older',
    timestamp_ms: 1000,
    deliverable: '| Location | Price |\n| Zurich | 499000 |',
    assessment_count: 2,
  },
  {
    trace_id: 'ungraded-newer',
    timestamp_ms: 2000,
    deliverable: 'Fresh ungraded answer',
    assessment_count: 0,
  },
];

const renderDialog = () =>
  render(
    <CrewOptimizeDialog
      open
      onClose={vi.fn()}
      crewId={CREW_ID}
      crewName="Gather apartments"
    />,
  );

beforeEach(() => {
  vi.clearAllMocks();
  service.listRuns.mockResolvedValue(RUNS);
  service.listCrewEvals.mockResolvedValue(EVALS);
  service.listJudges.mockResolvedValue(JUDGES);
  getEnabledModels.mockResolvedValue({ 'qwen-30b': {} });
});

describe('judge scoping', () => {
  it('shows assigned judges as chips and only unassigned library judges in the menu', async () => {
    renderDialog();
    // Assigned chip present
    expect(await screen.findByText('accuracy')).toBeInTheDocument();
    // Other crews' judges appear nowhere
    expect(screen.queryByText('foreign')).not.toBeInTheDocument();

    await userEvent.click(screen.getByText('Assign'));
    const menu = await screen.findByRole('menu');
    // Library judge available to assign
    expect(within(menu).getByText('citation')).toBeInTheDocument();
    // The library duplicate of the already-assigned name is excluded
    expect(within(menu).queryByText('accuracy')).not.toBeInTheDocument();
    expect(within(menu).queryByText('foreign')).not.toBeInTheDocument();
  });

  it('assigns a library judge to the crew', async () => {
    service.assignJudge.mockResolvedValue({ full_name: 'crew_88ab4478823c__citation' });
    renderDialog();
    await screen.findByText('accuracy');
    await userEvent.click(screen.getByText('Assign'));
    await userEvent.click(within(await screen.findByRole('menu')).getByText('citation'));
    await waitFor(() =>
      expect(service.assignJudge).toHaveBeenCalledWith('citation', CREW_ID),
    );
  });
});

describe('judge lifecycle', () => {
  it('opens the edit dialog from an assigned chip and updates by full name', async () => {
    service.updateJudge.mockResolvedValue({ name: 'accuracy' });
    renderDialog();
    await userEvent.click(await screen.findByText('accuracy'));
    expect(await screen.findByText(/Edit judge — accuracy/)).toBeInTheDocument();
    // Prefilled with the judge's current instructions
    expect(
      screen.getByDisplayValue('Assigned criteria for {{ outputs }}'),
    ).toBeInTheDocument();
    await userEvent.click(screen.getByRole('button', { name: 'Save' }));
    await waitFor(() =>
      expect(service.updateJudge).toHaveBeenCalledWith(
        'crew_88ab4478823c__accuracy',
        { instructions: 'Assigned criteria for {{ outputs }}', model: undefined },
      ),
    );
  });

  it('deletes a library judge after confirmation', async () => {
    service.deleteJudge.mockResolvedValue(true);
    renderDialog();
    await screen.findByText('accuracy');
    await userEvent.click(screen.getByText('Assign'));
    const menu = await screen.findByRole('menu');
    await userEvent.click(within(menu).getByTestId('DeleteOutlineIcon'));
    expect(await screen.findByText(/Delete judge/)).toBeInTheDocument();
    await userEvent.click(screen.getByRole('button', { name: 'Delete' }));
    await waitFor(() =>
      expect(service.deleteJudge).toHaveBeenCalledWith('citation'),
    );
  });

  it('creates a judge auto-assigned to this crew', async () => {
    service.createJudge.mockResolvedValue({ name: 'freshness' });
    renderDialog();
    await screen.findByText('accuracy');
    await userEvent.click(screen.getByText('+ Create judge'));
    await userEvent.type(screen.getByLabelText(/Judge name/), 'freshness');
    await userEvent.type(
      screen.getByLabelText(/Evaluation criteria/),
      'Listings must be recent.',
    );
    await userEvent.click(screen.getByRole('button', { name: 'Create judge' }));
    await waitFor(() =>
      expect(service.createJudge).toHaveBeenCalledWith(
        'freshness',
        'Listings must be recent.',
        undefined,
        CREW_ID,
      ),
    );
  });
});

describe('runs and progress chips', () => {
  it('renders the honest-progress chips for a completed run', async () => {
    renderDialog();
    expect(await screen.findByText('10/10 executions')).toBeInTheDocument();
    expect(screen.getByText('10 variants tried')).toBeInTheDocument();
    expect(screen.getByText('guided by 13 human notes')).toBeInTheDocument();
    expect(screen.getByText('score 0.57 → 0.97')).toBeInTheDocument();
  });

  it('starts a crew run with the budget as the hard execution cap', async () => {
    service.startCrewOptimization.mockResolvedValue({
      run_id: 'r-new',
      status: 'pending',
      dataset_size: 1,
    });
    renderDialog();
    await screen.findByText('10/10 executions');
    await userEvent.click(screen.getByRole('button', { name: 'Start GEPA' }));
    await waitFor(() => expect(service.startCrewOptimization).toHaveBeenCalled());
    const request = service.startCrewOptimization.mock.calls[0][0];
    expect(request.crew_id).toBe(CREW_ID);
    expect(request.max_metric_calls).toBe(10); // the default budget
  });
});

describe('evaluation answers', () => {
  it('sorts ungraded answers first and shows the grading counts', async () => {
    renderDialog();
    expect(
      await screen.findByText(/1 to grade, 1 graded/),
    ).toBeInTheDocument();
    const previews = screen.getAllByText(/Fresh ungraded answer|Location/);
    // The ungraded (newer) answer's preview renders above the graded one.
    expect(previews[0].textContent).toContain('Fresh ungraded answer');
  });

  it('renders the expanded answer as markdown, not raw text', async () => {
    renderDialog();
    await screen.findByText(/1 to grade, 1 graded/);
    const gradeButtons = screen.getAllByRole('button', { name: 'Grade' });
    await userEvent.click(gradeButtons[1]); // the graded, table-bearing answer
    const markdown = await screen.findByTestId('md');
    expect(markdown.textContent).toContain('| Location | Price |');
  });

  it('saves a grade with judge attribution riding the comment', async () => {
    service.addEvalFeedback.mockResolvedValue(true);
    renderDialog();
    await screen.findByText(/1 to grade, 1 graded/);
    await userEvent.click(screen.getAllByRole('button', { name: 'Grade' })[0]);
    await userEvent.type(screen.getByLabelText(/Grade \(0-10\)/), '3');
    await userEvent.click(screen.getByRole('button', { name: 'Save' }));
    await waitFor(() => expect(service.addEvalFeedback).toHaveBeenCalled());
    const [traceId, value] = service.addEvalFeedback.mock.calls[0];
    expect(traceId).toBe('ungraded-newer');
    expect(value).toBe(3);
  });
});
