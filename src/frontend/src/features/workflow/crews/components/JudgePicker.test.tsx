/**
 * Tests for JudgePicker: the two judge groups (the registry's judges and
 * MLflow's built-ins), the single badge per built-in, toggling a built-in, and
 * the label judges: badged "needs labels" and disabled with a reason until the
 * labels they read exist.
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import JudgePicker from './JudgePicker';

vi.mock('react-i18next', () => ({
  useTranslation: () => ({
    t: (key: string, options?: { defaultValue?: string }) => options?.defaultValue || key,
  }),
}));

const judge = (id: string, role: 'gate' | 'graded', labelFields: string[] = []) => ({
  id,
  label: id,
  description: `${id} description`,
  role,
  weight: 1,
  needs_labels: labelFields.length > 0,
  label_fields: labelFields,
  available: true,
});

const renderPicker = (
  selected: string[] = [],
  toggle = vi.fn(),
  judges = [judge('Safety', 'gate'), judge('Completeness', 'graded')],
  isEnabled = (j: { needs_labels: boolean }) => !j.needs_labels,
) =>
  render(
    <JudgePicker
      judgeRegistry={null}
      assignedJudges={[{ name: 'accuracy', full_name: 'crew_x__accuracy', crew_id: 'x' }]}
      libraryJudges={[]}
      aligning={null}
      showJudgeForm={false}
      onToggleJudgeForm={vi.fn()}
      onEdit={vi.fn()}
      onUnassign={vi.fn()}
      onAlign={vi.fn()}
      onAssign={vi.fn()}
      onDeleteLibrary={vi.fn()}
      builtin={{ judges, selected, toggle, isEnabled }}
    />,
  );

describe('JudgePicker', () => {
  it('renders both groups with a gate or no-labels badge per built-in', () => {
    renderPicker();
    expect(screen.getByText('Your judges')).toBeInTheDocument();
    expect(screen.getByText('accuracy')).toBeInTheDocument();
    expect(screen.getByText('MLflow built-in judges')).toBeInTheDocument();
    expect(screen.getByText('gate')).toBeInTheDocument();
    expect(screen.getByText('no labels needed')).toBeInTheDocument();
    expect(screen.getByText('Completeness description')).toBeInTheDocument();
  });

  it('reflects the selection and toggles a built-in by id', async () => {
    const toggle = vi.fn();
    renderPicker(['Safety'], toggle);
    expect(screen.getByRole('checkbox', { name: 'Safety' })).toBeChecked();
    expect(screen.getByRole('checkbox', { name: 'Completeness' })).not.toBeChecked();
    await userEvent.click(screen.getByRole('checkbox', { name: 'Completeness' }));
    expect(toggle).toHaveBeenCalledWith('Completeness');
  });

  it("names Kasal's own quality judge apart from MLflow's built-ins", () => {
    renderPicker();
    expect(screen.getByText('Quality (Kasal)')).toBeInTheDocument();
    expect(screen.queryByText('Quality (built-in)')).not.toBeInTheDocument();
  });

  it('badges a label judge and disables it with a reason until labels exist', () => {
    renderPicker([], vi.fn(), [
      judge('Correctness', 'graded', ['expected_facts', 'expected_response']),
      judge('ExpectationsGuidelines', 'graded', ['guidelines']),
    ]);
    expect(screen.getAllByText('needs labels')).toHaveLength(2);
    expect(screen.getByRole('checkbox', { name: 'Correctness' })).toBeDisabled();
    expect(screen.getByText('Add expected facts or an expected answer below')).toBeInTheDocument();
    expect(screen.getByText(/Needs review notes/)).toBeInTheDocument();
    expect(screen.queryByText('Correctness description')).not.toBeInTheDocument();
  });

  it('enables a label judge once its labels exist', async () => {
    const toggle = vi.fn();
    renderPicker([], toggle, [judge('Correctness', 'graded', ['expected_facts'])], () => true);
    const box = screen.getByRole('checkbox', { name: 'Correctness' });
    expect(box).toBeEnabled();
    expect(screen.getByText('Correctness description')).toBeInTheDocument();
    await userEvent.click(box);
    expect(toggle).toHaveBeenCalledWith('Correctness');
  });

  it('omits the built-in group when there is nothing to offer', () => {
    render(
      <JudgePicker
        judgeRegistry={null}
        assignedJudges={[]}
        libraryJudges={[]}
        aligning={null}
        showJudgeForm={false}
        onToggleJudgeForm={vi.fn()}
        onEdit={vi.fn()}
        onUnassign={vi.fn()}
        onAlign={vi.fn()}
        onAssign={vi.fn()}
        onDeleteLibrary={vi.fn()}
        builtin={{ judges: [], selected: [], toggle: vi.fn(), isEnabled: () => true }}
      />,
    );
    expect(screen.queryByText('MLflow built-in judges')).not.toBeInTheDocument();
  });
});
