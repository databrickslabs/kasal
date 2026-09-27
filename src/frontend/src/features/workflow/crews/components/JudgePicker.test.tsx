/**
 * Tests for JudgePicker: the two judge groups (the registry's judges and
 * MLflow's built-ins), the single badge per built-in, and toggling a built-in.
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

const judge = (id: string, role: 'gate' | 'graded') => ({
  id,
  label: id,
  description: `${id} description`,
  role,
  weight: 1,
  needs_labels: false,
  available: true,
});

const renderPicker = (selected: string[] = [], toggle = vi.fn()) =>
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
      builtin={{ judges: [judge('Safety', 'gate'), judge('Completeness', 'graded')], selected, toggle }}
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
        builtin={{ judges: [], selected: [], toggle: vi.fn() }}
      />,
    );
    expect(screen.queryByText('MLflow built-in judges')).not.toBeInTheDocument();
  });
});
