import { beforeEach, describe, expect, it, vi } from 'vitest';
import { fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import MLflowJudgeAndAdvanced from './MLflowJudgeAndAdvanced';
import type { MLflowSettings } from '../../../types/config/mlflow';

vi.mock('../../../api/config/ModelService', () => ({
  ModelService: {
    getInstance: () => ({
      getEnabledModels: vi.fn().mockResolvedValue({ 'judge-a': {}, 'judge-b': {} }),
    }),
  },
}));

const settings: MLflowSettings = {
  enabled: true,
  evaluation_enabled: true,
  evaluation_judge_model: null,
  evaluation_max_rows: 200,
  optimization_judge_samples: 3,
  backend: { kind: 'local' },
};

function renderIt(overrides: Partial<MLflowSettings> = {}) {
  const onPatch = vi.fn();
  render(<MLflowJudgeAndAdvanced settings={{ ...settings, ...overrides }} saving={false} onPatch={onPatch} />);
  return onPatch;
}

describe('MLflowJudgeAndAdvanced', () => {
  beforeEach(() => vi.clearAllMocks());

  it('saves the chosen judge model', async () => {
    const onPatch = renderIt();
    fireEvent.mouseDown(screen.getByRole('combobox'));
    const listbox = await screen.findByRole('listbox');
    fireEvent.click(await within(listbox).findByText('judge-b'));
    expect(onPatch).toHaveBeenCalledWith({ evaluation_judge_model: 'judge-b' });
  });

  it('clearing the judge sends an empty value', async () => {
    const onPatch = renderIt({ evaluation_judge_model: 'judge-a' });
    fireEvent.mouseDown(screen.getByRole('combobox'));
    const listbox = await screen.findByRole('listbox');
    fireEvent.click(within(listbox).getByText(/Not set/));
    expect(onPatch).toHaveBeenCalledWith({ evaluation_judge_model: '' });
  });

  it('keeps a stored judge selectable even when it is not an enabled model', async () => {
    renderIt({ evaluation_judge_model: 'retired-judge' });
    await waitFor(() => expect(screen.getByRole('combobox')).toHaveTextContent('retired-judge'));
  });

  it('saves and resets an Advanced value', () => {
    const onPatch = renderIt();
    fireEvent.click(screen.getByText('Advanced'));
    const rows = screen.getByLabelText('Evaluation rows');
    fireEvent.change(rows, { target: { value: '50' } });
    const saveButtons = screen.getAllByRole('button', { name: 'Save' });
    fireEvent.click(saveButtons[0]);
    expect(onPatch).toHaveBeenCalledWith({ evaluation_max_rows: 50 });

    fireEvent.click(screen.getAllByRole('button', { name: 'Reset to default' })[1]);
    expect(onPatch).toHaveBeenCalledWith({ optimization_judge_samples: null });
  });

  it('rejects out-of-range judge samples', () => {
    const onPatch = renderIt();
    fireEvent.click(screen.getByText('Advanced'));
    fireEvent.change(screen.getByLabelText('Judge samples'), { target: { value: '12' } });
    expect(screen.getByText('Whole number from 1 to 9.')).toBeInTheDocument();
    expect(screen.getAllByRole('button', { name: 'Save' })[1]).toBeDisabled();
    expect(onPatch).not.toHaveBeenCalled();
  });
});
