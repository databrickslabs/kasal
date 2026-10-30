import { vi, beforeEach, describe, test, expect } from 'vitest';
import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { ThemeProvider, createTheme } from '@mui/material/styles';
import { UCMVLearningPanel } from './UCMVLearningPanel';

const mockLearn = vi.hoisted(() => vi.fn());
vi.mock('../../../../api/tools/UcmvLearningService', () => ({
  UcmvLearningService: { learn: mockLearn },
}));

const theme = createTheme();
const wrap = (ui: React.ReactNode) => <ThemeProvider theme={theme}>{ui}</ThemeProvider>;

describe('UCMVLearningPanel', () => {
  const onApply = vi.fn();
  beforeEach(() => vi.clearAllMocks());

  test('analyze is disabled until a UCMV is uploaded', () => {
    render(wrap(<UCMVLearningPanel onApplyDomainContext={onApply} />));
    expect(
      screen.getByRole('button', { name: /Generate learned context/i })
    ).toBeDisabled();
  });

  test('upload → analyze → apply the distilled README (view name derived from filename)', async () => {
    mockLearn.mockResolvedValue({
      readme: '# Learned\n- prefer booked_cost',
      views_analyzed: 1,
      views_with_changes: 1,
      matched_views: ['fact_x'],
      unmatched_views: [],
    });
    render(wrap(<UCMVLearningPanel onApplyDomainContext={onApply} />));

    const input = document.querySelector('input[type="file"]') as HTMLInputElement;
    const file = new File(['measures: []'], 'fact_x_uc_metric_view.yml', {
      type: 'text/yaml',
    });
    fireEvent.change(input, { target: { files: [file] } });

    // the uploaded view appears as a chip (filename → view key)
    await waitFor(() => expect(screen.getByText('fact_x')).toBeInTheDocument());

    fireEvent.click(
      screen.getByRole('button', { name: /Generate learned context/i })
    );

    await waitFor(() =>
      expect(screen.getByText(/prefer booked_cost/)).toBeInTheDocument()
    );
    expect(mockLearn).toHaveBeenCalledWith(
      expect.objectContaining({ corrected: { fact_x: 'measures: []' } })
    );

    fireEvent.click(screen.getByRole('button', { name: /Use as Domain context/i }));
    expect(onApply).toHaveBeenCalledWith('# Learned\n- prefer booked_cost');
  });

  test('surfaces the note when no reusable guidance is produced', async () => {
    mockLearn.mockResolvedValue({
      readme: null,
      views_analyzed: 1,
      views_with_changes: 0,
      matched_views: ['fact_x'],
      unmatched_views: [],
      note: 'No material differences found.',
    });
    render(wrap(<UCMVLearningPanel onApplyDomainContext={onApply} />));

    const input = document.querySelector('input[type="file"]') as HTMLInputElement;
    fireEvent.change(input, {
      target: { files: [new File(['x'], 'fact_x.yml', { type: 'text/yaml' })] },
    });
    await waitFor(() => expect(screen.getByText('fact_x')).toBeInTheDocument());
    fireEvent.click(
      screen.getByRole('button', { name: /Generate learned context/i })
    );
    await waitFor(() =>
      expect(screen.getByText(/No material differences found/)).toBeInTheDocument()
    );
    expect(onApply).not.toHaveBeenCalled();
  });
});
