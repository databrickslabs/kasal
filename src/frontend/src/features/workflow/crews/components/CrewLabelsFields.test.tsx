/**
 * Tests for CrewLabelsFields: the two label fields, suggestions that are only
 * used once accepted (as-is or edited), and the skip reasons of the last run.
 */

import { describe, it, expect, vi } from 'vitest';
import { fireEvent, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import CrewLabelsFields from './CrewLabelsFields';
import { CrewLabelsState } from '../hooks/useCrewLabels';

vi.mock('react-i18next', () => ({
  useTranslation: () => ({
    t: (key: string, options?: { defaultValue?: string }) => options?.defaultValue || key,
  }),
}));

const state = (overrides: Partial<CrewLabelsState> = {}): CrewLabelsState => ({
  facts: '',
  setFacts: vi.fn(),
  answer: '',
  setAnswer: vi.fn(),
  suggestions: [],
  accept: vi.fn(),
  present: [],
  payload: vi.fn(),
  skipped: [],
  setSkipped: vi.fn(),
  ...overrides,
});

describe('CrewLabelsFields', () => {
  it('edits the expected facts and the expected answer', () => {
    const labels = state({ facts: 'Zurich is listed' });
    render(<CrewLabelsFields labels={labels} />);
    expect(screen.getByLabelText('Expected facts')).toHaveValue('Zurich is listed');
    fireEvent.change(screen.getByLabelText('Expected facts'), {
      target: { value: 'Zurich is listed\nPrices in CHF' },
    });
    expect(labels.setFacts).toHaveBeenCalledWith('Zurich is listed\nPrices in CHF');
    fireEvent.change(screen.getByLabelText('Expected answer'), { target: { value: 'A table' } });
    expect(labels.setAnswer).toHaveBeenCalledWith('A table');
  });

  it('shows a suggestion as suggested and accepts it as-is', async () => {
    const labels = state({ suggestions: ['German side only'] });
    render(<CrewLabelsFields labels={labels} />);
    expect(screen.getByText('German side only')).toBeInTheDocument();
    expect(screen.getByText(/not used until you accept it/)).toBeInTheDocument();
    expect(labels.accept).not.toHaveBeenCalled();
    await userEvent.click(screen.getByRole('button', { name: 'Accept' }));
    expect(labels.accept).toHaveBeenCalledWith('German side only');
  });

  it('adds an edited suggestion', async () => {
    const labels = state({ suggestions: ['German side only'] });
    render(<CrewLabelsFields labels={labels} />);
    await userEvent.click(screen.getByRole('button', { name: 'Edit' }));
    const field = screen.getByLabelText('Edit suggested fact');
    fireEvent.change(field, { target: { value: 'Only German-speaking cities' } });
    await userEvent.click(screen.getByRole('button', { name: 'Add' }));
    expect(labels.accept).toHaveBeenCalledWith('German side only', 'Only German-speaking cities');
  });

  it('says why a judge was skipped', () => {
    render(<CrewLabelsFields labels={state({ skipped: ['Correctness skipped: not labelled'] })} />);
    expect(screen.getByText('Correctness skipped: not labelled')).toBeInTheDocument();
  });
});
