/**
 * Tests for the labels a crew run sends: loaded labels, suggestions that stay
 * out of the request until accepted, the label fields present, and how that
 * gates the built-in judges' selection.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { act, renderHook, waitFor } from '@testing-library/react';
import { parseFacts, useCrewLabels } from './useCrewLabels';
import { useBuiltinJudges } from './useBuiltinJudges';
import { PromptOptimizationService } from '../../../../api/config/PromptOptimizationService';

vi.mock('../../../../api/config/PromptOptimizationService', () => ({
  PromptOptimizationService: {
    getCrewLabels: vi.fn(),
    listBuiltinJudges: vi.fn(),
  },
}));

const service = vi.mocked(PromptOptimizationService);

const loaded = async (info: Awaited<ReturnType<typeof service.getCrewLabels>>) => {
  service.getCrewLabels.mockResolvedValue(info);
  const hook = renderHook(() => useCrewLabels(true, 'crew-1'));
  await waitFor(() => expect(service.getCrewLabels).toHaveBeenCalledWith('crew-1'));
  return hook;
};

describe('parseFacts', () => {
  it('reads one fact per line and drops bullets and blanks', () => {
    expect(parseFacts('- Zurich\n\n * CHF \nplain')).toEqual(['Zurich', 'CHF', 'plain']);
  });
});

describe('useCrewLabels', () => {
  beforeEach(() => vi.clearAllMocks());

  it('fills the fields from the confirmed labels', async () => {
    const { result } = await loaded({
      labels: { expected_facts: ['Zurich', 'CHF'], expected_response: 'A table' },
      suggestions: [],
      has_review_notes: false,
    });
    await waitFor(() => expect(result.current.facts).toBe('Zurich\nCHF'));
    expect(result.current.present).toEqual(['expected_facts', 'expected_response']);
    expect(result.current.payload()).toEqual({
      expected_facts: ['Zurich', 'CHF'],
      expected_response: 'A table',
    });
  });

  it('never sends a suggestion until it is accepted', async () => {
    const { result } = await loaded({
      labels: null,
      suggestions: ['German side only', 'Cite sources'],
      has_review_notes: true,
    });
    await waitFor(() => expect(result.current.suggestions).toHaveLength(2));
    expect(result.current.payload()).toEqual({ expected_facts: [], expected_response: '' });
    expect(result.current.present).toEqual(['guidelines']);

    act(() => result.current.accept('German side only'));
    act(() => result.current.accept('Cite sources', 'Cite two sources'));
    expect(result.current.suggestions).toEqual([]);
    expect(result.current.payload()?.expected_facts).toEqual([
      'German side only',
      'Cite two sources',
    ]);
  });

  it('sends nothing over labels it could not read', async () => {
    service.getCrewLabels.mockRejectedValue(new Error('down'));
    const { result } = renderHook(() => useCrewLabels(true, 'crew-1'));
    await waitFor(() => expect(service.getCrewLabels).toHaveBeenCalled());
    expect(result.current.payload()).toBeUndefined();
    act(() => result.current.setFacts('typed fact'));
    expect(result.current.payload()).toEqual({
      expected_facts: ['typed fact'],
      expected_response: '',
    });
  });
});

describe('useBuiltinJudges with labels', () => {
  const correctness = {
    id: 'Correctness',
    label: 'Correctness',
    description: 'd',
    role: 'graded' as const,
    weight: 1,
    needs_labels: true,
    label_fields: ['expected_facts', 'expected_response'],
    available: true,
  };

  it('sends a label judge only while its labels are present', async () => {
    service.listBuiltinJudges.mockResolvedValue([correctness]);
    const { result, rerender } = renderHook(
      ({ present }) => useBuiltinJudges(true, present),
      { initialProps: { present: [] as string[] } },
    );
    await waitFor(() => expect(result.current.judges).toHaveLength(1));
    act(() => result.current.toggle('Correctness'));
    expect(result.current.isEnabled(correctness)).toBe(false);
    expect(result.current.selected).toEqual([]);
    rerender({ present: ['expected_facts'] });
    expect(result.current.selected).toEqual(['Correctness']);
  });
});
