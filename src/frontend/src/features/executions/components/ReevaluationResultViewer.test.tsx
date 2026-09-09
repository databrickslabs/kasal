/**
 * Tests for ReevaluationResultViewer — the UCMV Re-evaluation report.
 *
 * The sweep re-tries measures that failed on PAST runs and proposes the ones today's
 * transpiler can recover. This viewer must show the measure, why it failed before, and
 * the NEW SQL — and must clearly say "nothing to gain" when a run is already at the
 * current capability level.
 */
import { render, screen } from '@testing-library/react';
import { describe, it, expect } from 'vitest';
import ReevaluationResultViewer, {
  isReevaluationResult,
  type ReevaluationResult,
} from './ReevaluationResultViewer';

const withRecovery: ReevaluationResult = {
  capability: { fingerprint: 'abc123def4567890', pattern_count: 24, skill_file_count: 10 },
  settings: { use_llm: false },
  datasets: [
    {
      dataset_id: 'ds-old',
      workspace_id: 'ws1',
      fingerprint_changed: true,
      newly_translatable: [
        {
          original_name: 'Total Amount',
          table_key: 'fact_sales',
          previous_skip_reason: 'no matching pattern',
          previous_category: 'complex DAX — needs manual translation',
          new_sql: 'SUM(source.amount)',
          confidence: 'high',
          referenced_by: 7,
        },
      ],
      still_failing_count: 3,
      skipped_count: 2,
    },
  ],
  summary: {
    datasets_scanned: 1, measures_retried: 4, measures_recovered: 1,
    datasets_with_recoveries: 1,
  },
};

const noGains: ReevaluationResult = {
  capability: { fingerprint: 'abc123def4567890' },
  datasets: [
    {
      dataset_id: 'ds-current',
      fingerprint_changed: false,
      note: 'transpiler unchanged since this run — nothing to gain',
      newly_translatable: [],
      still_failing_count: 5,
      skipped_count: 0,
    },
  ],
  summary: {
    datasets_scanned: 1, measures_retried: 0, measures_recovered: 0,
    datasets_with_recoveries: 0,
  },
};

describe('isReevaluationResult', () => {
  it('detects a re-evaluation report', () => {
    expect(isReevaluationResult(withRecovery)).toBe(true);
  });

  it('rejects unrelated shapes', () => {
    expect(isReevaluationResult(null)).toBe(false);
    expect(isReevaluationResult({})).toBe(false);
    expect(isReevaluationResult({ datasets: [] })).toBe(false); // no summary
    expect(isReevaluationResult({ yaml: {}, sql: {}, stats: {} })).toBe(false); // UCMV result
  });
});

describe('ReevaluationResultViewer', () => {
  it('shows summary tiles including the recovered count', () => {
    render(<ReevaluationResultViewer result={withRecovery} />);
    expect(screen.getByText('Now Recoverable')).toBeInTheDocument();
    expect(screen.getByText('Datasets Scanned')).toBeInTheDocument();
    expect(screen.getByText('Measures Retried')).toBeInTheDocument();
  });

  it('lists a recovered measure with its previous reason and NEW SQL', () => {
    render(<ReevaluationResultViewer result={withRecovery} />);
    expect(screen.getByText('Total Amount')).toBeInTheDocument();
    expect(screen.getByText('no matching pattern')).toBeInTheDocument();
    expect(screen.getByText('SUM(source.amount)')).toBeInTheDocument();
    expect(screen.getByText('ds-old')).toBeInTheDocument();
  });

  it('states that nothing was applied automatically', () => {
    render(<ReevaluationResultViewer result={withRecovery} />);
    expect(screen.getByText(/nothing has been changed automatically/i)).toBeInTheDocument();
  });

  it('surfaces the capability fingerprint', () => {
    render(<ReevaluationResultViewer result={withRecovery} />);
    expect(screen.getByText(/capability abc123def4567890/)).toBeInTheDocument();
  });

  it('explains a no-gain sweep instead of looking broken', () => {
    render(<ReevaluationResultViewer result={noGains} />);
    expect(screen.getByText(/No previously-failed measure became translatable/i)).toBeInTheDocument();
    expect(screen.getByText(/transpiler unchanged since this run/i)).toBeInTheDocument();
    expect(screen.getByText('transpiler unchanged')).toBeInTheDocument();
  });

  it('renders an error from the sweep', () => {
    render(
      <ReevaluationResultViewer
        result={{ ...noGains, error: 'could not load stored extractions: db down' }}
      />,
    );
    expect(screen.getByText(/db down/)).toBeInTheDocument();
  });

  it('tolerates a report with no datasets', () => {
    render(<ReevaluationResultViewer result={{ datasets: [], summary: { datasets_scanned: 0 } }} />);
    expect(screen.getByText('UCMV Re-evaluation')).toBeInTheDocument();
  });
});
