/**
 * Tests that ValidatorResultViewer renders the COMPACT validator return shape
 * (per_table_summary + attention) as a proper table — not a raw JSON dump.
 * Regression: the backend compacted its return to avoid crew-agent context
 * overflow, which renamed per_table -> per_table_summary and dropped inline yaml.
 */
import { render, screen } from '@testing-library/react';
import { describe, it, expect } from 'vitest';
import ValidatorResultViewer, { isValidatorResult } from './ValidatorResultViewer';

const COMPACT = {
  summary: {
    tables_validated: 3,
    total_evaluated: 7,
    total_valid: 0,
    total_equivalent: 5,
    total_review: 1,
    total_invalid: 1,
  },
  per_table_summary: {
    C_Banner: {
      evaluated: 5, valid: 0, equivalent: 5, review: 0, invalid: 0,
      // slim per-measure list drives the expandable breakdown
      details: [
        { measure_name: 'sales', measure_eval_result: { status: 'EQUIVALENT', similarities: ['SUM match'] } },
        { measure_name: 'margin', measure_eval_result: { status: 'EQUIVALENT', similarities: [] } },
      ],
    },
    fact_pe004: { evaluated: 2, valid: 0, equivalent: 0, review: 1, invalid: 1 },
    Fact_HR_A: { evaluated: 0, valid: 0, equivalent: 0, review: 0, invalid: 0 },
  },
  attention: [
    { table: 'fact_pe004', measure_name: 'bad_ratio', status: 'INVALID',
      detail: { status: 'INVALID', differences: ['den mismatch'] } },
    { table: 'fact_pe004', measure_name: 'iffy', status: 'REVIEW',
      detail: { status: 'REVIEW' } },
  ],
  attention_truncated: false,
  full_detail_in_trace: true,
};

describe('ValidatorResultViewer — compact shape', () => {
  it('isValidatorResult accepts the compact per_table_summary shape', () => {
    expect(isValidatorResult(COMPACT)).toBe(true);
  });

  it('still accepts the legacy per_table shape', () => {
    expect(isValidatorResult({
      summary: { total_evaluated: 1 },
      per_table: { t: { evaluated: 1 } },
    })).toBe(true);
  });

  it('renders per-table rows (not a raw JSON dump)', () => {
    render(<ValidatorResultViewer result={COMPACT} />);
    // Table names appear as rows
    expect(screen.getByText('C_Banner')).toBeInTheDocument();
    expect(screen.getByText('fact_pe004')).toBeInTheDocument();
    // The trace notice is shown (full detail lives in the trace)
    expect(screen.getByText(/full per-measure detail/i)).toBeInTheDocument();
  });

  it('expands a table to show its per-measure breakdown (EQUIVALENT etc.)', async () => {
    const { default: userEvent } = await import('@testing-library/user-event');
    render(<ValidatorResultViewer result={COMPACT} />);
    // C_Banner has slim details → clicking its row reveals each measure + status.
    await userEvent.click(screen.getByText('C_Banner'));
    expect(screen.getByText('sales')).toBeInTheDocument();
    expect(screen.getByText('margin')).toBeInTheDocument();
    // status chip shown for the equivalent measures
    expect(screen.getAllByText('EQUIVALENT').length).toBeGreaterThan(0);
  });

  it('shows YAML download controls when the compact result carries yaml', () => {
    // The validator return keeps `yaml` (small, ~11 KB) so the 1:1 metric-view
    // downloads work even though per-measure details were stripped.
    const withYaml = {
      ...COMPACT,
      yaml: {
        C_Banner: "version: '1.1'\nsource: cat.sch.c_banner\n",
        fact_pe004: "version: '1.1'\nsource: cat.sch.fact_pe004\n",
      },
    };
    render(<ValidatorResultViewer result={withYaml} />);
    expect(screen.getByText(/Download All YAMLs/i)).toBeInTheDocument();
  });
});
