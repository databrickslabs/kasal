/**
 * Tests for the "Not transpiled" section in ValidatorResultViewer — the final
 * validation view. The per-table counts say HOW MANY measures were not emitted;
 * this section says WHICH ones and WHY (reason + original DAX), so a reviewer can
 * investigate without opening the YAML comment block.
 */
import { render, screen } from '@testing-library/react';
import { describe, it, expect } from 'vitest';
import ValidatorResultViewer, {
  type ValidatorResult,
  type ValidatorUntranslatableItem,
} from './ValidatorResultViewer';

const ITEMS: ValidatorUntranslatableItem[] = [
  {
    table_key: 'fact_sales',
    view_name: 'mv_fact_sales',
    original_name: 'YoY Growth',
    dax_expression: 'CALCULATE([Sales], SAMEPERIODLASTYEAR(cal[date]))',
    skip_reason: 'period-shift not expressible in a static UC metric view',
    category: 'prior-year time-intelligence',
    referenced_by: 4,
    proposal: 'Add a calendar date_py join or a window offset to express prior-year.',
  },
  {
    table_key: 'fact_sales',
    view_name: 'mv_fact_sales',
    original_name: 'Sales Color',
    dax_expression: 'IF([Sales] > 0, "green", "red")',
    skip_reason: 'formatting/label only — not a data measure',
    category: 'display artifact',
    referenced_by: 0,
  },
];

const makeResult = (overrides: Partial<ValidatorResult> = {}): ValidatorResult => ({
  summary: { tables_validated: 1, total_evaluated: 5, total_valid: 5 } as ValidatorResult['summary'],
  per_table_summary: {
    fact_sales: { evaluated: 5, valid: 5, equivalent: 0, review: 0, invalid: 0, total_measures: 7 },
  },
  untranslatable_items: ITEMS,
  ...overrides,
});

describe('ValidatorResultViewer — Not transpiled section', () => {
  it('shows a summary tile with the non-transpiled count', () => {
    render(<ValidatorResultViewer result={makeResult()} />);
    expect(screen.getByText('Not Transpiled')).toBeInTheDocument();
  });

  it('lists each non-transpiled measure with its reason and DAX', () => {
    render(<ValidatorResultViewer result={makeResult()} />);
    expect(screen.getByText('Not transpiled')).toBeInTheDocument();
    expect(screen.getByText('YoY Growth')).toBeInTheDocument();
    expect(screen.getByText('Sales Color')).toBeInTheDocument();
    // Reason (category chip + skip_reason text)
    expect(screen.getByText('prior-year time-intelligence')).toBeInTheDocument();
    expect(screen.getByText(/period-shift not expressible/)).toBeInTheDocument();
    // Original DAX visible for investigation
    expect(screen.getByText(/SAMEPERIODLASTYEAR/)).toBeInTheDocument();
  });

  it('renders the Proposed approach column with the proposal text', () => {
    render(<ValidatorResultViewer result={makeResult()} />);
    expect(screen.getByText('Proposed approach')).toBeInTheDocument();
    expect(screen.getByText(/Add a calendar date_py join or a window offset/)).toBeInTheDocument();
  });

  it('orders items by impact (referenced_by) descending', () => {
    render(<ValidatorResultViewer result={makeResult()} />);
    const html = document.body.innerHTML;
    expect(html.indexOf('YoY Growth')).toBeLessThan(html.indexOf('Sales Color'));
  });

  it('renders nothing extra when there are no non-transpiled items', () => {
    render(<ValidatorResultViewer result={makeResult({ untranslatable_items: [] })} />);
    expect(screen.queryByText('Not transpiled')).not.toBeInTheDocument();
    expect(screen.queryByText('Not Transpiled')).not.toBeInTheDocument();
  });

  it('tolerates a result with the field absent entirely (back-compat)', () => {
    const r = makeResult();
    delete r.untranslatable_items;
    render(<ValidatorResultViewer result={r} />);
    expect(screen.queryByText('Not transpiled')).not.toBeInTheDocument();
  });
});
