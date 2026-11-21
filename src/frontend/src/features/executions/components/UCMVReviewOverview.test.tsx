/**
 * The review overview of a verify-numbers run: one row per view, one column per count, and
 * the review CSV as a download.
 */
import { render, screen, within } from '@testing-library/react';
import { describe, it, expect } from 'vitest';
import UCMVResultViewer, { type UCMVResult } from './UCMVResultViewer';
import UCMVReviewOverview from './UCMVReviewOverview';
import type { UCMVReviewCounts } from './ucmvResultTypes';

const counts = (over: Partial<UCMVReviewCounts>): UCMVReviewCounts => ({
  in_scope: 0,
  translated: 0,
  match: 0,
  below_bar: 0,
  not_compared: 0,
  lost_on_deploy: 0,
  not_translated: 0,
  column_sums: 0,
  column_sums_match: 0,
  display: 0,
  ...over,
});

const overview = {
  fact_orders: counts({ in_scope: 98, translated: 69, match: 60, below_bar: 9, not_translated: 29, column_sums: 50, column_sums_match: 50 }),
  fact_sales: counts({ in_scope: 6, translated: 6, match: 5, below_bar: 1, column_sums: 4, column_sums_match: 4 }),
  dim_only_display: counts({ display: 3 }),
  total: counts({ in_scope: 104, translated: 75, match: 65, below_bar: 10, not_translated: 29, column_sums: 54, column_sums_match: 54 }),
};
const summary = {
  fact_orders: { status: 'at_or_above_bar', pct_aligned: 97.99 },
  fact_sales: { status: 'below_bar', pct_aligned: 84.4 },
};

describe('UCMVReviewOverview', () => {
  it('lists every view with its counts, largest scope first, and the total last', () => {
    render(<UCMVReviewOverview overview={overview} summary={summary} barPct={90} />);
    const rows = screen.getAllByRole('row');
    expect(within(rows[1]).getByText('fact_orders')).toBeInTheDocument();
    expect(within(rows[1]).getByText('98')).toBeInTheDocument();
    expect(within(rows[1]).getByText('50 of 50')).toBeInTheDocument();
    expect(within(rows[1]).getByText('98.0 %')).toBeInTheDocument();
    expect(within(rows[2]).getByText('fact_sales')).toBeInTheDocument();
    expect(within(rows[2]).getByText('below the bar')).toBeInTheDocument();
    expect(within(rows[rows.length - 1]).getByText('Total')).toBeInTheDocument();
    expect(within(rows[rows.length - 1]).getByText('104')).toBeInTheDocument();
  });

  it('leaves out a view that has neither in-scope measures nor column sums', () => {
    render(<UCMVReviewOverview overview={overview} />);
    expect(screen.queryByText('dim_only_display')).not.toBeInTheDocument();
  });

  it('offers the review CSV only when the run carries one', () => {
    const { rerender } = render(<UCMVReviewOverview overview={overview} />);
    expect(screen.queryByText('Download Review')).not.toBeInTheDocument();
    rerender(<UCMVReviewOverview overview={overview} csv={'view,measure\nfact_sales,m\n'} />);
    expect(screen.getByText('Download Review')).toBeInTheDocument();
  });
});

describe('UCMVResultViewer — review overview', () => {
  const base: UCMVResult = {
    yaml: { fact_sales: "version: '1.1'\nsource: cat.sch.fact_sales\nmeasures:\n  - name: sales\n    expr: SUM(source.amount)\n" },
    sql: {},
    stats: {},
    views_generated: 1,
  };

  it('shows the section for a verify-numbers result and not for a plain one', () => {
    const { rerender } = render(<UCMVResultViewer result={base} />);
    expect(screen.queryByText('Review: measures per view')).not.toBeInTheDocument();
    rerender(<UCMVResultViewer result={{ ...base, review_overview: overview, review_summary: summary, review_bar_pct: 90, review_csv: 'view\n' }} />);
    expect(screen.getByText('Review: measures per view')).toBeInTheDocument();
    expect(screen.getByText('Download Review')).toBeInTheDocument();
  });

  it('says the run in one sentence and names the periods that were left out', () => {
    render(
      <UCMVReviewOverview
        overview={overview}
        summary={{
          ...summary,
          fact_sales: {
            status: 'below_bar',
            pct_aligned: 84.4,
            periods_not_in_power_bi: { periods: 88, first: '2014003', last: '2021012' },
          },
        }}
        barPct={90}
        headline={{ in_scope: 104, in_view: 75, compared: 75, matching: 65, matching_pct: 86.7 }}
      />,
    );
    expect(
      screen.getByText(/75 of 104 in-scope measures are in a deployed view\. 75 were compared with Power BI; 65 match \(86\.7 %\)\./),
    ).toBeInTheDocument();
    expect(screen.getByText(/fact_sales \(88 periods, 2014003 to 2021012\)/)).toBeInTheDocument();
  });

  it('shows neither line when the run does not carry them', () => {
    render(<UCMVReviewOverview overview={overview} summary={summary} barPct={90} />);
    expect(screen.queryByText(/in-scope measures are in a deployed view/)).not.toBeInTheDocument();
    expect(screen.queryByText(/Power BI returns nothing for these periods/)).not.toBeInTheDocument();
  });
});
