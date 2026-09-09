/**
 * Tests for the "Not transpiled" review panel in UCMVResultViewer. The backend
 * emits `untranslatable_items` (measures that could not be transpiled); the
 * viewer surfaces them as reviewable rows and lets a reviewer assign a status +
 * note, which round-trips via the existing onResultChange/onSave path as
 * `untranslatable_review`.
 */
import { render, screen, fireEvent, within } from '@testing-library/react';
import { describe, it, expect, vi } from 'vitest';
import UCMVResultViewer, {
  type UCMVResult,
  type UntranslatableItem,
  untranslatableKey,
} from './UCMVResultViewer';

const ITEMS: UntranslatableItem[] = [
  {
    table_key: 'mv_fact_sales',
    view_name: 'mv_fact_sales',
    original_name: 'YoY Growth',
    dax_expression: 'CALCULATE([Sales], SAMEPERIODLASTYEAR(cal[date]))',
    skip_reason: 'prior-year time-intelligence',
    category: 'prior-year time-intelligence',
    dax_class: 'unsupported',
    referenced_by: 3,
    proposal: 'Add a calendar date_py join or a window offset to express prior-year.',
  },
  {
    table_key: 'mv_fact_sales',
    view_name: 'mv_fact_sales',
    original_name: 'Sales Color',
    dax_expression: 'IF([Sales] > 0, "green", "red")',
    skip_reason: 'display artifact',
    category: 'display artifact',
    dax_class: 'display_layer',
    referenced_by: 0,
  },
];

const makeResult = (overrides: Partial<UCMVResult> = {}): UCMVResult => ({
  yaml: { mv_fact_sales: "version: '1.1'\nsource: cat.sch.fact_sales\nmeasures:\n  - name: total_sales\n    expr: SUM(source.amount)\n" },
  sql: {},
  stats: {},
  views_generated: 1,
  untranslatable_items: ITEMS,
  ...overrides,
});

describe('UCMVResultViewer — Not transpiled panel', () => {
  it('renders a section + header chip listing the non-transpiled items', () => {
    render(<UCMVResultViewer result={makeResult()} />);
    expect(screen.getByText('Not transpiled')).toBeInTheDocument();
    expect(screen.getByText('2 not transpiled')).toBeInTheDocument();
    // Both measures + their DAX are shown
    expect(screen.getByText('YoY Growth')).toBeInTheDocument();
    expect(screen.getByText('Sales Color')).toBeInTheDocument();
    expect(screen.getByText(/SAMEPERIODLASTYEAR/)).toBeInTheDocument();
  });

  it('renders the Proposed approach column with the proposal text', () => {
    render(<UCMVResultViewer result={makeResult()} />);
    expect(screen.getByText('Proposed approach')).toBeInTheDocument();
    expect(screen.getByText(/Add a calendar date_py join or a window offset/)).toBeInTheDocument();
  });

  it('sorts items by impact (referenced_by) descending', () => {
    render(<UCMVResultViewer result={makeResult()} />);
    const html = document.body.innerHTML;
    // YoY Growth (3) before Sales Color (0)
    expect(html.indexOf('YoY Growth')).toBeLessThan(html.indexOf('Sales Color'));
  });

  it('hides the panel entirely when there are no items', () => {
    render(<UCMVResultViewer result={makeResult({ untranslatable_items: [] })} />);
    expect(screen.queryByText('Not transpiled')).not.toBeInTheDocument();
    expect(screen.queryByText(/not transpiled$/)).not.toBeInTheDocument();
  });

  it('fires onResultChange with untranslatable_review when a note is entered', () => {
    const onResultChange = vi.fn();
    render(
      <UCMVResultViewer result={makeResult()} editable onResultChange={onResultChange} />,
    );
    // Find the YoY Growth row, type into its Note field
    const row = screen.getByText('YoY Growth').closest('tr')!;
    const note = within(row).getByPlaceholderText('Add note…');
    fireEvent.change(note, { target: { value: 'will hand-write' } });

    expect(onResultChange).toHaveBeenCalled();
    const lastArg = onResultChange.mock.calls.at(-1)![0] as UCMVResult;
    const key = untranslatableKey(ITEMS[0]);
    expect(lastArg.untranslatable_review?.[key]?.note).toBe('will hand-write');
  });

  it('pre-fills annotations from a persisted result', () => {
    const key = untranslatableKey(ITEMS[0]);
    render(
      <UCMVResultViewer
        result={makeResult({ untranslatable_review: { [key]: { note: 'saved note', status: 'hand_written' } } })}
        editable
      />,
    );
    const row = screen.getByText('YoY Growth').closest('tr')!;
    const note = within(row).getByPlaceholderText('Add note…') as HTMLInputElement;
    expect(note.value).toBe('saved note');
  });

  it('disables editing controls when not editable', () => {
    render(<UCMVResultViewer result={makeResult()} />);
    const row = screen.getByText('YoY Growth').closest('tr')!;
    const note = within(row).getByPlaceholderText('Add note…') as HTMLInputElement;
    expect(note).toBeDisabled();
  });
});
