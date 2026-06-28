import { describe, it, expect } from 'vitest';
import { screen, fireEvent } from '@testing-library/react';
import { GenieTraceDetail, GenieAnswerCard, matchesGenieDetail } from './GenieTraceDetail';
import { findInlineTraceRenderer } from './registry';
import { renderWithChatTheme as render } from '../../../chatTestRender';

// A full Genie tool result, in the labeled-section format the backend
// (genie_tool.py) emits. "Open in Genie:" is on its own line here so it parses
// as its own section (covers the link renderer).
const GENIE_DETAIL = [
  'Question:',
  'What are the top countries by customers?',
  '',
  'What the query does:',
  'Counts customers grouped by country.',
  '',
  'SQL Query:',
  'SELECT country, COUNT(*) FROM customers GROUP BY country',
  '',
  'Answer:',
  '**Switzerland** leads the customer base, then Germany.',
  '',
  'Query Results:',
  '| Country | Customers | Share |',
  '| --- | --- | --- |',
  '| Switzerland | 120 | 57.1% |',
  '| Germany | $90 | 42.9% |',
  '',
  'Suggested follow-up questions:',
  '- Which country grew fastest?',
  '- Show the monthly trend',
  '',
  'Open in Genie:',
  'https://example.com/genie/123',
].join('\n');

describe('matchesGenieDetail', () => {
  it('matches Genie output containing SQL Query / Query Results', () => {
    expect(matchesGenieDetail(GENIE_DETAIL)).toBe(true);
    expect(matchesGenieDetail('SQL Query:\nSELECT 1')).toBe(true);
  });
  it('does not match plain or empty text', () => {
    expect(matchesGenieDetail('just a normal tool result')).toBe(false);
    expect(matchesGenieDetail('')).toBe(false);
  });
});

describe('findInlineTraceRenderer', () => {
  it('returns the inline card for Genie output', () => {
    expect(findInlineTraceRenderer(GENIE_DETAIL)).toBe(GenieAnswerCard);
  });
  it('returns undefined for non-Genie output', () => {
    expect(findInlineTraceRenderer('not genie')).toBeUndefined();
  });
});

describe('GenieAnswerCard', () => {
  it('renders the answer inline and keeps question + SQL collapsed by default', () => {
    const { container } = render(<GenieAnswerCard detail={GENIE_DETAIL} durationMs={2500} />);
    // Answer (markdown) is visible.
    expect(container.textContent).toContain('leads the customer base');
    // Latency badge.
    expect(screen.getByText('· 2.50s')).toBeInTheDocument();
    // Question + SQL are NOT shown until expanded.
    expect(screen.queryByText(/SELECT country/)).not.toBeInTheDocument();
    expect(screen.queryByText(/top countries by customers/)).not.toBeInTheDocument();
    // The toggle is present and collapsed.
    expect(screen.getByText('Show question & SQL')).toBeInTheDocument();
  });

  it('reveals the question + SQL when the toggle is clicked', () => {
    render(<GenieAnswerCard detail={GENIE_DETAIL} />);
    fireEvent.click(screen.getByText('Show question & SQL'));
    expect(screen.getByText(/SELECT country/)).toBeInTheDocument();
    expect(screen.getByText(/top countries by customers/)).toBeInTheDocument();
    expect(screen.getByText('Hide question & SQL')).toBeInTheDocument();
  });

  it('renders the result table + chart and the follow-ups and link', () => {
    render(<GenieAnswerCard detail={GENIE_DETAIL} />);
    // "120" shows in the table cell AND the bar-chart value label.
    expect(screen.getAllByText('120').length).toBeGreaterThan(0);
    expect(screen.getByText('Customers')).toBeInTheDocument();
    // Bar chart caption ("<metric> by <label>").
    expect(screen.getByText('Customers by Country')).toBeInTheDocument();
    // Follow-ups.
    expect(screen.getByText('Which country grew fastest?')).toBeInTheDocument();
    // Genie link.
    const link = screen.getByText('https://example.com/genie/123');
    expect(link.closest('a')).toHaveAttribute('href', 'https://example.com/genie/123');
  });

  it('omits the latency badge when no duration is given', () => {
    render(<GenieAnswerCard detail={GENIE_DETAIL} />);
    expect(screen.queryByText(/^· /)).not.toBeInTheDocument();
  });

  it('renders a SQL-only result (no answer/results/followups) with just the collapsible', () => {
    render(<GenieAnswerCard detail={'SQL Query:\nSELECT 1'} />);
    expect(screen.getByText('Show question & SQL')).toBeInTheDocument();
    fireEvent.click(screen.getByText('Show question & SQL'));
    expect(screen.getByText('SELECT 1')).toBeInTheDocument();
  });

  it('renders nothing for non-Genie output (caller falls back)', () => {
    const { container } = render(<GenieAnswerCard detail={'plain text'} />);
    expect(container).toBeEmptyDOMElement();
  });
});

describe('GenieTraceDetail (expanded pill view)', () => {
  it('renders every section, with the SQL visible', () => {
    render(<GenieTraceDetail detail={GENIE_DETAIL} />);
    expect(screen.getByText(/SELECT country/)).toBeInTheDocument();
    expect(screen.getByText(/top countries by customers/)).toBeInTheDocument();
    expect(screen.getAllByText('120').length).toBeGreaterThan(0);
  });

  it('falls back to a raw text block when Query Results is not a markdown table', () => {
    render(<GenieTraceDetail detail={'SQL Query:\nSELECT 1\n\nQuery Results:\nno rows returned'} />);
    expect(screen.getByText('no rows returned')).toBeInTheDocument();
  });

  it('does not render a chart for a single-row table (not chartable)', () => {
    const detail = [
      'SQL Query:',
      'SELECT 1',
      '',
      'Query Results:',
      '| Country | Customers |',
      '| --- | --- |',
      '| Switzerland | 120 |',
    ].join('\n');
    render(<GenieTraceDetail detail={detail} />);
    expect(screen.getByText('120')).toBeInTheDocument();
    // < 2 data rows -> no bar chart caption
    expect(screen.queryByText(/ by /)).not.toBeInTheDocument();
  });

  it('returns null for non-Genie output', () => {
    const { container } = render(<GenieTraceDetail detail={'nope'} />);
    expect(container).toBeEmptyDOMElement();
  });
});

describe('GenieTraceDetail — parse / table / chart branches', () => {
  const results = (...rows: string[]) => ['Query Results:', ...rows].join('\n');

  it('treats a marker with only empty sections as non-Genie (no usable body)', () => {
    // includes the "SQL Query:" marker but has no body → parse yields no sections
    expect(matchesGenieDetail('SQL Query:')).toBe(false);
  });

  it('ignores any preamble text that appears before the first section header', () => {
    render(<GenieTraceDetail detail={'preamble before any header\nSQL Query:\nSELECT 1'} />);
    expect(screen.getByText('SELECT 1')).toBeInTheDocument();
    expect(screen.queryByText('preamble before any header')).toBeNull();
  });

  it('parses a table with no separator row and ragged / symbol-only cells', () => {
    // No "| --- |" row; "Bern" row is missing its 3rd cell; "$" → no numeric value.
    const detail = results(
      '| City | People | Note |',
      '| Zurich | 100 | a |',
      '| Bern | 50 |',
      '| Geneva | $ | c |',
    );
    render(<GenieTraceDetail detail={detail} />);
    expect(screen.getAllByText('Zurich').length).toBeGreaterThan(0); // table + chart axis
    expect(screen.getByText('People by City')).toBeInTheDocument(); // chart rendered (People numeric)
  });

  it('renders a line chart for a time-like axis (and tolerates an empty label)', () => {
    const detail = results(
      '| Month | Sales |',
      '| --- | --- |',
      '| 2024-01 | 10 |',
      '| 2024-02 | 20 |',
      '|  | 30 |', // empty label cell
    );
    render(<GenieTraceDetail detail={detail} />);
    expect(screen.getByText('Sales by Month')).toBeInTheDocument();
  });

  it('skips the chart when every value is zero', () => {
    render(<GenieTraceDetail detail={results('| Name | Val |', '| --- | --- |', '| A | 0 |', '| B | 0 |')} />);
    expect(screen.getByText('A')).toBeInTheDocument(); // table still shows
    expect(screen.queryByText(/ by /)).toBeNull(); // no chart caption
  });

  it('skips the chart when there is no distinct label column (all numeric)', () => {
    render(<GenieTraceDetail detail={results('| X | Y |', '| --- | --- |', '| 1 | 2 |', '| 3 | 4 |')} />);
    expect(screen.getByText('X')).toBeInTheDocument();
    expect(screen.queryByText(/ by /)).toBeNull();
  });

  it('charts a table whose label column comes after the metric (missing label cell → blank)', () => {
    const detail = results('| Val | Name |', '| --- | --- |', '| 10 | A |', '| 20 |');
    render(<GenieTraceDetail detail={detail} />);
    expect(screen.getByText('Val by Name')).toBeInTheDocument();
  });
});

describe('GenieAnswerCard — sub-second duration', () => {
  it('formats a sub-second latency as ms', () => {
    render(<GenieAnswerCard detail={'SQL Query:\nSELECT 1'} durationMs={640} />);
    expect(screen.getByText('· 640ms')).toBeInTheDocument();
  });
});
