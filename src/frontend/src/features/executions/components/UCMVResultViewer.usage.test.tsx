/**
 * Tests for the "Used by" (referenced-by) column in UCMVResultViewer's measure
 * table. The backend appends "— referenced by N measures" to each measure's
 * YAML comment; the viewer extracts + surfaces it so reviewers prioritize
 * high-impact measures.
 */
import { render, screen } from '@testing-library/react';
import { describe, it, expect } from 'vitest';
import UCMVResultViewer, { type UCMVResult } from './UCMVResultViewer';

const yamlWithUsage = `version: '1.1'
source: cat.sch.fact_test

measures:
  - name: low_use
    expr: SUM(source.a)
    comment: "Low — referenced by 1 measure"
  - name: high_use
    expr: SUM(source.b)
    comment: "High — referenced by 9 measures"
  - name: no_use
    expr: SUM(source.c)
    comment: "Leaf measure"
`;

const makeResult = (yamlStr: string): UCMVResult => ({
  yaml: { mv_fact_test: yamlStr },
  sql: {},
  stats: {},
  views_generated: 1,
});

describe('UCMVResultViewer — Used by column', () => {
  it('renders a Used by column with the extracted counts', () => {
    render(<UCMVResultViewer result={makeResult(yamlWithUsage)} />);
    // Column header present
    expect(screen.getByText('Used by')).toBeInTheDocument();
    // Counts surfaced
    expect(screen.getByText('9')).toBeInTheDocument();
    expect(screen.getByText('1')).toBeInTheDocument();
  });

  it('sorts measures by usage descending (high-impact first)', () => {
    render(<UCMVResultViewer result={makeResult(yamlWithUsage)} />);
    const html = document.body.innerHTML;
    // high_use (9) must appear before low_use (1) before no_use (0)
    expect(html.indexOf('high_use')).toBeLessThan(html.indexOf('low_use'));
    expect(html.indexOf('low_use')).toBeLessThan(html.indexOf('no_use'));
  });

  it('omits the Used by column entirely when no measure carries a count', () => {
    const plain = `version: '1.1'
source: cat.sch.fact_test

measures:
  - name: a
    expr: SUM(source.a)
    comment: "plain"
`;
    render(<UCMVResultViewer result={makeResult(plain)} />);
    expect(screen.queryByText('Used by')).not.toBeInTheDocument();
  });
});

const yamlWithVisualUsage = `version: '1.1'
source: cat.sch.fact_test

measures:
  - name: on_scorecard
    expr: SUM(source.a)
    comment: "PBI: A · Used on: OTC Scorecard"
  - name: on_two_pages
    expr: SUM(source.b)
    comment: "PBI: B — referenced by 2 measures · Used on: OTC NPS, DCC Score"
  - name: nowhere
    expr: SUM(source.c)
    comment: "Leaf measure, not on any visual"
`;

describe('UCMVResultViewer — Used on (visual reference) column', () => {
  it('renders a Used on column with each report page the measure appears on', () => {
    render(<UCMVResultViewer result={makeResult(yamlWithVisualUsage)} />);
    expect(screen.getByText('Used on')).toBeInTheDocument();
    // Page names surfaced as chips (parsed from the "· Used on: …" suffix).
    expect(screen.getByText('OTC Scorecard')).toBeInTheDocument();
    expect(screen.getByText('OTC NPS')).toBeInTheDocument();
    expect(screen.getByText('DCC Score')).toBeInTheDocument();
  });

  it('ranks measures drawn on a visual above those drawn nowhere', () => {
    render(<UCMVResultViewer result={makeResult(yamlWithVisualUsage)} />);
    const html = document.body.innerHTML;
    expect(html.indexOf('on_scorecard')).toBeLessThan(html.indexOf('nowhere'));
    expect(html.indexOf('on_two_pages')).toBeLessThan(html.indexOf('nowhere'));
  });

  it('omits the Used on column entirely when no measure is used on a visual', () => {
    const plain = `version: '1.1'
source: cat.sch.fact_test

measures:
  - name: a
    expr: SUM(source.a)
    comment: "plain"
`;
    render(<UCMVResultViewer result={makeResult(plain)} />);
    expect(screen.queryByText('Used on')).not.toBeInTheDocument();
  });
});
