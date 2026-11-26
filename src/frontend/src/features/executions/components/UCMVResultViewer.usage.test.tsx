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

// Enriched suffix: HOW OFTEN (total + per-page ×N), HOW (drawn/filter), WHERE (page + type).
const yamlWithVisualUsage = `version: '1.1'
source: cat.sch.fact_test

measures:
  - name: on_scorecard
    expr: SUM(source.a)
    comment: "PBI: A · Used on 1 visual: OTC Scorecard (card·drawn)"
  - name: on_many
    expr: SUM(source.b)
    comment: "PBI: B — referenced by 2 measures · Used on 4 visuals: OTC NPS ×3 (matrix·drawn), DCC Score (slicer·filter)"
  - name: nowhere
    expr: SUM(source.c)
    comment: "Leaf measure, not on any visual"
`;

describe('UCMVResultViewer — Used on (visual reference) column', () => {
  it('renders a Used on column with the page(s) and visual type each measure appears on', () => {
    render(<UCMVResultViewer result={makeResult(yamlWithVisualUsage)} />);
    expect(screen.getByText('Used on')).toBeInTheDocument();
    // Page + visual type surfaced as chips (parsed from the enriched suffix).
    expect(screen.getByText('OTC Scorecard · card')).toBeInTheDocument();
    // A page with >1 visual carries its own "×N" tally.
    expect(screen.getByText('OTC NPS ×3 · matrix')).toBeInTheDocument();
    expect(screen.getByText('DCC Score · slicer')).toBeInTheDocument();
  });

  it('shows HOW OFTEN — the visual-occurrence count', () => {
    render(<UCMVResultViewer result={makeResult(yamlWithVisualUsage)} />);
    expect(screen.getByText('1 visual')).toBeInTheDocument();
    expect(screen.getByText('4 visuals')).toBeInTheDocument();
  });

  it('reconciles the total with a single page when every visual is on it (×N on the chip)', () => {
    // The field case: "7 visuals" all on one page → the page chip shows ×7 so
    // the count and the single chip no longer look like a mismatch.
    const yaml = `version: '1.1'
source: cat.sch.fact_test

measures:
  - name: otc
    expr: SUM(source.a)
    comment: "PBI: X · Used on 7 visuals: OTC Scorecard ×7 (pivotTable/clusteredBarChart·filter)"
`;
    render(<UCMVResultViewer result={makeResult(yaml)} />);
    expect(screen.getByText('7 visuals')).toBeInTheDocument();
    expect(screen.getByText('OTC Scorecard ×7 · pivotTable/clusteredBarChart')).toBeInTheDocument();
  });

  it('ranks measures used on more visuals above those used on fewer / none', () => {
    render(<UCMVResultViewer result={makeResult(yamlWithVisualUsage)} />);
    const html = document.body.innerHTML;
    // on_many (4) before on_scorecard (1) before nowhere (0)
    expect(html.indexOf('on_many')).toBeLessThan(html.indexOf('on_scorecard'));
    expect(html.indexOf('on_scorecard')).toBeLessThan(html.indexOf('nowhere'));
  });

  it('still parses the older bare "Used on: <page>" form (pre-enrichment runs)', () => {
    const legacy = `version: '1.1'
source: cat.sch.fact_test

measures:
  - name: legacy
    expr: SUM(source.a)
    comment: "PBI: L · Used on: OTC Scorecard, OTC NPS"
`;
    render(<UCMVResultViewer result={makeResult(legacy)} />);
    expect(screen.getByText('Used on')).toBeInTheDocument();
    expect(screen.getByText('OTC Scorecard')).toBeInTheDocument();
    expect(screen.getByText('OTC NPS')).toBeInTheDocument();
    // No explicit count in the legacy form → falls back to the page count.
    expect(screen.getByText('2 visuals')).toBeInTheDocument();
  });

  it('shows indirect (backtraced) usage distinctly and keeps direct parsing clean', () => {
    const yaml = `version: '1.1'
source: cat.sch.fact_test

measures:
  - name: promoters
    expr: COUNT(source.p)
    comment: "PBI: Promoters · Indirectly used via [OTC Health Score] on: OTC Scorecard, Exec Summary"
  - name: health
    expr: DIVIDE(...)
    comment: "PBI: Health · Used on 1 visual: OTC Scorecard (slicer·filter) · Indirectly used via [Exec KPI] on: Exec Summary"
`;
    render(<UCMVResultViewer result={makeResult(yaml)} />);
    // The indirect-only sub-KPI surfaces as a distinct "↳ via <parent>" chip.
    expect(screen.getByText('↳ via OTC Health Score')).toBeInTheDocument();
    // A measure with BOTH direct + indirect: direct chip intact, indirect distinct.
    expect(screen.getByText('OTC Scorecard · slicer')).toBeInTheDocument();
    expect(screen.getByText('↳ via Exec KPI')).toBeInTheDocument();
    // The indirect clause must NOT be swallowed into the direct count — the only
    // direct count chip is health's "1 visual" (promoters has no direct usage).
    expect(screen.getAllByText(/^\d+ visuals?$/)).toHaveLength(1);
    expect(screen.getByText('1 visual')).toBeInTheDocument();
  });

  it('flags live-connection views with which semantic model/table to parse', () => {
    const result: UCMVResult = {
      yaml: { mv_live: "version: '1.1'\nmeasures:\n  - name: a\n    expr: SUM(source.a)\n" },
      sql: {},
      stats: {},
      views_generated: 1,
      live_connections: {
        mv_live: { server: 'powerbi://myorg/OTC', database: 'OTC Model', table: 'Live_Sales' },
      },
    };
    render(<UCMVResultViewer result={result} />);
    expect(screen.getByText(/Live connection to a semantic model/i)).toBeInTheDocument();
    expect(screen.getByText('OTC Model')).toBeInTheDocument();
    expect(screen.getByText('Live_Sales')).toBeInTheDocument();
  });

  it('shows no live-connection note when there are none', () => {
    render(<UCMVResultViewer result={makeResult(yamlWithUsage)} />);
    expect(screen.queryByText(/Live connection to a semantic model/i)).not.toBeInTheDocument();
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
