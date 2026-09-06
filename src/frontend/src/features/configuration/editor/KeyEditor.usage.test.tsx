/**
 * Tests for the "used by N" chip on Config Editor dict entries (e.g.
 * measure_resolutions). The chip shows how many other measures reference a
 * TODO entry so reviewers prioritize high-impact gaps (customer example:
 * "C_Last Refreshed → TODO" gets a usage count beside it).
 */
import { render, screen } from '@testing-library/react';
import { describe, it, expect, vi } from 'vitest';
import KeyEditor from './KeyEditor';

const resolutions = {
  'C_Last Refreshed': { base_expr: 'TODO: fill SQL expression', base_filters: [] },
  'C_EPL': { base_expr: 'SUM(source.epl)', base_filters: [] },
};

describe('KeyEditor — used by chip', () => {
  it('shows the usage count next to a TODO entry keyed by a measure name', () => {
    render(
      <KeyEditor
        configKey="measure_resolutions"
        value={resolutions}
        onChange={vi.fn()}
        usage={{ 'C_Last Refreshed': 7, 'C_EPL': 0 }}
      />,
    );
    // The high-usage TODO entry shows its count
    expect(screen.getByText('used by 7')).toBeInTheDocument();
    // TODO marker still present for that entry
    expect(screen.getAllByText('TODO').length).toBeGreaterThan(0);
  });

  it('omits the chip when usage is 0 or the measure is absent from the map', () => {
    render(
      <KeyEditor
        configKey="measure_resolutions"
        value={resolutions}
        onChange={vi.fn()}
        usage={{ 'C_Last Refreshed': 0 }}
      />,
    );
    expect(screen.queryByText(/used by/)).not.toBeInTheDocument();
  });

  it('shows a nested (max) count for switch_decompositions keyed by table name', () => {
    // switch_decompositions is keyed by TABLE (C_Banner); measures are nested
    // list items. The chip should reflect the most-referenced nested measure.
    const switchDecomps = {
      C_Banner: [
        { name: 'f_start_date', raw_expr: 'TODO: ...', comment: 'SWITCH' },
        { name: 'py_end_date', raw_expr: 'TODO: ...', comment: 'SWITCH' },
      ],
    };
    render(
      <KeyEditor
        configKey="switch_decompositions"
        value={switchDecomps}
        onChange={vi.fn()}
        usage={{ f_start_date: 2, py_end_date: 8 }}
      />,
    );
    // Max of the nested measures (8), labeled with ≤ to signal it's a max.
    expect(screen.getByText('used by ≤8')).toBeInTheDocument();
  });

  it('renders without a usage map (backwards compatible)', () => {
    render(
      <KeyEditor
        configKey="measure_resolutions"
        value={resolutions}
        onChange={vi.fn()}
      />,
    );
    expect(screen.queryByText(/used by/)).not.toBeInTheDocument();
    // Entry keys still rendered
    expect(screen.getByText('C_Last Refreshed')).toBeInTheDocument();
  });
});
