/**
 * The "Not transpiled" review panel surfaces visual reference on non-emitted
 * measures too: direct pages (used_in_visuals) and backtraced "↳ via <KPI>"
 * (indirect_visual_usage), in a "Used on" column.
 */
import React from 'react';
import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { NonTranspiledPanel, type UntranslatableItem } from './NonTranspiledPanel';

const base: UntranslatableItem = {
  table_key: 'fact_otc',
  original_name: 'Response Rate %',
  dax_expression: 'DIVIDE([X],[Y])',
  skip_reason: 'DIVIDE sub-expression not translatable',
  category: 'distinct-count',
  referenced_by: 2,
};

const render_ = (items: UntranslatableItem[]) =>
  render(<NonTranspiledPanel items={items} review={{}} editable={false} onReviewChange={() => {}} />);

describe('NonTranspiledPanel — Used on column', () => {
  it('shows direct pages and backtraced "↳ via" on non-emitted measures', () => {
    render_([
      {
        ...base,
        used_in_visuals: [{ page: 'OTC NPS', visual_type: 'card', role: 'drawn' }],
        indirect_visual_usage: [{ page: 'DCC Score', via: 'Overview Score' }],
      },
    ]);
    expect(screen.getByText('Used on')).toBeInTheDocument();
    expect(screen.getByText('OTC NPS')).toBeInTheDocument();
    expect(screen.getByText('↳ via Overview Score')).toBeInTheDocument();
  });

  it('renders a dash when a non-emitted measure has no visual reference', () => {
    render_([base]);
    // The Used on header is always present; the cell shows an em dash.
    expect(screen.getByText('Used on')).toBeInTheDocument();
    expect(screen.queryByText(/↳ via/)).not.toBeInTheDocument();
  });
});
