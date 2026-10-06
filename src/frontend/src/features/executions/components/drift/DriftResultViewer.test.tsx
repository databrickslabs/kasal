import { describe, it, expect } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import DriftResultViewer from './DriftResultViewer';
import { describeMeasure, diffYamlLines, isDriftResult, sortMeasures, type DriftResult } from './driftResult';
import { isUCMVResult } from '../UCMVResultViewer';

const BASE = 'version: 1.1\nsource: c.s.f\nmeasures:\n  - name: revenue\n    expr: SUM(source.amount)';
const PROPOSED = `${BASE}\n  - name: cost\n    expr: SUM(source.cost)`;

const result: DriftResult = {
  drift_monitor: true,
  views: [
    {
      full_name: 'c.s.mv_sales',
      catalog: 'c',
      schema: 's',
      name: 'mv_sales',
      baseline_yaml: BASE,
      proposed_yaml: PROPOSED,
      changes: [{ kind: 'added_measure', name: 'cost' }],
      invariant_problems: [],
      counts: { measures_added: 1, measures_updated: 0, joins_added: 0, untranslated: 0 },
      measures: [
        { status: 'unchanged', original_name: 'Revenue', baseline_expr: 'SUM(source.amount)' },
        { status: 'new_in_pbi', original_name: 'Cost', current_dax: 'SUM(S[Cost])', proposed_expr: 'SUM(source.cost)', translation: 'llm', applied: true },
        { status: 'removed_from_pbi', original_name: 'Old KPI', baseline_expr: 'SUM(source.old)', note: 'kept' },
      ],
    },
  ],
  unassigned: [{ status: 'unassigned', original_name: 'Stock', allocations: ['FactStock'] }],
  summary: { views_checked: 1, new_in_pbi: 1, measures_added: 1, removed_from_pbi: 1, unassigned: 1, unchanged: 1 },
  yaml: { mv_sales: PROPOSED },
  deploy_ddl: { 'c.s.mv_sales': 'CREATE OR REPLACE VIEW c.s.mv_sales ...' },
};

describe('driftResult helpers', () => {
  it('detects drift reports and never collides with the UCMV viewer guard', () => {
    expect(isDriftResult(result)).toBe(true);
    expect(isDriftResult({ views: [] })).toBe(false);
    expect(isUCMVResult(result)).toBe(false);
  });

  it('labels PBI changes separately from LLM additions', () => {
    expect(describeMeasure({ status: 'new_in_pbi', original_name: 'x', applied: true, translation: 'llm' }).label).toBe('New → LLM added');
    expect(describeMeasure({ status: 'changed_in_pbi', original_name: 'x', applied: true }).label).toBe('PBI changed → updated');
    expect(describeMeasure({ status: 'possibly_changed', original_name: 'x' }).label).toBe('Suspected change');
  });

  it('marks only inserted lines as added', () => {
    const lines = diffYamlLines(BASE, PROPOSED);
    expect(lines.filter((l) => l.added).map((l) => l.text.trim())).toEqual(['- name: cost', 'expr: SUM(source.cost)']);
  });

  it('marks a replaced expr line, not its neighbours', () => {
    const changed = BASE.replace('SUM(source.amount)', 'SUM(source.net)');
    const added = diffYamlLines(BASE, changed).filter((l) => l.added);
    expect(added).toHaveLength(1);
    expect(added[0].text).toContain('source.net');
  });

  it('sorts applied changes first', () => {
    const sorted = sortMeasures(result.views[0].measures!);
    expect(sorted[0].original_name).toBe('Cost');
  });
});

describe('DriftResultViewer', () => {
  it('renders summary, per-view chips and the proposal notice', () => {
    render(<DriftResultViewer result={result} />);
    expect(screen.getByText('UCMV Drift Monitor')).toBeInTheDocument();
    expect(screen.getByText('c.s.mv_sales')).toBeInTheDocument();
    expect(screen.getByText('+1 added')).toBeInTheDocument();
    expect(screen.getByText(/Nothing was deployed/)).toBeInTheDocument();
    expect(screen.getByText('New → LLM added')).toBeInTheDocument();
    expect(screen.getAllByText('Removed in PBI').length).toBe(2);
    expect(screen.getByText(/Unassigned new measures/)).toBeInTheDocument();
  });

  it('hides unchanged measures until asked', () => {
    render(<DriftResultViewer result={result} />);
    expect(screen.queryByText('Revenue')).not.toBeInTheDocument();
    fireEvent.click(screen.getByLabelText('Show unchanged measures'));
    expect(screen.getByText('Revenue')).toBeInTheDocument();
  });

  it('shows the proposed YAML tab', () => {
    render(<DriftResultViewer result={result} />);
    fireEvent.click(screen.getByRole('tab', { name: 'Proposed YAML' }));
    expect(screen.getByText(/Highlighted lines are new/)).toBeInTheDocument();
    expect(screen.getByText('Copy deploy SQL')).toBeInTheDocument();
  });

  it('reports in-sync views and rejected patches', () => {
    const inSync: DriftResult = {
      ...result,
      yaml: {},
      unassigned: [],
      views: [{ ...result.views[0], changes: [], measures: [], invariant_problems: ['top-level \'source\' changed'] }],
    };
    render(<DriftResultViewer result={inSync} />);
    expect(screen.getByText(/in sync with Power BI/)).toBeInTheDocument();
    expect(screen.getByText(/patch was rejected/)).toBeInTheDocument();
  });

  it('shows the tool error', () => {
    render(<DriftResultViewer result={{ drift_monitor: true, views: [], summary: {}, error: 'ucmv_names is required' }} />);
    expect(screen.getByText('ucmv_names is required')).toBeInTheDocument();
  });
});
