/**
 * Tests for the three new UCMV generation artifacts rendered by UCMVResultViewer:
 *   - source_layer_ddl  — CREATE VIEW DDL per PBI table with TODO steps + error chip
 *   - pbi_only_ingestion_tasks — tables needing snapshot ingestion (non-SQL sources)
 *   - pbi_validation    — validation loop status chip + optional reason / per-view detail
 */
import { render, screen, within } from '@testing-library/react';
import { describe, it, expect } from 'vitest';
import UCMVResultViewer, { type UCMVResult } from './UCMVResultViewer';

/** Minimal valid result that contains views so the viewer renders normally. */
const baseResult = (): UCMVResult => ({
  yaml: {
    mv_fact_sales:
      "version: '1.1'\nsource: cat.sch.fact_sales\nmeasures:\n  - name: sales\n    expr: SUM(source.amount)\n",
  },
  sql: {},
  stats: {},
  views_generated: 1,
});

/* ------------------------------------------------------------------ */
/*  source_layer_ddl                                                   */
/* ------------------------------------------------------------------ */

describe('UCMVResultViewer — source_layer_ddl', () => {
  it('renders a "Source layer (N views)" section with the table DDL', () => {
    const result: UCMVResult = {
      ...baseResult(),
      source_layer_ddl: {
        dim_product: {
          ddl: 'CREATE OR REPLACE VIEW cat.sch.dim_product AS SELECT id, name FROM raw.product;',
          todo_steps: [],
          error: null,
        },
      },
    };
    render(<UCMVResultViewer result={result} />);
    expect(screen.getByText('Source layer (1 view)')).toBeInTheDocument();
    // table name appears in both the label Typography and the prism-highlighted DDL
    expect(screen.getAllByText(/dim_product/).length).toBeGreaterThanOrEqual(1);
    // prism splits tokens into spans; check the concatenated text content instead
    expect(document.body.textContent).toMatch(/CREATE OR REPLACE VIEW/);
  });

  it('shows TODO steps as a warning list under the table entry', () => {
    const result: UCMVResult = {
      ...baseResult(),
      source_layer_ddl: {
        fact_sales: {
          ddl: 'CREATE OR REPLACE VIEW cat.sch.fact_sales AS SELECT * FROM raw.sales;',
          todo_steps: [
            'Map column raw_amount → amount (rename)',
            'Filter out test rows (is_test = 1)',
          ],
          error: null,
        },
      },
    };
    render(<UCMVResultViewer result={result} />);
    expect(screen.getByText('TODO steps:')).toBeInTheDocument();
    expect(screen.getByText('Map column raw_amount → amount (rename)')).toBeInTheDocument();
    expect(screen.getByText('Filter out test rows (is_test = 1)')).toBeInTheDocument();
  });

  it('shows an error chip when the DDL entry carries an error', () => {
    const result: UCMVResult = {
      ...baseResult(),
      source_layer_ddl: {
        dim_date: {
          ddl: '-- could not generate DDL',
          todo_steps: [],
          error: 'Column mapping failed',
        },
      },
    };
    render(<UCMVResultViewer result={result} />);
    expect(screen.getByText('Column mapping failed')).toBeInTheDocument();
  });

  it('shows plural "views" in the header for multiple entries', () => {
    const result: UCMVResult = {
      ...baseResult(),
      source_layer_ddl: {
        dim_a: { ddl: 'CREATE VIEW a AS SELECT 1;', todo_steps: [], error: null },
        dim_b: { ddl: 'CREATE VIEW b AS SELECT 2;', todo_steps: [], error: null },
      },
    };
    render(<UCMVResultViewer result={result} />);
    expect(screen.getByText('Source layer (2 views)')).toBeInTheDocument();
  });

  it('renders nothing for the section when source_layer_ddl is empty', () => {
    const result: UCMVResult = { ...baseResult(), source_layer_ddl: {} };
    render(<UCMVResultViewer result={result} />);
    expect(screen.queryByText(/Source layer/)).not.toBeInTheDocument();
  });

  it('renders nothing when source_layer_ddl is absent', () => {
    render(<UCMVResultViewer result={baseResult()} />);
    expect(screen.queryByText(/Source layer/)).not.toBeInTheDocument();
  });
});

/* ------------------------------------------------------------------ */
/*  pbi_only_ingestion_tasks                                           */
/* ------------------------------------------------------------------ */

describe('UCMVResultViewer — pbi_only_ingestion_tasks', () => {
  it('renders a warning alert listing each ingestion task', () => {
    const result: UCMVResult = {
      ...baseResult(),
      pbi_only_ingestion_tasks: [
        {
          table: 'Budget',
          uc_target: 'cat.sch.budget',
          kind: 'Excel',
          source_description: 'Budget.xlsx on SharePoint',
          recommended_approach: 'Use Lakeflow Connect Excel connector to ingest into cat.sch.budget.',
          snapshot_loader_stub: '# stub\ndef load_budget(): ...',
        },
      ],
    };
    render(<UCMVResultViewer result={result} />);
    expect(screen.getByText('PBI-only tables need ingestion (1)')).toBeInTheDocument();
    expect(screen.getByText('Budget')).toBeInTheDocument();
    // kind chip
    expect(screen.getByText('Excel')).toBeInTheDocument();
    // recommended_approach text
    expect(
      screen.getByText(/Use Lakeflow Connect Excel connector/),
    ).toBeInTheDocument();
  });

  it('shows the uc_target alongside the table name', () => {
    const result: UCMVResult = {
      ...baseResult(),
      pbi_only_ingestion_tasks: [
        {
          table: 'Targets',
          uc_target: 'mycat.mysch.targets',
          kind: 'SharePoint',
          source_description: 'Targets.xlsx',
          recommended_approach: 'Ingest via connector.',
          snapshot_loader_stub: '',
        },
      ],
    };
    render(<UCMVResultViewer result={result} />);
    expect(screen.getByText('mycat.mysch.targets')).toBeInTheDocument();
  });

  it('shows the snapshot loader stub accordion', () => {
    const result: UCMVResult = {
      ...baseResult(),
      pbi_only_ingestion_tasks: [
        {
          table: 'Budget',
          uc_target: 'cat.sch.budget',
          kind: 'Excel',
          source_description: '',
          recommended_approach: '',
          snapshot_loader_stub: 'def load(): pass',
        },
      ],
    };
    render(<UCMVResultViewer result={result} />);
    expect(screen.getByText('Snapshot loader stub')).toBeInTheDocument();
  });

  it('renders nothing when pbi_only_ingestion_tasks is empty', () => {
    const result: UCMVResult = { ...baseResult(), pbi_only_ingestion_tasks: [] };
    render(<UCMVResultViewer result={result} />);
    expect(screen.queryByText(/PBI-only tables need ingestion/)).not.toBeInTheDocument();
  });

  it('renders nothing when pbi_only_ingestion_tasks is absent', () => {
    render(<UCMVResultViewer result={baseResult()} />);
    expect(screen.queryByText(/PBI-only tables need ingestion/)).not.toBeInTheDocument();
  });
});

/* ------------------------------------------------------------------ */
/*  pbi_validation                                                     */
/* ------------------------------------------------------------------ */

describe('UCMVResultViewer — pbi_validation', () => {
  it('renders a "skipped" chip with the reason', () => {
    const result: UCMVResult = {
      ...baseResult(),
      pbi_validation: { status: 'skipped', reason: 'No warehouse configured' },
    };
    render(<UCMVResultViewer result={result} />);
    expect(screen.getByText('Validation:')).toBeInTheDocument();
    expect(screen.getByText('skipped')).toBeInTheDocument();
    expect(screen.getByText('No warehouse configured')).toBeInTheDocument();
  });

  it('renders a "ran" chip (success color) when status is ran', () => {
    const result: UCMVResult = {
      ...baseResult(),
      pbi_validation: { status: 'ran', views: { mv_fact_sales: 'OK — 1 row' } },
    };
    render(<UCMVResultViewer result={result} />);
    const chip = screen.getByText('ran');
    expect(chip).toBeInTheDocument();
    // per-view detail rendered
    expect(screen.getByText(/OK — 1 row/)).toBeInTheDocument();
  });

  it('renders an "error" chip when status is error', () => {
    const result: UCMVResult = {
      ...baseResult(),
      pbi_validation: { status: 'error', reason: 'SQL compilation error' },
    };
    render(<UCMVResultViewer result={result} />);
    expect(screen.getByText('error')).toBeInTheDocument();
    expect(screen.getByText('SQL compilation error')).toBeInTheDocument();
  });

  it('renders the validation label but no reason text when reason is absent', () => {
    const result: UCMVResult = {
      ...baseResult(),
      pbi_validation: { status: 'skipped' },
    };
    render(<UCMVResultViewer result={result} />);
    expect(screen.getByText('Validation:')).toBeInTheDocument();
    expect(screen.getByText('skipped')).toBeInTheDocument();
  });

  it('renders nothing when pbi_validation is absent', () => {
    render(<UCMVResultViewer result={baseResult()} />);
    expect(screen.queryByText('Validation:')).not.toBeInTheDocument();
  });
});

/* ------------------------------------------------------------------ */
/*  All-empty / no-crash guard                                         */
/* ------------------------------------------------------------------ */

describe('UCMVResultViewer — empty artifact fields do not crash', () => {
  it('renders without error when all three new fields are undefined', () => {
    const result: UCMVResult = baseResult();
    expect(() => render(<UCMVResultViewer result={result} />)).not.toThrow();
    // Normal viewer header still present
    expect(screen.getByText('UC Metric View Results')).toBeInTheDocument();
  });

  it('renders without error when all three are explicitly empty/null', () => {
    const result: UCMVResult = {
      ...baseResult(),
      source_layer_ddl: {},
      pbi_only_ingestion_tasks: [],
      pbi_validation: undefined,
    };
    expect(() => render(<UCMVResultViewer result={result} />)).not.toThrow();
    // None of the new sections should appear
    expect(screen.queryByText(/Source layer/)).not.toBeInTheDocument();
    expect(screen.queryByText(/PBI-only tables need ingestion/)).not.toBeInTheDocument();
    expect(screen.queryByText('Validation:')).not.toBeInTheDocument();
  });

  it('renders all three sections together without conflict', () => {
    const result: UCMVResult = {
      ...baseResult(),
      source_layer_ddl: {
        fact_sales: {
          ddl: 'CREATE VIEW cat.sch.fact_sales AS SELECT * FROM raw.sales;',
          todo_steps: ['Check nulls'],
          error: null,
        },
      },
      pbi_only_ingestion_tasks: [
        {
          table: 'Budget',
          uc_target: 'cat.sch.budget',
          kind: 'Excel',
          source_description: '',
          recommended_approach: 'Ingest via Excel connector.',
          snapshot_loader_stub: '',
        },
      ],
      pbi_validation: { status: 'ran', views: { mv_fact_sales: 'PASS' } },
    };
    render(<UCMVResultViewer result={result} />);
    expect(screen.getByText('Source layer (1 view)')).toBeInTheDocument();
    expect(screen.getByText('PBI-only tables need ingestion (1)')).toBeInTheDocument();
    expect(screen.getByText('ran')).toBeInTheDocument();
  });

  it('renders the ingestion section title with correct count for multiple tasks', () => {
    const task = {
      table: 'T',
      uc_target: 'c.s.t',
      kind: 'Web',
      source_description: '',
      recommended_approach: '',
      snapshot_loader_stub: '',
    };
    const result: UCMVResult = {
      ...baseResult(),
      pbi_only_ingestion_tasks: [task, { ...task, table: 'T2', uc_target: 'c.s.t2' }],
    };
    render(<UCMVResultViewer result={result} />);
    expect(screen.getByText('PBI-only tables need ingestion (2)')).toBeInTheDocument();
    // Both table names visible
    const { queryByText } = within(document.body);
    expect(queryByText('T')).toBeInTheDocument();
  });
});

/* ------------------------------------------------------------------ */
/*  BIArtifactsView wiring — verify the three fields are threaded      */
/* ------------------------------------------------------------------ */

// The BIArtifactsView wiring is covered by the ConverterService mock pattern
// already used in BIArtifactsView.test.tsx. This test block keeps the
// artifact-field coverage here while importing BIArtifactsView separately.
import React from 'react';
import { vi, beforeEach } from 'vitest';
import { waitFor } from '@testing-library/react';

const mockListHistory = vi.fn();
vi.mock('../../../api/tools/ConverterService', () => ({
  ConverterService: { listHistory: (...args: unknown[]) => mockListHistory(...args) },
}));

// Dynamic import so the mock is registered before the module is loaded.
const { default: BIArtifactsView } = await import('./BIArtifactsView');

beforeEach(() => vi.clearAllMocks());

const baseRecord = (extra: Record<string, unknown>) => ({
  target_format: 'uc_metrics',
  created_at: '2026-09-24T10:00:00Z',
  output_data: {
    yaml: { mv_otc: "version: '1.1'\nmeasures:\n  - name: a\n    expr: SUM(source.a)\n" },
    sql: {},
    stats: {},
    ...extra,
  },
});

describe('BIArtifactsView — new artifact fields threaded from output_data', () => {
  it('renders the source layer section when output_data carries source_layer_ddl', async () => {
    mockListHistory.mockResolvedValue({
      history: [
        baseRecord({
          source_layer_ddl: {
            dim_product: {
              ddl: 'CREATE VIEW cat.sch.dim_product AS SELECT id FROM raw.product;',
              todo_steps: [],
              error: null,
            },
          },
        }),
      ],
    });
    render(<BIArtifactsView jobId="run-src" />);
    await waitFor(() =>
      expect(screen.getByText('Source layer (1 view)')).toBeInTheDocument(),
    );
  });

  it('renders the ingestion section when output_data carries pbi_only_ingestion_tasks', async () => {
    mockListHistory.mockResolvedValue({
      history: [
        baseRecord({
          pbi_only_ingestion_tasks: [
            {
              table: 'Budget',
              uc_target: 'cat.sch.budget',
              kind: 'Excel',
              source_description: '',
              recommended_approach: 'Ingest via connector.',
              snapshot_loader_stub: '',
            },
          ],
        }),
      ],
    });
    render(<BIArtifactsView jobId="run-ing" />);
    await waitFor(() =>
      expect(screen.getByText('PBI-only tables need ingestion (1)')).toBeInTheDocument(),
    );
  });

  it('renders the validation chip when output_data carries pbi_validation', async () => {
    mockListHistory.mockResolvedValue({
      history: [
        baseRecord({
          pbi_validation: { status: 'skipped', reason: 'No warehouse' },
        }),
      ],
    });
    render(<BIArtifactsView jobId="run-val" />);
    await waitFor(() => expect(screen.getByText('skipped')).toBeInTheDocument());
    expect(screen.getByText('No warehouse')).toBeInTheDocument();
  });
});
