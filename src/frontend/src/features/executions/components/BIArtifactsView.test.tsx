/**
 * BIArtifactsView reconstructs the downloadable UCMV/config artifacts from
 * conversion_history at the builder/gate step. Regression guard: the PBI<->UCMV
 * mapping is persisted in output_data.pbi_ucmv_mapping and must be carried
 * through so UCMVResultViewer's "Download Mapping" button renders here — not
 * only when the viewer holds the live in-session tool result.
 */
import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';

const mockListHistory = vi.fn();
vi.mock('../../../api/tools/ConverterService', () => ({
  ConverterService: { listHistory: (...args: unknown[]) => mockListHistory(...args) },
}));

import BIArtifactsView from './BIArtifactsView';

const ucmvRecord = (extra: Record<string, unknown>) => ({
  target_format: 'uc_metrics',
  created_at: '2026-09-22T10:00:00Z',
  output_data: {
    yaml: { mv_otc: "version: '1.1'\nmeasures:\n  - name: a\n    expr: SUM(source.a)\n" },
    sql: {},
    stats: {},
    ...extra,
  },
});

beforeEach(() => vi.clearAllMocks());

describe('BIArtifactsView — PBI<->UCMV mapping download', () => {
  it('renders the Download Mapping button when the record carries pbi_ucmv_mapping', async () => {
    mockListHistory.mockResolvedValue({
      history: [
        ucmvRecord({
          pbi_ucmv_mapping: {
            mv_otc: '# mv_otc.mapping_candidates.yml\nbinding:\n  ucmv: mv_otc\n',
          },
        }),
      ],
    });
    render(<BIArtifactsView jobId="run-1" />);
    // The button is Tooltip-wrapped (its accessible name becomes the tooltip
    // text), so match the visible label rather than the role name.
    expect(await screen.findByText('Download Mapping')).toBeInTheDocument();
  });

  it('omits the Download Mapping button when no mapping was persisted', async () => {
    mockListHistory.mockResolvedValue({ history: [ucmvRecord({})] });
    render(<BIArtifactsView jobId="run-2" />);
    // The YAML viewer renders (proves the fetch resolved), but no mapping button.
    await waitFor(() => expect(screen.getByText('UC Metric View Results')).toBeInTheDocument());
    expect(screen.queryByText('Download Mapping')).not.toBeInTheDocument();
  });
});
