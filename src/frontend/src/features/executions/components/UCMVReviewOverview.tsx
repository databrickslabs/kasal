/**
 * UCMVReviewOverview — where a verify-numbers run stands, per metric view.
 *
 * One row per view, one column per count: the Power BI measures in scope (no display or
 * slicer logic), how many are translated, match Power BI, are below the bar, were not
 * compared, were lost on deploy or are not translated, and the plain column sums. The
 * counts come from the run (`review_overview`); nothing is computed here. The review CSV
 * (one row per measure with its status and reason, statistics only) is one click away.
 */
import React, { useCallback } from 'react';
import {
  Box,
  Button,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Tooltip,
  Typography,
} from '@mui/material';
import DownloadIcon from '@mui/icons-material/Download';
import type { UCMVReviewCounts, UCMVReviewHeadline, UCMVReviewViewStatus } from './ucmvResultTypes';

const COLUMNS: { key: keyof UCMVReviewCounts; label: string; help: string }[] = [
  { key: 'in_scope', label: 'In scope', help: 'Power BI measures on this table, without display or slicer logic' },
  { key: 'translated', label: 'Translated', help: 'In the deployed metric view' },
  { key: 'match', label: 'Match Power BI', help: 'Compared with Power BI and at or above the bar' },
  { key: 'below_bar', label: 'Below the bar', help: 'Compared with Power BI and below the bar' },
  { key: 'not_compared', label: 'Not compared', help: 'Translated, but not compared with Power BI' },
  { key: 'lost_on_deploy', label: 'Lost on deploy', help: 'Translated, but removed so the view could deploy' },
  { key: 'not_translated', label: 'Not translated', help: 'No metric-view expression; the reason is in the review CSV' },
];

const STATUS_LABEL: Record<string, string> = {
  at_or_above_bar: 'at or above the bar',
  below_bar: 'below the bar',
  not_compared: 'not compared',
  not_deployed: 'not deployed',
  no_view: 'no view',
};

export interface UCMVReviewOverviewProps {
  overview: Record<string, UCMVReviewCounts>;
  summary?: Record<string, UCMVReviewViewStatus>;
  barPct?: number;
  csv?: string;
  headline?: UCMVReviewHeadline;
}

const UCMVReviewOverview: React.FC<UCMVReviewOverviewProps> = ({ overview, summary, barPct, csv, headline }) => {
  const handleDownload = useCallback(() => {
    const blob = new Blob([csv || ''], { type: 'text/csv;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'review.csv';
    a.click();
    URL.revokeObjectURL(url);
  }, [csv]);

  const views = Object.keys(overview)
    .filter((view) => view !== 'total' && (overview[view].in_scope > 0 || overview[view].column_sums > 0))
    .sort((a, b) => overview[b].in_scope - overview[a].in_scope || a.localeCompare(b));
  const total = overview.total;
  const leftOut = Object.entries(summary || {})
    .filter(([, status]) => status.periods_not_in_power_bi)
    .sort(([a], [b]) => a.localeCompare(b));
  const row = (name: string, counts: UCMVReviewCounts, bold: boolean) => {
    const status = summary?.[name];
    const weight = bold ? 600 : 400;
    return (
      <TableRow key={name}>
        <TableCell sx={{ fontWeight: weight, fontFamily: bold ? undefined : 'monospace' }}>{name}</TableCell>
        {COLUMNS.map((column) => (
          <TableCell key={column.key} align="right" sx={{ fontWeight: weight }}>
            {counts[column.key]}
          </TableCell>
        ))}
        <TableCell align="right" sx={{ fontWeight: weight }}>
          {counts.column_sums_match} of {counts.column_sums}
        </TableCell>
        <TableCell align="right">
          {status?.pct_aligned === null || status?.pct_aligned === undefined ? '' : `${status.pct_aligned.toFixed(1)} %`}
        </TableCell>
        <TableCell>{status ? STATUS_LABEL[status.status] || status.status : ''}</TableCell>
      </TableRow>
    );
  };

  return (
    <Box sx={{ p: 2 }}>
      {headline && (
        <Typography variant="body1" sx={{ mb: 1, fontWeight: 500 }}>
          {headline.in_view} of {headline.in_scope} in-scope measures are in a deployed view.{' '}
          {headline.compared} were compared with Power BI; {headline.matching} match
          {headline.matching_pct === null ? '' : ` (${headline.matching_pct.toFixed(1)} %)`}.
        </Typography>
      )}
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, mb: 1 }}>
        <Typography variant="body2" color="text.secondary" sx={{ flex: 1 }}>
          Power BI measures per view, without display logic. A measure matches when at least{' '}
          {barPct ?? 90} % of its cells agree with Power BI. Column sums are plain sums of a table column.
        </Typography>
        {csv && (
          <Tooltip title="One row per measure: status, share of matching cells and the reason. Statistics only, no values.">
            <Button size="small" variant="outlined" startIcon={<DownloadIcon />} onClick={handleDownload}>
              Download Review
            </Button>
          </Tooltip>
        )}
      </Box>
      <TableContainer>
        <Table size="small" aria-label="Review overview per view">
          <TableHead>
            <TableRow>
              <TableCell>View</TableCell>
              {COLUMNS.map((column) => (
                <Tooltip key={column.key} title={column.help}>
                  <TableCell align="right">{column.label}</TableCell>
                </Tooltip>
              ))}
              <TableCell align="right">Column sums matching</TableCell>
              <TableCell align="right">Cells matching</TableCell>
              <TableCell>Status</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {views.map((view) => row(view, overview[view], false))}
            {total && row('Total', total, true)}
          </TableBody>
        </Table>
      </TableContainer>
      {leftOut.length > 0 && (
        <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mt: 1 }}>
          Not compared, because Power BI returns nothing for these periods:{' '}
          {leftOut
            .map(([view, status]) => {
              const periods = status.periods_not_in_power_bi;
              return periods ? `${view} (${periods.periods} periods, ${periods.first} to ${periods.last})` : view;
            })
            .join('; ')}
          .
        </Typography>
      )}
    </Box>
  );
};

export default UCMVReviewOverview;
