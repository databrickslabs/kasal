/**
 * ReevaluationResultViewer — displays the UCMV Re-evaluation tool's report.
 *
 * WHY: Kasal's DAX→UC-Metric-View capability keeps improving, but those gains only
 * ever helped NEW conversions. The re-evaluation sweep re-tries the measures that
 * failed on PAST runs and reports which ones today's transpiler can recover. This
 * viewer surfaces those proposals — measure, why it failed before, and the NEW SQL —
 * so a human can decide to re-transpile.
 *
 * Read-only by design: the sweep proposes, the human applies through the normal
 * UCMV route. See src/docs/powerbi/ucmv-reevaluation-recoverable-measures.md
 */
import React, { useMemo } from 'react';
import {
  Box,
  Typography,
  Paper,
  Chip,
  Divider,
  Table,
  TableHead,
  TableBody,
  TableRow,
  TableCell,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  Alert,
} from '@mui/material';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import AutoFixHighIcon from '@mui/icons-material/AutoFixHigh';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';

/* ------------------------------------------------------------------ */
/*  Types                                                              */
/* ------------------------------------------------------------------ */

export interface RecoveredMeasure {
  original_name: string;
  table_key?: string;
  view_name?: string;
  dax_expression?: string;
  previous_skip_reason?: string;
  previous_category?: string;
  new_sql: string;
  confidence?: string;
  dax_class?: string | null;
  referenced_by?: number;
}

export interface ReevaluationDataset {
  dataset_id?: string;
  workspace_id?: string;
  converted_at?: string;
  conversion_id?: number | string;
  fingerprint_changed?: boolean;
  note?: string;
  newly_translatable?: RecoveredMeasure[];
  still_failing_count?: number;
  skipped_count?: number;
}

export interface ReevaluationResult {
  capability?: {
    fingerprint?: string;
    pattern_count?: number;
    skill_file_count?: number;
  };
  settings?: Record<string, unknown>;
  datasets?: ReevaluationDataset[];
  summary?: {
    datasets_scanned?: number;
    measures_retried?: number;
    measures_recovered?: number;
    datasets_with_recoveries?: number;
  };
  error?: string;
}

/* ------------------------------------------------------------------ */
/*  Detection helper                                                    */
/* ------------------------------------------------------------------ */

// eslint-disable-next-line react-refresh/only-export-components
export function isReevaluationResult(value: unknown): value is ReevaluationResult {
  if (typeof value !== 'object' || value === null) return false;
  const obj = value as Record<string, unknown>;
  const hasDatasets = Array.isArray(obj.datasets);
  const summary = obj.summary as Record<string, unknown> | undefined;
  const hasSummary =
    typeof summary === 'object' && summary !== null && 'datasets_scanned' in summary;
  return hasDatasets && hasSummary;
}

/* ------------------------------------------------------------------ */
/*  Component                                                           */
/* ------------------------------------------------------------------ */

const ReevaluationResultViewer: React.FC<{ result: ReevaluationResult }> = ({ result }) => {
  const summary = result.summary || {};
  const datasets = useMemo(
    () => (Array.isArray(result.datasets) ? result.datasets : []),
    [result.datasets],
  );

  // Datasets with actual proposals come first — that's what a reviewer acts on.
  const ordered = useMemo(() => {
    return [...datasets].sort(
      (a, b) => (b.newly_translatable?.length ?? 0) - (a.newly_translatable?.length ?? 0),
    );
  }, [datasets]);

  const recovered = summary.measures_recovered ?? 0;

  return (
    <Box display="flex" flexDirection="column" gap={2}>
      <Box display="flex" alignItems="center" gap={1}>
        <AutoFixHighIcon color="primary" />
        <Typography variant="h6">UCMV Re-evaluation</Typography>
        {result.capability?.fingerprint && (
          <Chip
            size="small"
            variant="outlined"
            label={`capability ${result.capability.fingerprint}`}
            title={`${result.capability.pattern_count ?? '?'} translator patterns, ${result.capability.skill_file_count ?? '?'} skill files`}
          />
        )}
      </Box>

      {result.error && <Alert severity="error">{result.error}</Alert>}

      {/* Summary tiles */}
      <Box display="flex" gap={2} flexWrap="wrap">
        <Paper variant="outlined" sx={{ p: 2, flex: 1, minWidth: 150, textAlign: 'center' }}>
          <Typography variant="h4" sx={{ fontWeight: 700 }}>
            {summary.datasets_scanned ?? 0}
          </Typography>
          <Typography variant="caption" color="text.secondary">Datasets Scanned</Typography>
        </Paper>
        <Paper variant="outlined" sx={{ p: 2, flex: 1, minWidth: 150, textAlign: 'center' }}>
          <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 1 }}>
            <CheckCircleIcon sx={{ color: recovered > 0 ? 'success.main' : 'text.disabled' }} />
            <Typography
              variant="h4"
              sx={{ fontWeight: 700, color: recovered > 0 ? 'success.main' : 'text.secondary' }}
            >
              {recovered}
            </Typography>
          </Box>
          <Typography variant="caption" color="text.secondary">Now Recoverable</Typography>
        </Paper>
        <Paper variant="outlined" sx={{ p: 2, flex: 1, minWidth: 150, textAlign: 'center' }}>
          <Typography variant="h4" sx={{ fontWeight: 700 }}>
            {summary.measures_retried ?? 0}
          </Typography>
          <Typography variant="caption" color="text.secondary">Measures Retried</Typography>
        </Paper>
        <Paper variant="outlined" sx={{ p: 2, flex: 1, minWidth: 150, textAlign: 'center' }}>
          <Typography variant="h4" sx={{ fontWeight: 700 }}>
            {summary.datasets_with_recoveries ?? 0}
          </Typography>
          <Typography variant="caption" color="text.secondary">Datasets w/ Gains</Typography>
        </Paper>
      </Box>

      {recovered === 0 && !result.error && (
        <Alert severity="info">
          No previously-failed measure became translatable in this sweep. Either the
          transpiler has not changed since those runs, or the remaining gaps are
          permanent limitations (display artifacts, slicer scalars, prior-year
          time-intelligence).
        </Alert>
      )}

      {recovered > 0 && (
        <Alert severity="success">
          {recovered} measure{recovered === 1 ? '' : 's'} that previously failed can now
          be transpiled. Re-run the UCMV generator for the affected dataset(s) to apply
          — nothing has been changed automatically.
        </Alert>
      )}

      <Divider />

      {ordered.map((ds, i) => {
        const items = ds.newly_translatable || [];
        return (
          <Accordion
            key={`${ds.dataset_id || 'ds'}-${i}`}
            variant="outlined"
            disableGutters
            defaultExpanded={items.length > 0}
            sx={{ '&:before': { display: 'none' } }}
          >
            <AccordionSummary expandIcon={<ExpandMoreIcon />}>
              <Box display="flex" alignItems="center" gap={1} flexWrap="wrap">
                <Typography variant="subtitle2" sx={{ fontFamily: 'monospace' }}>
                  {ds.dataset_id || '(unknown dataset)'}
                </Typography>
                {items.length > 0 ? (
                  <Chip size="small" color="success" label={`${items.length} recoverable`} />
                ) : (
                  <Chip size="small" variant="outlined" label="no gains" />
                )}
                {ds.still_failing_count ? (
                  <Chip
                    size="small"
                    variant="outlined"
                    color="warning"
                    label={`${ds.still_failing_count} still failing`}
                  />
                ) : null}
                {ds.fingerprint_changed === false && (
                  <Chip size="small" variant="outlined" label="transpiler unchanged" />
                )}
              </Box>
            </AccordionSummary>
            <AccordionDetails sx={{ p: 0 }}>
              {ds.note && (
                <Typography variant="caption" color="text.secondary" sx={{ px: 2, py: 1, display: 'block' }}>
                  {ds.note}
                </Typography>
              )}
              {items.length > 0 && (
                <Table size="small" sx={{ tableLayout: 'fixed' }}>
                  <TableHead>
                    <TableRow>
                      <TableCell sx={{ fontWeight: 600, width: '18%' }}>Measure</TableCell>
                      <TableCell sx={{ fontWeight: 600, width: '22%' }}>Previously failed because</TableCell>
                      <TableCell sx={{ fontWeight: 600, width: '38%' }}>New SQL</TableCell>
                      <TableCell sx={{ fontWeight: 600, width: '10%' }}>Confidence</TableCell>
                      <TableCell sx={{ fontWeight: 600, width: '8%' }} align="right" title="How many other measures reference this one">Used by</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {items.map((m, j) => (
                      <TableRow key={`${m.original_name}-${j}`} hover>
                        <TableCell sx={{ fontFamily: 'monospace', fontSize: '0.75rem', wordBreak: 'break-word' }}>
                          {m.original_name}
                        </TableCell>
                        <TableCell sx={{ fontSize: '0.7rem', wordBreak: 'break-word' }}>
                          {m.previous_category && (
                            <Chip size="small" variant="outlined" color="warning" label={m.previous_category} sx={{ fontSize: '0.6rem' }} />
                          )}
                          {m.previous_skip_reason && (
                            <Typography variant="caption" display="block" color="text.secondary" sx={{ mt: 0.5 }}>
                              {m.previous_skip_reason}
                            </Typography>
                          )}
                        </TableCell>
                        <TableCell sx={{ fontFamily: 'monospace', fontSize: '0.7rem', wordBreak: 'break-word', whiteSpace: 'pre-wrap', color: 'success.main' }}>
                          {m.new_sql}
                        </TableCell>
                        <TableCell sx={{ fontSize: '0.7rem' }}>{m.confidence || '—'}</TableCell>
                        <TableCell sx={{ fontSize: '0.75rem' }} align="right">
                          {m.referenced_by && m.referenced_by > 0 ? m.referenced_by : '—'}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              )}
            </AccordionDetails>
          </Accordion>
        );
      })}
    </Box>
  );
};

export default ReevaluationResultViewer;
