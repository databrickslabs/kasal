/**
 * ValidatorResultViewer — Structured display for UCMV Quality Validator output.
 *
 * Shows per-table validation status with green/amber/red indicators,
 * measure counts, and expandable details.
 */
import React, { useState, useMemo } from 'react';
import {
  Box,
  Typography,
  Chip,
  Table,
  TableHead,
  TableBody,
  TableRow,
  TableCell,
  Paper,
  Divider,
  LinearProgress,
  Accordion,
  AccordionSummary,
  AccordionDetails,
} from '@mui/material';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import WarningAmberIcon from '@mui/icons-material/WarningAmber';
import ErrorOutlineIcon from '@mui/icons-material/ErrorOutline';
import VerifiedIcon from '@mui/icons-material/Verified';
import DownloadIcon from '@mui/icons-material/Download';
import VisibilityIcon from '@mui/icons-material/Visibility';
import CloseIcon from '@mui/icons-material/Close';
import Button from '@mui/material/Button';
import IconButton from '@mui/material/IconButton';
import Tooltip from '@mui/material/Tooltip';
import Dialog from '@mui/material/Dialog';
import DialogTitle from '@mui/material/DialogTitle';
import DialogContent from '@mui/material/DialogContent';
import DialogActions from '@mui/material/DialogActions';

/* ------------------------------------------------------------------ */
/*  Types                                                              */
/* ------------------------------------------------------------------ */

interface ValidatorPerTable {
  evaluated?: number;
  valid?: number;
  equivalent?: number;
  review?: number;
  invalid?: number;
  measures?: number;
  base?: number;
  dax?: number;
  switch?: number;
  untranslatable?: number;
  skipped?: string;
  details?: Array<{
    measure_eval?: string;
    measure_name?: string;
    measure_eval_result?: {
      status?: string;
      is_valid?: boolean;
      confidence?: string;
      differences?: string[];
      similarities?: string[];
    };
  }>;
}

interface ValidatorSummary {
  tables_validated?: number;
  total_measures?: number;
  total_evaluated?: number;
  total_valid?: number;
  total_equivalent?: number;
  total_review?: number;
  total_invalid?: number;
  source?: string;
}

export interface ValidatorResult {
  summary: ValidatorSummary;
  per_table: Record<string, ValidatorPerTable>;
  yaml?: Record<string, string>;
  stats?: Record<string, {
    total?: number;
    translated?: number;
    untranslatable?: number;
    base?: number;
    dax?: number;
    switch?: number;
    manual_override?: number;
  }>;
}

/* ------------------------------------------------------------------ */
/*  Detection helper                                                    */
/* ------------------------------------------------------------------ */

// eslint-disable-next-line react-refresh/only-export-components
export function isValidatorResult(value: unknown): value is ValidatorResult {
  if (typeof value !== 'object' || value === null) return false;
  const obj = value as Record<string, unknown>;
  return (
    typeof obj.summary === 'object' &&
    obj.summary !== null &&
    typeof obj.per_table === 'object' &&
    obj.per_table !== null &&
    'total_evaluated' in (obj.summary as Record<string, unknown>)
  );
}

/* ------------------------------------------------------------------ */
/*  Status helpers                                                      */
/* ------------------------------------------------------------------ */

function getTableStatus(data: ValidatorPerTable): 'pass' | 'warning' | 'fail' | 'info' {
  if (data.skipped) return 'info';
  if (data.invalid && data.invalid > 0) return 'fail';
  if (data.review && data.review > 0) return 'warning';
  if ((data.valid && data.valid > 0) || (data.equivalent && data.equivalent > 0)) return 'pass';
  if (data.measures && data.measures > 0 && !data.evaluated) return 'info';
  return 'info';
}

function getStatusIcon(status: string) {
  switch (status) {
    case 'pass': return <CheckCircleIcon sx={{ color: 'success.main', fontSize: 20 }} />;
    case 'warning': return <WarningAmberIcon sx={{ color: 'warning.main', fontSize: 20 }} />;
    case 'fail': return <ErrorOutlineIcon sx={{ color: 'error.main', fontSize: 20 }} />;
    default: return <CheckCircleIcon sx={{ color: 'text.disabled', fontSize: 20 }} />;
  }
}

function getStatusColor(status: string): 'success' | 'warning' | 'error' | 'default' {
  switch (status) {
    case 'pass': return 'success';
    case 'warning': return 'warning';
    case 'fail': return 'error';
    default: return 'default';
  }
}

/* ------------------------------------------------------------------ */
/*  Component                                                           */
/* ------------------------------------------------------------------ */

const ValidatorResultViewer: React.FC<{ result: ValidatorResult }> = ({ result }) => {
  const [expandedTable, setExpandedTable] = useState<string | null>(null);
  const [viewYamlTable, setViewYamlTable] = useState<string | null>(null);

  const { summary, per_table: perTable, stats, yaml: yamlData } = result;
  const hasYaml = yamlData && Object.keys(yamlData).length > 0;

  const handleDownloadYaml = (tableName: string) => {
    if (!yamlData?.[tableName]) return;
    const blob = new Blob([yamlData[tableName]], { type: 'text/yaml;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${tableName}_uc_metric_view.yml`;
    a.click();
    URL.revokeObjectURL(url);
  };

  const handleDownloadAllYamls = () => {
    if (!yamlData) return;
    // Download each metric view as its OWN .yml file (one per fact table) so
    // each can be deployed independently — rather than one concatenated file.
    // Downloads are staggered (~150ms apart) because browsers throttle/deny
    // rapid back-to-back programmatic downloads.
    const entries = Object.entries(yamlData).filter(([, v]) => v && v.trim());
    entries.forEach(([tableName, content], idx) => {
      setTimeout(() => {
        const blob = new Blob([content], { type: 'text/yaml;charset=utf-8' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `${tableName}_uc_metric_view.yml`;
        a.click();
        URL.revokeObjectURL(url);
      }, idx * 150);
    });
  };

  // Calculate aggregate stats
  const tableEntries = useMemo(() => {
    return Object.entries(perTable).map(([name, data]) => ({
      name,
      data,
      status: getTableStatus(data),
      stat: stats?.[name],
    }));
  }, [perTable, stats]);

  const passCount = tableEntries.filter(t => t.status === 'pass').length;
  const warnCount = tableEntries.filter(t => t.status === 'warning').length;
  const failCount = tableEntries.filter(t => t.status === 'fail').length;
  const totalTables = tableEntries.length;

  const totalMeasures = summary.total_measures ||
    tableEntries.reduce((sum, t) => sum + (t.stat?.translated || t.data.measures || 0), 0);
  const totalEval = summary.total_evaluated || 0;
  const totalValid = (summary.total_valid || 0) + (summary.total_equivalent || 0);
  const coveragePct = totalMeasures > 0 ? Math.round((totalValid / Math.max(totalEval, 1)) * 100) : 0;

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
      {/* Header */}
      <Box display="flex" alignItems="center" gap={1.5} flexWrap="wrap">
        <VerifiedIcon color="primary" />
        <Typography variant="h6" sx={{ fontWeight: 600 }}>
          UC Metric View Quality Report
        </Typography>
        {summary.source && (
          <Chip size="small" label={summary.source} variant="outlined" />
        )}
        {hasYaml && (
          <Box sx={{ ml: 'auto' }}>
            <Button
              size="small"
              variant="contained"
              startIcon={<DownloadIcon />}
              onClick={handleDownloadAllYamls}
              title="Downloads each metric view as a separate .yml file"
            >
              Download All YAMLs
            </Button>
          </Box>
        )}
      </Box>

      {/* Summary cards */}
      <Box display="flex" gap={2} flexWrap="wrap">
        <Paper variant="outlined" sx={{ p: 2, flex: 1, minWidth: 150, textAlign: 'center' }}>
          <Typography variant="h4" sx={{ fontWeight: 700, color: 'primary.main' }}>
            {totalTables}
          </Typography>
          <Typography variant="caption" color="text.secondary">Tables Validated</Typography>
        </Paper>
        <Paper variant="outlined" sx={{ p: 2, flex: 1, minWidth: 150, textAlign: 'center' }}>
          <Typography variant="h4" sx={{ fontWeight: 700, color: 'text.primary' }}>
            {totalMeasures}
          </Typography>
          <Typography variant="caption" color="text.secondary">Total Measures</Typography>
        </Paper>
        <Paper variant="outlined" sx={{ p: 2, flex: 1, minWidth: 150, textAlign: 'center' }}>
          <Typography variant="h4" sx={{ fontWeight: 700, color: 'success.main' }}>
            {totalValid}/{totalEval}
          </Typography>
          <Typography variant="caption" color="text.secondary">Valid / Evaluated</Typography>
        </Paper>
        <Paper variant="outlined" sx={{ p: 2, flex: 1, minWidth: 150, textAlign: 'center' }}>
          <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 1 }}>
            <CheckCircleIcon sx={{ color: 'success.main' }} />
            <Typography variant="h4" sx={{ fontWeight: 700, color: 'success.main' }}>
              {passCount}
            </Typography>
          </Box>
          <Typography variant="caption" color="text.secondary">Tables Passing</Typography>
        </Paper>
      </Box>

      {/* Overall progress bar */}
      <Box>
        <Box display="flex" justifyContent="space-between" mb={0.5}>
          <Typography variant="body2" color="text.secondary">
            Validation Coverage
          </Typography>
          <Typography variant="body2" sx={{ fontWeight: 600 }}>
            {coveragePct}%
          </Typography>
        </Box>
        <LinearProgress
          variant="determinate"
          value={coveragePct}
          sx={{
            height: 8,
            borderRadius: 4,
            backgroundColor: 'grey.200',
            '& .MuiLinearProgress-bar': {
              borderRadius: 4,
              backgroundColor: coveragePct >= 80 ? 'success.main' : coveragePct >= 50 ? 'warning.main' : 'error.main',
            },
          }}
        />
      </Box>

      {/* Status summary chips */}
      <Box display="flex" gap={1}>
        {passCount > 0 && <Chip icon={<CheckCircleIcon />} label={`${passCount} passing`} color="success" size="small" />}
        {warnCount > 0 && <Chip icon={<WarningAmberIcon />} label={`${warnCount} review`} color="warning" size="small" />}
        {failCount > 0 && <Chip icon={<ErrorOutlineIcon />} label={`${failCount} failing`} color="error" size="small" />}
      </Box>

      <Divider />

      {/* Per-table results */}
      <Table size="small">
        <TableHead>
          <TableRow>
            <TableCell sx={{ fontWeight: 600, width: 40 }}>Status</TableCell>
            <TableCell sx={{ fontWeight: 600 }}>Table</TableCell>
            <TableCell sx={{ fontWeight: 600, width: 80 }} align="center">Measures</TableCell>
            <TableCell sx={{ fontWeight: 600, width: 80 }} align="center">Evaluated</TableCell>
            <TableCell sx={{ fontWeight: 600, width: 80 }} align="center">Valid</TableCell>
            <TableCell sx={{ fontWeight: 600, width: 100 }} align="center">Breakdown</TableCell>
            {hasYaml && <TableCell sx={{ fontWeight: 600, width: 80 }} align="center">YAML</TableCell>}
          </TableRow>
        </TableHead>
        <TableBody>
          {tableEntries.map(({ name, data, status, stat }) => {
            const evaluated = data.evaluated || 0;
            const valid = (data.valid || 0) + (data.equivalent || 0);
            const measures = stat?.translated || data.measures || 0;
            const untranslatable = stat?.untranslatable || data.untranslatable || 0;

            return (
              <React.Fragment key={name}>
                <TableRow
                  hover
                  sx={{ cursor: data.details ? 'pointer' : 'default' }}
                  onClick={() => data.details && setExpandedTable(expandedTable === name ? null : name)}
                >
                  <TableCell>{getStatusIcon(status)}</TableCell>
                  <TableCell>
                    <Typography variant="body2" sx={{ fontWeight: 500, fontFamily: 'monospace' }}>
                      {name}
                    </Typography>
                    {data.skipped && (
                      <Typography variant="caption" color="text.secondary">{data.skipped}</Typography>
                    )}
                  </TableCell>
                  <TableCell align="center">
                    <Typography variant="body2">{measures}</Typography>
                  </TableCell>
                  <TableCell align="center">
                    <Typography variant="body2">{evaluated || '—'}</Typography>
                  </TableCell>
                  <TableCell align="center">
                    {evaluated > 0 ? (
                      <Chip
                        size="small"
                        label={`${valid}/${evaluated}`}
                        color={getStatusColor(status)}
                        variant="outlined"
                      />
                    ) : '—'}
                  </TableCell>
                  <TableCell align="center">
                    {stat && (
                      <Box display="flex" gap={0.5} justifyContent="center" flexWrap="wrap">
                        {(stat.base || 0) > 0 && <Chip size="small" label={`B:${stat.base}`} variant="outlined" sx={{ fontSize: '0.65rem', height: 20 }} />}
                        {(stat.dax || 0) > 0 && <Chip size="small" label={`D:${stat.dax}`} variant="outlined" sx={{ fontSize: '0.65rem', height: 20 }} />}
                        {(stat.switch || 0) > 0 && <Chip size="small" label={`S:${stat.switch}`} color="secondary" variant="outlined" sx={{ fontSize: '0.65rem', height: 20 }} />}
                        {untranslatable > 0 && <Chip size="small" label={`U:${untranslatable}`} color="error" variant="outlined" sx={{ fontSize: '0.65rem', height: 20 }} />}
                      </Box>
                    )}
                  </TableCell>
                  {hasYaml && (
                    <TableCell align="center">
                      {yamlData?.[name] ? (
                        <Box display="flex" gap={0.5} justifyContent="center">
                          <Tooltip title="View YAML">
                            <IconButton size="small" onClick={(e) => { e.stopPropagation(); setViewYamlTable(name); }}>
                              <VisibilityIcon fontSize="small" />
                            </IconButton>
                          </Tooltip>
                          <Tooltip title="Download YAML">
                            <IconButton size="small" onClick={(e) => { e.stopPropagation(); handleDownloadYaml(name); }}>
                              <DownloadIcon fontSize="small" />
                            </IconButton>
                          </Tooltip>
                        </Box>
                      ) : '—'}
                    </TableCell>
                  )}
                </TableRow>
                {expandedTable === name && data.details && (
                  <TableRow>
                    <TableCell colSpan={6} sx={{ p: 0 }}>
                      <Accordion expanded disableGutters>
                        <AccordionSummary expandIcon={<ExpandMoreIcon />} onClick={() => setExpandedTable(null)}>
                          <Typography variant="body2" sx={{ fontWeight: 600 }}>
                            Measure Details ({data.details.length})
                          </Typography>
                        </AccordionSummary>
                        <AccordionDetails>
                          <Table size="small">
                            <TableHead>
                              <TableRow>
                                <TableCell sx={{ fontWeight: 600 }}>Measure</TableCell>
                                <TableCell sx={{ fontWeight: 600, width: 80 }}>Status</TableCell>
                                <TableCell sx={{ fontWeight: 600 }}>Notes</TableCell>
                              </TableRow>
                            </TableHead>
                            <TableBody>
                              {data.details.map((d, i) => {
                                const mStatus = d.measure_eval_result?.status || d.measure_eval || 'unknown';
                                const isGood = mStatus === 'VALID' || mStatus === 'EQUIVALENT' || mStatus === 'simple';
                                return (
                                  <TableRow key={i}>
                                    <TableCell sx={{ fontFamily: 'monospace', fontSize: '0.75rem' }}>
                                      {d.measure_name || '—'}
                                    </TableCell>
                                    <TableCell>
                                      <Chip
                                        size="small"
                                        label={mStatus}
                                        color={isGood ? 'success' : mStatus === 'REVIEW' ? 'warning' : mStatus === 'matched' ? 'info' : 'default'}
                                        variant="outlined"
                                        sx={{ fontSize: '0.65rem' }}
                                      />
                                    </TableCell>
                                    <TableCell sx={{ fontSize: '0.75rem' }}>
                                      {d.measure_eval_result?.similarities?.join('; ') ||
                                       d.measure_eval_result?.differences?.join('; ') || '—'}
                                    </TableCell>
                                  </TableRow>
                                );
                              })}
                            </TableBody>
                          </Table>
                        </AccordionDetails>
                      </Accordion>
                    </TableCell>
                  </TableRow>
                )}
              </React.Fragment>
            );
          })}
        </TableBody>
      </Table>

      {/* YAML Viewer Dialog */}
      <Dialog
        open={viewYamlTable !== null}
        onClose={() => setViewYamlTable(null)}
        maxWidth="md"
        fullWidth
        sx={{ zIndex: 9999 }}
      >
        <DialogTitle sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
          <Typography variant="h6" sx={{ fontFamily: 'monospace' }}>
            {viewYamlTable}_uc_metric_view.yml
          </Typography>
          <IconButton onClick={() => setViewYamlTable(null)} size="small">
            <CloseIcon />
          </IconButton>
        </DialogTitle>
        <DialogContent dividers>
          <pre style={{
            fontFamily: 'monospace',
            fontSize: '0.8rem',
            lineHeight: 1.5,
            whiteSpace: 'pre-wrap',
            wordBreak: 'break-word',
            margin: 0,
            padding: 16,
            backgroundColor: '#f5f5f5',
            borderRadius: 4,
            maxHeight: '60vh',
            overflow: 'auto',
          }}>
            {viewYamlTable && yamlData?.[viewYamlTable] || ''}
          </pre>
        </DialogContent>
        <DialogActions>
          <Button
            startIcon={<DownloadIcon />}
            onClick={() => viewYamlTable && handleDownloadYaml(viewYamlTable)}
          >
            Download
          </Button>
          <Button onClick={() => setViewYamlTable(null)}>Close</Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

export default ValidatorResultViewer;
