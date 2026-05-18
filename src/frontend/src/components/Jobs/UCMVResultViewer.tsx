/**
 * UCMVResultViewer — Structured display for UC Metric View Builder output.
 *
 * Detects the UCMV result shape ({ yaml, sql, stats, migration_report? })
 * and renders each metric view with collapsible sections for source SQL,
 * joins, dimensions, measures, deploy SQL, and raw YAML.
 *
 * Supports an optional edit mode where the Raw YAML section becomes editable
 * and changes are propagated via `onResultChange`.
 */
import React, { useState, useMemo, useCallback } from 'react';
import {
  Box,
  Typography,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  List,
  ListItemButton,
  ListItemText,
  Chip,
  Table,
  TableHead,
  TableBody,
  TableRow,
  TableCell,
  useTheme,
  Paper,
  Divider,
  TextField,
  IconButton,
  Tooltip,
  Alert,
} from '@mui/material';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import StorageIcon from '@mui/icons-material/Storage';
import LinkIcon from '@mui/icons-material/Link';
import ViewColumnIcon from '@mui/icons-material/ViewColumn';
import FunctionsIcon from '@mui/icons-material/Functions';
import CodeIcon from '@mui/icons-material/Code';
import DescriptionIcon from '@mui/icons-material/Description';
import EditIcon from '@mui/icons-material/Edit';
import EditOffIcon from '@mui/icons-material/EditOff';
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutline';
import UndoIcon from '@mui/icons-material/Undo';
import DownloadIcon from '@mui/icons-material/Download';
import SaveIcon from '@mui/icons-material/Save';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import Button from '@mui/material/Button';
import { Highlight, themes } from 'prism-react-renderer';
import yaml from 'js-yaml';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import rehypeSanitize from 'rehype-sanitize';

/* ------------------------------------------------------------------ */
/*  Types                                                              */
/* ------------------------------------------------------------------ */

interface MetricViewDef {
  version?: string;
  source?: string;
  filter?: string;
  dimensions?: Array<{ name: string; expr?: string; comment?: string }>;
  measures?: Array<{ name: string; expr?: string; comment?: string; format?: string }>;
  joins?: Array<{ name: string; source?: string; on?: string; using?: string[] }>;
}

interface UCMVStats {
  total_measures_translated?: number;
  total_untranslatable?: number;
  total_artifact_measures?: number;
  coverage_pct?: number;
  [key: string]: unknown;
}

export interface UCMVResult {
  yaml: Record<string, string>;
  sql: Record<string, string>;
  stats: Record<string, UCMVStats> | UCMVStats;
  migration_report?: string;
}

interface UCMVResultViewerProps {
  result: UCMVResult;
  /** Enable editing of individual metric view YAMLs */
  editable?: boolean;
  /** Called whenever the user edits YAML. Returns the full updated result. */
  onResultChange?: (updated: UCMVResult) => void;
  /** Called when user clicks Save — persists current edits to the database. */
  onSave?: (updated: UCMVResult) => Promise<void>;
}

/* ------------------------------------------------------------------ */
/*  Detection helper (exported for consumers)                          */
/* ------------------------------------------------------------------ */

// eslint-disable-next-line react-refresh/only-export-components
export function isUCMVResult(value: unknown): value is UCMVResult {
  if (typeof value !== 'object' || value === null) return false;
  const obj = value as Record<string, unknown>;
  return (
    typeof obj.yaml === 'object' &&
    obj.yaml !== null &&
    typeof obj.sql === 'object' &&
    obj.sql !== null &&
    typeof obj.stats === 'object' &&
    obj.stats !== null
  );
}

/* ------------------------------------------------------------------ */
/*  SQL Highlight block                                                */
/* ------------------------------------------------------------------ */

const SQLBlock: React.FC<{ code: string; language?: string }> = ({ code, language = 'sql' }) => {
  const muiTheme = useTheme();
  const isDark = muiTheme.palette.mode === 'dark';
  const prismTheme = isDark ? themes.oneDark : themes.oneLight;

  return (
    <Highlight theme={prismTheme} code={code.trim()} language={language}>
      {({ style, tokens, getLineProps, getTokenProps }) => (
        <pre
          style={{
            ...style,
            margin: 0,
            padding: 12,
            overflow: 'auto',
            maxHeight: 400,
            fontSize: '0.8rem',
            fontFamily: 'monospace',
            lineHeight: 1.5,
            borderRadius: 4,
          }}
        >
          {tokens.map((line, i) => (
            <div key={i} {...getLineProps({ line })}>
              {line.map((token, key) => (
                <span key={key} {...getTokenProps({ token })} />
              ))}
            </div>
          ))}
        </pre>
      )}
    </Highlight>
  );
};

/* ------------------------------------------------------------------ */
/*  Compact table for dimensions / measures                            */
/* ------------------------------------------------------------------ */

const FieldTable: React.FC<{
  fields: Array<{ name: string; expr?: string; comment?: string; format?: string }>;
  showFormat?: boolean;
}> = ({ fields, showFormat }) => (
  <Table size="small" sx={{ tableLayout: 'fixed' }}>
    <TableHead>
      <TableRow>
        <TableCell sx={{ fontWeight: 600, width: '30%' }}>Name</TableCell>
        <TableCell sx={{ fontWeight: 600, width: showFormat ? '40%' : '70%' }}>Expression</TableCell>
        {showFormat && <TableCell sx={{ fontWeight: 600, width: '30%' }}>Comment / Format</TableCell>}
      </TableRow>
    </TableHead>
    <TableBody>
      {fields.map((f) => (
        <TableRow key={f.name} hover>
          <TableCell sx={{ fontFamily: 'monospace', fontSize: '0.8rem', wordBreak: 'break-word' }}>
            {f.name}
          </TableCell>
          <TableCell sx={{ fontFamily: 'monospace', fontSize: '0.8rem', wordBreak: 'break-word' }}>
            {f.expr || '—'}
          </TableCell>
          {showFormat && (
            <TableCell sx={{ fontSize: '0.8rem', wordBreak: 'break-word' }}>
              {f.comment || f.format || '—'}
            </TableCell>
          )}
        </TableRow>
      ))}
    </TableBody>
  </Table>
);

/* ------------------------------------------------------------------ */
/*  Joins table                                                        */
/* ------------------------------------------------------------------ */

const JoinsTable: React.FC<{
  joins: Array<{ name: string; source?: string; on?: string; using?: string[] }>;
}> = ({ joins }) => (
  <Table size="small">
    <TableHead>
      <TableRow>
        <TableCell sx={{ fontWeight: 600 }}>Name</TableCell>
        <TableCell sx={{ fontWeight: 600 }}>Source</TableCell>
        <TableCell sx={{ fontWeight: 600 }}>On / Using</TableCell>
      </TableRow>
    </TableHead>
    <TableBody>
      {joins.map((j) => (
        <TableRow key={j.name} hover>
          <TableCell sx={{ fontFamily: 'monospace', fontSize: '0.8rem' }}>{j.name}</TableCell>
          <TableCell sx={{ fontFamily: 'monospace', fontSize: '0.8rem' }}>{j.source || '—'}</TableCell>
          <TableCell sx={{ fontFamily: 'monospace', fontSize: '0.8rem' }}>
            {j.on || (j.using ? j.using.join(', ') : '—')}
          </TableCell>
        </TableRow>
      ))}
    </TableBody>
  </Table>
);

/* ------------------------------------------------------------------ */
/*  Stats bar helper                                                   */
/* ------------------------------------------------------------------ */

const StatsChips: React.FC<{ stats: UCMVStats }> = ({ stats }) => (
  <Box display="flex" gap={1} flexWrap="wrap">
    {stats.total_measures_translated != null && (
      <Chip size="small" label={`${stats.total_measures_translated} translated`} color="success" variant="outlined" />
    )}
    {stats.total_untranslatable != null && (
      <Chip
        size="small"
        label={`${stats.total_untranslatable} untranslatable`}
        color={stats.total_untranslatable > 0 ? 'warning' : 'default'}
        variant="outlined"
      />
    )}
    {stats.total_artifact_measures != null && (
      <Chip
        size="small"
        label={`${stats.total_artifact_measures} artifacts`}
        color={stats.total_artifact_measures > 0 ? 'info' : 'default'}
        variant="outlined"
      />
    )}
    {stats.coverage_pct != null && (
      <Chip
        size="small"
        label={`${Math.round(stats.coverage_pct)}% coverage`}
        color={stats.coverage_pct >= 100 ? 'success' : stats.coverage_pct >= 80 ? 'warning' : 'error'}
        variant="filled"
      />
    )}
  </Box>
);

/* ------------------------------------------------------------------ */
/*  Section accordion                                                  */
/* ------------------------------------------------------------------ */

const Section: React.FC<{
  title: string;
  icon: React.ReactNode;
  count?: number;
  defaultExpanded?: boolean;
  action?: React.ReactNode;
  children: React.ReactNode;
}> = ({ title, icon, count, defaultExpanded = false, action, children }) => (
  <Accordion defaultExpanded={defaultExpanded} disableGutters variant="outlined" sx={{ '&:before': { display: 'none' } }}>
    <AccordionSummary expandIcon={<ExpandMoreIcon />}>
      <Box display="flex" alignItems="center" gap={1} flexGrow={1}>
        {icon}
        <Typography variant="subtitle2">{title}</Typography>
        {count != null && <Chip size="small" label={count} sx={{ height: 20, fontSize: '0.75rem' }} />}
        {action && <Box sx={{ ml: 'auto', mr: 1 }}>{action}</Box>}
      </Box>
    </AccordionSummary>
    <AccordionDetails sx={{ p: 0, overflow: 'auto' }}>{children}</AccordionDetails>
  </Accordion>
);

/* ------------------------------------------------------------------ */
/*  Main component                                                     */
/* ------------------------------------------------------------------ */

const UCMVResultViewer: React.FC<UCMVResultViewerProps> = ({ result, editable = false, onResultChange, onSave }) => {
  const viewNames = useMemo(() => Object.keys(result.yaml).sort(), [result.yaml]);
  const [selected, setSelected] = useState(viewNames[0] ?? '');

  // Track which views are in edit mode (key → draft YAML string)
  const [editingYaml, setEditingYaml] = useState<Record<string, string>>({});
  // Track YAML parse errors per view
  const [yamlErrors, setYamlErrors] = useState<Record<string, string>>({});
  // Track which views have been removed
  const [removedViews, setRemovedViews] = useState<Set<string>>(new Set());
  // Save state
  const [isSaving, setIsSaving] = useState(false);
  const [saveSuccess, setSaveSuccess] = useState(false);

  // The "live" YAML for the selected view — draft if editing, else original
  const currentYaml = editingYaml[selected] ?? result.yaml[selected] ?? '';

  // Parse the selected YAML (use draft if editing)
  const parsed = useMemo<MetricViewDef | null>(() => {
    if (!currentYaml) return null;
    try {
      return yaml.load(currentYaml) as MetricViewDef;
    } catch {
      return null;
    }
  }, [currentYaml]);

  // Per-view stats or global stats
  const viewStats = useMemo<UCMVStats | null>(() => {
    if (!result.stats) return null;
    const s = result.stats as Record<string, UCMVStats>;
    if (s[selected]) return s[selected];
    if ('total_measures_translated' in result.stats || 'coverage_pct' in result.stats) {
      return result.stats as UCMVStats;
    }
    return null;
  }, [result.stats, selected]);

  // Aggregate stats for header
  const aggregateStats = useMemo(() => {
    let totalMeasures = 0;
    let totalDims = 0;
    const activeViews = viewNames.filter((n) => !removedViews.has(n));
    for (const name of activeViews) {
      const raw = editingYaml[name] ?? result.yaml[name];
      if (!raw) continue;
      try {
        const p = yaml.load(raw) as MetricViewDef;
        totalMeasures += p?.measures?.length ?? 0;
        totalDims += p?.dimensions?.length ?? 0;
      } catch {
        // skip
      }
    }
    return { totalMeasures, totalDims, activeViews: activeViews.length };
  }, [result.yaml, editingYaml, removedViews, viewNames]);

  // Build the current full result (with edits applied) for parent
  const buildEditedResult = useCallback(
    (overrides?: { yamlEdits?: Record<string, string>; removed?: Set<string> }): UCMVResult => {
      const edits = overrides?.yamlEdits ?? editingYaml;
      const removed = overrides?.removed ?? removedViews;
      const newYaml: Record<string, string> = {};
      const newSql: Record<string, string> = {};
      const newStats: Record<string, UCMVStats> = {};
      for (const name of viewNames) {
        if (removed.has(name)) continue;
        newYaml[name] = edits[name] ?? result.yaml[name];
        if (result.sql[name]) newSql[name] = result.sql[name];
        const s = result.stats as Record<string, UCMVStats>;
        if (s[name]) newStats[name] = s[name];
      }
      return { ...result, yaml: newYaml, sql: newSql, stats: newStats };
    },
    [editingYaml, removedViews, viewNames, result]
  );

  // Start editing a view
  const handleStartEdit = (name: string) => {
    setEditingYaml((prev) => ({ ...prev, [name]: prev[name] ?? result.yaml[name] }));
  };

  // Stop editing (discard draft)
  const handleCancelEdit = (name: string) => {
    setEditingYaml((prev) => {
      const next = { ...prev };
      delete next[name];
      return next;
    });
    setYamlErrors((prev) => {
      const next = { ...prev };
      delete next[name];
      return next;
    });
  };

  // Update draft YAML
  const handleYamlChange = (name: string, value: string) => {
    // Validate YAML
    try {
      yaml.load(value);
      setYamlErrors((prev) => {
        const next = { ...prev };
        delete next[name];
        return next;
      });
    } catch (e) {
      setYamlErrors((prev) => ({
        ...prev,
        [name]: e instanceof Error ? e.message : 'Invalid YAML',
      }));
    }

    const newEdits = { ...editingYaml, [name]: value };
    setEditingYaml(newEdits);
    onResultChange?.(buildEditedResult({ yamlEdits: newEdits }));
  };

  // Remove a view
  const handleRemoveView = (name: string) => {
    const newRemoved = new Set(removedViews);
    newRemoved.add(name);
    setRemovedViews(newRemoved);
    // Select next available view
    const remaining = viewNames.filter((n) => !newRemoved.has(n));
    if (remaining.length > 0 && name === selected) {
      setSelected(remaining[0]);
    }
    onResultChange?.(buildEditedResult({ removed: newRemoved }));
  };

  // Restore a removed view
  const handleRestoreView = (name: string) => {
    const newRemoved = new Set(removedViews);
    newRemoved.delete(name);
    setRemovedViews(newRemoved);
    onResultChange?.(buildEditedResult({ removed: newRemoved }));
  };

  const isEditing = selected in editingYaml;
  const hasEdits = Object.keys(editingYaml).length > 0 || removedViews.size > 0;

  const handleSave = useCallback(async () => {
    if (!onSave) return;
    setIsSaving(true);
    setSaveSuccess(false);
    try {
      await onSave(buildEditedResult());
      setSaveSuccess(true);
      setTimeout(() => setSaveSuccess(false), 3000);
    } finally {
      setIsSaving(false);
    }
  }, [onSave, buildEditedResult]);

  // Download a single YAML file
  const handleDownloadYaml = useCallback((tableName: string) => {
    const yamlContent = result.yaml[tableName];
    if (!yamlContent) return;
    const blob = new Blob([yamlContent], { type: 'text/yaml;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${tableName}_uc_metric_view.yml`;
    a.click();
    URL.revokeObjectURL(url);
  }, [result.yaml]);

  // Download all YAMLs as individual files (triggers multiple downloads)
  // or as a combined file
  const handleDownloadAllYamls = useCallback(() => {
    const combined: string[] = [];
    for (const name of viewNames) {
      const yamlContent = result.yaml[name];
      if (yamlContent && yamlContent.trim()) {
        combined.push(`# === ${name} ===\n${yamlContent}`);
      }
    }
    const blob = new Blob([combined.join('\n\n---\n\n')], { type: 'text/yaml;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'uc_metric_views_all.yml';
    a.click();
    URL.revokeObjectURL(url);
  }, [result.yaml, viewNames]);

  // Download all deploy SQL
  const handleDownloadAllSql = useCallback(() => {
    const combined: string[] = [];
    for (const name of viewNames) {
      const sqlContent = result.sql[name];
      if (sqlContent && sqlContent.trim()) {
        combined.push(`-- === ${name} ===\n${sqlContent}`);
      }
    }
    const blob = new Blob([combined.join('\n\n')], { type: 'text/sql;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'deploy_metric_views_all.sql';
    a.click();
    URL.revokeObjectURL(url);
  }, [result.sql, viewNames]);

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', height: '100%', minHeight: 400 }}>
      {/* Header */}
      <Box display="flex" alignItems="center" gap={1.5} mb={1.5} flexWrap="wrap">
        <StorageIcon color="primary" />
        <Typography variant="h6" sx={{ fontWeight: 600 }}>
          UC Metric View Results
        </Typography>
        <Chip size="small" label={`${aggregateStats.activeViews} views`} />
        <Chip size="small" label={`${aggregateStats.totalMeasures} measures`} variant="outlined" />
        <Chip size="small" label={`${aggregateStats.totalDims} dimensions`} variant="outlined" />
        {editable && hasEdits && (
          <Chip size="small" label="edited" color="warning" variant="filled" />
        )}
        <Box sx={{ ml: 'auto', display: 'flex', gap: 1 }}>
          {editable && onSave && hasEdits && (
            <Button
              size="small"
              variant="contained"
              color={saveSuccess ? 'success' : 'primary'}
              startIcon={saveSuccess ? <CheckCircleIcon /> : <SaveIcon />}
              onClick={handleSave}
              disabled={isSaving}
            >
              {isSaving ? 'Saving…' : saveSuccess ? 'Saved' : 'Save Changes'}
            </Button>
          )}
          <Button
            size="small"
            variant="outlined"
            startIcon={<DownloadIcon />}
            onClick={handleDownloadAllYamls}
          >
            Download YAMLs
          </Button>
          <Button
            size="small"
            variant="outlined"
            startIcon={<DownloadIcon />}
            onClick={handleDownloadAllSql}
          >
            Download SQL
          </Button>
        </Box>
      </Box>

      {/* Migration Report (collapsible) */}
      {result.migration_report && (
        <Section
          title="Migration Report"
          icon={<DescriptionIcon fontSize="small" color="action" />}
          defaultExpanded={false}
        >
          <Box sx={{ p: 2, '& table': { borderCollapse: 'collapse', width: '100%' }, '& th, & td': { border: '1px solid', borderColor: 'divider', px: 1, py: 0.5, fontSize: '0.8rem' } }}>
            <ReactMarkdown remarkPlugins={[remarkGfm]} rehypePlugins={[rehypeSanitize]}>
              {result.migration_report}
            </ReactMarkdown>
          </Box>
        </Section>
      )}

      <Divider sx={{ my: 1 }} />

      {/* Body: sidebar + detail */}
      <Box sx={{ display: 'flex', flexGrow: 1, minHeight: 0, gap: 1 }}>
        {/* Sidebar list */}
        <Paper
          variant="outlined"
          sx={{
            width: 220,
            minWidth: 220,
            overflow: 'auto',
            flexShrink: 0,
          }}
        >
          <List dense disablePadding>
            {viewNames.map((name) => {
              const isRemoved = removedViews.has(name);
              const isModified = name in editingYaml;
              return (
                <ListItemButton
                  key={name}
                  selected={name === selected}
                  onClick={() => !isRemoved && setSelected(name)}
                  disabled={isRemoved}
                  sx={{
                    px: 1.5,
                    py: 0.5,
                    opacity: isRemoved ? 0.4 : 1,
                    textDecoration: isRemoved ? 'line-through' : 'none',
                  }}
                >
                  <ListItemText
                    primary={
                      <Box display="flex" alignItems="center" gap={0.5}>
                        <Typography
                          variant="body2"
                          fontFamily="monospace"
                          fontSize="0.8rem"
                          noWrap
                          sx={{ flexGrow: 1 }}
                        >
                          {name}
                        </Typography>
                        {isModified && !isRemoved && (
                          <Box sx={{ width: 6, height: 6, borderRadius: '50%', bgcolor: 'warning.main', flexShrink: 0 }} />
                        )}
                      </Box>
                    }
                  />
                  {editable && (
                    <Box sx={{ ml: 0.5, flexShrink: 0 }} onClick={(e) => e.stopPropagation()}>
                      {isRemoved ? (
                        <Tooltip title="Restore view">
                          <IconButton size="small" onClick={() => handleRestoreView(name)} sx={{ p: 0.25 }}>
                            <UndoIcon sx={{ fontSize: 14 }} />
                          </IconButton>
                        </Tooltip>
                      ) : (
                        <Tooltip title="Remove view">
                          <IconButton size="small" onClick={() => handleRemoveView(name)} sx={{ p: 0.25 }}>
                            <DeleteOutlineIcon sx={{ fontSize: 14 }} />
                          </IconButton>
                        </Tooltip>
                      )}
                    </Box>
                  )}
                </ListItemButton>
              );
            })}
          </List>
        </Paper>

        {/* Detail pane */}
        <Box sx={{ flexGrow: 1, overflow: 'auto', display: 'flex', flexDirection: 'column', gap: 1 }}>
          {/* Per-view header with stats and download */}
          <Box display="flex" alignItems="center" gap={1} mb={0.5}>
            {viewStats && <StatsChips stats={viewStats} />}
            <Tooltip title={`Download ${selected} YAML`}>
              <IconButton size="small" onClick={() => handleDownloadYaml(selected)}>
                <DownloadIcon fontSize="small" />
              </IconButton>
            </Tooltip>
          </Box>

          {/* Source SQL */}
          {result.sql[selected] && (
            <Section
              title="Source SQL"
              icon={<StorageIcon fontSize="small" color="action" />}
              defaultExpanded
            >
              <SQLBlock code={result.sql[selected]} />
            </Section>
          )}

          {/* Joins */}
          {parsed?.joins && parsed.joins.length > 0 && (
            <Section
              title="Joins"
              icon={<LinkIcon fontSize="small" color="action" />}
              count={parsed.joins.length}
              defaultExpanded
            >
              <JoinsTable joins={parsed.joins} />
            </Section>
          )}

          {/* Dimensions */}
          {parsed?.dimensions && parsed.dimensions.length > 0 && (
            <Section
              title="Dimensions"
              icon={<ViewColumnIcon fontSize="small" color="action" />}
              count={parsed.dimensions.length}
              defaultExpanded
            >
              <FieldTable fields={parsed.dimensions} />
            </Section>
          )}

          {/* Measures */}
          {parsed?.measures && parsed.measures.length > 0 && (
            <Section
              title="Measures"
              icon={<FunctionsIcon fontSize="small" color="action" />}
              count={parsed.measures.length}
              defaultExpanded
            >
              <FieldTable fields={parsed.measures} showFormat />
            </Section>
          )}

          {/* Editable YAML / Raw YAML */}
          {currentYaml && (
            <Section
              title={isEditing ? 'Edit YAML' : 'Raw YAML'}
              icon={<CodeIcon fontSize="small" color="action" />}
              defaultExpanded={isEditing}
              action={
                editable ? (
                  isEditing ? (
                    <Tooltip title="Discard edits for this view">
                      <IconButton
                        size="small"
                        onClick={(e) => { e.stopPropagation(); handleCancelEdit(selected); }}
                      >
                        <EditOffIcon fontSize="small" />
                      </IconButton>
                    </Tooltip>
                  ) : (
                    <Tooltip title="Edit YAML">
                      <IconButton
                        size="small"
                        onClick={(e) => { e.stopPropagation(); handleStartEdit(selected); }}
                      >
                        <EditIcon fontSize="small" />
                      </IconButton>
                    </Tooltip>
                  )
                ) : undefined
              }
            >
              {isEditing ? (
                <Box sx={{ p: 1 }}>
                  <TextField
                    fullWidth
                    multiline
                    minRows={10}
                    maxRows={30}
                    value={editingYaml[selected]}
                    onChange={(e) => handleYamlChange(selected, e.target.value)}
                    error={!!yamlErrors[selected]}
                    InputProps={{
                      sx: { fontFamily: 'monospace', fontSize: '0.8rem', lineHeight: 1.5 },
                    }}
                  />
                  {yamlErrors[selected] && (
                    <Alert severity="error" sx={{ mt: 1 }}>
                      {yamlErrors[selected]}
                    </Alert>
                  )}
                </Box>
              ) : (
                <SQLBlock code={currentYaml} language="yaml" />
              )}
            </Section>
          )}
        </Box>
      </Box>

      {/* Global stats footer */}
      {result.stats && 'coverage_pct' in (result.stats as Record<string, unknown>) && (
        <>
          <Divider sx={{ mt: 1 }} />
          <Box display="flex" alignItems="center" gap={1} pt={1}>
            <Typography variant="caption" color="text.secondary" fontWeight={600}>
              Overall:
            </Typography>
            <StatsChips stats={result.stats as UCMVStats} />
          </Box>
        </>
      )}
    </Box>
  );
};

export default UCMVResultViewer;
