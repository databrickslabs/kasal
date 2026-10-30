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
  TableContainer,
  Table,
  TableHead,
  TableBody,
  TableRow,
  TableCell,
  Paper,
  Divider,
  TextField,
  IconButton,
  Tooltip,
  Alert,
  Snackbar,
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
import yaml from 'js-yaml';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import {
  NonTranspiledPanel,
  untranslatableKey,
  type UntranslatableItem,
  type ReviewAnnotation,
} from './NonTranspiledPanel';
// Re-export the panel's public contract so consumers/tests can import it from here.
export { untranslatableKey };
export type { UntranslatableItem, ReviewAnnotation, ReviewStatus } from './NonTranspiledPanel';
import rehypeSanitize from 'rehype-sanitize';
import { safeImageComponents } from '../../../shared/a2ui/lib/markdown';
import type { MetricViewDef, UCMVStats, UCMVResult, FallbackExtractRow, UCMVResultViewerProps } from './ucmvResultTypes';
import { SQLBlock, StatsChips, Section, CopyButton } from './UCMVResultPrimitives';
import { FieldTable, JoinsTable } from './UCMVFieldTables';
export type { UCMVResult, FallbackExtractRow } from './ucmvResultTypes';


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
/*  Main component                                                     */
/* ------------------------------------------------------------------ */

const UCMVResultViewer: React.FC<UCMVResultViewerProps> = ({ result, editable = false, onResultChange, onSave }) => {
  const viewNames = useMemo(() => Object.keys(result.yaml).sort(), [result.yaml]);
  const fallbackRows = useMemo<FallbackExtractRow[]>(
    () => (Array.isArray(result.fallback_extract) ? result.fallback_extract : []),
    [result.fallback_extract],
  );
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
  const [saveError, setSaveError] = useState<string | null>(null);

  // Non-transpiled review: the measures that weren't emitted, plus the reviewer's
  // triage annotations. Seeded from any persisted review on the result; edits are
  // held locally and flow up via onResultChange so a Save persists them.
  const untranslatableItems = useMemo<UntranslatableItem[]>(
    () => (Array.isArray(result.untranslatable_items) ? result.untranslatable_items : []),
    [result.untranslatable_items],
  );
  const [reviewAnnotations, setReviewAnnotations] = useState<Record<string, ReviewAnnotation>>(
    () => result.untranslatable_review ?? {},
  );
  const handleReviewChange = useCallback(
    (key: string, patch: Partial<ReviewAnnotation>) => {
      setReviewAnnotations((prev) => {
        const next = { ...prev, [key]: { ...prev[key], ...patch } };
        const persist: Record<string, ReviewAnnotation> = {};
        for (const [k, v] of Object.entries(next)) {
          if (v && (v.status || (v.note && v.note.trim()))) persist[k] = v;
        }
        onResultChange?.({ ...result, untranslatable_review: persist });
        return next;
      });
    },
    [onResultChange, result],
  );

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
    setSaveError(null);
    try {
      await onSave(buildEditedResult());
      setSaveSuccess(true);
      setTimeout(() => setSaveSuccess(false), 3000);
    } catch (err) {
      setSaveError(err instanceof Error ? err.message : 'Save failed');
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

  // Download each metric view YAML as its OWN .yml file (one per fact table)
  // so each can be deployed independently. Downloads are staggered (~150ms)
  // because browsers throttle/deny rapid back-to-back programmatic downloads.
  const handleDownloadAllYamls = useCallback(() => {
    const entries = viewNames
      .map((name) => [name, result.yaml[name]] as const)
      .filter(([, v]) => v && v.trim());
    entries.forEach(([name, yamlContent], idx) => {
      setTimeout(() => {
        const blob = new Blob([yamlContent], { type: 'text/yaml;charset=utf-8' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `${name}_uc_metric_view.yml`;
        a.click();
        URL.revokeObjectURL(url);
      }, idx * 150);
    });
  }, [result.yaml, viewNames]);

  // Download each deploy SQL as its OWN .sql file (one per fact table), staggered.
  const handleDownloadAllSql = useCallback(() => {
    const entries = viewNames
      .map((name) => [name, result.sql[name]] as const)
      .filter(([, v]) => v && v.trim());
    entries.forEach(([name, sqlContent], idx) => {
      setTimeout(() => {
        const blob = new Blob([sqlContent], { type: 'text/sql;charset=utf-8' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `${name}_deploy_metric_view.sql`;
        a.click();
        URL.revokeObjectURL(url);
      }, idx * 150);
    });
  }, [result.sql, viewNames]);

  // Download raw JSON (original DAX measures or original M-Query) as a file
  const downloadJson = useCallback((data: unknown[], filename: string) => {
    const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);
  }, []);

  // Download each view's PBI<->UCMV reconciliation mapping draft as its OWN
  // .mapping_candidates.yml file, staggered like the YAML/SQL downloads.
  const handleDownloadAllMappings = useCallback(() => {
    const mapping = result.pbi_ucmv_mapping || {};
    const entries = Object.entries(mapping).filter(([, v]) => v && v.trim());
    entries.forEach(([name, yamlContent], idx) => {
      setTimeout(() => {
        const blob = new Blob([yamlContent], { type: 'text/yaml;charset=utf-8' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `${name}.mapping_candidates.yml`;
        a.click();
        URL.revokeObjectURL(url);
      }, idx * 150);
    });
  }, [result.pbi_ucmv_mapping]);

  const hasDax = Array.isArray(result.measures_with_dax) && result.measures_with_dax.length > 0;
  const hasMquery = Array.isArray(result.mquery_raw) && result.mquery_raw.length > 0;
  const hasMapping =
    !!result.pbi_ucmv_mapping && Object.keys(result.pbi_ucmv_mapping).length > 0;

  // New artifact fields
  const sourceLayerEntries = useMemo(
    () => (result.source_layer_ddl ? Object.entries(result.source_layer_ddl) : []),
    [result.source_layer_ddl],
  );
  const hasIngestionTasks =
    Array.isArray(result.pbi_only_ingestion_tasks) && result.pbi_only_ingestion_tasks.length > 0;

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
          {hasDax && (
            <Tooltip title="Download the original DAX measure expressions as JSON">
              <Button
                size="small"
                variant="outlined"
                startIcon={<DownloadIcon />}
                onClick={() => downloadJson(result.measures_with_dax as unknown[], 'original_dax_measures.json')}
              >
                Download DAX
              </Button>
            </Tooltip>
          )}
          {hasMapping && (
            <Tooltip title="Download the deterministic PBI<->UCMV measure mapping draft (one file per view) for the KPI-reconciliation pipeline">
              <Button
                size="small"
                variant="outlined"
                startIcon={<DownloadIcon />}
                onClick={handleDownloadAllMappings}
              >
                Download Mapping
              </Button>
            </Tooltip>
          )}
          {hasMquery && (
            <Tooltip title="Download the original M-Query source as JSON">
              <Button
                size="small"
                variant="outlined"
                startIcon={<DownloadIcon />}
                onClick={() => downloadJson(result.mquery_raw as unknown[], 'original_mquery.json')}
              >
                Download M-Query
              </Button>
            </Tooltip>
          )}
        </Box>
      </Box>

      {/* Live-connection note: these views are backed by a live connection to a
          semantic model, so the transpiler can't resolve them — tell the team
          which model/table to parse. */}
      {result.live_connections && Object.keys(result.live_connections).length > 0 && (
        <Alert severity="warning" sx={{ mb: 1.5 }}>
          <Typography variant="body2" sx={{ fontWeight: 600, mb: 0.5 }}>
            Live connection to a semantic model — parse the upstream model to complete these
          </Typography>
          <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mb: 0.75 }}>
            These view(s) are sourced from a live connection (AnalysisServices.Database), not a
            warehouse table, so their measures/columns can't be resolved here. Parse the semantic
            model below to finish the mapping.
          </Typography>
          <Box component="ul" sx={{ m: 0, pl: 2.5, fontSize: '0.8rem' }}>
            {Object.entries(result.live_connections).map(([view, lc]) => (
              <li key={view}>
                <Box component="span" sx={{ fontFamily: 'monospace', fontWeight: 600 }}>{view}</Box>
                {' → semantic model '}
                <Box component="span" sx={{ fontFamily: 'monospace' }}>{lc.database}</Box>
                {' (table '}
                <Box component="span" sx={{ fontFamily: 'monospace' }}>{lc.table}</Box>
                {', server '}
                <Box component="span" sx={{ fontFamily: 'monospace' }}>{lc.server}</Box>
                {')'}
              </li>
            ))}
          </Box>
        </Alert>
      )}

      {/* PBI-only ingestion tasks: tables sourced from non-SQL files that need
          a snapshot ingestion pipeline before their metric views can run. */}
      {hasIngestionTasks && (
        <Alert severity="warning" sx={{ mb: 1.5 }}>
          <Typography variant="body2" sx={{ fontWeight: 600, mb: 0.5 }}>
            PBI-only tables need ingestion ({result.pbi_only_ingestion_tasks!.length})
          </Typography>
          <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mb: 0.75 }}>
            These tables are sourced from non-SQL files (Excel, SharePoint, Web, or typed data)
            and require a snapshot ingestion pipeline before the metric views can read them.
          </Typography>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1.5 }}>
            {result.pbi_only_ingestion_tasks!.map((task) => (
              <Box key={task.table}>
                <Box display="flex" alignItems="center" gap={0.75} flexWrap="wrap" mb={0.25}>
                  <Typography variant="caption" fontFamily="monospace" fontWeight={600}>
                    {task.table}
                  </Typography>
                  <Typography variant="caption" color="text.secondary">→</Typography>
                  <Chip
                    size="small"
                    label={task.kind}
                    color="warning"
                    variant="outlined"
                    sx={{ height: 18, fontSize: '0.7rem' }}
                  />
                  {task.uc_target && (
                    <Typography variant="caption" color="text.secondary" fontFamily="monospace">
                      {task.uc_target}
                    </Typography>
                  )}
                </Box>
                {task.recommended_approach && (
                  <Typography variant="caption" sx={{ display: 'block', mb: 0.25 }}>
                    {task.recommended_approach}
                  </Typography>
                )}
                {task.snapshot_loader_stub && (
                  <Accordion
                    defaultExpanded={false}
                    disableGutters
                    variant="outlined"
                    sx={{ mt: 0.5, '&:before': { display: 'none' } }}
                  >
                    <AccordionSummary
                      expandIcon={<ExpandMoreIcon sx={{ fontSize: 14 }} />}
                      sx={{ minHeight: 28, '& .MuiAccordionSummary-content': { my: 0.5 } }}
                    >
                      <Box display="flex" alignItems="center" gap={0.5}>
                        <Typography variant="caption">Snapshot loader stub</Typography>
                        <Box onClick={(e) => e.stopPropagation()}>
                          <CopyButton content={task.snapshot_loader_stub} />
                        </Box>
                      </Box>
                    </AccordionSummary>
                    <AccordionDetails sx={{ p: 0 }}>
                      <SQLBlock code={task.snapshot_loader_stub} language="python" />
                    </AccordionDetails>
                  </Accordion>
                )}
              </Box>
            ))}
          </Box>
        </Alert>
      )}

      {/* Source layer DDL: CREATE VIEW per PBI table — the base views that
          metric views read from. Each entry shows DDL, an optional copy button,
          TODO steps (if any manual work remains), and an error chip on failure. */}
      {sourceLayerEntries.length > 0 && (
        <Section
          title={`Source layer (${sourceLayerEntries.length} view${sourceLayerEntries.length !== 1 ? 's' : ''})`}
          icon={<CodeIcon fontSize="small" color="action" />}
          defaultExpanded={false}
        >
          <Box sx={{ p: 1.5, display: 'flex', flexDirection: 'column', gap: 2 }}>
            {sourceLayerEntries.map(([tableName, entry]) => (
              <Box key={tableName}>
                <Box display="flex" alignItems="center" gap={1} mb={0.5}>
                  <Typography
                    variant="body2"
                    fontFamily="monospace"
                    fontWeight={600}
                    sx={{ flexGrow: 1 }}
                  >
                    {tableName}
                  </Typography>
                  {entry.error && (
                    <Chip
                      size="small"
                      color="error"
                      label={entry.error}
                      variant="outlined"
                      sx={{ maxWidth: 260, fontSize: '0.7rem', height: 20 }}
                    />
                  )}
                  <CopyButton content={entry.ddl} />
                </Box>
                <SQLBlock code={entry.ddl} />
                {Array.isArray(entry.todo_steps) && entry.todo_steps.length > 0 && (
                  <Alert severity="warning" sx={{ mt: 0.75 }}>
                    <Typography variant="caption" sx={{ fontWeight: 600, display: 'block', mb: 0.5 }}>
                      TODO steps:
                    </Typography>
                    <Box component="ul" sx={{ m: 0, pl: 2 }}>
                      {entry.todo_steps.map((step, i) => (
                        <li key={i}>
                          <Typography variant="caption">{step}</Typography>
                        </li>
                      ))}
                    </Box>
                  </Alert>
                )}
              </Box>
            ))}
          </Box>
        </Section>
      )}

      {/* Reconciliation was requested but did not run — surface the reason loudly
          so a single-pass is never silent (e.g. reference_years not provided). */}
      {result.reconciliation_skipped && (
        <Alert severity="warning" sx={{ mb: 1.5 }}>
          <strong>Power BI reconciliation did not run.</strong>{' '}
          {result.reconciliation_skipped}
        </Alert>
      )}

      {/* LLM translation was on but produced nothing — surface WHY (provider /
          throttle / timeout) instead of a silent empty result. */}
      {result.llm_translation_warning && (
        <Alert severity="error" sx={{ mb: 1.5 }}>
          <strong>LLM translation produced no views.</strong>{' '}
          {result.llm_translation_warning}
        </Alert>
      )}

      {/* Validation status: compact chip row + optional per-view result list. */}
      {result.pbi_validation && (
        <Box sx={{ mb: 1.5 }}>
          <Box display="flex" alignItems="center" gap={1} flexWrap="wrap">
            <Typography variant="caption" color="text.secondary" sx={{ fontWeight: 600 }}>
              Validation:
            </Typography>
            <Chip
              size="small"
              label={result.pbi_validation.status}
              color={
                result.pbi_validation.status === 'error'
                  ? 'error'
                  : result.pbi_validation.status === 'ran'
                    ? 'success'
                    : 'default'
              }
              variant="outlined"
              sx={{ height: 20, fontSize: '0.75rem' }}
            />
            {result.pbi_validation.reason && (
              <Typography variant="caption" color="text.secondary">
                {result.pbi_validation.reason}
              </Typography>
            )}
          </Box>
          {result.pbi_validation.views && Object.keys(result.pbi_validation.views).length > 0 && (
            <Box component="ul" sx={{ m: 0, mt: 0.5, pl: 2.5 }}>
              {Object.entries(result.pbi_validation.views).map(([view, detail]) => (
                <li key={view}>
                  <Typography variant="caption">
                    <Box component="span" fontFamily="monospace">{view}</Box>
                    {': '}
                    {detail}
                  </Typography>
                </li>
              ))}
            </Box>
          )}
        </Box>
      )}

      {/* Migration Report (collapsible) */}
      {result.migration_report && (
        <Section
          title="Migration Report"
          icon={<DescriptionIcon fontSize="small" color="action" />}
          defaultExpanded={false}
        >
          <Box sx={{ p: 2, '& table': { borderCollapse: 'collapse', width: '100%' }, '& th, & td': { border: '1px solid', borderColor: 'divider', px: 1, py: 0.5, fontSize: '0.8rem' } }}>
            <ReactMarkdown remarkPlugins={[remarkGfm]} rehypePlugins={[rehypeSanitize]} components={safeImageComponents}>
              {result.migration_report}
            </ReactMarkdown>
          </Box>
        </Section>
      )}

      {/* Not transpiled — non-emitted measures for review + triage annotation */}
      {untranslatableItems.length > 0 && (
        <Section
          title="Not transpiled"
          icon={<DescriptionIcon fontSize="small" color="warning" />}
          defaultExpanded={false}
          action={
            <Chip
              size="small"
              color="warning"
              variant="outlined"
              label={`${untranslatableItems.length} not transpiled`}
            />
          }
        >
          <Box sx={{ p: 1.5 }}>
            <NonTranspiledPanel
              items={untranslatableItems}
              review={reviewAnnotations}
              editable={editable}
              onReviewChange={handleReviewChange}
            />
          </Box>
        </Section>
      )}

      <Divider sx={{ my: 1 }} />

      {/* Worst-case fallback: no views generated → show the per-table extract
          (M-Query source + associated measures/DAX) so the run still yields
          something usable. */}
      {viewNames.length === 0 && fallbackRows.length > 0 && (
        <Box sx={{ flexGrow: 1, minHeight: 0, overflow: 'auto' }}>
          <Alert severity="warning" sx={{ mb: 1.5 }}>
            No UC Metric Views could be generated for this model (its tables are
            sourced from raw Power Query M without a resolvable SQL source). The
            extracted source material is shown below so you can build the views
            manually or feed it into the M-Query conversion step.
          </Alert>
          <Box sx={{ mb: 1, display: 'flex', gap: 1 }}>
            <Button
              size="small"
              variant="outlined"
              startIcon={<DownloadIcon />}
              onClick={() => downloadJson(fallbackRows as unknown[], 'ucmv_fallback_extract.json')}
            >
              Download Extract (JSON)
            </Button>
          </Box>
          <TableContainer component={Paper} variant="outlined">
            <Table size="small" stickyHeader>
              <TableHead>
                <TableRow>
                  <TableCell sx={{ fontWeight: 600 }}>Table</TableCell>
                  <TableCell sx={{ fontWeight: 600 }}>M-Query source</TableCell>
                  <TableCell sx={{ fontWeight: 600 }}>Measures (DAX)</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {fallbackRows.map((row) => (
                  <TableRow key={row.table_name} hover>
                    <TableCell sx={{ verticalAlign: 'top', fontFamily: 'monospace', fontSize: '0.8rem', whiteSpace: 'nowrap' }}>
                      {row.table_name}
                    </TableCell>
                    <TableCell sx={{ verticalAlign: 'top', maxWidth: 380 }}>
                      {row.mquery ? (
                        <Box component="pre" sx={{ m: 0, fontSize: '0.72rem', whiteSpace: 'pre-wrap', wordBreak: 'break-word', maxHeight: 160, overflow: 'auto' }}>
                          {row.mquery}
                        </Box>
                      ) : (
                        <Typography variant="caption" color="text.secondary">—</Typography>
                      )}
                    </TableCell>
                    <TableCell sx={{ verticalAlign: 'top' }}>
                      {row.measures.length === 0 ? (
                        <Typography variant="caption" color="text.secondary">—</Typography>
                      ) : (
                        row.measures.map((m, i) => (
                          <Box key={i} sx={{ mb: 0.75 }}>
                            <Typography variant="caption" sx={{ fontWeight: 600 }}>{m.measure_name}</Typography>
                            {m.dax_expression && (
                              <Box component="pre" sx={{ m: 0, fontSize: '0.72rem', whiteSpace: 'pre-wrap', wordBreak: 'break-word', color: 'text.secondary' }}>
                                {m.dax_expression}
                              </Box>
                            )}
                          </Box>
                        ))
                      )}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
        </Box>
      )}

      {/* Body: sidebar + detail */}
      {viewNames.length > 0 && (
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

          {/* Source M (Power Query) — the FULL raw M the Source SQL was derived
              from, plus a warning for transformation steps that may not have made
              it into that SQL (filters, group-by, joins, …). */}
          {result.source_mquery?.[selected] && (
            <Section
              title="Source M (Power Query)"
              icon={<StorageIcon fontSize="small" color="action" />}
              defaultExpanded={!result.sql[selected] || !!result.source_transform_todos?.[selected]?.length}
            >
              {result.source_transform_todos?.[selected]?.length ? (
                <Alert severity="warning" variant="outlined" sx={{ mb: 1 }}>
                  <Typography variant="caption" sx={{ fontWeight: 600 }}>
                    These Power Query steps are in the source M but may NOT be reflected in the Source SQL above — review:
                  </Typography>
                  <Box component="ul" sx={{ pl: 2, m: 0 }}>
                    {result.source_transform_todos[selected].map((t) => (
                      <li key={t}><Typography variant="caption">{t}</Typography></li>
                    ))}
                  </Box>
                </Alert>
              ) : null}
              <Box
                component="pre"
                sx={{
                  m: 0,
                  p: 1,
                  fontFamily: 'monospace',
                  fontSize: '0.75rem',
                  whiteSpace: 'pre-wrap',
                  wordBreak: 'break-word',
                  bgcolor: 'action.hover',
                  borderRadius: 1,
                  maxHeight: 400,
                  overflow: 'auto',
                }}
              >
                {result.source_mquery[selected]}
              </Box>
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
      )}

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

      {/* Save feedback */}
      <Snackbar open={saveSuccess} autoHideDuration={3000} onClose={() => setSaveSuccess(false)}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'center' }}>
        <Alert severity="success" variant="filled" sx={{ width: '100%' }}>
          Changes saved to database
        </Alert>
      </Snackbar>
      <Snackbar open={!!saveError} autoHideDuration={5000} onClose={() => setSaveError(null)}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'center' }}>
        <Alert severity="error" variant="filled" sx={{ width: '100%' }}>
          Save failed: {saveError}
        </Alert>
      </Snackbar>
    </Box>
  );
};

export default UCMVResultViewer;
