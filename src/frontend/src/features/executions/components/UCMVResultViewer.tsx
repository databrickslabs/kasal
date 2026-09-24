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
  TableContainer,
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
import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import Button from '@mui/material/Button';
import { Highlight, themes } from 'prism-react-renderer';
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
  /** Raw source measures with their original DAX expressions (echoed from the generator). */
  measures_with_dax?: unknown[];
  /** Raw source M-Query entries (echoed from the generator). */
  mquery_raw?: unknown[];
  /** Per-table tabular extract (M-Query source + associated measures/DAX). Always
   *  present; the primary usable artifact when no views could be generated. */
  fallback_extract?: FallbackExtractRow[];
  /** Number of UC metric views actually generated (0 → show the fallback table). */
  views_generated?: number;
  /** Flat list of measures that were NOT transpiled (original DAX + reason +
   *  proposed approach), for the "Not transpiled" review panel. */
  untranslatable_items?: UntranslatableItem[];
  /** Persisted reviewer triage annotations, keyed by untranslatableKey(item). */
  untranslatable_review?: Record<string, ReviewAnnotation>;
  /** Deterministic PBI<->UCMV reconciliation mapping draft, one YAML-text
   *  entry per view (keyed by view name), from pbi_ucmv_mapping.py. Feeds the
   *  downstream KPI-reconciliation pipeline once reviewed. */
  pbi_ucmv_mapping?: Record<string, string>;
  /** Views backed by a LIVE connection to a semantic model (M-Query
   *  AnalysisServices.Database) — the transpiler can't resolve these, so the UI
   *  flags which semantic model/table to parse. {view_name: {server, database, table}}. */
  live_connections?: Record<string, { server: string; database: string; table: string }>;
  /** Source layer DDL: per-table CREATE VIEW for the base source layer UCMVs read from. */
  source_layer_ddl?: Record<string, {
    ddl: string;
    todo_steps: string[];
    error: string | null;
  }>;
  /** Tables sourced from non-SQL files (Excel, SharePoint, Web, typed) — need a snapshot
   *  ingestion pipeline before the metric views can read them. */
  pbi_only_ingestion_tasks?: Array<{
    table: string;
    uc_target: string;
    kind: string;
    source_description: string;
    recommended_approach: string;
    snapshot_loader_stub: string;
  }>;
  /** Validation loop outcome. */
  pbi_validation?: {
    status: 'skipped' | 'ran' | 'error';
    reason?: string;
    candidate_views?: string[];
    views?: Record<string, string>;
  };
}

export interface FallbackExtractRow {
  table_name: string;
  mquery: string;
  measures: Array<{ measure_name: string; dax_expression: string }>;
  measure_count: number;
  has_mquery: boolean;
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

/** Extract the "referenced by N measures" count the backend appends to a
 *  measure's comment. Returns null when the measure is referenced by nothing
 *  (backend omits the suffix at 0). Lets reviewers see + sort by usage so they
 *  prioritize high-impact measures/gaps. */
const extractReferencedBy = (comment?: string): number | null => {
  if (!comment) return null;
  const m = comment.match(/referenced by (\d+) measures?/i);
  return m ? parseInt(m[1], 10) : null;
};

/** One report page a measure is used on: how many visuals on that page (count),
 *  HOW (drawn vs filter) and WHERE (the visual type(s) on that page). */
interface UsedOnEntry {
  page: string;
  types: string[];
  role: 'drawn' | 'filter' | '';
  /** Number of visuals on this page (the "×N" tally; 1 when not shown). */
  count: number;
}
interface UsedOnInfo {
  /** HOW OFTEN — total visuals across the report (from the suffix). */
  count: number;
  entries: UsedOnEntry[];
}

/** Parse the measure comment's visual-usage suffix
 *  (yaml_emitter._visual_usage_suffix). This is the signal reviewers use to
 *  judge which measures actually matter — a measure shown on a report page is
 *  business-relevant; one referenced by nothing and drawn nowhere is likely an
 *  intermediate building block.
 *
 *  Current format carries a total count + per-page "×N" tally + type/role, e.g.
 *    "· Used on 7 visuals: OTC Scorecard ×7 (pivotTable/clusteredBarChart·filter)"
 *    "· Used on 3 visuals: OTC Scorecard ×2 (card/tableEx·drawn), OTC NPS (slicer·filter) (+1 more)"
 *  Also tolerates earlier forms: no per-page "×N", and the bare
 *  "· Used on: OTC Scorecard, OTC NPS" (no count/annotation). The suffix is
 *  always last on the comment, so the page list captures to end of string. */
const extractUsedOn = (comment?: string): UsedOnInfo => {
  if (!comment) return { count: 0, entries: [] };
  const m = comment.match(/Used on(?: (\d+) visuals?)?:\s*(.+)$/);
  if (!m) return { count: 0, entries: [] };
  // The indirect-usage clause ("· Indirectly used via …") can follow the direct
  // one on the same comment — stop before it so its text isn't parsed as pages.
  let list = m[2].split(/\s*·\s*Indirectly used/)[0].trim();
  const more = list.match(/\s*\(\+\d+ more\)\s*$/);
  if (more?.index != null) list = list.slice(0, more.index).trim();
  const entries: UsedOnEntry[] = list
    .split(',')
    .map((tok) => tok.trim())
    .filter(Boolean)
    .map((tok) => {
      // Peel the trailing "(annotation)", then a trailing "×N", leaving the page.
      const paren = tok.match(/\(([^)]*)\)\s*$/);
      const annot = paren ? paren[1] : '';
      let prefix = paren?.index != null ? tok.slice(0, paren.index).trim() : tok.trim();
      const tally = prefix.match(/[×x](\d+)$/);
      const count = tally ? parseInt(tally[1], 10) : 1;
      if (tally?.index != null) prefix = prefix.slice(0, tally.index).trim();
      const [typesPart, rolePart] = annot.split('·');
      const role =
        rolePart === 'filter' || rolePart === 'drawn'
          ? rolePart
          : typesPart === 'drawn' || typesPart === 'filter'
            ? (typesPart as 'drawn' | 'filter')
            : '';
      const types =
        rolePart === undefined
          ? typesPart === 'drawn' || typesPart === 'filter' || typesPart === ''
            ? []
            : typesPart.split('/').filter(Boolean)
          : typesPart.split('/').filter(Boolean);
      return { page: prefix, types, role, count };
    });
  const total = m[1]
    ? parseInt(m[1], 10)
    : entries.reduce((sum, e) => sum + e.count, 0);
  return { count: total, entries };
};

/** One backtraced indirect-usage entry: a visual-placed measure (`via`) whose
 *  DAX reaches this measure, and the report page(s) it's shown on. */
interface IndirectUsedOnEntry {
  via: string;
  pages: string[];
}

/** Parse the measure comment's indirect-usage clause
 *  (yaml_emitter._indirect_visual_usage_suffix):
 *    "· Indirectly used via [OTC Health Score] on: OTC Scorecard, Exec Summary; [X] on: P (+1 more)"
 *  Returns one entry per `via` measure. Empty when the measure has no indirect
 *  usage (the common case). This measure isn't drawn in a visual itself — a
 *  visual-placed KPI depends on it — so it's shown distinctly from direct use. */
const extractIndirectUsedOn = (comment?: string): IndirectUsedOnEntry[] => {
  if (!comment) return [];
  const m = comment.match(/Indirectly used via (.+)$/);
  if (!m) return [];
  let rest = m[1].trim();
  const more = rest.match(/\s*\(\+\d+ more\)\s*$/);
  if (more?.index != null) rest = rest.slice(0, more.index).trim();
  return rest
    .split(';')
    .map((tok) => tok.trim())
    .filter(Boolean)
    .map((tok) => {
      const vm = tok.match(/^\[([^\]]+)\](?:\s*on:\s*(.+))?$/);
      if (!vm) return { via: tok.replace(/^\[|\]$/g, ''), pages: [] };
      const pages = vm[2] ? vm[2].split(',').map((p) => p.trim()).filter(Boolean) : [];
      return { via: vm[1].trim(), pages };
    });
};

const FieldTable: React.FC<{
  fields: Array<{ name: string; expr?: string; comment?: string; format?: string }>;
  showFormat?: boolean;
}> = ({ fields, showFormat }) => {
  // Only show the "Used by" column for measures (showFormat) and only when at
  // least one measure carries a usage count — keeps dimension tables unchanged.
  const showUsage = !!showFormat && fields.some((f) => extractReferencedBy(f.comment) !== null);
  // Same, for the visual-usage "Used on" column (report pages the measure appears
  // on) — only for measures, only when at least one carries DIRECT or INDIRECT
  // (backtraced) usage, so a sub-KPI that's only referenced by a visual KPI
  // still shows up.
  const showUsedOn =
    !!showFormat &&
    fields.some(
      (f) =>
        extractUsedOn(f.comment).entries.length > 0 ||
        extractIndirectUsedOn(f.comment).length > 0,
    );
  // Sort measures so the highest-impact surface first: measures used on more
  // visuals rank above those used on fewer, then by how many other measures
  // reference them.
  const rows =
    showUsage || showUsedOn
      ? [...fields].sort((a, b) => {
          const va = extractUsedOn(a.comment).count;
          const vb = extractUsedOn(b.comment).count;
          if (va !== vb) return vb - va;
          return (extractReferencedBy(b.comment) ?? -1) - (extractReferencedBy(a.comment) ?? -1);
        })
      : fields;
  // Column widths depend on which optional columns are present (all measures
  // carry a Comment column via showFormat; dimensions never show usage/usedOn).
  const nameW = showUsedOn && showUsage ? '20%' : showUsage || showUsedOn ? '24%' : showFormat ? '30%' : '30%';
  const exprW = !showFormat ? '70%' : showUsedOn && showUsage ? '26%' : showUsage || showUsedOn ? '32%' : '40%';
  const commentW = showUsedOn && showUsage ? '22%' : showUsedOn ? '26%' : '30%';
  return (
    <Table size="small" sx={{ tableLayout: 'fixed' }}>
      <TableHead>
        <TableRow>
          <TableCell sx={{ fontWeight: 600, width: nameW }}>Name</TableCell>
          <TableCell sx={{ fontWeight: 600, width: exprW }}>Expression</TableCell>
          {showFormat && <TableCell sx={{ fontWeight: 600, width: commentW }}>Comment / Format</TableCell>}
          {showUsedOn && <TableCell sx={{ fontWeight: 600, width: '20%' }} title="Report page(s) this measure is drawn on or filtered by">Used on</TableCell>}
          {showUsage && <TableCell sx={{ fontWeight: 600, width: '12%' }} align="right" title="How many other measures reference this measure">Used by</TableCell>}
        </TableRow>
      </TableHead>
      <TableBody>
        {rows.map((f) => {
          const usage = extractReferencedBy(f.comment);
          const usedOn = extractUsedOn(f.comment);
          const indirect = extractIndirectUsedOn(f.comment);
          return (
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
              {showUsedOn && (
                <TableCell sx={{ fontSize: '0.8rem' }}>
                  {usedOn.entries.length > 0 || indirect.length > 0 ? (
                    <Box display="flex" flexDirection="column" gap={0.5} alignItems="flex-start">
                      {usedOn.entries.length > 0 && (
                        <>
                          {/* HOW OFTEN — visual-occurrence count. */}
                          <Chip
                            size="small"
                            color="info"
                            variant="filled"
                            label={`${usedOn.count} visual${usedOn.count !== 1 ? 's' : ''}`}
                            sx={{ height: 18, fontSize: '0.7rem' }}
                          />
                          {/* WHERE + HOW — page, visual type(s), and drawn vs filter. */}
                          <Box display="flex" gap={0.5} flexWrap="wrap">
                            {usedOn.entries.map((e) => {
                              // "×N" on the page chip so a count of 7 all on one page
                              // reads as "OTC Scorecard ×7" instead of looking like a
                              // mismatch with the total.
                              const tally = e.count > 1 ? ` ×${e.count}` : '';
                              const typeLabel = e.types.length ? ` · ${e.types.join('/')}` : '';
                              const roleWord =
                                e.role === 'filter' ? 'used as a filter' : e.role === 'drawn' ? 'drawn (shown)' : '';
                              return (
                                <Chip
                                  key={e.page}
                                  size="small"
                                  variant="outlined"
                                  color={e.role === 'filter' ? 'default' : 'info'}
                                  label={`${e.page}${tally}${typeLabel}`}
                                  title={[
                                    e.count > 1 ? `${e.count} visuals on this page` : '',
                                    roleWord,
                                    e.types.length ? `in ${e.types.join(', ')}` : '',
                                  ]
                                    .filter(Boolean)
                                    .join(' · ')}
                                  sx={{ height: 18, fontSize: '0.7rem', maxWidth: '100%' }}
                                />
                              );
                            })}
                          </Box>
                        </>
                      )}
                      {/* INDIRECT — backtraced: a visual-placed KPI depends on this
                          measure. Distinct style (dashed, muted, "↳ via …") so it
                          never reads as direct usage. */}
                      {indirect.map((ind) => (
                        <Chip
                          key={`via-${ind.via}`}
                          size="small"
                          variant="outlined"
                          label={`↳ via ${ind.via}`}
                          title={
                            `Indirectly used — [${ind.via}] references this measure` +
                            (ind.pages.length ? ` · shown on ${ind.pages.join(', ')}` : '')
                          }
                          sx={{
                            height: 18,
                            fontSize: '0.7rem',
                            maxWidth: '100%',
                            borderStyle: 'dashed',
                            color: 'text.secondary',
                          }}
                        />
                      ))}
                    </Box>
                  ) : (
                    '—'
                  )}
                </TableCell>
              )}
              {showUsage && (
                <TableCell sx={{ fontSize: '0.8rem' }} align="right">
                  {usage != null && usage > 0 ? usage : '—'}
                </TableCell>
              )}
            </TableRow>
          );
        })}
      </TableBody>
    </Table>
  );
};

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
/*  Copy button (used by DDL blocks and snapshot stubs)               */
/* ------------------------------------------------------------------ */

const CopyButton: React.FC<{ content: string }> = ({ content }) => {
  const [copied, setCopied] = useState(false);
  const handleCopy = useCallback(async () => {
    try {
      await navigator.clipboard.writeText(content);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch {
      // clipboard unavailable (non-https or restricted env)
    }
  }, [content]);
  return (
    <Tooltip title={copied ? 'Copied!' : 'Copy to clipboard'}>
      <IconButton size="small" onClick={handleCopy} sx={{ p: 0.25 }}>
        {copied
          ? <CheckCircleIcon sx={{ fontSize: 14 }} color="success" />
          : <ContentCopyIcon sx={{ fontSize: 14 }} />}
      </IconButton>
    </Tooltip>
  );
};

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
            <ReactMarkdown remarkPlugins={[remarkGfm]} rehypePlugins={[rehypeSanitize]}>
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
