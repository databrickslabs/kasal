/**
 * Cognitive Memory Browser — explore what a crew remembers.
 *
 * Design goals:
 *  - Derive provenance from the scope path (/crew/{id}/agent/{role}) instead
 *    of expecting the agent to be in a dedicated column.
 *  - Collapse naming variants of the same concept (energy-markets ≡
 *    energy_markets ≡ "energy markets") behind a single normalised label so
 *    facet counts reflect reality.
 *  - Facet navigation: click a category chip anywhere to pin it to the
 *    filter set; click an agent row to narrow to that agent.
 *  - Two complementary views: a card stream ranked by importance + recency,
 *    and a category bubble map that surfaces what the crew thinks about
 *    most, weighted by how important those thoughts are.
 */

import React, { useCallback, useEffect, useMemo, useState } from 'react';
import {
  Alert,
  Box,
  Button,
  Chip,
  Collapse,
  Dialog,
  DialogActions,
  DialogContent,
  DialogContentText,
  DialogTitle,
  Divider,
  IconButton,
  InputAdornment,
  LinearProgress,
  MenuItem,
  Paper,
  Slider,
  Snackbar,
  Stack,
  TextField,
  ToggleButton,
  ToggleButtonGroup,
  Tooltip,
  Typography,
} from '@mui/material';
import CloseIcon from '@mui/icons-material/Close';
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutline';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import FilterAltIcon from '@mui/icons-material/FilterAlt';
import PersonOutlineIcon from '@mui/icons-material/PersonOutline';
import RefreshIcon from '@mui/icons-material/Refresh';
import SearchIcon from '@mui/icons-material/Search';
import StarIcon from '@mui/icons-material/Star';
import ViewAgendaIcon from '@mui/icons-material/ViewAgenda';
import BubbleChartIcon from '@mui/icons-material/BubbleChart';
import AccountTreeIcon from '@mui/icons-material/AccountTree';

import { apiClient } from '../../config/api/ApiConfig';
import { ConceptForceGraph } from './ConceptForceGraph';
import { runService } from '../../api/ExecutionHistoryService';
import { Run } from '../../types/run';

// Sentinel run id for the opt-in "show every run at once" (full graph) view.
const ALL_RUNS = '__all__';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface MemoryRecord {
  id: string | null;
  content: string;
  scope: string;
  categories: string[];
  importance: number;
  source?: string | null;
  private: boolean;
  metadata: Record<string, unknown>;
  created_at: string | null;
  last_accessed: string | null;
}

interface RecordsResponse {
  backend: string;
  records: MemoryRecord[];
  count: number;
  // Total records available in the store for the active scope. The browser
  // pages through them with `offset` until loaded === total.
  total?: number;
}

interface MemoryRecordsBrowserProps {
  open: boolean;
  onClose: () => void;
  /** Open scoped to a specific run (job_id) instead of defaulting to latest. */
  initialRunId?: string;
  /** Open directly on a given view (e.g. 'graph' from a ChatMode run). */
  initialView?: 'cards' | 'concepts' | 'graph';
}

// ---------------------------------------------------------------------------
// Data helpers
// ---------------------------------------------------------------------------

// Records fetched per page for the card list. We page through the store with
// `offset` ("Load more") so we never mount thousands of card DOM nodes at once.
const PAGE_SIZE = 250;

// The concept/graph views need the WHOLE store at once (they aggregate every
// record into a single visualization that re-runs an expensive force
// simulation on each data change). So they fetch the entire remainder in ONE
// request — one round-trip, one simulation — capped at this many records.
const BULK_FETCH = 5000;

// Memory can be written a little after a run's completed_at; extend the window
// end by this much so trailing writes still fall inside the run.
const RUN_END_BUFFER_MS = 2 * 60 * 1000;

/**
 * Parse a timestamp to epoch ms; 0 when missing/invalid.
 *
 * Memory records come from Python `str(datetime)` ("2026-06-21 13:00:00.123456"
 * — space separator + microseconds, which `Date.parse` rejects) while run
 * timestamps are ISO ("2026-06-21T13:02:00"). Both are naive UTC. We normalize
 * both to comparable UTC epochs: space→T, trim fractional seconds to ms, and
 * append 'Z' when no timezone is present (treat as UTC).
 */
const timeMs = (iso: string | null | undefined): number => {
  if (!iso) return 0;
  let s = iso.trim().replace(' ', 'T').replace(/(\.\d{3})\d+/, '$1');
  const hasTz = /[Zz]$/.test(s) || /[+-]\d{2}:?\d{2}$/.test(s);
  if (!hasTz) s += 'Z';
  const t = Date.parse(s);
  return Number.isNaN(t) ? 0 : t;
};

/** Normalise a category label so variants collapse to the same key. */
const normalizeCategory = (raw: string): string =>
  raw
    .toLowerCase()
    .replace(/[_\s]+/g, '-')
    .replace(/[^a-z0-9-]/g, '')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '')
    .trim();

/** Extract the agent name from a scope path of the form .../agent/<name>/... */
const parseAgentFromScope = (scope: string): string | null => {
  const match = scope.match(/\/agent\/([^/]+)/);
  return match ? match[1] : null;
};

/** Extract the crew hash from a scope path of the form .../_crew_<hash>... */
const parseCrewFromScope = (scope: string): string | null => {
  const match = scope.match(/_crew_([0-9a-f]+)/i);
  return match ? match[1] : null;
};

const formatRelative = (iso: string | null): string => {
  if (!iso) return '—';
  const then = new Date(iso).getTime();
  if (Number.isNaN(then)) return iso;
  const diff = Date.now() - then;
  const min = 60_000;
  const hr = 60 * min;
  const day = 24 * hr;
  if (diff < min) return 'just now';
  if (diff < hr) return `${Math.round(diff / min)}m ago`;
  if (diff < day) return `${Math.round(diff / hr)}h ago`;
  if (diff < 7 * day) return `${Math.round(diff / day)}d ago`;
  return new Date(iso).toLocaleDateString();
};

interface CategoryStat {
  key: string;          // normalised slug
  label: string;        // canonical display form (most common variant)
  variants: Set<string>;
  count: number;
  recordIds: Set<string>;
  totalImportance: number;
  avgImportance: number;
}

interface AgentStat {
  name: string;
  count: number;
  crews: Set<string>;
  totalImportance: number;
  avgImportance: number;
}

interface DerivedIndex {
  categories: Map<string, CategoryStat>;
  agents: Map<string, AgentStat>;
  crews: Map<string, number>;
  coOccurrence: Map<string, Map<string, number>>;
  avgImportance: number;
}

function deriveIndex(records: MemoryRecord[]): DerivedIndex {
  const categories = new Map<string, CategoryStat>();
  const agents = new Map<string, AgentStat>();
  const crews = new Map<string, number>();
  const coOccurrence = new Map<string, Map<string, number>>();
  let totalImportance = 0;

  for (const record of records) {
    totalImportance += record.importance;
    const crew = parseCrewFromScope(record.scope);
    const agent = parseAgentFromScope(record.scope);

    if (crew) {
      crews.set(crew, (crews.get(crew) ?? 0) + 1);
    }
    if (agent) {
      const stat = agents.get(agent) ?? {
        name: agent,
        count: 0,
        crews: new Set<string>(),
        totalImportance: 0,
        avgImportance: 0,
      };
      stat.count += 1;
      stat.totalImportance += record.importance;
      if (crew) stat.crews.add(crew);
      stat.avgImportance = stat.totalImportance / stat.count;
      agents.set(agent, stat);
    }

    const normalisedInRecord = new Set<string>();
    for (const raw of record.categories ?? []) {
      const key = normalizeCategory(raw);
      if (!key) continue;
      normalisedInRecord.add(key);
      const stat = categories.get(key) ?? {
        key,
        label: raw,
        variants: new Set<string>(),
        count: 0,
        recordIds: new Set<string>(),
        totalImportance: 0,
        avgImportance: 0,
      };
      stat.variants.add(raw);
      stat.count += 1;
      stat.totalImportance += record.importance;
      if (record.id) stat.recordIds.add(record.id);
      // Pick the most frequent raw form as the canonical label.
      if ([...stat.variants].length === 1 || raw.length < stat.label.length) {
        stat.label = raw;
      }
      stat.avgImportance = stat.totalImportance / stat.count;
      categories.set(key, stat);
    }

    // Build symmetric co-occurrence counts.
    const keys = [...normalisedInRecord];
    for (let i = 0; i < keys.length; i += 1) {
      for (let j = i + 1; j < keys.length; j += 1) {
        const [a, b] = [keys[i], keys[j]];
        const mapA = coOccurrence.get(a) ?? new Map<string, number>();
        mapA.set(b, (mapA.get(b) ?? 0) + 1);
        coOccurrence.set(a, mapA);
        const mapB = coOccurrence.get(b) ?? new Map<string, number>();
        mapB.set(a, (mapB.get(a) ?? 0) + 1);
        coOccurrence.set(b, mapB);
      }
    }
  }

  return {
    categories,
    agents,
    crews,
    coOccurrence,
    avgImportance: records.length ? totalImportance / records.length : 0,
  };
}

// ---------------------------------------------------------------------------
// Sub-components
// ---------------------------------------------------------------------------

interface StatTileProps {
  label: string;
  value: string | number;
  hint?: string;
}

const StatTile: React.FC<StatTileProps> = ({ label, value, hint }) => (
  <Paper
    variant="outlined"
    sx={{
      px: 2,
      py: 1.25,
      borderRadius: 2,
      minWidth: 120,
      flex: '1 1 140px',
    }}
  >
    <Typography variant="caption" color="text.secondary">
      {label}
    </Typography>
    <Typography variant="h6" sx={{ lineHeight: 1.1, mt: 0.25 }}>
      {value}
    </Typography>
    {hint && (
      <Typography variant="caption" color="text.secondary">
        {hint}
      </Typography>
    )}
  </Paper>
);

interface ImportanceBadgeProps {
  value: number;
}

const importanceColor = (v: number): string => {
  if (v >= 0.75) return '#6366f1'; // indigo — high
  if (v >= 0.6)  return '#3b82f6'; // blue
  if (v >= 0.45) return '#06b6d4'; // cyan
  return '#94a3b8';                // slate — low
};

const ImportanceBadge: React.FC<ImportanceBadgeProps> = ({ value }) => (
  <Box sx={{ display: 'inline-flex', alignItems: 'center', gap: 0.5 }}>
    <StarIcon sx={{ fontSize: 14, color: importanceColor(value) }} />
    <Typography
      variant="caption"
      sx={{ fontWeight: 600, color: importanceColor(value), minWidth: 26 }}
    >
      {value.toFixed(2)}
    </Typography>
  </Box>
);

interface RecordCardProps {
  record: MemoryRecord;
  expanded: boolean;
  onToggleExpanded: () => void;
  onToggleCategory: (key: string) => void;
  activeCategories: Set<string>;
}

const RecordCard: React.FC<RecordCardProps> = ({
  record,
  expanded,
  onToggleExpanded,
  onToggleCategory,
  activeCategories,
}) => {
  const agent = parseAgentFromScope(record.scope) || 'unknown agent';
  const crew = parseCrewFromScope(record.scope);
  const tail = record.scope
    .split('/agent/')
    .slice(1)
    .join('/agent/')
    .split('/')
    .slice(1) // drop the role segment itself
    .join('/');
  const subScope = tail ? `/${tail}` : '';

  return (
    <Paper
      variant="outlined"
      sx={{
        px: 2,
        py: 1.5,
        borderRadius: 2,
        borderLeftWidth: 4,
        borderLeftColor: importanceColor(record.importance),
        transition: 'box-shadow 120ms ease',
        '&:hover': { boxShadow: 2 },
      }}
    >
      <Stack direction="row" spacing={2} alignItems="flex-start">
        <Box sx={{ flex: 1, minWidth: 0 }}>
          <Typography
            variant="body2"
            sx={{
              display: '-webkit-box',
              WebkitLineClamp: expanded ? 'unset' : 2,
              WebkitBoxOrient: 'vertical',
              overflow: 'hidden',
              lineHeight: 1.45,
              mb: 1,
            }}
          >
            {record.content}
          </Typography>

          <Stack
            direction="row"
            spacing={0.5}
            alignItems="center"
            flexWrap="wrap"
            useFlexGap
            sx={{ mb: 0.75 }}
          >
            {(record.categories || []).map((cat) => {
              const key = normalizeCategory(cat);
              const active = activeCategories.has(key);
              return (
                <Chip
                  key={`${record.id}-${cat}`}
                  label={cat}
                  size="small"
                  variant={active ? 'filled' : 'outlined'}
                  color={active ? 'primary' : 'default'}
                  onClick={() => onToggleCategory(key)}
                  sx={{ fontSize: 11 }}
                />
              );
            })}
          </Stack>

          <Stack
            direction="row"
            spacing={1.5}
            alignItems="center"
            flexWrap="wrap"
            useFlexGap
            sx={{ color: 'text.secondary', rowGap: 0.5 }}
          >
            <Tooltip title={agent}>
              <Stack
                direction="row"
                spacing={0.5}
                alignItems="center"
                sx={{ minWidth: 0, maxWidth: '100%' }}
              >
                <PersonOutlineIcon sx={{ fontSize: 14, flexShrink: 0 }} />
                <Typography
                  variant="caption"
                  sx={{
                    fontFamily: 'monospace',
                    overflow: 'hidden',
                    textOverflow: 'ellipsis',
                    whiteSpace: 'nowrap',
                    maxWidth: 220,
                  }}
                >
                  {agent}
                </Typography>
              </Stack>
            </Tooltip>
            <Divider orientation="vertical" flexItem sx={{ my: 0.5 }} />
            <Tooltip title={record.created_at || ''}>
              <Typography variant="caption">{formatRelative(record.created_at)}</Typography>
            </Tooltip>
            {crew && (
              <>
                <Divider orientation="vertical" flexItem sx={{ my: 0.5 }} />
                <Typography variant="caption" sx={{ fontFamily: 'monospace', opacity: 0.7 }}>
                  crew:{crew.slice(0, 8)}
                </Typography>
              </>
            )}
          </Stack>

          <Collapse in={expanded} unmountOnExit>
            <Box
              sx={{
                mt: 1.25,
                pt: 1.25,
                borderTop: '1px dashed',
                borderColor: 'divider',
                display: 'grid',
                gridTemplateColumns: 'auto 1fr',
                columnGap: 1.5,
                rowGap: 0.5,
                fontSize: 12,
                color: 'text.secondary',
              }}
            >
              {subScope && (
                <>
                  <Typography variant="caption" sx={{ fontWeight: 600 }}>Scope</Typography>
                  <Typography
                    variant="caption"
                    sx={{
                      fontFamily: 'monospace',
                      wordBreak: 'break-all',
                      color: 'text.primary',
                    }}
                  >
                    {subScope}
                  </Typography>
                </>
              )}
              {crew && (
                <>
                  <Typography variant="caption" sx={{ fontWeight: 600 }}>Crew</Typography>
                  <Typography
                    variant="caption"
                    sx={{ fontFamily: 'monospace', wordBreak: 'break-all', color: 'text.primary' }}
                  >
                    {crew}
                  </Typography>
                </>
              )}
              {record.created_at && (
                <>
                  <Typography variant="caption" sx={{ fontWeight: 600 }}>Created</Typography>
                  <Typography variant="caption" sx={{ color: 'text.primary' }}>
                    {new Date(record.created_at).toLocaleString()}
                  </Typography>
                </>
              )}
              <Typography variant="caption" sx={{ fontWeight: 600 }}>Importance</Typography>
              <Typography variant="caption" sx={{ color: 'text.primary' }}>
                {record.importance.toFixed(2)}
              </Typography>
              {record.id && (
                <>
                  <Typography variant="caption" sx={{ fontWeight: 600 }}>Record ID</Typography>
                  <Typography
                    variant="caption"
                    sx={{ fontFamily: 'monospace', wordBreak: 'break-all', color: 'text.primary' }}
                  >
                    {record.id}
                  </Typography>
                </>
              )}
            </Box>
          </Collapse>
        </Box>

        <Stack spacing={0.5} alignItems="flex-end" sx={{ minWidth: 48 }}>
          <ImportanceBadge value={record.importance} />
          <Tooltip title={expanded ? 'Collapse' : 'Expand'}>
            <IconButton
              size="small"
              onClick={onToggleExpanded}
              sx={{
                transform: expanded ? 'rotate(180deg)' : 'none',
                transition: 'transform 150ms ease',
              }}
            >
              <ExpandMoreIcon fontSize="small" />
            </IconButton>
          </Tooltip>
        </Stack>
      </Stack>
    </Paper>
  );
};

// ---------------------------------------------------------------------------
// Category bubble map — categories sized by count, coloured by avg importance
// ---------------------------------------------------------------------------

interface CategoryBubbleMapProps {
  categories: CategoryStat[];
  activeCategories: Set<string>;
  onToggleCategory: (key: string) => void;
}

const CategoryBubbleMap: React.FC<CategoryBubbleMapProps> = ({
  categories,
  activeCategories,
  onToggleCategory,
}) => {
  if (!categories.length) {
    return (
      <Alert severity="info">
        Run a crew with cognitive memory enabled to populate category insights.
      </Alert>
    );
  }

  const maxCount = Math.max(...categories.map((c) => c.count));
  return (
    <Box
      sx={{
        display: 'flex',
        flexWrap: 'wrap',
        gap: 1.5,
        alignItems: 'center',
        justifyContent: 'center',
        py: 2,
      }}
    >
      {categories.map((cat) => {
        const scale = 0.6 + (cat.count / maxCount) * 1.4; // 0.6x – 2x
        const active = activeCategories.has(cat.key);
        return (
          <Tooltip
            key={cat.key}
            arrow
            title={
              <Box>
                <Typography variant="caption">
                  {cat.count} record{cat.count === 1 ? '' : 's'} · avg importance{' '}
                  {cat.avgImportance.toFixed(2)}
                </Typography>
                {cat.variants.size > 1 && (
                  <Typography variant="caption" sx={{ display: 'block', opacity: 0.8 }}>
                    Variants: {[...cat.variants].join(', ')}
                  </Typography>
                )}
              </Box>
            }
          >
            <Chip
              label={
                <Box component="span" sx={{ display: 'inline-flex', alignItems: 'center', gap: 0.5 }}>
                  <span>{cat.label}</span>
                  <Box
                    component="span"
                    sx={{
                      fontSize: `${0.65 * scale}rem`,
                      opacity: 0.7,
                      fontWeight: 700,
                    }}
                  >
                    {cat.count}
                  </Box>
                </Box>
              }
              onClick={() => onToggleCategory(cat.key)}
              variant={active ? 'filled' : 'outlined'}
              color={active ? 'primary' : 'default'}
              sx={{
                fontSize: `${0.75 * scale}rem`,
                height: 28 * scale,
                px: scale,
                fontWeight: 600,
                borderWidth: 2,
                borderColor: importanceColor(cat.avgImportance),
                backgroundColor: active ? undefined : `${importanceColor(cat.avgImportance)}18`,
                '& .MuiChip-label': {
                  px: 1.25,
                },
              }}
            />
          </Tooltip>
        );
      })}
    </Box>
  );
};

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------

export const MemoryRecordsBrowser: React.FC<MemoryRecordsBrowserProps> = ({
  open,
  onClose,
  initialRunId,
  initialView,
}) => {
  const [records, setRecords] = useState<MemoryRecord[]>([]);
  const [total, setTotal] = useState(0);
  // True once a fetch returns fewer rows than asked for — i.e. we've pulled the
  // whole store. Derived from the result (not records.length >= total) so an
  // over-estimated `total` can't drive an endless reload loop.
  const [fullyLoaded, setFullyLoaded] = useState(false);
  const [backend, setBackend] = useState<string>('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Crew runs (from the executions API) define the time boundaries we bucket
  // memory records into. The graph/concepts views default to the LATEST run;
  // ALL_RUNS is the opt-in full graph across every run.
  const [runs, setRuns] = useState<Run[]>([]);
  const [runsLoaded, setRunsLoaded] = useState(false);
  // For a selected run: 'saved' = what it persisted (records in its window);
  // 'recalled' = what it READ (record ids from its memory_retrieval traces).
  const [memoryMode, setMemoryMode] = useState<'saved' | 'recalled'>('saved');
  const [recalledIds, setRecalledIds] = useState<Set<string>>(new Set());
  // When opened scoped to a specific run (e.g. from a ChatMode message), start
  // on that run; otherwise '' lets the latest run-with-memory be auto-picked.
  const [selectedRunId, setSelectedRunId] = useState<string>(initialRunId || '');
  // Load the WHOLE store only on demand — the default (latest run) loads just
  // the first page so opening is fast and doesn't flicker. Turns true when the
  // user picks a run / "All runs", or when opened pinned to a run (ChatMode),
  // because then we need every record to show that run's full context.
  const [loadAll, setLoadAll] = useState<boolean>(Boolean(initialRunId));

  const [search, setSearch] = useState('');
  const [selectedAgents, setSelectedAgents] = useState<Set<string>>(new Set());
  const [selectedCategories, setSelectedCategories] = useState<Set<string>>(new Set());
  const [importanceRange, setImportanceRange] = useState<[number, number]>([0, 1]);
  const [expandedIds, setExpandedIds] = useState<Set<string>>(new Set());
  const [viewMode, setViewMode] = useState<'cards' | 'concepts' | 'graph'>(
    initialView || 'cards',
  );

  // Delete state
  const [confirmOpen, setConfirmOpen] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const [feedback, setFeedback] = useState<{
    severity: 'success' | 'error';
    message: string;
  } | null>(null);

  // Load the newest `limit` records from the top of the store, REPLACING the
  // list. We always read from offset 0 (no append/dedup paging) so there's no
  // offset drift and no per-chunk re-render loop — one fetch, one update. Two
  // sizes are used: PAGE_SIZE for the fast default, BULK_FETCH for "load all".
  const load = useCallback(async (limit: number) => {
    setLoading(true);
    setError(null);
    try {
      const response = await apiClient.get<RecordsResponse>(
        '/memory-backend/records',
        { params: { limit, offset: 0 } },
      );
      const page = response.data.records || [];
      setRecords(page);
      setBackend(response.data.backend || '');
      setTotal(response.data.total ?? page.length);
      // Fewer rows than requested ⇒ that's the whole store.
      setFullyLoaded(page.length < limit);
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      setError(`Failed to load memory records: ${msg}`);
      setRecords([]);
      setTotal(0);
    } finally {
      setLoading(false);
    }
  }, []);

  const fetchRecords = useCallback(() => {
    // Re-pick the default run once the fresh page is in — unless we were opened
    // pinned to a specific run (ChatMode graph), in which case keep that.
    setSelectedRunId(initialRunId || '');
    // Back to the fast default (latest run only) unless pinned to a run.
    setLoadAll(Boolean(initialRunId));
    return load(PAGE_SIZE);
  }, [load, initialRunId]);

  const hasMore = !fullyLoaded && records.length < total;

  useEffect(() => {
    if (open) {
      fetchRecords();
    }
  }, [open, fetchRecords]);

  // Load the group's crew runs, newest first — exactly like the crew-canvas job
  // history. We bypass the runs cache so a crew you JUST ran shows up as the
  // latest instead of a stale entry.
  const loadRuns = useCallback(async () => {
    try {
      runService.invalidateRunsCache();
      const resp = await runService.getRuns(100);
      const sorted = [...(resp.runs || [])].sort(
        (a, b) => timeMs(b.created_at) - timeMs(a.created_at),
      );
      setRuns(sorted);
    } catch {
      setRuns([]);
    } finally {
      setRunsLoaded(true);
    }
  }, []);

  useEffect(() => {
    if (open) loadRuns();
  }, [open, loadRuns]);

  // For the selected run, pull the record ids it RECALLED from its
  // memory_retrieval traces (the trace content embeds id='<uuid>' for each
  // retrieved MemoryRecord). We then look those ids up in the loaded store
  // records to build the "Recalled" graph with full categories.
  useEffect(() => {
    if (!open || !selectedRunId || selectedRunId === ALL_RUNS) {
      setRecalledIds(new Set());
      return;
    }
    let cancelled = false;
    (async () => {
      try {
        const resp = await apiClient.get<{
          traces?: Array<{ event_type?: string; output?: unknown }>;
        }>(`/traces/job/${selectedRunId}`);
        const ids = new Set<string>();
        for (const tr of resp.data?.traces || []) {
          if (!/memory_retrieval/.test(tr.event_type || '')) continue;
          const text =
            typeof tr.output === 'string' ? tr.output : JSON.stringify(tr.output ?? '');
          for (const m of text.matchAll(/id='([0-9a-fA-F-]{36})'/g)) ids.add(m[1]);
        }
        if (!cancelled) setRecalledIds(ids);
      } catch {
        if (!cancelled) setRecalledIds(new Set());
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [open, selectedRunId]);

  // The concept bubble map and force graph aggregate the WHOLE store into one
  // visualization (you pan/scroll it — there are no pages) and re-run an
  // expensive force simulation on every data change. So when either view is
  // open, pull the entire remaining store in a SINGLE request — one round-trip,
  // one simulation — instead of dozens of 250-record pages. Cards view stays
  // manually paged ("Load more") to avoid mounting thousands of card nodes.

  // Per-run [start, end] time window. Each run owns only the records written
  // during its OWN execution — start = created_at, end = completed_at (+ buffer
  // Time window for the selected run, anchored on completed_at.
  //
  // IMPORTANT: a run's `created_at` is stored in LOCAL time, while `completed_at`
  // and memory record `created_at` are UTC — so created_at is NOT comparable to
  // memory timestamps (it can be hours off). We therefore window on completed_at:
  // a run owns records written after the previous (older) run finished, up to
  // its own completion (+ a small buffer for trailing writes). For a still-
  // running latest run we extend to "now". We test only the SELECTED run's own
  // window, so runs can't steal each other's records (the old "swap").
  const runWindow = useMemo(() => {
    if (selectedRunId === ALL_RUNS || !selectedRunId) return null;
    const idx = runs.findIndex((r) => r.job_id === selectedRunId);
    if (idx < 0) return null;
    const completedMs = (r: Run | undefined) =>
      r && r.completed_at ? timeMs(r.completed_at) : null;
    const end = (completedMs(runs[idx]) ?? Date.now()) + RUN_END_BUFFER_MS;
    // Start = the nearest OLDER run that has a completion time.
    let start = -Infinity;
    for (let j = idx + 1; j < runs.length; j++) {
      const c = completedMs(runs[j]);
      if (c != null) {
        start = c;
        break;
      }
    }
    return { start, end };
  }, [runs, selectedRunId]);

  // Default the view to the LATEST run (newest first), like the job history —
  // that's the run you just kicked off. Set once, as soon as runs load.
  useEffect(() => {
    if (!runsLoaded || selectedRunId) return;
    setSelectedRunId(runs.length ? runs[0].job_id : ALL_RUNS);
  }, [runsLoaded, selectedRunId, runs]);

  const latestRunId = runs[0]?.job_id;

  // On demand, load the WHOLE store in ONE request (offset 0, replace) — no
  // paging loop, so no flicker. Runs once when the user asks for more than the
  // default latest run (loadAll) and we don't already have everything.
  useEffect(() => {
    if (!open || !runsLoaded || !loadAll || loading || fullyLoaded) return;
    load(BULK_FETCH);
  }, [open, runsLoaded, loadAll, loading, fullyLoaded, load]);

  // ------------------------------------------------------------------
  // Derived data
  // ------------------------------------------------------------------

  // Records scoped to the selected run (or all records for the full graph).
  // Every view (cards/concepts/graph) and the derived index work off this set,
  // so picking a run focuses the whole browser on that run.
  const scopedRecords = useMemo(() => {
    if (selectedRunId === ALL_RUNS || !selectedRunId) return records;
    if (memoryMode === 'recalled') {
      // What the run READ: records whose ids appear in its retrieval traces.
      if (recalledIds.size === 0) return [];
      return records.filter((r) => r.id && recalledIds.has(r.id));
    }
    // What the run SAVED: records written within its window.
    if (!runWindow) return records;
    return records.filter((r) => {
      const t = timeMs(r.created_at);
      return t > runWindow.start && t <= runWindow.end;
    });
  }, [records, selectedRunId, memoryMode, recalledIds, runWindow]);

  const index = useMemo(() => deriveIndex(scopedRecords), [scopedRecords]);

  const categoryStats = useMemo(
    () => [...index.categories.values()].sort((a, b) => b.count - a.count || b.avgImportance - a.avgImportance),
    [index.categories],
  );

  const agentStats = useMemo(
    () => [...index.agents.values()].sort((a, b) => b.count - a.count),
    [index.agents],
  );

  const filteredRecords = useMemo(() => {
    const q = search.trim().toLowerCase();
    const filtered = scopedRecords.filter((record) => {
      if (record.importance < importanceRange[0] || record.importance > importanceRange[1]) {
        return false;
      }
      if (selectedAgents.size > 0) {
        const agent = parseAgentFromScope(record.scope);
        if (!agent || !selectedAgents.has(agent)) return false;
      }
      if (selectedCategories.size > 0) {
        const recordKeys = new Set((record.categories || []).map(normalizeCategory));
        for (const pinned of selectedCategories) {
          if (!recordKeys.has(pinned)) return false;
        }
      }
      if (q) {
        const hay = `${record.content} ${record.scope} ${(record.categories || []).join(' ')}`.toLowerCase();
        if (!hay.includes(q)) return false;
      }
      return true;
    });
    return filtered.sort((a, b) => {
      const byImp = b.importance - a.importance;
      if (byImp !== 0) return byImp;
      return (b.created_at || '').localeCompare(a.created_at || '');
    });
  }, [scopedRecords, search, selectedAgents, selectedCategories, importanceRange]);

  // ------------------------------------------------------------------
  // Handlers
  // ------------------------------------------------------------------

  const toggleCategory = useCallback((key: string) => {
    setSelectedCategories((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  }, []);

  const toggleAgent = useCallback((name: string) => {
    setSelectedAgents((prev) => {
      const next = new Set(prev);
      if (next.has(name)) next.delete(name);
      else next.add(name);
      return next;
    });
  }, []);

  const toggleExpanded = useCallback((id: string | null) => {
    if (!id) return;
    setExpandedIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }, []);

  const resetFilters = () => {
    setSearch('');
    setSelectedAgents(new Set());
    setSelectedCategories(new Set());
    setImportanceRange([0, 1]);
  };

  const deleteAll = useCallback(async () => {
    setDeleting(true);
    try {
      const response = await apiClient.delete<{
        backend: string;
        deleted: number;
      }>('/memory-backend/records');
      const deleted = response.data?.deleted ?? 0;
      setFeedback({
        severity: 'success',
        message:
          deleted > 0
            ? `Deleted ${deleted} local memory store${deleted === 1 ? '' : 's'}.`
            : 'No memory stores were found to delete.',
      });
      resetFilters();
      await fetchRecords();
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      setFeedback({ severity: 'error', message: `Delete failed: ${msg}` });
    } finally {
      setDeleting(false);
      setConfirmOpen(false);
    }
  }, [fetchRecords]);

  const filtersActive =
    !!search ||
    selectedAgents.size > 0 ||
    selectedCategories.size > 0 ||
    importanceRange[0] > 0 ||
    importanceRange[1] < 1;

  // ------------------------------------------------------------------
  // Render
  // ------------------------------------------------------------------

  const backendChipLabel = backend === 'default' ? 'Local (LanceDB)' : backend || '…';
  const backendChipColor: 'primary' | 'success' | 'default' =
    backend === 'databricks' ? 'primary' : backend === 'lakebase' ? 'success' : 'default';

  return (
    <Dialog open={open} onClose={onClose} maxWidth="lg" fullWidth>
      <DialogTitle sx={{ pb: 1 }}>
        <Stack direction="row" alignItems="center" spacing={1.5}>
          <Typography variant="h6" component="div">
            Cognitive Memory Browser
          </Typography>
          <Chip size="small" label={backendChipLabel} color={backendChipColor} variant="outlined" />
          <Box sx={{ flexGrow: 1 }} />
          <Tooltip title="Refresh">
            <IconButton
              size="small"
              onClick={() => {
                loadRuns();
                fetchRecords();
              }}
              disabled={loading}
            >
              <RefreshIcon fontSize="small" />
            </IconButton>
          </Tooltip>
          <Tooltip title="Delete all memory records for this group">
            <span>
              <IconButton
                size="small"
                color="error"
                onClick={() => setConfirmOpen(true)}
                disabled={loading || deleting || records.length === 0}
              >
                <DeleteOutlineIcon fontSize="small" />
              </IconButton>
            </span>
          </Tooltip>
          <IconButton size="small" onClick={onClose}>
            <CloseIcon fontSize="small" />
          </IconButton>
        </Stack>
        <Stack direction="row" spacing={1.5} sx={{ mt: 1.5 }} useFlexGap flexWrap="wrap">
          <StatTile
            label="Records"
            value={selectedRunId === ALL_RUNS ? (total || records.length) : scopedRecords.length}
            hint={
              selectedRunId === ALL_RUNS
                ? hasMore ? `${records.length} loaded` : 'all runs'
                : `this run · ${total} total`
            }
          />
          <StatTile
            label="Concepts"
            value={index.categories.size}
            hint={`${agentStats.length} agent${agentStats.length === 1 ? '' : 's'}`}
          />
          <StatTile
            label="Avg importance"
            value={index.avgImportance.toFixed(2)}
            hint={`Range ${Math.min(...scopedRecords.map((r) => r.importance) || [0]).toFixed(2)} – ${
              Math.max(...scopedRecords.map((r) => r.importance) || [0]).toFixed(2)
            }`}
          />
          <StatTile label="Visible" value={filteredRecords.length} hint={filtersActive ? 'filtered' : 'all'} />
        </Stack>
      </DialogTitle>

      <DialogContent dividers sx={{ p: 0 }}>
        {loading && <LinearProgress />}
        {error && (
          <Alert severity="error" sx={{ m: 2 }}>
            {error}
          </Alert>
        )}

        <Box sx={{ display: 'flex', minHeight: 520 }}>
          {/* ── Filter sidebar ───────────────────────────────────────── */}
          <Box
            sx={{
              width: 260,
              flexShrink: 0,
              borderRight: '1px solid',
              borderColor: 'divider',
              p: 2,
              overflowY: 'auto',
              maxHeight: 'calc(90vh - 220px)',
            }}
          >
            <Stack direction="row" alignItems="center" spacing={1} sx={{ mb: 1.5 }}>
              <FilterAltIcon fontSize="small" />
              <Typography variant="subtitle2">Filters</Typography>
              {filtersActive && (
                <Button size="small" onClick={resetFilters} sx={{ ml: 'auto', minWidth: 0 }}>
                  clear
                </Button>
              )}
            </Stack>

            <TextField
              fullWidth
              size="small"
              placeholder="Search content, scope, categories"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              sx={{ mb: 2 }}
              InputProps={{
                startAdornment: (
                  <InputAdornment position="start">
                    <SearchIcon fontSize="small" />
                  </InputAdornment>
                ),
              }}
            />

            <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mb: 0.5 }}>
              Importance range
            </Typography>
            <Slider
              size="small"
              value={importanceRange}
              onChange={(_e, v) => setImportanceRange(v as [number, number])}
              valueLabelDisplay="auto"
              valueLabelFormat={(v) => v.toFixed(2)}
              min={0}
              max={1}
              step={0.05}
              marks={[
                { value: 0, label: '0' },
                { value: 0.5, label: '.5' },
                { value: 1, label: '1' },
              ]}
            />

            <Typography
              variant="caption"
              color="text.secondary"
              sx={{ display: 'block', mt: 2, mb: 0.5 }}
            >
              Agents ({agentStats.length})
            </Typography>
            <Stack spacing={0.25}>
              {agentStats.length === 0 && (
                <Typography variant="caption" color="text.secondary" sx={{ fontStyle: 'italic' }}>
                  none yet
                </Typography>
              )}
              {agentStats.map((agent) => {
                const active = selectedAgents.has(agent.name);
                return (
                  <Box
                    key={agent.name}
                    onClick={() => toggleAgent(agent.name)}
                    sx={{
                      display: 'flex',
                      alignItems: 'center',
                      gap: 1,
                      px: 1,
                      py: 0.5,
                      borderRadius: 1,
                      cursor: 'pointer',
                      bgcolor: active ? 'action.selected' : 'transparent',
                      '&:hover': { bgcolor: 'action.hover' },
                    }}
                  >
                    <PersonOutlineIcon sx={{ fontSize: 14, color: active ? 'primary.main' : 'text.secondary' }} />
                    <Typography
                      variant="caption"
                      sx={{
                        flex: 1,
                        fontFamily: 'monospace',
                        overflow: 'hidden',
                        textOverflow: 'ellipsis',
                        whiteSpace: 'nowrap',
                        fontWeight: active ? 600 : 400,
                      }}
                    >
                      {agent.name}
                    </Typography>
                    <Typography variant="caption" color="text.secondary">
                      {agent.count}
                    </Typography>
                  </Box>
                );
              })}
            </Stack>

            <Typography
              variant="caption"
              color="text.secondary"
              sx={{ display: 'block', mt: 2, mb: 0.5 }}
            >
              Top concepts
            </Typography>
            <Stack direction="row" flexWrap="wrap" spacing={0.5} useFlexGap>
              {categoryStats.slice(0, 15).map((cat) => {
                const active = selectedCategories.has(cat.key);
                return (
                  <Chip
                    key={cat.key}
                    size="small"
                    label={`${cat.label} · ${cat.count}`}
                    variant={active ? 'filled' : 'outlined'}
                    color={active ? 'primary' : 'default'}
                    onClick={() => toggleCategory(cat.key)}
                    sx={{ fontSize: 11, borderColor: importanceColor(cat.avgImportance) }}
                  />
                );
              })}
            </Stack>
          </Box>

          {/* ── Main pane ────────────────────────────────────────────── */}
          <Box sx={{ flex: 1, p: 2, overflow: 'auto', maxHeight: 'calc(90vh - 220px)' }}>
            <Stack direction="row" alignItems="center" spacing={1} sx={{ mb: 1.5 }}>
              <ToggleButtonGroup
                size="small"
                value={viewMode}
                exclusive
                onChange={(_e, v) => {
                  if (v) setViewMode(v);
                }}
              >
                <ToggleButton value="cards" aria-label="cards">
                  <ViewAgendaIcon fontSize="small" sx={{ mr: 0.5 }} />
                  Records
                </ToggleButton>
                <ToggleButton value="concepts" aria-label="concepts">
                  <BubbleChartIcon fontSize="small" sx={{ mr: 0.5 }} />
                  Concepts
                </ToggleButton>
                <ToggleButton value="graph" aria-label="graph">
                  <AccountTreeIcon fontSize="small" sx={{ mr: 0.5 }} />
                  Graph
                </ToggleButton>
              </ToggleButtonGroup>

              <TextField
                select
                size="small"
                label="Run"
                value={runsLoaded && selectedRunId ? selectedRunId : ''}
                onChange={(e) => {
                  setSelectedRunId(e.target.value);
                  setMemoryMode('saved');
                  // Only "All runs" reads the entire store. Picking a single run
                  // just filters what's already loaded — no full-memory read.
                  setLoadAll(e.target.value === ALL_RUNS);
                }}
                disabled={!runsLoaded}
                sx={{ minWidth: 240 }}
              >
                {(!runsLoaded || !selectedRunId) && (
                  <MenuItem value="" disabled>
                    {runsLoaded ? 'Select a run…' : 'Loading runs…'}
                  </MenuItem>
                )}
                {runs.map((r) => (
                  <MenuItem key={r.job_id} value={r.job_id}>
                    {(r.job_id === latestRunId ? 'Latest · ' : '') +
                      (r.run_name || `Run ${r.job_id.slice(0, 8)}`) +
                      ` · ${formatRelative(r.created_at)}`}
                  </MenuItem>
                ))}
                <MenuItem value={ALL_RUNS}>All runs (full graph)</MenuItem>
              </TextField>

              {/* For a single run: what it SAVED vs what it RECALLED (read). */}
              {selectedRunId && selectedRunId !== ALL_RUNS && (
                <ToggleButtonGroup
                  size="small"
                  exclusive
                  value={memoryMode}
                  onChange={(_e, v) => {
                    if (!v) return;
                    setMemoryMode(v);
                    // Recalled memories can come from any past run, so the whole
                    // store must be loaded to resolve their ids.
                    if (v === 'recalled') setLoadAll(true);
                  }}
                >
                  <ToggleButton value="saved" aria-label="saved">
                    Saved
                  </ToggleButton>
                  <ToggleButton value="recalled" aria-label="recalled">
                    Recalled
                  </ToggleButton>
                </ToggleButtonGroup>
              )}

              {selectedCategories.size > 0 && (
                <Stack direction="row" spacing={0.5} alignItems="center">
                  <Typography variant="caption" color="text.secondary">
                    Pinned:
                  </Typography>
                  {[...selectedCategories].map((key) => {
                    const stat = index.categories.get(key);
                    return (
                      <Chip
                        key={key}
                        size="small"
                        color="primary"
                        label={stat?.label || key}
                        onDelete={() => toggleCategory(key)}
                      />
                    );
                  })}
                </Stack>
              )}
              <Box sx={{ flexGrow: 1 }} />
              <Typography variant="caption" color="text.secondary">
                Sorted by importance · most recent first
              </Typography>
            </Stack>

            {viewMode === 'cards' && (
              <Stack spacing={1.25}>
                {filteredRecords.length === 0 && !loading && (
                  <Alert severity="info">
                    No records match the current filters.{' '}
                    {filtersActive && (
                      <Button size="small" onClick={resetFilters}>
                        reset
                      </Button>
                    )}
                  </Alert>
                )}
                {filteredRecords.map((record, idx) => (
                  <RecordCard
                    key={record.id ?? idx}
                    record={record}
                    expanded={expandedIds.has(record.id ?? `idx-${idx}`)}
                    onToggleExpanded={() => toggleExpanded(record.id ?? `idx-${idx}`)}
                    onToggleCategory={toggleCategory}
                    activeCategories={selectedCategories}
                  />
                ))}
              </Stack>
            )}

            {viewMode === 'concepts' && (
              <Box>
                <CategoryBubbleMap
                  categories={categoryStats}
                  activeCategories={selectedCategories}
                  onToggleCategory={toggleCategory}
                />
                <Collapse in={selectedCategories.size > 0}>
                  <Divider sx={{ my: 2 }} />
                  <Typography variant="subtitle2" sx={{ mb: 1 }}>
                    Records mentioning pinned concepts
                  </Typography>
                  <Stack spacing={1.25}>
                    {filteredRecords.map((record, idx) => (
                      <RecordCard
                        key={record.id ?? idx}
                        record={record}
                        expanded={expandedIds.has(record.id ?? `idx-${idx}`)}
                        onToggleExpanded={() => toggleExpanded(record.id ?? `idx-${idx}`)}
                        onToggleCategory={toggleCategory}
                        activeCategories={selectedCategories}
                      />
                    ))}
                  </Stack>
                </Collapse>
              </Box>
            )}

            {viewMode === 'graph' && (
              <Box>
                <ConceptForceGraph
                  nodes={categoryStats.map((c) => ({
                    id: c.key,
                    label: c.label,
                    count: c.count,
                    avgImportance: c.avgImportance,
                  }))}
                  edges={(() => {
                    const seen = new Set<string>();
                    const out: { source: string; target: string; weight: number }[] = [];
                    for (const [src, map] of index.coOccurrence.entries()) {
                      for (const [dst, weight] of map.entries()) {
                        const key = src < dst ? `${src}|${dst}` : `${dst}|${src}`;
                        if (seen.has(key)) continue;
                        seen.add(key);
                        out.push({ source: src, target: dst, weight });
                      }
                    }
                    return out;
                  })()}
                  activeIds={selectedCategories}
                  onToggleNode={toggleCategory}
                  importanceColor={importanceColor}
                />
                <Collapse in={selectedCategories.size > 0}>
                  <Divider sx={{ my: 2 }} />
                  <Typography variant="subtitle2" sx={{ mb: 1 }}>
                    Records mentioning pinned concepts
                  </Typography>
                  <Stack spacing={1.25}>
                    {filteredRecords.map((record, idx) => (
                      <RecordCard
                        key={record.id ?? idx}
                        record={record}
                        expanded={expandedIds.has(record.id ?? `idx-${idx}`)}
                        onToggleExpanded={() => toggleExpanded(record.id ?? `idx-${idx}`)}
                        onToggleCategory={toggleCategory}
                        activeCategories={selectedCategories}
                      />
                    ))}
                  </Stack>
                </Collapse>
              </Box>
            )}

          </Box>
        </Box>
      </DialogContent>

      {/* ── Delete confirmation ───────────────────────────────────── */}
      <Dialog
        open={confirmOpen}
        onClose={() => !deleting && setConfirmOpen(false)}
        maxWidth="xs"
        fullWidth
      >
        <DialogTitle>Delete all memory records?</DialogTitle>
        <DialogContent>
          <DialogContentText>
            This permanently removes every cognitive-memory record your crews
            have stored on the active backend ({backendChipLabel}). Running
            crews will start with empty recall until new records are saved.
          </DialogContentText>
          {records.length > 0 && (
            <Alert severity="warning" sx={{ mt: 2 }}>
              {records.length} record{records.length === 1 ? '' : 's'} currently visible
              {backend === 'default'
                ? ' (stored locally across per-crew LanceDB directories)'
                : ''}
              .
            </Alert>
          )}
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setConfirmOpen(false)} disabled={deleting}>
            Cancel
          </Button>
          <Button color="error" onClick={deleteAll} disabled={deleting}>
            {deleting ? 'Deleting…' : 'Delete all'}
          </Button>
        </DialogActions>
      </Dialog>

      <Snackbar
        open={!!feedback}
        autoHideDuration={5000}
        onClose={() => setFeedback(null)}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'center' }}
      >
        {feedback ? (
          <Alert severity={feedback.severity} onClose={() => setFeedback(null)}>
            {feedback.message}
          </Alert>
        ) : undefined}
      </Snackbar>
    </Dialog>
  );
};

export default MemoryRecordsBrowser;
