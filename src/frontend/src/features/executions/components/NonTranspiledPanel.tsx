/* ------------------------------------------------------------------ */
/*  Non-transpiled review panel                                        */
/* ------------------------------------------------------------------ */
/**
 * Interactive review table for measures that were NOT transpiled (they land as
 * `-- comment` lines in the UCMV YAML, and are carried through the generator +
 * validator output as `untranslatable_items`). Reviewers see the original DAX,
 * why it was skipped, and the generator's proposed approach, and can triage each
 * with a status + free-text note. Read-only for DAX/reason/proposal; Status/Note
 * are editable and flow up via `onReviewChange`.
 *
 * Extracted as its own module so both UCMVResultViewer and ValidatorResultViewer
 * can render it from the same `untranslatable_items` contract.
 */
import React, { useMemo, useState } from 'react';
import {
  Box,
  Chip,
  MenuItem,
  Select,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  TextField,
  Typography,
} from '@mui/material';

/** One non-emitted measure (mirrors the backend `untranslatable_items` row from
 *  uc_metric_view_generator_tool / metric_view_validator_tool). */
export interface UntranslatableItem {
  table_key: string;
  view_name?: string;
  original_name: string;
  dax_expression: string;
  skip_reason: string;
  category: string;
  dax_class?: string | null;
  referenced_by: number;
  /** Actionable next-step for handling this measure (HOW), from the LLM's own
   *  recipe or a class-based default. Emitted alongside the terse skip_reason (WHY). */
  proposal?: string;
  explanation?: string | null;
  /** Labeled, UNVERIFIED CREATE VIEW scaffold for cross-fact / multi-stage cases
   *  (proposal artifact, never an emitted measure). Also present in the YAML. */
  source_view_sql_draft?: string | null;
}

/** Triage status a reviewer can assign to a non-transpiled item. */
export type ReviewStatus = 'todo' | 'wont_fix' | 'hand_written' | 'needs_info';

export interface ReviewAnnotation {
  status?: ReviewStatus;
  note?: string;
}

/** Stable per-item key for the review annotation map. */
export const untranslatableKey = (item: { table_key: string; original_name: string }): string =>
  `${item.table_key}::${item.original_name}`;

/** Human labels for the review status dropdown. */
export const REVIEW_STATUS_OPTIONS: Array<{ value: ReviewStatus; label: string }> = [
  { value: 'todo', label: 'To translate' },
  { value: 'hand_written', label: 'Hand-written' },
  { value: 'wont_fix', label: 'Not needed' },
  { value: 'needs_info', label: 'Needs info' },
];

export const NonTranspiledPanel: React.FC<{
  items: UntranslatableItem[];
  review: Record<string, ReviewAnnotation>;
  editable: boolean;
  onReviewChange: (key: string, patch: Partial<ReviewAnnotation>) => void;
}> = ({ items, review, editable, onReviewChange }) => {
  const [categoryFilter, setCategoryFilter] = useState<string | null>(null);

  // Distinct categories for the filter chip row.
  const categories = useMemo(() => {
    const set = new Set<string>();
    items.forEach((i) => i.category && set.add(i.category));
    return Array.from(set).sort();
  }, [items]);

  // Sort by impact (referenced_by desc), then filter.
  const rows = useMemo(() => {
    const sorted = [...items].sort((a, b) => (b.referenced_by ?? 0) - (a.referenced_by ?? 0));
    return categoryFilter ? sorted.filter((i) => i.category === categoryFilter) : sorted;
  }, [items, categoryFilter]);

  return (
    <Box>
      <Typography variant="body2" color="text.secondary" sx={{ mb: 1 }}>
        These measures could not be transpiled and are not emitted as UC metric-view
        measures (they appear as comments in the YAML). Review each and mark how it
        should be handled.
      </Typography>

      {categories.length > 1 && (
        <Box display="flex" gap={0.5} flexWrap="wrap" sx={{ mb: 1 }}>
          <Chip
            size="small"
            label={`All (${items.length})`}
            color={categoryFilter === null ? 'primary' : 'default'}
            variant={categoryFilter === null ? 'filled' : 'outlined'}
            onClick={() => setCategoryFilter(null)}
          />
          {categories.map((c) => (
            <Chip
              key={c}
              size="small"
              label={`${c} (${items.filter((i) => i.category === c).length})`}
              color={categoryFilter === c ? 'primary' : 'default'}
              variant={categoryFilter === c ? 'filled' : 'outlined'}
              onClick={() => setCategoryFilter(c)}
            />
          ))}
        </Box>
      )}

      <Table size="small" sx={{ tableLayout: 'fixed' }}>
        <TableHead>
          <TableRow>
            <TableCell sx={{ fontWeight: 600, width: '15%' }}>Measure</TableCell>
            <TableCell sx={{ fontWeight: 600, width: '24%' }}>DAX</TableCell>
            <TableCell sx={{ fontWeight: 600, width: '14%' }}>Reason</TableCell>
            <TableCell sx={{ fontWeight: 600, width: '16%' }} title="Suggested next step for handling this measure">Proposed approach</TableCell>
            <TableCell sx={{ fontWeight: 600, width: '6%' }} align="right" title="How many other measures reference this one">Used by</TableCell>
            <TableCell sx={{ fontWeight: 600, width: '13%' }}>Status</TableCell>
            <TableCell sx={{ fontWeight: 600, width: '12%' }}>Note</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {rows.map((item) => {
            const key = untranslatableKey(item);
            const ann = review[key] || {};
            return (
              <TableRow key={key} hover>
                <TableCell sx={{ fontFamily: 'monospace', fontSize: '0.8rem', wordBreak: 'break-word' }}>
                  {item.original_name}
                </TableCell>
                <TableCell sx={{ fontFamily: 'monospace', fontSize: '0.75rem', wordBreak: 'break-word', whiteSpace: 'pre-wrap' }}>
                  {item.dax_expression || '—'}
                </TableCell>
                <TableCell sx={{ fontSize: '0.75rem', wordBreak: 'break-word' }}>
                  {item.category ? <Chip size="small" label={item.category} variant="outlined" color="warning" /> : null}
                  {item.skip_reason && (
                    <Typography variant="caption" display="block" color="text.secondary" sx={{ mt: 0.5 }}>
                      {item.skip_reason}
                    </Typography>
                  )}
                </TableCell>
                <TableCell sx={{ fontSize: '0.75rem', wordBreak: 'break-word' }}>
                  {item.proposal ? (
                    <Typography variant="caption" display="block" color="text.secondary">
                      {item.proposal}
                    </Typography>
                  ) : '—'}
                </TableCell>
                <TableCell sx={{ fontSize: '0.8rem' }} align="right">
                  {item.referenced_by > 0 ? item.referenced_by : '—'}
                </TableCell>
                <TableCell>
                  <Select
                    size="small"
                    fullWidth
                    displayEmpty
                    disabled={!editable}
                    value={ann.status ?? ''}
                    onChange={(e) =>
                      onReviewChange(key, { status: (e.target.value || undefined) as ReviewStatus | undefined })
                    }
                    sx={{ fontSize: '0.75rem' }}
                    renderValue={(v) =>
                      v ? (REVIEW_STATUS_OPTIONS.find((o) => o.value === v)?.label ?? String(v)) : '—'
                    }
                  >
                    <MenuItem value=""><em>—</em></MenuItem>
                    {REVIEW_STATUS_OPTIONS.map((o) => (
                      <MenuItem key={o.value} value={o.value} sx={{ fontSize: '0.8rem' }}>
                        {o.label}
                      </MenuItem>
                    ))}
                  </Select>
                </TableCell>
                <TableCell>
                  <TextField
                    size="small"
                    fullWidth
                    variant="standard"
                    placeholder="Add note…"
                    disabled={!editable}
                    value={ann.note ?? ''}
                    onChange={(e) => onReviewChange(key, { note: e.target.value })}
                    InputProps={{ sx: { fontSize: '0.75rem' } }}
                  />
                </TableCell>
              </TableRow>
            );
          })}
        </TableBody>
      </Table>
    </Box>
  );
};

export default NonTranspiledPanel;
