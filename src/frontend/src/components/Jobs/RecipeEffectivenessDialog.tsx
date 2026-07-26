/**
 * Is workflow reuse helping? — the measurement report.
 *
 * Deliberately NOT a recipe library. Curation happens in the run list, in the
 * Reusable column, because marking a crew reusable is a claim about its OUTPUT
 * and the run list is the only place the result and trace are one click away. A
 * standalone library that listed recipes without their results would be asking
 * people to vote on something they cannot see.
 *
 * This dialog is the other half: not "which crews", but "is any of this
 * working". Read `comparable` first — it is the difference between a measurement
 * and a number that merely looks like one.
 */

import React, { useEffect, useState } from 'react';
import {
  Alert,
  Box,
  Chip,
  CircularProgress,
  Dialog,
  DialogContent,
  DialogTitle,
  IconButton,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  Typography,
} from '@mui/material';
import CloseIcon from '@mui/icons-material/Close';

import {
  RecipeEffectiveness,
  WorkflowRecipeService,
} from '../../api/workflow/WorkflowRecipeService';

const pct = (value: number | null | undefined): string =>
  value === null || value === undefined ? '—' : `${Math.round(value * 100)}%`;

const ms = (value: number | null): string => {
  if (!value) return '—';
  const seconds = value / 1000;
  if (seconds < 90) return `${seconds.toFixed(0)}s`;
  return `${Math.floor(seconds / 60)}m ${Math.round(seconds % 60)}s`;
};

const ARM_LABEL: Record<string, string> = {
  exemplar: 'Got exemplars',
  control: 'Withheld (control)',
  none_available: 'Nothing available',
};

interface Props {
  open: boolean;
  onClose: () => void;
}

export const RecipeEffectivenessDialog: React.FC<Props> = ({ open, onClose }) => {
  const [data, setData] = useState<RecipeEffectiveness | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!open) return;
    setLoading(true);
    setError(null);
    WorkflowRecipeService.getEffectiveness()
      .then(setData)
      .catch(() => setError('Could not load the effectiveness report'))
      .finally(() => setLoading(false));
  }, [open]);

  return (
    <Dialog open={open} onClose={onClose} maxWidth="md" fullWidth>
      <DialogTitle sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
        <Box sx={{ flex: 1 }}>
          Is workflow reuse helping?
          <Typography variant="body2" color="text.secondary">
            Crews marked reusable are offered as examples when generating similar
            ones. This is whether that changes the result.
          </Typography>
        </Box>
        <IconButton size="small" onClick={onClose}>
          <CloseIcon fontSize="small" />
        </IconButton>
      </DialogTitle>

      <DialogContent>
        {loading && <CircularProgress size={24} />}
        {error && <Alert severity="error">{error}</Alert>}

        {data && (
          <Box>
            <Box sx={{ display: 'flex', gap: 3, flexWrap: 'wrap', mb: 2 }}>
              <Stat
                label="Generations"
                value={String(data.generations)}
                hint={`last ${data.window_days} days`}
              />
              <Stat
                label="Had a match"
                value={pct(data.coverage_rate)}
                hint={`${data.with_candidates} of ${data.generations}`}
              />
              <Stat
                label="Got exemplars"
                value={pct(data.injection_rate)}
                hint={`${data.with_blessed_candidates} had a curated match`}
              />
              <Stat
                label="Holdout"
                value={pct(data.holdout_fraction)}
                hint="control arm"
              />
            </Box>

            {data.generations === 0 && (
              <Alert severity="info" sx={{ mb: 2 }}>
                No crew generations recorded yet in this window. Rows appear here
                once someone generates a crew from a prompt.
              </Alert>
            )}

            {/* The caveat goes before the table, so the numbers cannot be read
                as causal when they are not. */}
            <Alert severity={data.comparable ? 'success' : 'warning'} sx={{ mb: 2 }}>
              {data.comparable
                ? 'Both arms have data — “Got exemplars” vs “Withheld” is a fair comparison, because both had a curated match available and differ only in treatment.'
                : 'No control arm yet, so no causal claim is available. Set WORKFLOW_RECIPE_HOLDOUT (e.g. 0.2) to withhold exemplars from a random share of eligible generations. Comparing against “Nothing available” instead would measure how familiar the request was, not what the exemplars did.'}
            </Alert>

            {data.generations > 0 && (
              <Table size="small">
                <TableHead>
                  <TableRow>
                    <TableCell>Arm</TableCell>
                    <TableCell align="right">Generations</TableCell>
                    <TableCell align="right">Runs</TableCell>
                    <TableCell align="right">Completed</TableCell>
                    <TableCell align="right">Median time</TableCell>
                    <TableCell align="right">Median errors</TableCell>
                    <TableCell align="right">Agents / tasks</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {Object.entries(data.arms).map(([arm, s]) => (
                    <TableRow key={arm}>
                      <TableCell>
                        <Chip
                          size="small"
                          label={ARM_LABEL[arm] || arm}
                          color={arm === 'exemplar' ? 'primary' : 'default'}
                          variant={arm === 'none_available' ? 'outlined' : 'filled'}
                        />
                      </TableCell>
                      <TableCell align="right">{s.generations}</TableCell>
                      <TableCell align="right">{s.linked_runs}</TableCell>
                      <TableCell align="right">
                        {pct(s.completion_rate)}
                        {s.linked_runs > 0 && (
                          <Typography variant="caption" color="text.secondary">
                            {' '}
                            ({s.completed}/{s.linked_runs})
                          </Typography>
                        )}
                      </TableCell>
                      <TableCell align="right">{ms(s.median_duration_ms)}</TableCell>
                      <TableCell align="right">{s.median_error_spans ?? '—'}</TableCell>
                      <TableCell align="right">
                        {s.median_agents ?? '—'} / {s.median_tasks ?? '—'}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            )}

            <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mt: 2 }}>
              Rates are over runs that actually happened — a generated crew nobody
              ran is not a crew that failed. Similarity floor: {data.min_similarity}.
            </Typography>
          </Box>
        )}
      </DialogContent>
    </Dialog>
  );
};

const Stat: React.FC<{ label: string; value: string; hint?: string }> = ({
  label,
  value,
  hint,
}) => (
  <Box>
    <Typography variant="caption" color="text.secondary">
      {label}
    </Typography>
    <Typography variant="h6" sx={{ lineHeight: 1.1 }}>
      {value}
    </Typography>
    {hint && (
      <Typography variant="caption" color="text.secondary">
        {hint}
      </Typography>
    )}
  </Box>
);

export default RecipeEffectivenessDialog;
