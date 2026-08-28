import React, { useCallback, useEffect, useMemo, useState } from 'react';
import {
  Alert,
  Autocomplete,
  Box,
  Button,
  Chip,
  CircularProgress,
  FormControl,
  IconButton,
  InputLabel,
  MenuItem,
  Paper,
  Select,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  TextField,
  Tooltip,
  Typography,
} from '@mui/material';
import BoltIcon from '@mui/icons-material/Bolt';
import DeleteIcon from '@mui/icons-material/Delete';
import RefreshIcon from '@mui/icons-material/Refresh';

import { TriggersService } from '../../api/execution/TriggersService';
import { CrewService } from '../../api/workflow/CrewService';
import { FlowService } from '../../api/workflow/FlowService';
import SubscriptionsSection from './SubscriptionsSection';
import StepHeader from './StepHeader';
import { detectVariablesFromNodes } from '../../utils/variableDetector';
import { deriveFlowInputs } from '../../utils/flowInputs';
import { EnqueueTrigger, TriggerEvent } from '../../types/execution/triggers';

type ChipColor = 'default' | 'info' | 'success' | 'warning' | 'error';

const STATUS_COLOR: Record<string, ChipColor> = {
  pending: 'info',
  claimed: 'warning',
  dispatched: 'success',
  failed: 'error',
  dead: 'error',
};

/** A pickable target: a saved crew or flow, shown by friendly name. */
interface TargetOption {
  id: string;
  name: string;
  kind: 'flow' | 'crew';
  /** Input variable names the crew/flow declares ({placeholders} / router
   * conditions). Empty means it takes no inputs, so we hide the Inputs field. */
  inputs: string[];
}

/** Build the initial per-field input map (each declared variable → empty). */
const emptyInputs = (vars: string[]): Record<string, string> =>
  Object.fromEntries(vars.map((v) => [v, '']));

interface TriggersPanelProps {
  /** When rendered inside TriggersDialog, the dialog supplies the title — skip
   * the panel's own heading so it doesn't appear twice. */
  embedded?: boolean;
}

/**
 * Manage the event-trigger queue: fire an event to trigger a saved crew/flow and
 * inspect/delete queued events. Users pick a crew or flow BY NAME (no ids) and
 * optionally pass inputs — there is deliberately no raw agents/tasks YAML here.
 */
export const TriggersPanel: React.FC<TriggersPanelProps> = ({ embedded = false }) => {
  const [events, setEvents] = useState<TriggerEvent[]>([]);
  const [loading, setLoading] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [options, setOptions] = useState<TargetOption[]>([]);
  const [selected, setSelected] = useState<TargetOption | null>(null);
  const [harness, setHarness] = useState<'' | 'kasal' | 'crewai'>('');
  // One value per input variable the selected crew/flow declares (keyed by name).
  const [inputValues, setInputValues] = useState<Record<string, string>>({});

  const loadEvents = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await TriggersService.list(undefined, 50);
      setEvents(res.events);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load events');
    } finally {
      setLoading(false);
    }
  }, []);

  const loadOptions = useCallback(async () => {
    // Flows and crews are separate lists; either failing shouldn't block the
    // other (or the inline escape hatch).
    const [flows, crews] = await Promise.all([
      FlowService.getFlows().catch(() => []),
      CrewService.getCrews().catch(() => []),
    ]);
    const opts: TargetOption[] = [
      ...flows.map((f) => {
        const rec = f as { id: string; name?: unknown; nodes?: unknown[]; edges?: unknown[] };
        return {
          id: String(rec.id),
          name: String(rec.name ?? rec.id),
          kind: 'flow' as const,
          inputs: deriveFlowInputs(rec.nodes ?? [], rec.edges ?? []).map(
            (v) => v.name,
          ),
        };
      }),
      ...crews.map((c) => {
        const rec = c as { id: string; name?: unknown; nodes?: unknown[] };
        return {
          id: String(rec.id),
          name: String(rec.name ?? rec.id),
          kind: 'crew' as const,
          inputs: detectVariablesFromNodes(rec.nodes ?? []).map((v) => v.name),
        };
      }),
    ];
    setOptions(opts);
  }, []);

  useEffect(() => {
    loadEvents();
    loadOptions();
  }, [loadEvents, loadOptions]);

  // Resolve a queued event's target to its friendly crew/flow name (fall back to
  // the id only when the crew/flow can't be found).
  const nameByKey = useMemo(() => {
    const m: Record<string, string> = {};
    options.forEach((o) => {
      m[`${o.kind}:${o.id}`] = o.name;
    });
    return m;
  }, [options]);

  const targetLabel = (target: TriggerEvent['target']): string => {
    if (!target) return '—';
    if (target.kind === 'inline') return 'inline config';
    if (!target.id) return target.kind;
    const name = nameByKey[`${target.kind}:${target.id}`];
    return name ? `${target.kind}: ${name}` : `${target.kind}: ${target.id}`;
  };

  const handleFire = async () => {
    setSubmitting(true);
    setError(null);
    try {
      // Inputs come from the per-field form (one per declared variable).
      const inputs: Record<string, unknown> = { ...inputValues };

      if (!selected) {
        setError('Select a crew or flow to run');
        return;
      }
      const target: EnqueueTrigger['target'] = {
        kind: selected.kind,
        id: selected.id,
      };
      if (harness) {
        target.harness = harness;
      }

      await TriggersService.enqueue({ target, payload: { inputs } });
      setSelected(null);
      setInputValues({});
      // Fire = enqueue THEN drain, so a single click actually launches the run.
      // (The queue still decouples the two steps; this just spares you the manual
      // "Dispatch now" click that only exists because the background consumer is
      // off in local dev.)
      await TriggersService.dispatch();
      await loadEvents();
      // The run launches in the background; a second refresh catches the
      // pending → dispatched transition.
      setTimeout(loadEvents, 1500);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to fire event');
    } finally {
      setSubmitting(false);
    }
  };

  const handleDelete = async (id: number) => {
    setError(null);
    try {
      await TriggersService.delete(id);
      await loadEvents();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to delete event');
    }
  };

  return (
    <Box>
      {!embedded && (
        <Typography variant="h6" gutterBottom>
          Event Triggers
        </Typography>
      )}
      <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
        Chain crews and flows with events: one emits an event when it finishes,
        and another runs when that event fires.
      </Typography>

      {error && (
        <Alert severity="error" sx={{ mb: 2 }} onClose={() => setError(null)}>
          {error}
        </Alert>
      )}

      {/* Steps 1 & 2 — emit an event, then run a crew/flow on it. */}
      <SubscriptionsSection />

      {/* Step 3 — fire an event manually to test the chain. */}
      <Box sx={{ mt: 3 }}>
        <StepHeader
          n={3}
          title="Fire an event now (test)"
          subtitle="Drop an event on the queue by hand to try the chain out."
        />
        <Paper variant="outlined" sx={{ p: 1.5 }}>
          <Stack
            direction="row"
            spacing={1}
            alignItems="center"
            flexWrap="wrap"
            useFlexGap
          >
            <Autocomplete
              size="small"
              sx={{ flex: 1, minWidth: 220 }}
              options={options}
              value={selected}
              onChange={(_e, value) => {
                setSelected(value);
                // Reset the per-field form to the vars this target declares.
                setInputValues(value ? emptyInputs(value.inputs) : {});
              }}
              getOptionLabel={(o) => o.name}
              groupBy={(o) => (o.kind === 'flow' ? 'Flows' : 'Crews')}
              isOptionEqualToValue={(a, b) => a.id === b.id && a.kind === b.kind}
              renderInput={(params) => (
                <TextField
                  {...params}
                  label="Crew or Flow"
                  placeholder="Search by name…"
                />
              )}
            />

            <FormControl size="small" sx={{ minWidth: 120 }}>
              <InputLabel id="trigger-harness-label">Harness</InputLabel>
              <Select
                labelId="trigger-harness-label"
                label="Harness"
                value={harness}
                onChange={(e) =>
                  setHarness(e.target.value as '' | 'kasal' | 'crewai')
                }
              >
                <MenuItem value="">Default</MenuItem>
                <MenuItem value="kasal">Kasal</MenuItem>
                <MenuItem value="crewai">CrewAI</MenuItem>
              </Select>
            </FormControl>

            <Button
              variant="contained"
              startIcon={<BoltIcon />}
              onClick={handleFire}
              disabled={submitting}
            >
              {submitting ? 'Firing…' : 'Fire event'}
            </Button>
          </Stack>

          {selected && selected.inputs.length > 0 && (
            <Box sx={{ mt: 1.5 }}>
              <Typography variant="caption" color="text.secondary">
                Inputs for {selected.name}
              </Typography>
              <Stack spacing={1.5} sx={{ mt: 1 }}>
                {selected.inputs.map((name) => (
                  <TextField
                    key={name}
                    size="small"
                    label={name}
                    value={inputValues[name] ?? ''}
                    onChange={(e) =>
                      setInputValues((prev) => ({
                        ...prev,
                        [name]: e.target.value,
                      }))
                    }
                    fullWidth
                  />
                ))}
              </Stack>
            </Box>
          )}
        </Paper>
      </Box>

      <Stack
        direction="row"
        alignItems="center"
        justifyContent="space-between"
        sx={{ mb: 1, mt: 3 }}
      >
        <Typography variant="subtitle2">Recent events</Typography>
        <Tooltip title="Refresh">
          <span>
            <IconButton size="small" onClick={loadEvents} disabled={loading}>
              <RefreshIcon fontSize="small" />
            </IconButton>
          </span>
        </Tooltip>
      </Stack>

      {loading ? (
        <CircularProgress size={24} />
      ) : (
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell>ID</TableCell>
              <TableCell>Status</TableCell>
              <TableCell>Target</TableCell>
              <TableCell>Attempts</TableCell>
              <TableCell>Created</TableCell>
              <TableCell align="right">Actions</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {events.length === 0 ? (
              <TableRow>
                <TableCell colSpan={6}>
                  <Typography variant="body2" color="text.secondary">
                    No events yet.
                  </Typography>
                </TableCell>
              </TableRow>
            ) : (
              events.map((ev) => (
                <TableRow key={ev.id}>
                  <TableCell>{ev.id}</TableCell>
                  <TableCell>
                    <Chip
                      size="small"
                      label={ev.status}
                      color={STATUS_COLOR[ev.status] || 'default'}
                    />
                  </TableCell>
                  <TableCell>{targetLabel(ev.target)}</TableCell>
                  <TableCell>{ev.attempts}</TableCell>
                  <TableCell>
                    {ev.created_at
                      ? new Date(ev.created_at).toLocaleString()
                      : '-'}
                  </TableCell>
                  <TableCell align="right">
                    <Tooltip title="Delete">
                      <IconButton
                        size="small"
                        onClick={() => handleDelete(ev.id)}
                      >
                        <DeleteIcon fontSize="small" />
                      </IconButton>
                    </Tooltip>
                  </TableCell>
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      )}
    </Box>
  );
};

export default TriggersPanel;
