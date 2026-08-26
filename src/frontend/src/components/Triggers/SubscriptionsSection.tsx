import React, { useCallback, useEffect, useMemo, useState } from 'react';
import {
  Alert,
  Autocomplete,
  Box,
  Chip,
  IconButton,
  Paper,
  Stack,
  TextField,
  Tooltip,
  Typography,
} from '@mui/material';
import AddIcon from '@mui/icons-material/Add';
import DeleteIcon from '@mui/icons-material/Delete';
import ArrowForwardIcon from '@mui/icons-material/ArrowForward';
import WarningAmberIcon from '@mui/icons-material/WarningAmber';

import { TriggersService } from '../../api/execution/TriggersService';
import { CrewService } from '../../api/workflow/CrewService';
import { FlowService } from '../../api/workflow/FlowService';
import { SchemaService } from '../../api/workflow/SchemaService';
import StepHeader from './StepHeader';
import {
  EmitRule,
  Subscription,
  TriggerTarget,
} from '../../types/execution/triggers';

interface TargetOption {
  id: string;
  name: string;
  kind: 'flow' | 'crew';
}

const targetKey = (t?: TriggerTarget | null) => (t ? `${t.kind}:${t.id}` : '');

// Standard lifecycle event types — a constant, mirrored from the backend
// EventType enum (src/services/triggers/event_types.py). No free text.
const EVENT_TYPES = ['completed', 'failed'] as const;

// The distinct event name a producer emits: {kind}:{id}:{type} — matches
// canonical_event_name() on the backend. None of the three parts contain a
// colon, so it splits back cleanly.
const canonicalEventName = (kind: string, id: string, type: string) =>
  `${kind}:${id}:${type}`;

const parseCanonical = (
  name: string,
): { kind: string; id: string; type: string } | null => {
  const parts = name.split(':');
  return parts.length === 3
    ? { kind: parts[0], id: parts[1], type: parts[2] }
    : null;
};

/**
 * The choreography config: which crew/flow runs on which event (subscriptions),
 * and which crew/flow emits which event on completion (emit rules). Rendered as
 * plain sentences; crews/flows are shown BY NAME. The two halves stay separate
 * on the backend — they are only joined here by the shared event name so a
 * dangling rule can be flagged inline.
 */
export const SubscriptionsSection: React.FC = () => {
  const [options, setOptions] = useState<TargetOption[]>([]);
  const [schemaNames, setSchemaNames] = useState<string[]>([]);
  const [subs, setSubs] = useState<Subscription[]>([]);
  const [rules, setRules] = useState<EmitRule[]>([]);
  const [error, setError] = useState<string | null>(null);

  // Subscription create form (subEvent holds a canonical event name)
  const [subEvent, setSubEvent] = useState('');
  const [subTarget, setSubTarget] = useState<TargetOption | null>(null);
  const [subSchema, setSubSchema] = useState('');

  // Emit-rule create form (emitEvent holds a lifecycle type from EVENT_TYPES)
  const [emitTarget, setEmitTarget] = useState<TargetOption | null>(null);
  const [emitEvent, setEmitEvent] = useState<string>('completed');
  const [emitSchema, setEmitSchema] = useState('');

  const nameByKey = useMemo(() => {
    const m: Record<string, string> = {};
    options.forEach((o) => {
      m[`${o.kind}:${o.id}`] = o.name;
    });
    return m;
  }, [options]);

  const targetLabel = (t?: TriggerTarget | null) => {
    if (!t) return '—';
    return nameByKey[targetKey(t)] || `${t.kind}: ${t.id}`;
  };

  // The canonical event name a given emit rule produces.
  const ruleEvent = (r: EmitRule): string =>
    r.on_target
      ? canonicalEventName(
          r.on_target.kind,
          String(r.on_target.id ?? ''),
          r.event_type,
        )
      : r.event_type;

  // A canonical event name rendered as "{producer name} · {type}".
  const friendlyEvent = (name: string): string => {
    const p = parseCanonical(name);
    if (!p) return name;
    const producer =
      nameByKey[`${p.kind}:${p.id}`] || `${p.kind}: ${p.id}`;
    return `${producer} · ${p.type}`;
  };

  // Join the two halves by canonical event name to flag danglers:
  // an emit rule nobody listens to, or a subscription nothing emits.
  const emittedEvents = useMemo(
    () => new Set(rules.map(ruleEvent)),
    [rules],
  );
  const subscribedEvents = useMemo(
    () => new Set(subs.map((s) => s.event_type)),
    [subs],
  );

  // The Listen dropdown's options: one per emit rule, keyed by its canonical
  // event, labelled by producer + type. This is how step 2 can only pick events
  // that step 1 actually emits.
  const emittedOptions = useMemo(
    () =>
      rules.map((r) => ({
        value: ruleEvent(r),
        label: friendlyEvent(ruleEvent(r)),
      })),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [rules, nameByKey],
  );

  const load = useCallback(async () => {
    setError(null);
    try {
      const [flows, crews, list, schemas] = await Promise.all([
        FlowService.getFlows().catch(() => []),
        CrewService.getCrews().catch(() => []),
        TriggersService.listSubscriptions(),
        SchemaService.getInstance()
          .getSchemas()
          .catch(() => []),
      ]);
      setSchemaNames(schemas.map((s) => s.name));
      setOptions([
        ...flows.map((f) => ({
          id: String(f.id),
          name: f.name || String(f.id),
          kind: 'flow' as const,
        })),
        ...crews.map((c) => {
          const rec = c as { id: string; name?: unknown };
          return {
            id: String(rec.id),
            name: String(rec.name ?? rec.id),
            kind: 'crew' as const,
          };
        }),
      ]);
      setSubs(list.subscriptions);
      setRules(list.emit_rules);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load subscriptions');
    }
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  const addSubscription = async () => {
    if (!subEvent || !subTarget) {
      setError('Pick an event to listen for and the crew/flow to run');
      return;
    }
    setError(null);
    try {
      await TriggersService.createSubscription({
        event_type: subEvent, // canonical name from the dropdown
        target: { kind: subTarget.kind, id: subTarget.id },
        schema_ref: subSchema.trim() || undefined,
      });
      setSubEvent('');
      setSubTarget(null);
      setSubSchema('');
      await load();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to add subscription');
    }
  };

  const addEmitRule = async () => {
    if (!emitTarget || !emitEvent) {
      setError('Pick the crew/flow that emits and an event type');
      return;
    }
    setError(null);
    try {
      await TriggersService.createEmitRule({
        on_target: { kind: emitTarget.kind, id: emitTarget.id },
        event_type: emitEvent, // lifecycle type: completed | failed
        schema_ref: emitSchema.trim() || undefined,
      });
      setEmitTarget(null);
      setEmitEvent('completed');
      setEmitSchema('');
      await load();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to add emit rule');
    }
  };

  const targetPicker = (
    value: TargetOption | null,
    onChange: (v: TargetOption | null) => void,
    label: string,
  ) => (
    <Autocomplete
      size="small"
      sx={{ minWidth: 200, flex: 1 }}
      options={options}
      value={value}
      onChange={(_e, v) => onChange(v)}
      getOptionLabel={(o) => o.name}
      groupBy={(o) => (o.kind === 'flow' ? 'Flows' : 'Crews')}
      isOptionEqualToValue={(a, b) => a.id === b.id && a.kind === b.kind}
      renderInput={(params) => <TextField {...params} label={label} />}
    />
  );

  // Step 1 picks a STANDARD lifecycle type (completed/failed) — a fixed dropdown,
  // never free text. The backend concatenates the producer id to make the event
  // name distinct ({kind}:{id}:{type}).
  const emitTypePicker = (value: string, onChange: (v: string) => void) => (
    <Autocomplete
      size="small"
      sx={{ minWidth: 150 }}
      options={[...EVENT_TYPES]}
      value={value || 'completed'}
      onChange={(_e, v) => onChange(v)}
      disableClearable
      renderInput={(params) => <TextField {...params} label="Event type" />}
    />
  );

  // Step 2 can only listen for an event that Step 1 actually emits — a strict
  // dropdown of the canonical events, labelled "{producer} · {type}".
  const listenEventPicker = (value: string, onChange: (v: string) => void) => (
    <Autocomplete
      size="small"
      sx={{ minWidth: 240 }}
      options={emittedOptions}
      value={emittedOptions.find((o) => o.value === value) ?? null}
      onChange={(_e, v) => onChange(v ? v.value : '')}
      getOptionLabel={(o) => o.label}
      isOptionEqualToValue={(a, b) => a.value === b.value}
      noOptionsText="Add an emit rule in step 1 first"
      renderInput={(params) => (
        <TextField {...params} label="When this event fires" />
      )}
    />
  );

  // Schema is an Object Management schema, chosen from a dropdown — not free text.
  const schemaPicker = (value: string, onChange: (v: string) => void) => (
    <Autocomplete
      size="small"
      sx={{ minWidth: 160 }}
      options={schemaNames}
      value={value || null}
      onChange={(_e, v) => onChange(v ?? '')}
      renderInput={(params) => <TextField {...params} label="Schema" />}
    />
  );

  const eventChip = (label: string) => (
    <Chip size="small" color="primary" variant="outlined" label={label} />
  );

  const targetText = (t?: TriggerTarget | null) => (
    <Typography variant="body2" fontWeight={600} component="span">
      {targetLabel(t)}
    </Typography>
  );

  const arrow = (
    <ArrowForwardIcon fontSize="small" color="disabled" sx={{ mx: 0.5 }} />
  );

  const warnChip = (label: string, tip: string) => (
    <Tooltip title={tip}>
      <Chip
        size="small"
        variant="outlined"
        color="warning"
        icon={<WarningAmberIcon />}
        label={label}
        data-testid="dangling-warning"
      />
    </Tooltip>
  );

  const deleteBtn = (onClick: () => void) => (
    <Tooltip title="Delete">
      <IconButton size="small" onClick={onClick}>
        <DeleteIcon fontSize="small" />
      </IconButton>
    </Tooltip>
  );

  const schemaChip = (schema?: string | null) =>
    schema ? (
      <Chip
        size="small"
        variant="outlined"
        label={schema}
        sx={{ color: 'text.secondary', borderColor: 'divider' }}
      />
    ) : null;

  const rowSx = {
    px: 1.5,
    py: 1,
    borderTop: 1,
    borderColor: 'divider',
  } as const;

  return (
    <Box>
      {error && (
        <Alert severity="error" sx={{ mb: 2 }} onClose={() => setError(null)}>
          {error}
        </Alert>
      )}

      {/* Step 1 — emit rules: "when CREW/FLOW finishes → emit EVENT" */}
      <StepHeader
        n={1}
        title="Emit an event when a crew or flow finishes"
        subtitle="The producer — its output announces an event others can react to."
      />
      <Paper variant="outlined" sx={{ mt: 1, mb: 3 }}>
        {/* create row */}
        <Stack
          direction="row"
          spacing={1}
          alignItems="center"
          flexWrap="wrap"
          useFlexGap
          sx={{ p: 1.5 }}
        >
          {targetPicker(emitTarget, setEmitTarget, 'When this crew/flow finishes')}
          {arrow}
          {emitTypePicker(emitEvent, setEmitEvent)}
          {schemaPicker(emitSchema, setEmitSchema)}
          <IconButton
            color="primary"
            onClick={addEmitRule}
            aria-label="Add emit rule"
          >
            <AddIcon />
          </IconButton>
        </Stack>

        {rules.length === 0 ? (
          <Box sx={rowSx}>
            <Typography variant="body2" color="text.secondary">
              No emit rules yet.
            </Typography>
          </Box>
        ) : (
          rules.map((r) => (
            <Stack
              key={r.id}
              direction="row"
              alignItems="center"
              spacing={1}
              sx={rowSx}
            >
              {targetText(r.on_target)}
              {arrow}
              {eventChip(r.event_type)}
              <Box sx={{ flex: 1 }} />
              {schemaChip(r.schema_ref)}
              {!subscribedEvents.has(ruleEvent(r)) &&
                warnChip(
                  'no subscriber',
                  'No crew/flow subscribes to this event yet.',
                )}
              {deleteBtn(async () => {
                await TriggersService.deleteEmitRule(r.id);
                await load();
              })}
            </Stack>
          ))
        )}
      </Paper>

      {/* Step 2 — subscriptions: "when EVENT fires → run CREW/FLOW" */}
      <StepHeader
        n={2}
        title="Run a crew or flow when an event fires"
        subtitle="The consumer — it reacts to an event emitted in step 1."
      />
      <Paper variant="outlined" sx={{ mt: 1 }}>
        {/* create row */}
        <Stack
          direction="row"
          spacing={1}
          alignItems="center"
          flexWrap="wrap"
          useFlexGap
          sx={{ p: 1.5 }}
        >
          {listenEventPicker(subEvent, setSubEvent)}
          {arrow}
          {targetPicker(subTarget, setSubTarget, 'Run this crew/flow')}
          {schemaPicker(subSchema, setSubSchema)}
          <IconButton
            color="primary"
            onClick={addSubscription}
            aria-label="Add subscription"
          >
            <AddIcon />
          </IconButton>
        </Stack>

        {subs.length === 0 ? (
          <Box sx={rowSx}>
            <Typography variant="body2" color="text.secondary">
              No event triggers yet.
            </Typography>
          </Box>
        ) : (
          subs.map((s) => (
            <Stack
              key={s.id}
              direction="row"
              alignItems="center"
              spacing={1}
              sx={rowSx}
            >
              {eventChip(friendlyEvent(s.event_type))}
              {arrow}
              {targetText(s.target)}
              <Box sx={{ flex: 1 }} />
              {schemaChip(s.schema_ref)}
              {!emittedEvents.has(s.event_type) &&
                warnChip(
                  'orphaned',
                  'Nothing emits this event — its emit rule was removed.',
                )}
              {deleteBtn(async () => {
                await TriggersService.deleteSubscription(s.id);
                await load();
              })}
            </Stack>
          ))
        )}
      </Paper>
    </Box>
  );
};

export default SubscriptionsSection;
