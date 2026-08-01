/**
 * The flow-level state declaration, as a section other dialogs can host.
 *
 * Lives next to the per-edge checkpoint switch rather than behind its own
 * button, because the two are only useful together: a conversation needs
 * somewhere to live (this) AND something to write it (checkpoint), and having
 * them in different places is how a flow ends up with one of the two.
 *
 * Note the asymmetry, which the copy states plainly: checkpoint belongs to the
 * EDGE you opened, this belongs to the whole FLOW. Editing it from any edge
 * changes the flow.
 *
 * The channel list is read live off the canvas — the same derivation that runs
 * on every save and every run — so it never invents a channel and cannot
 * disagree with what the backend receives. What it adds is the two decisions no
 * static analysis can make: how each channel merges, and whether the flow holds
 * a conversation.
 */

import React, { useCallback, useEffect, useMemo } from 'react';
import {
  Alert,
  Box,
  FormControlLabel,
  MenuItem,
  Stack,
  Switch,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  TextField,
  Typography,
} from '@mui/material';
import { useTabManagerStore } from '../../store/tabManager';
import { useFlowStateStore } from '../../store/flowState';
import { FlowService } from '../../api/workflow/FlowService';
import type { DeclaredFlowState } from '../../store/flowState';
import { flowStateNames } from '../../utils/flowStateSchema';
import type { FlowStateReducer } from '../../utils/flowStateSchema';

const REDUCERS: { value: FlowStateReducer; label: string; help: string }[] = [
  { value: 'replace', label: 'Replace', help: 'Newest value wins. The default.' },
  { value: 'append', label: 'Append', help: 'Adds to a list. Use for anything that accumulates.' },
  { value: 'merge', label: 'Merge', help: 'Shallow dict merge, new keys winning.' },
  { value: 'add', label: 'Add', help: 'Numeric sum, for counters.' },
];

/** Channels ConversationState brings; shown so the list is not a half-truth. */
const CONVERSATION_CHANNELS = [
  { name: 'messages', reducer: 'append', note: 'The conversation so far' },
  { name: 'last_user_message', reducer: 'replace', note: 'What was said this turn' },
  { name: 'last_intent', reducer: 'replace', note: 'This turn’s classification' },
  { name: 'session_ready', reducer: 'replace', note: 'One-time bootstrap marker' },
];

const FlowStateSection: React.FC = () => {
  const activeTabId = useTabManagerStore((state) => state.activeTabId);
  const declared = useFlowStateStore((state) => state.declared);
  const { setReducer, setConversational } = useFlowStateStore();

  // Hydrate from the SAVED flow when this tab has no declaration yet.
  //
  // The declaration used to be seeded only by the flow-load handler, which
  // writes to whatever tab was active at that instant — and a flow opened into
  // a NEW canvas activates its tab around the same moment, so the declaration
  // landed on the wrong tab and the dialog opened showing defaults for a flow
  // that had the toggle on. Reading it here instead removes the ordering
  // question altogether: whatever tab this dialog belongs to, it asks for that
  // tab's flow.
  //
  // Only when the store has nothing: an unsaved edit must win over what is on
  // disk, or opening the dialog twice would silently discard a change.
  useEffect(() => {
    if (!activeTabId) return;
    if (useFlowStateStore.getState().getDeclared(activeTabId)) return;
    const flowId = useTabManagerStore.getState().getActiveTab()?.savedFlowId;
    if (!flowId) return;

    let cancelled = false;
    FlowService.getFlow(flowId)
      .then((flow) => {
        const declared = (
          flow?.flowConfig ?? (flow as { flow_config?: { state?: DeclaredFlowState } })?.flow_config
        )?.state as DeclaredFlowState | undefined;
        if (!cancelled && declared) {
          useFlowStateStore.getState().setDeclared(activeTabId, declared);
        }
      })
      .catch(() => {
        // Best-effort: the dialog still opens, showing the derived channels and
        // no declaration. Failing to read the saved flow must not block editing.
      });
    return () => {
      cancelled = true;
    };
  }, [activeTabId]);


  const channels = useMemo(() => {
    const tab = useTabManagerStore.getState().getActiveTab();
    return flowStateNames(tab?.flowNodes ?? [], tab?.flowEdges ?? []);
  }, []);

  const current = activeTabId ? declared[activeTabId] : undefined;
  const conversational = !!current?.conversational;

  const reducerFor = useCallback(
    (channel: string): FlowStateReducer =>
      (current?.model?.properties?.[channel]?.reducer as FlowStateReducer) ?? 'replace',
    [current],
  );

  const [saving, setSaving] = React.useState(false);
  const [savedAt, setSavedAt] = React.useState<number | null>(null);

  // Write straight through to the saved flow.
  //
  // This control is edited from an EDGE dialog whose Save commits the edge to
  // the canvas and nothing else, so leaving the declaration to ride along on
  // the next flow save made it look like the toggle did nothing — configure,
  // press the Save in front of you, and the database never changes. Only the
  // state block is sent, so an unsaved canvas edit is neither saved nor lost.
  //
  // An UNSAVED flow has nowhere to write to; there the store still carries it
  // and the first save picks it up.
  const persist = useCallback(async () => {
    const flowId = useTabManagerStore.getState().getActiveTab()?.savedFlowId;
    if (!flowId || !activeTabId) return;
    setSaving(true);
    const declaration = useFlowStateStore.getState().getDeclared(activeTabId);
    const ok = await FlowService.updateFlowState(
      flowId,
      declaration as Record<string, unknown> | undefined,
    );
    setSaving(false);
    if (ok) setSavedAt(Date.now());
  }, [activeTabId]);

  const handleReducer = (channel: string, reducer: FlowStateReducer) => {
    if (!activeTabId) return;
    setReducer(activeTabId, channel, reducer);
    void persist();
  };

  return (
  <Stack spacing={2} sx={{ mt: 1 }}>
    <Box>
      <Typography variant="body2" sx={{ fontWeight: 600 }}>
        Flow state
      </Typography>
      <Typography variant="caption" color="text.secondary">
        Applies to the WHOLE flow, not just this connection — unlike the
        checkpoint above. Channels are read from the flow&apos;s router
        conditions and task placeholders; choose how each one merges when
        something writes to it.
      </Typography>
    </Box>

    <FormControlLabel
      control={
        <Switch
          checked={conversational}
          onChange={(e) => {
            if (!activeTabId) return;
            setConversational(activeTabId, e.target.checked);
            void persist();
          }}
        />
      }
      label="Hold a conversation across turns"
    />
    {conversational && (
      <Alert severity="info" variant="outlined">
        Each message in a chat session continues this flow&apos;s state
        instead of starting a new run. The flow gains the channels below,
        and its history is capped at the most recent 100 messages.
      </Alert>
    )}

    {channels.length === 0 && !conversational && (
      <Alert severity="warning" variant="outlined">
        This flow reads no state — no router condition and no
        {' '}
        <code>{'{placeholder}'}</code> names one — so there is nothing to
        declare. It will keep running on an untyped state.
      </Alert>
    )}

    {(channels.length > 0 || conversational) && (
      <Table size="small">
        <TableHead>
          <TableRow>
            <TableCell>Channel</TableCell>
            <TableCell width={200}>On write</TableCell>
            <TableCell>Notes</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {conversational &&
            CONVERSATION_CHANNELS.map((channel) => (
              <TableRow key={channel.name}>
                <TableCell>
                  <code>{channel.name}</code>
                </TableCell>
                <TableCell>
                  <Typography variant="body2" color="text.secondary">
                    {channel.reducer}
                  </Typography>
                </TableCell>
                <TableCell>
                  <Typography variant="body2" color="text.secondary">
                    {channel.note}
                  </Typography>
                </TableCell>
              </TableRow>
            ))}
          {channels.map((channel) => (
            <TableRow key={channel}>
              <TableCell>
                <code>{channel}</code>
              </TableCell>
              <TableCell>
                <TextField
                  select
                  size="small"
                  fullWidth
                  value={reducerFor(channel)}
                  onChange={(e) =>
                    handleReducer(channel, e.target.value as FlowStateReducer)
                  }
                >
                  {REDUCERS.map((reducer) => (
                    <MenuItem key={reducer.value} value={reducer.value}>
                      {reducer.label}
                    </MenuItem>
                  ))}
                </TextField>
              </TableCell>
              <TableCell>
                <Typography variant="body2" color="text.secondary">
                  {REDUCERS.find((r) => r.value === reducerFor(channel))?.help}
                </Typography>
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    )}

    <Box>
      <Typography variant="caption" color="text.secondary">
        {saving
          ? 'Saving…'
          : savedAt
            ? 'Saved to the flow.'
            : 'Saved to the flow as soon as you change it — the Save below is for this connection.'}
      </Typography>
    </Box>
  </Stack>
  );
};

export default FlowStateSection;
