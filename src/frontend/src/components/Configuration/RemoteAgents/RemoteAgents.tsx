import React, { useCallback, useEffect, useState } from 'react';
import {
  Alert,
  Box,
  Button,
  Chip,
  CircularProgress,
  FormControlLabel,
  IconButton,
  Paper,
  Snackbar,
  Stack,
  Switch,
  Tooltip,
  Typography,
} from '@mui/material';
import AddIcon from '@mui/icons-material/Add';
import DeleteIcon from '@mui/icons-material/Delete';
import EditIcon from '@mui/icons-material/Edit';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import ErrorIcon from '@mui/icons-material/Error';
import SyncIcon from '@mui/icons-material/Sync';
import {
  A2AAgent,
  A2AAgentInput,
  A2AAgentService,
} from '../../../api/tools/A2AAgentService';
import RemoteAgentDialog from './RemoteAgentDialog';

interface Props {
  /**
   * `system` is the Kasal-admin catalogue: register agents and choose which are
   * offered to workspaces. `workspace` is the opt-in view: turn an offered
   * agent on or off here, nothing else.
   *
   * The same split MCP servers have, and for the same reason — a remote agent
   * row carries an outbound URL and a credential.
   */
  mode?: 'system' | 'workspace';
}

/**
 * Remote agents — the outbound half of A2A.
 *
 * MCP servers give an agent TOOLS; a remote agent gives it a COLLEAGUE. They sit
 * beside each other in Configuration for that reason, and this page deliberately
 * looks like the MCP one: same shape, same two modes, so an operator who has
 * attached one knows how to attach the other.
 */
const RemoteAgents: React.FC<Props> = ({ mode = 'workspace' }) => {
  const isSystem = mode === 'system';

  const [agents, setAgents] = useState<A2AAgent[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [toast, setToast] = useState<string | null>(null);
  const [dialogOpen, setDialogOpen] = useState(false);
  const [editing, setEditing] = useState<A2AAgent | null>(null);
  const [testing, setTesting] = useState<number | null>(null);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      setAgents(isSystem ? await A2AAgentService.listBase() : await A2AAgentService.list());
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Could not load remote agents.');
    } finally {
      setLoading(false);
    }
  }, [isSystem]);

  useEffect(() => {
    void load();
  }, [load]);

  const handleSave = async (input: A2AAgentInput) => {
    const saved = editing
      ? await A2AAgentService.update(editing.id, input)
      : await A2AAgentService.create(input);
    // Patched in place rather than refetching: a full reload here would flash
    // the whole list for a one-row change.
    setAgents((prev) =>
      editing ? prev.map((a) => (a.id === saved.id ? saved : a)) : [...prev, saved],
    );
    setToast(
      saved.last_error
        ? `Saved, but ${saved.name} could not be reached.`
        : `${saved.name} saved.`,
    );
  };

  const handleDelete = async (agent: A2AAgent) => {
    await A2AAgentService.remove(agent.id);
    setAgents((prev) => prev.filter((a) => a.id !== agent.id));
    setToast(`${agent.name} removed, along with every workspace's opt-in.`);
  };

  const handleToggle = async (agent: A2AAgent) => {
    try {
      const desired = !agent.enabled;
      // Toggling an inherited global agent returns the workspace's own copy,
      // which has a DIFFERENT id — so the row is replaced by name, not by id.
      const saved = isSystem
        ? await A2AAgentService.setGlobalAvailability(agent.id, desired)
        : await A2AAgentService.setWorkspaceEnabled(agent.id, desired);
      setAgents((prev) => prev.map((a) => (a.name === saved.name ? saved : a)));
    } catch (err) {
      setToast(err instanceof Error ? err.message : 'Could not change that.');
    }
  };

  const handleTest = async (agent: A2AAgent) => {
    setTesting(agent.id);
    try {
      const result = await A2AAgentService.test(agent.id);
      setToast(result.message);
      await load();
    } catch (err) {
      setToast(err instanceof Error ? err.message : 'The test failed.');
    } finally {
      setTesting(null);
    }
  };

  return (
    <Box>
      <Stack
        direction="row"
        justifyContent="space-between"
        alignItems="flex-start"
        spacing={2}
        sx={{ mb: 2 }}
      >
        {/* The text yields, the actions do not — see SkillsConfiguration. */}
        <Box sx={{ minWidth: 0 }}>
          <Typography variant="h6">
            {isSystem ? 'Remote Agents (Global)' : 'Remote Agents'}
          </Typography>
          <Typography variant="body2" color="text.secondary">
            {isSystem
              ? 'Agents outside Kasal, registered once and offered to workspaces. Turning one off here withdraws it everywhere.'
              : 'Agents your crews can delegate to over A2A. Turn one on for this teamspace, then give an agent the Remote Agent tool.'}
          </Typography>
        </Box>
        {/* Only the global view registers agents; the workspace view opts in. */}
        {isSystem && (
          <Button
            size="small"
            startIcon={<AddIcon />}
            variant="contained"
            onClick={() => {
              setEditing(null);
              setDialogOpen(true);
            }}
            sx={{ flexShrink: 0, whiteSpace: 'nowrap' }}
          >
            Add agent
          </Button>
        )}
      </Stack>

      {error && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {error}
        </Alert>
      )}

      {loading ? (
        <Box sx={{ display: 'flex', justifyContent: 'center', py: 4 }}>
          <CircularProgress size={28} />
        </Box>
      ) : agents.length === 0 ? (
        <Paper variant="outlined" sx={{ p: 3, textAlign: 'center' }}>
          <Typography variant="body2" color="text.secondary">
            {isSystem
              ? 'No remote agents yet. Add one with its URL — Kasal reads its Agent Card to discover what it can do.'
              : 'No remote agents have been made available globally yet.'}
          </Typography>
        </Paper>
      ) : (
        <Stack spacing={1.5}>
          {agents.map((agent) => {
            // In the workspace view a row with no group_id is INHERITED: it can
            // be toggled here, but it is edited and deleted in the global view.
            const isInheritedGlobal = !isSystem && !agent.group_id;
            return (
              <Paper key={agent.id} variant="outlined" sx={{ p: 2 }}>
                <Stack
                  direction="row"
                  justifyContent="space-between"
                  alignItems="flex-start"
                  spacing={2}
                >
                  <Box sx={{ minWidth: 0, flex: 1 }}>
                    <Stack direction="row" spacing={1} alignItems="center">
                      <Typography variant="subtitle1" noWrap>
                        {agent.name}
                      </Typography>
                      {agent.last_error ? (
                        <Tooltip title={agent.last_error}>
                          <ErrorIcon color="error" fontSize="small" />
                        </Tooltip>
                      ) : (
                        <Tooltip title="Card fetched successfully">
                          <CheckCircleIcon color="success" fontSize="small" />
                        </Tooltip>
                      )}
                      {isInheritedGlobal && (
                        <Chip
                          size="small"
                          color="primary"
                          variant="outlined"
                          label="Global"
                        />
                      )}
                    </Stack>

                    <Typography variant="body2" color="text.secondary" noWrap>
                      {agent.card_url}
                    </Typography>

                    {agent.description && (
                      <Typography variant="body2" sx={{ mt: 0.5 }}>
                        {agent.description}
                      </Typography>
                    )}

                    {agent.skills.length > 0 && (
                      <Stack
                        direction="row"
                        spacing={0.5}
                        sx={{ mt: 1, flexWrap: 'wrap', gap: 0.5 }}
                      >
                        {agent.skills.map((skill) => (
                          <Tooltip key={skill.id} title={skill.description || ''}>
                            <Chip size="small" variant="outlined" label={skill.name} />
                          </Tooltip>
                        ))}
                      </Stack>
                    )}
                  </Box>

                  <Stack direction="row" spacing={0.5} alignItems="center">
                    <FormControlLabel
                      control={
                        <Switch
                          size="small"
                          checked={agent.enabled}
                          onChange={() => void handleToggle(agent)}
                        />
                      }
                      label={
                        <Typography variant="caption">
                          {isSystem
                            ? agent.enabled
                              ? 'Available'
                              : 'Unavailable'
                            : agent.enabled
                              ? 'Enabled'
                              : 'Disabled'}
                        </Typography>
                      }
                    />
                    {/* Agents are edited, tested and deleted only in the global
                        view; the workspace view just opts in and out. */}
                    {isSystem && (
                      <>
                        <Tooltip title="Fetch the card again">
                          <span>
                            <IconButton
                              size="small"
                              onClick={() => void handleTest(agent)}
                              disabled={testing === agent.id}
                            >
                              {testing === agent.id ? (
                                <CircularProgress size={18} />
                              ) : (
                                <SyncIcon fontSize="small" />
                              )}
                            </IconButton>
                          </span>
                        </Tooltip>
                        <IconButton
                          size="small"
                          onClick={() => {
                            setEditing(agent);
                            setDialogOpen(true);
                          }}
                        >
                          <EditIcon fontSize="small" />
                        </IconButton>
                        <IconButton
                          size="small"
                          color="error"
                          onClick={() => void handleDelete(agent)}
                        >
                          <DeleteIcon fontSize="small" />
                        </IconButton>
                      </>
                    )}
                  </Stack>
                </Stack>
              </Paper>
            );
          })}
        </Stack>
      )}

      <RemoteAgentDialog
        open={dialogOpen}
        agent={editing}
        onClose={() => setDialogOpen(false)}
        onSave={handleSave}
      />

      <Snackbar
        open={Boolean(toast)}
        autoHideDuration={4000}
        onClose={() => setToast(null)}
        message={toast ?? ''}
      />
    </Box>
  );
};

export default RemoteAgents;
