import React, { useCallback, useEffect, useState } from 'react';
import {
  Alert,
  Box,
  Button,
  Chip,
  CircularProgress,
  IconButton,
  Paper,
  Snackbar,
  Stack,
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

/**
 * Remote agents — the outbound half of A2A.
 *
 * MCP servers give an agent TOOLS; a remote agent gives it a COLLEAGUE. They sit
 * beside each other in Configuration for that reason, and this page deliberately
 * looks like the MCP one: same shape, same actions, so an operator who has
 * attached one knows how to attach the other.
 */
const RemoteAgents: React.FC = () => {
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
      setAgents(await A2AAgentService.list());
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Could not load remote agents.');
    } finally {
      setLoading(false);
    }
  }, []);

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
    setToast(`${agent.name} removed.`);
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
        alignItems="center"
        sx={{ mb: 2 }}
      >
        <Box>
          <Typography variant="h6">Remote Agents</Typography>
          <Typography variant="body2" color="text.secondary">
            Agents outside Kasal that your crews can delegate to over A2A. Give an
            agent the <strong>Remote Agent</strong> tool to let it use these.
          </Typography>
        </Box>
        <Button
          startIcon={<AddIcon />}
          variant="contained"
          onClick={() => {
            setEditing(null);
            setDialogOpen(true);
          }}
        >
          Add agent
        </Button>
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
            No remote agents yet. Add one with its URL — Kasal reads its Agent Card
            to discover what it can do.
          </Typography>
        </Paper>
      ) : (
        <Stack spacing={1.5}>
          {agents.map((agent) => (
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
                    {!agent.enabled && <Chip size="small" label="Disabled" />}
                    {agent.global_enabled && (
                      <Chip size="small" color="primary" label="All agents" />
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

                <Stack direction="row" spacing={0.5}>
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
                </Stack>
              </Stack>
            </Paper>
          ))}
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
