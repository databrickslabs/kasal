import React, { useEffect, useState } from 'react';
import {
  Alert,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  FormControl,
  FormControlLabel,
  InputLabel,
  MenuItem,
  Select,
  Stack,
  Switch,
  TextField,
  Typography,
} from '@mui/material';
import { A2AAgent, A2AAgentInput } from '../../../api/tools/A2AAgentService';

interface Props {
  open: boolean;
  agent: A2AAgent | null;
  onClose: () => void;
  onSave: (input: A2AAgentInput) => Promise<void>;
}

const EMPTY: A2AAgentInput = {
  name: '',
  card_url: '',
  description: '',
  auth_type: 'obo',
  enabled: true,
  global_enabled: false,
  timeout_seconds: 300,
};

/**
 * Add or edit a remote A2A agent.
 *
 * The API key field is write-only in both directions: the backend never returns
 * a stored key, so an empty field on an existing agent means "leave it alone",
 * not "clear it". Clearing is an explicit action, which is why there is a button
 * for it rather than a subtlety of an empty string.
 */
const RemoteAgentDialog: React.FC<Props> = ({ open, agent, onClose, onSave }) => {
  const [form, setForm] = useState<A2AAgentInput>(EMPTY);
  const [apiKey, setApiKey] = useState('');
  const [clearKey, setClearKey] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!open) return;
    setApiKey('');
    setClearKey(false);
    setError(null);
    setForm(
      agent
        ? {
            name: agent.name,
            card_url: agent.card_url,
            description: agent.description ?? '',
            auth_type: agent.auth_type,
            enabled: agent.enabled,
            global_enabled: agent.global_enabled,
            timeout_seconds: agent.timeout_seconds,
          }
        : EMPTY,
    );
  }, [open, agent]);

  const set = <K extends keyof A2AAgentInput>(key: K, value: A2AAgentInput[K]) =>
    setForm((prev) => ({ ...prev, [key]: value }));

  const handleSave = async () => {
    if (!form.name.trim() || !form.card_url.trim()) {
      setError('A name and a URL are required.');
      return;
    }
    setSaving(true);
    setError(null);
    try {
      const payload: A2AAgentInput = { ...form };
      if (clearKey) payload.api_key = '';
      else if (apiKey) payload.api_key = apiKey;
      await onSave(payload);
      onClose();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Could not save the agent.');
    } finally {
      setSaving(false);
    }
  };

  return (
    <Dialog open={open} onClose={onClose} maxWidth="sm" fullWidth>
      <DialogTitle>{agent ? `Edit ${agent.name}` : 'Add a remote agent'}</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ mt: 1 }}>
          {error && <Alert severity="error">{error}</Alert>}

          <TextField
            label="Name"
            value={form.name}
            onChange={(e) => set('name', e.target.value)}
            fullWidth
            size="small"
            helperText="What your agents will call this remote."
          />

          <TextField
            label="Agent URL"
            value={form.card_url}
            onChange={(e) => set('card_url', e.target.value)}
            fullWidth
            size="small"
            placeholder="https://agent.example.com"
            helperText="The agent's address, or its Agent Card URL. Either works."
          />

          <TextField
            label="Description"
            value={form.description ?? ''}
            onChange={(e) => set('description', e.target.value)}
            fullWidth
            size="small"
            multiline
            minRows={2}
            helperText="Left blank, the remote's own description is used."
          />

          <FormControl fullWidth size="small">
            <InputLabel>Authentication</InputLabel>
            <Select
              label="Authentication"
              value={form.auth_type ?? 'obo'}
              onChange={(e) =>
                set('auth_type', e.target.value as A2AAgentInput['auth_type'])
              }
            >
              <MenuItem value="obo">
                Forward the user&apos;s token (on-behalf-of)
              </MenuItem>
              <MenuItem value="api_key">API key</MenuItem>
              <MenuItem value="none">None</MenuItem>
            </Select>
          </FormControl>

          {form.auth_type === 'api_key' && (
            <>
              <TextField
                label={agent?.has_api_key ? 'Replace API key' : 'API key'}
                value={apiKey}
                onChange={(e) => {
                  setApiKey(e.target.value);
                  setClearKey(false);
                }}
                type="password"
                fullWidth
                size="small"
                helperText={
                  agent?.has_api_key
                    ? 'A key is stored. Leave blank to keep it.'
                    : 'Stored encrypted and never shown again.'
                }
              />
              {agent?.has_api_key && (
                <FormControlLabel
                  control={
                    <Switch
                      checked={clearKey}
                      onChange={(e) => {
                        setClearKey(e.target.checked);
                        if (e.target.checked) setApiKey('');
                      }}
                    />
                  }
                  label="Remove the stored key"
                />
              )}
            </>
          )}

          <TextField
            label="Timeout (seconds)"
            type="number"
            value={form.timeout_seconds ?? 300}
            onChange={(e) => set('timeout_seconds', Number(e.target.value))}
            size="small"
            helperText="How long a delegating agent waits before giving up on an answer. The remote keeps working."
          />

          <FormControlLabel
            control={
              <Switch
                checked={form.enabled ?? true}
                onChange={(e) => set('enabled', e.target.checked)}
              />
            }
            label="Enabled"
          />

          <FormControlLabel
            control={
              <Switch
                checked={form.global_enabled ?? false}
                onChange={(e) => set('global_enabled', e.target.checked)}
              />
            }
            label="Available to every agent without being selected"
          />

          <Typography variant="caption" color="text.secondary">
            The card is fetched as soon as you save, so a wrong URL shows up here
            rather than at run time.
          </Typography>
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Cancel</Button>
        <Button onClick={handleSave} variant="contained" disabled={saving}>
          {saving ? 'Saving…' : 'Save'}
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default RemoteAgentDialog;
