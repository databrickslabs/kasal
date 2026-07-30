import React, { useCallback, useEffect, useState } from 'react';
import {
  Alert,
  Box,
  Button,
  Chip,
  CircularProgress,
  Divider,
  FormControlLabel,
  Link,
  Switch,
  TextField,
  Typography,
} from '@mui/material';
import {
  Launch as LaunchIcon,
  Refresh as RefreshIcon,
} from '@mui/icons-material';
import { apiClient } from '../../config/api/ApiConfig';
import { useMLflowStore } from '../../store/mlflow';

/**
 * MLflow tracing settings — their own section, not a corner of the Databricks one.
 *
 * These controls used to live inside DatabricksConfiguration, which was coherent
 * while MLflow *was* Databricks. It stopped being coherent once tracing could
 * also target a local OSS server, and it was not merely untidy: the enable flag
 * was a column on the Databricks config row, so a workspace with no Databricks
 * configuration could never switch MLflow on at all. The old UI showed the same
 * seam from the other side — "Please save Databricks settings first to persist
 * MLflow" — which in a dev environment with nothing to save is a dead end.
 *
 * Memory is the precedent: it can use Databricks Vector Search or a local store,
 * and it has its own section rather than living inside the Databricks one.
 *
 * **The backend is shown, not chosen.** Which MLflow receives traces is derived
 * from what is actually configured (Databricks when a workspace is set, else a
 * local server). A dropdown here could only ever let someone select a backend
 * that is not there. Reachability is surfaced for the same reason a date-
 * awareness log line was added elsewhere in this codebase: a setting that looks
 * applied and silently does nothing is the most expensive kind.
 */

interface MLflowBackend {
  kind: 'databricks' | 'local' | 'none';
  uri?: string | null;
  reachable?: boolean | null;
  experiment?: string | null;
  url?: string | null;
}

interface MLflowSettings {
  enabled: boolean;
  evaluation_enabled: boolean;
  experiment_name?: string | null;
  backend: MLflowBackend;
}

const BACKEND_LABEL: Record<MLflowBackend['kind'], string> = {
  databricks: 'Databricks workspace',
  local: 'Local server',
  none: 'None available',
};

const MLflowConfiguration: React.FC = () => {
  const [settings, setSettings] = useState<MLflowSettings | null>(null);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [experimentDraft, setExperimentDraft] = useState('');
  // Publish the flag so the crew catalog, flow catalog and Prompts tab react
  // to a toggle immediately instead of on their next remount.
  const publishEnabled = useMLflowStore((s) => s.setEnabled);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const resp = await apiClient.get<MLflowSettings>('/mlflow/settings');
      setSettings(resp.data);
      setExperimentDraft(resp.data.experiment_name || '');
      publishEnabled(resp.data.enabled);
    } catch {
      setError('Could not load MLflow settings.');
    } finally {
      setLoading(false);
    }
  }, [publishEnabled]);

  useEffect(() => {
    void load();
  }, [load]);

  const patch = useCallback(async (body: Partial<MLflowSettings>) => {
    setSaving(true);
    setError(null);
    try {
      const resp = await apiClient.patch<MLflowSettings>('/mlflow/settings', body);
      setSettings(resp.data);
      setExperimentDraft(resp.data.experiment_name || '');
      publishEnabled(resp.data.enabled);
    } catch {
      setError('Could not save MLflow settings.');
    } finally {
      setSaving(false);
    }
  }, [publishEnabled]);

  if (loading) {
    return (
      <Box sx={{ display: 'flex', justifyContent: 'center', p: 4 }}>
        <CircularProgress size={28} />
      </Box>
    );
  }

  if (!settings) {
    return (
      <Box sx={{ p: 2 }}>
        <Alert severity="error" action={<Button onClick={() => void load()}>Retry</Button>}>
          {error || 'MLflow settings are unavailable.'}
        </Alert>
      </Box>
    );
  }

  const { backend } = settings;
  const noBackend = backend.kind === 'none';
  const localUnreachable = backend.kind === 'local' && backend.reachable === false;

  return (
    <Box sx={{ p: 2, maxWidth: 720 }}>
      <Typography variant="h6" sx={{ mb: 0.5 }}>
        MLflow
      </Typography>
      <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
        Trace every crew execution — agents, tasks, tool calls and LLM requests —
        to MLflow for inspection and evaluation.
      </Typography>

      {error && (
        <Alert severity="error" sx={{ mb: 2 }} onClose={() => setError(null)}>
          {error}
        </Alert>
      )}

      <FormControlLabel
        control={
          <Switch
            checked={settings.enabled}
            disabled={saving || noBackend}
            onChange={(e) => void patch({ enabled: e.target.checked })}
          />
        }
        label={settings.enabled ? 'Tracing enabled' : 'Tracing disabled'}
      />

      <Divider sx={{ my: 2 }} />

      {/* Derived, never chosen — see the component docstring. */}
      <Typography variant="subtitle2" sx={{ mb: 1 }}>
        Backend
      </Typography>
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, flexWrap: 'wrap', mb: 1 }}>
        <Typography variant="body2">{BACKEND_LABEL[backend.kind]}</Typography>
        {backend.uri && (
          <Typography variant="body2" color="text.secondary">
            · {backend.uri}
          </Typography>
        )}
        {backend.reachable === true && (
          <Chip size="small" color="success" variant="outlined" label="reachable" />
        )}
        {backend.reachable === false && (
          <Chip size="small" color="warning" variant="outlined" label="not reachable" />
        )}
        {backend.url && (
          <Link
            href={backend.url}
            target="_blank"
            rel="noopener"
            sx={{ display: 'inline-flex', alignItems: 'center', gap: 0.5, fontSize: '0.8rem' }}
          >
            Open MLflow <LaunchIcon sx={{ fontSize: 14 }} />
          </Link>
        )}
        <Button size="small" startIcon={<RefreshIcon />} onClick={() => void load()}>
          Recheck
        </Button>
      </Box>

      {backend.kind === 'local' && (
        <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mb: 1 }}>
          Databricks is not configured, so traces go to your local MLflow server.
          Configure a Databricks workspace to send them there instead.
        </Typography>
      )}

      {localUnreachable && (
        <Alert severity="warning" sx={{ mb: 2 }}>
          Nothing is answering at {backend.uri}. Tracing stays off until the server
          is running — runs are never blocked by it.
        </Alert>
      )}

      {noBackend && (
        <Alert severity="info" sx={{ mb: 2 }}>
          No MLflow backend is available. Configure a Databricks workspace, or start
          a local MLflow server and set <code>MLFLOW_TRACKING_URI</code> before
          launching Kasal.
        </Alert>
      )}

      <Divider sx={{ my: 2 }} />

      <Typography variant="subtitle2" sx={{ mb: 1 }}>
        Experiment
      </Typography>
      <Box sx={{ display: 'flex', gap: 1, alignItems: 'flex-start' }}>
        <TextField
          size="small"
          fullWidth
          value={experimentDraft}
          disabled={saving || noBackend}
          onChange={(e) => setExperimentDraft(e.target.value)}
          // The placeholder is what an empty field WILL use, so it has to be
          // the derived name — a hardcoded one contradicts the helper text
          // directly below it.
          placeholder={backend.experiment || undefined}
          helperText={
            backend.experiment
              ? `Traces are written to "${backend.experiment}".`
              : 'Leave blank for the default.'
          }
        />
        <Button
          variant="outlined"
          size="small"
          sx={{ mt: 0.25 }}
          disabled={saving || experimentDraft === (settings.experiment_name || '')}
          onClick={() => void patch({ experiment_name: experimentDraft })}
        >
          Save
        </Button>
      </Box>

      <Divider sx={{ my: 2 }} />

      <FormControlLabel
        control={
          <Switch
            checked={settings.evaluation_enabled}
            disabled={saving || noBackend || !settings.enabled}
            onChange={(e) => void patch({ evaluation_enabled: e.target.checked })}
          />
        }
        label="Run LLM-judge evaluation on finished runs"
      />
      <Typography variant="caption" color="text.secondary" sx={{ display: 'block' }}>
        A separate, more expensive opt-in than tracing: each evaluated run costs an
        extra model call. Requires tracing to be on.
      </Typography>
    </Box>
  );
};

export default MLflowConfiguration;
