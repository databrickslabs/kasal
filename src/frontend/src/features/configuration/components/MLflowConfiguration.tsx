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
import { apiClient } from '../../../shared/api/client';
import { useMLflowStore } from '../../../store/mlflow';
import type { MLflowBackend, MLflowSettings } from '../../../types/config/mlflow';

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
 * Memory is the precedent: it can use Lakebase or a local store,
 * and it has its own section rather than living inside the Databricks one.
 *
 * **The backend is shown, not chosen.** Which MLflow receives traces is derived
 * from what is actually configured (Databricks when a workspace is set, else a
 * local server). A dropdown here could only ever let someone select a backend
 * that is not there. Every candidate backend the environment offers is listed
 * (Databricks / Local / None) with its own reachability, and the ACTIVE one is
 * marked — so it is clear both what is available and which one a run will use.
 * Reachability is surfaced for the same reason a date-awareness log line was
 * added elsewhere in this codebase: a setting that looks applied and silently
 * does nothing is the most expensive kind.
 */

const BACKEND_LABEL: Record<MLflowBackend['kind'], string> = {
  databricks: 'Databricks workspace',
  local: 'Local server',
  none: 'None available',
};

// The three possible backends, shown as a color-coded status chip so which one
// is active reads at a glance: Databricks (blue), local server (green), none
// (grey). Still DERIVED, not chosen — see the component docstring.
const BACKEND_CHIP_COLOR: Record<
  MLflowBackend['kind'],
  'primary' | 'success' | 'default'
> = {
  databricks: 'primary',
  local: 'success',
  none: 'default',
};

const MLflowConfiguration: React.FC = () => {
  const [settings, setSettings] = useState<MLflowSettings | null>(null);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [openingExperiment, setOpeningExperiment] = useState(false);
  const [experimentLink, setExperimentLink] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [savedMsg, setSavedMsg] = useState<string | null>(null);
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
    setSavedMsg(null);
    try {
      const resp = await apiClient.patch<MLflowSettings>('/mlflow/settings', body);
      setSettings(resp.data);
      setExperimentDraft(resp.data.experiment_name || '');
      publishEnabled(resp.data.enabled);
      // Saving the experiment name OR enabling tracing creates the experiment on
      // Databricks (backend). Tell the admin to attach it to the app as an MLflow
      // resource — that is what grants the app's service principal MLflow access.
      // Shown on enable too (not just rename): re-enabling recreates it, and the
      // admin needs the same attach-then-redeploy prompt without renaming.
      const exp = resp.data.backend?.experiment;
      const created =
        'experiment_name' in body || body.enabled === true;
      if (created && !resp.data.installation_managed && resp.data.backend?.kind === 'databricks' && exp) {
        setSavedMsg(
          `Experiment "${exp}" is ready. In the Databricks App settings, add it as ` +
            `an MLflow experiment resource (Permission: Can Edit) so the app can ` +
            `write traces and register prompts, then redeploy the app.`,
        );
      }
    } catch {
      setError('Could not save MLflow settings.');
    } finally {
      setSaving(false);
    }
  }, [publishEnabled]);

  const openExperiment = async (selectedBackend: MLflowBackend) => {
    if (!selectedBackend.uri) return;
    // Reserve the tab during the click so the async lookup is not popup-blocked.
    const tab = window.open('about:blank', '_blank');
    if (tab) tab.opener = null;
    setOpeningExperiment(true);
    setExperimentLink(null);
    setError(null);
    try {
      // The backend resolves the current team's actual tracing destination and
      // initializes it with UC storage if this is the team's first visit.
      const { data } = await apiClient.get<{ experiment_id: string }>('/mlflow/experiment-info');
      if (!data.experiment_id) throw new Error('Experiment unavailable');
      const url = `${selectedBackend.uri.replace(/\/$/, '')}/ml/experiments/${encodeURIComponent(data.experiment_id)}/traces`;
      if (tab && !tab.closed) tab.location.replace(url);
      else setExperimentLink(url);
    } catch {
      tab?.close();
      setError('Could not open the tracing experiment. Check the app’s experiment and Unity Catalog permissions, then try again.');
    } finally {
      setOpeningExperiment(false);
    }
  };

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
      <Typography data-settings-page-title variant="h6" sx={{ mb: 0.5 }}>
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

      {settings.installation_managed && (
        <Alert severity={settings.resource_error ? 'warning' : 'info'} sx={{ mb: 2 }}>
          {settings.resource_error || 'Tracing is enabled by default on Databricks Apps. Personal spaces and teamspaces use separate trace destinations, provisioned automatically.'}
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

      {/* Derived, never chosen — see the component docstring. All candidate
          backends are listed so Databricks / Local / None read side by side;
          the ACTIVE one (the row whose kind === backend.kind) is marked. */}
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
        <Typography variant="subtitle2">Backend</Typography>
        <Button size="small" startIcon={<RefreshIcon />} onClick={() => void load()}>
          Recheck
        </Button>
      </Box>
      {(settings.available && settings.available.length > 0
        ? settings.available
        : [backend]
      ).map((b) => {
        const isActive = b.kind === backend.kind && backend.kind !== 'none';
        const isAvailable = b.available !== false;
        return (
          <Box
            key={b.kind}
            sx={{
              display: 'flex',
              alignItems: 'center',
              gap: 1,
              flexWrap: 'wrap',
              mb: 0.75,
              // Dim backends that are not usable right now so the active/available
              // ones stand out, but keep them visible so all options are known.
              opacity: isAvailable ? 1 : 0.5,
            }}
          >
            <Chip
              size="small"
              color={BACKEND_CHIP_COLOR[b.kind]}
              variant={isActive ? 'filled' : 'outlined'}
              label={BACKEND_LABEL[b.kind]}
            />
            {isActive && (
              <Chip size="small" color="info" variant="outlined" label="active" />
            )}
            {!isAvailable && (
              <Chip size="small" variant="outlined" label="not configured" />
            )}
            {b.uri && (
              <Typography variant="body2" color="text.secondary">
                {b.uri}
              </Typography>
            )}
            {b.reachable === true && (
              <Chip size="small" color="success" variant="outlined" label="reachable" />
            )}
            {b.reachable === false && (
              <Chip size="small" color="warning" variant="outlined" label="not reachable" />
            )}
            {b.kind === 'databricks' && isAvailable && b.uri ? (
              <Button
                size="small"
                variant="text"
                disabled={openingExperiment || !!settings.resource_error}
                onClick={() => void openExperiment(b)}
                endIcon={openingExperiment ? <CircularProgress size={14} /> : <LaunchIcon sx={{ fontSize: 14 }} />}
                sx={{ fontSize: '0.8rem' }}
              >
                {openingExperiment ? 'Opening experiment…' : 'Open MLflow'}
              </Button>
            ) : b.url && (
              <Link
                href={b.url}
                target="_blank"
                rel="noopener"
                sx={{ display: 'inline-flex', alignItems: 'center', gap: 0.5, fontSize: '0.8rem' }}
              >
                Open MLflow <LaunchIcon sx={{ fontSize: 14 }} />
              </Link>
            )}
          </Box>
        );
      })}
      {experimentLink && (
        <Link href={experimentLink} target="_blank" rel="noopener noreferrer">
          Open tracing experiment
        </Link>
      )}
      {/* Both backends are always listed above (unavailable ones greyed +
          "not configured"). Explain the resolution so it is not a mystery. */}
      {backend.kind === 'databricks' && (
        <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mt: 0.5, mb: 1 }}>
          Databricks wins whenever a workspace is configured. To trace to a local
          server instead, clear the Databricks workspace URL.
        </Typography>
      )}

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
          disabled={saving || noBackend || settings.installation_managed}
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
          disabled={saving || settings.installation_managed || experimentDraft === (settings.experiment_name || '')}
          onClick={() => void patch({ experiment_name: experimentDraft })}
        >
          Save
        </Button>
      </Box>

      {savedMsg && (
        <Alert severity="success" sx={{ mt: 1.5 }} onClose={() => setSavedMsg(null)}>
          {savedMsg}
        </Alert>
      )}

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
