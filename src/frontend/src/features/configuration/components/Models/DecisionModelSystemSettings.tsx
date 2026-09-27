import React, { useEffect, useState } from 'react';
import { Alert, Button, Paper, Stack, TextField, Typography } from '@mui/material';
import { useTranslation } from 'react-i18next';
import { EngineConfigService } from '../../../../api/config/EngineConfigService';
import { DECISION_MODEL_PROVIDER } from './decisionModelProvider';

const K = 'configuration.models.decisionModel';

/**
 * System administration → Models: where the decision model's provider (Jev)
 * lives for this deployment. Each workspace still opts in on its own
 * (Workspace settings → Models) with its own provider key; an empty URL keeps
 * the decision model off everywhere.
 *
 * The value is the `jev_api_base` system setting (it replaced the JEV_API_BASE
 * environment variable), read and written through the same engine-settings
 * API as before. Hidden unless the API lets the viewer read it (system admins).
 */
const DecisionModelSystemSettings: React.FC = () => {
  const { t } = useTranslation();
  const provider = DECISION_MODEL_PROVIDER;
  const [stored, setStored] = useState<string | null | undefined>(undefined);
  const [draft, setDraft] = useState('');
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState('');
  const [saved, setSaved] = useState(false);

  useEffect(() => {
    let active = true;
    EngineConfigService.getSettings()
      .then((loaded) => {
        if (!active) return;
        setStored(loaded.jev_api_base ?? null);
        setDraft(loaded.jev_api_base ?? '');
      })
      .catch(() => {
        // Not a system administrator (403) or unavailable: show nothing.
      });
    return () => {
      active = false;
    };
  }, []);

  if (stored === undefined) return null;

  const trimmed = draft.trim();
  const invalid = trimmed !== '' && !trimmed.startsWith('https://');
  const changed = trimmed !== (stored ?? '');
  const urlLabel = t(`${K}.urlLabel`, { defaultValue: '{{name}} API URL', name: provider.name });

  const save = async () => {
    setSaving(true);
    setError('');
    setSaved(false);
    try {
      const result = await EngineConfigService.updateSettings({ jev_api_base: trimmed || null });
      setStored(result.jev_api_base ?? null);
      setDraft(result.jev_api_base ?? '');
      setSaved(true);
    } catch (err) {
      const detail = (err as { response?: { data?: { detail?: string } } }).response?.data?.detail;
      setError(detail || t(`${K}.urlSaveError`, { defaultValue: 'Could not save the {{label}}.', label: urlLabel }));
    } finally {
      setSaving(false);
    }
  };

  return (
    <Paper variant="outlined" sx={{ p: 2.5, borderRadius: 2, mt: 3 }}>
      <Typography variant="subtitle1" fontWeight={600}>
        {t(`${K}.title`, { defaultValue: 'Decision model' })}
      </Typography>
      <Typography variant="body2" color="text.secondary">
        {t(`${K}.provider`, { defaultValue: 'Provider: {{name}}', name: provider.name })}
      </Typography>
      <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mb: 2 }}>
        {t(`${K}.systemCaption`, {
          defaultValue: 'Applies to every workspace. Each workspace admin still turns the decision model on '
            + "for their workspace in its Models settings, with the workspace's own {{key}}.",
          key: provider.apiKeyName,
        })}
      </Typography>
      {error && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {error}
        </Alert>
      )}
      {saved && (
        <Alert severity="success" sx={{ mb: 2 }}>
          {t(`${K}.urlSaved`, { defaultValue: '{{label}} saved.', label: urlLabel })}
        </Alert>
      )}
      <Stack direction="row" spacing={1} alignItems="flex-start">
        <TextField
          size="small"
          label={urlLabel}
          value={draft}
          onChange={(e) => {
            setDraft(e.target.value);
            setSaved(false);
          }}
          placeholder="https://jev.example.com"
          error={invalid}
          helperText={
            invalid
              ? t(`${K}.urlHttps`, { defaultValue: 'Must start with https://' })
              : t(`${K}.urlHelp`, {
                defaultValue: 'Where the {{name}} decisions API lives. Empty keeps the decision model off for every workspace.',
                name: provider.name,
              })
          }
          inputProps={{ 'aria-label': urlLabel }}
          sx={{ flex: 1, maxWidth: 480 }}
        />
        <Button
          size="small"
          variant="outlined"
          disabled={saving || invalid || !changed}
          onClick={() => void save()}
          sx={{ mt: 0.5 }}
        >
          {t(`${K}.save`, { defaultValue: 'Save' })}
        </Button>
      </Stack>
    </Paper>
  );
};

export default DecisionModelSystemSettings;
