import React, { useEffect, useState } from 'react';
import { Alert, Button, FormControlLabel, Paper, Stack, Switch, Typography } from '@mui/material';
import { useTranslation } from 'react-i18next';
import { DecisionConfigService } from '../../../../api/config/DecisionConfigService';
import { useGroupStore } from '../../../../store/groups';
import DecisionModelRecommendation from './DecisionModelRecommendation';
import { DECISION_MODEL_PROVIDER } from './decisionModelProvider';

const K = 'configuration.models.decisionModel';

/** A failed load or save, with the server's reason when it gave one. */
interface Failure {
  kind: 'load' | 'save';
  detail?: string;
}

/** The server's reason (a 400/403/409 `detail`), or undefined when it gave none. */
function serverDetail(err: unknown): string | undefined {
  const detail = (err as { response?: { data?: { detail?: unknown } } })?.response?.data?.detail;
  return typeof detail === 'string' && detail.trim() ? detail : undefined;
}

function openApiKeys() {
  window.dispatchEvent(new CustomEvent('kasal:navigate-config', { detail: { section: 'api-keys' } }));
}

/**
 * Workspace settings → Models: this workspace's decision-model opt-in. The key
 * is the workspace's provider key from API Keys; the provider endpoint is the
 * deployment's (System administration → Models). Only workspace admins may
 * change it — the API enforces that, and the Models section is shown to
 * workspace admins only.
 *
 * Remount on workspace changes: neither credentials nor pending responses may
 * populate the next workspace's form.
 */
const DecisionModelConfiguration: React.FC = () => {
  const groupId = useGroupStore((state) => state.currentGroupId);
  return <DecisionModelForm key={groupId ?? 'default'} />;
};

const DecisionModelForm: React.FC = () => {
  const { t } = useTranslation();
  const provider = DECISION_MODEL_PROVIDER;
  const [enabled, setEnabled] = useState(false);
  const [configured, setConfigured] = useState(false);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [failure, setFailure] = useState<Failure | null>(null);
  const [saved, setSaved] = useState(false);

  useEffect(() => {
    let active = true;
    void DecisionConfigService.getConfig().then((config) => {
      if (!active) return;
      setEnabled(config.enabled);
      setConfigured(config.api_key_configured);
      setLoading(false);
    }).catch((err: unknown) => {
      if (!active) return;
      setFailure({ kind: 'load', detail: serverDetail(err) });
    });
    return () => { active = false; };
  }, []);

  const save = async (value: boolean) => {
    const previous = enabled;
    setEnabled(value);
    setSaving(true);
    setSaved(false);
    setFailure(null);
    try {
      const config = await DecisionConfigService.saveConfig(value);
      setEnabled(config.enabled);
      setConfigured(config.api_key_configured);
      setSaved(true);
    } catch (err) {
      setEnabled(previous);
      setFailure({ kind: 'save', detail: serverDetail(err) });
    } finally {
      setSaving(false);
    }
  };

  const error = failure && (failure.detail ?? (failure.kind === 'load'
    ? t(`${K}.loadError`, { defaultValue: 'Could not load decision model settings. Reopen Configuration to retry.' })
    : t(`${K}.saveError`, {
      defaultValue: 'Could not save decision model settings. A workspace admin and an API key are required to enable the decision model.',
    })));

  return (
    <Paper variant="outlined" sx={{ p: 2.5, borderRadius: 2, mt: 3 }}>
      <Stack spacing={1.5}>
        <Typography variant="subtitle1" fontWeight={600}>
          {t(`${K}.title`, { defaultValue: 'Decision model' })}
        </Typography>
        <Typography variant="body2" color="text.secondary">
          {t(`${K}.provider`, { defaultValue: 'Provider: {{name}}', name: provider.name })}
        </Typography>
        <Typography variant="body2" color="text.secondary">
          {t(`${K}.disclosure`, {
            defaultValue: 'Off by default. Enable a decision model for supported decisions in this workspace. '
              + 'When off, Kasal uses its existing approach. Unavailable or uncertain decision model answers '
              + 'also fall back to that approach. Relevant prompts and candidate content are sent to the '
              + 'decision model provider when enabled.',
          })}
        </Typography>
        <FormControlLabel label={t(`${K}.toggle`, { defaultValue: 'Use a decision model' })} control={
          <Switch checked={enabled} disabled={loading || saving || (!enabled && !configured)}
            onChange={(_, value) => { void save(value); }} />
        } />
        <Stack direction="row" spacing={1} alignItems="center" flexWrap="wrap">
          <Typography variant="body2" color="text.secondary">
            {configured
              ? t(`${K}.keyConfigured`, { defaultValue: 'Uses {{key}} from Configuration → API Keys.', key: provider.apiKeyName })
              : t(`${K}.keyMissing`, { defaultValue: 'Add {{key}} in Configuration → API Keys, then reopen this panel.', key: provider.apiKeyName })}
          </Typography>
          {!configured && !loading && (
            <Button size="small" onClick={openApiKeys}>
              {t(`${K}.openApiKeys`, { defaultValue: 'Open API Keys' })}
            </Button>
          )}
        </Stack>
        {error && <Alert severity="error">{error}</Alert>}
        {saved && <Alert severity="success">{t(`${K}.saved`, { defaultValue: 'Decision model settings saved.' })}</Alert>}
        {saving && (
          <Typography role="status" variant="body2">
            {t(`${K}.saving`, { defaultValue: 'Saving decision model settings…' })}
          </Typography>
        )}
        {enabled && !loading && !saving && <DecisionModelRecommendation />}
      </Stack>
    </Paper>
  );
};

export default DecisionModelConfiguration;
