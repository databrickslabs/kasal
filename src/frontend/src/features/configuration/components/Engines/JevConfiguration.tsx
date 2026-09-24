import React, { useEffect, useState } from 'react';
import { Alert, FormControlLabel, Paper, Stack, Switch, Typography } from '@mui/material';
import { DecisionConfigService } from '../../../../api/config/DecisionConfigService';
import { useGroupStore } from '../../../../store/groups';
import JevRecommendation from './JevRecommendation';

// Remount on workspace changes: neither credentials nor pending responses may
// populate the next workspace's form.
const JevConfiguration: React.FC = () => {
  const groupId = useGroupStore((state) => state.currentGroupId);
  return <JevForm key={groupId ?? 'default'} />;
};

const JevForm: React.FC = () => {
  const [enabled, setEnabled] = useState(false);
  const [configured, setConfigured] = useState(false);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState('');
  const [saved, setSaved] = useState(false);

  useEffect(() => {
    let active = true;
    void DecisionConfigService.getConfig().then((config) => {
      if (!active) return;
      setEnabled(config.enabled);
      setConfigured(config.api_key_configured);
      setLoading(false);
    }).catch(() => {
      if (!active) return;
      setError('Could not load Jev settings. Reopen Configuration to retry.');
    });
    return () => { active = false; };
  }, []);

  const save = async (value: boolean) => {
    const previous = enabled;
    setEnabled(value);
    setSaving(true);
    setSaved(false);
    setError('');
    try {
      const config = await DecisionConfigService.saveConfig(value);
      setEnabled(config.enabled);
      setConfigured(config.api_key_configured);
      setSaved(true);
    } catch {
      setEnabled(previous);
      setError('Could not save Jev settings. A workspace admin and an API key are required to enable Jev.');
    } finally {
      setSaving(false);
    }
  };

  return (
    <Paper variant="outlined" sx={{ p: 2.5, borderRadius: 2 }}>
      <Stack spacing={1.5}>
        <Typography variant="subtitle1" fontWeight={600}>Jev decisions</Typography>
        <Typography variant="body2" color="text.secondary">
          Off by default. Enable Jev for supported decisions in this workspace.
          When off, Kasal uses its existing approach. Unavailable or uncertain Jev
          answers also fall back to that approach. Relevant prompts and candidate
          content are sent to Jev when enabled.
        </Typography>
        <FormControlLabel label="Use Jev for decisions" control={
          <Switch checked={enabled} disabled={loading || saving || (!enabled && !configured)}
            onChange={(_, value) => { void save(value); }} />
        } />
        <Typography variant="body2" color="text.secondary">
          {configured ? 'Uses JEV_API_KEY from Configuration → API Keys.' : 'Add JEV_API_KEY in Configuration → API Keys, then reopen this panel.'}
        </Typography>
        {error && <Alert severity="error">{error}</Alert>}
        {saved && <Alert severity="success">Jev settings saved.</Alert>}
        {saving && <Typography role="status" variant="body2">Saving Jev settings…</Typography>}
        {enabled && !loading && !saving && <JevRecommendation />}
      </Stack>
    </Paper>
  );
};

export default JevConfiguration;
