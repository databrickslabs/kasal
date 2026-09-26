import React, { useEffect, useState } from 'react';
import {
  Accordion,
  AccordionDetails,
  AccordionSummary,
  Alert,
  Box,
  Button,
  Paper,
  Stack,
  TextField,
  Typography,
} from '@mui/material';
import { ExpandMore as ExpandMoreIcon } from '@mui/icons-material';
import { EngineConfigService } from '../../../../api/config/EngineConfigService';
import type { EngineSettings, EngineSettingsPatch } from '../../../../types/config/engines';
import AdvancedNumberField from '../AdvancedNumberField';

const MODE_LABELS: Record<string, string> = { deep: 'Deep research' };

const BUDGET_FIELDS: Record<string, { label: string; helper: string; max: number }> = {
  max_iter: { label: 'Tool rounds per agent call', helper: 'Tool-calling rounds in one agent call.', max: 500 },
  max_execution_time: { label: 'Seconds per agent call', helper: 'Wall clock for one agent call.', max: 86400 },
  run_wall_clock: { label: 'Seconds per run', helper: 'Wall clock for the whole run, retries included.', max: 86400 },
  guardrail_max_retries: { label: 'Guardrail retries', helper: 'Times a rejected task is retried.', max: 20 },
};

/**
 * System-wide engine settings: the Jev API URL and, under Advanced, the agent
 * time limit and run budgets. These replaced the JEV_API_BASE,
 * KASAL_AGENT_MAX_EXECUTION_TIME and KASAL_BUDGET_<MODE>_<FIELD> environment
 * variables, which a Databricks App never sets. Hidden unless the API lets the
 * viewer read them (system administrators).
 */
const EngineSystemSettings: React.FC = () => {
  const [settings, setSettings] = useState<EngineSettings | null>(null);
  const [jevDraft, setJevDraft] = useState('');
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState('');

  useEffect(() => {
    let active = true;
    EngineConfigService.getSettings()
      .then((loaded) => {
        if (!active) return;
        setSettings(loaded);
        setJevDraft(loaded.jev_api_base ?? '');
      })
      .catch(() => {
        // Not a system administrator (403) or unavailable: show nothing.
      });
    return () => {
      active = false;
    };
  }, []);

  if (!settings) return null;

  const patch = async (body: EngineSettingsPatch) => {
    setSaving(true);
    setError('');
    try {
      const saved = await EngineConfigService.updateSettings(body);
      setSettings(saved);
      setJevDraft(saved.jev_api_base ?? '');
    } catch (err) {
      const detail = (err as { response?: { data?: { detail?: string } } }).response?.data?.detail;
      setError(detail || 'Could not save the engine settings.');
    } finally {
      setSaving(false);
    }
  };

  const jevTrimmed = jevDraft.trim();
  const jevInvalid = jevTrimmed !== '' && !jevTrimmed.startsWith('https://');
  const jevChanged = jevTrimmed !== (settings.jev_api_base ?? '');

  return (
    <Paper variant="outlined" sx={{ p: 2.5, borderRadius: 2 }}>
      <Typography variant="subtitle1" fontWeight={600}>
        System settings
      </Typography>
      <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mb: 2 }}>
        Apply to every workspace. Changes apply to runs started after saving.
      </Typography>

      {error && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {error}
        </Alert>
      )}

      <Stack direction="row" spacing={1} alignItems="flex-start" sx={{ mb: 2 }}>
        <TextField
          size="small"
          label="Jev API URL"
          value={jevDraft}
          onChange={(e) => setJevDraft(e.target.value)}
          placeholder="https://jev.example.com"
          error={jevInvalid}
          helperText={
            jevInvalid
              ? 'Must start with https://'
              : 'Where the Jev decisions API lives. Empty keeps Jev off for every workspace.'
          }
          inputProps={{ 'aria-label': 'Jev API URL' }}
          sx={{ flex: 1, maxWidth: 480 }}
        />
        <Button
          size="small"
          variant="outlined"
          disabled={saving || jevInvalid || !jevChanged}
          onClick={() => void patch({ jev_api_base: jevTrimmed || null })}
          sx={{ mt: 0.5 }}
        >
          Save
        </Button>
      </Stack>

      <Accordion disableGutters variant="outlined">
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="subtitle2">Advanced</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <AdvancedNumberField
            label="Agent time limit (seconds)"
            helper={`One agent call, when the agent sets none. 0 turns it off. Default ${settings.agent_max_execution_time_default}.`}
            value={settings.agent_max_execution_time}
            min={0}
            max={86400}
            saving={saving}
            onSave={(value) => void patch({ agent_max_execution_time: value })}
          />
          {Object.entries(settings.budgets).map(([mode, values]) => (
            <Box key={mode} sx={{ mt: 1 }}>
              <Typography variant="subtitle2" sx={{ mb: 1.5 }}>
                Run budget: {MODE_LABELS[mode] ?? mode}
              </Typography>
              {Object.entries(values).map(([field, value]) => {
                const meta = BUDGET_FIELDS[field] ?? { label: field, helper: '', max: 86400 };
                const fallback = settings.budget_defaults[mode]?.[field];
                return (
                  <AdvancedNumberField
                    key={field}
                    label={meta.label}
                    helper={`${meta.helper} Default ${fallback}.`}
                    value={value}
                    min={1}
                    max={meta.max}
                    saving={saving}
                    onSave={(next) => void patch({ budgets: { [mode]: { [field]: next } } })}
                  />
                );
              })}
            </Box>
          ))}
        </AccordionDetails>
      </Accordion>
    </Paper>
  );
};

export default EngineSystemSettings;
