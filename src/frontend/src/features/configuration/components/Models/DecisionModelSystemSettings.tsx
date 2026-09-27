import React, { useEffect, useState } from 'react';
import {
  Alert,
  Button,
  FormControlLabel,
  Paper,
  Radio,
  RadioGroup,
  Stack,
  TextField,
  Typography,
} from '@mui/material';
import { useTranslation } from 'react-i18next';
import { EngineConfigService } from '../../../../api/config/EngineConfigService';
import type { EngineSettings } from '../../../../types/config/engines';
import {
  DECISION_CONNECTIONS,
  decisionConnection,
  type DecisionConnectionId,
} from './decisionModelProvider';
import { notifyModelsChanged } from '../../../../store/models';

const K = 'configuration.models.decisionModel';

/** The form: the chosen connection and each connection's URL. */
interface Form {
  connection: DecisionConnectionId;
  jev: string;
  openrouter: string;
}

function formOf(settings: EngineSettings): Form {
  return {
    connection: decisionConnection(settings.decision_connection).id,
    jev: settings.jev_api_base ?? '',
    openrouter: settings.openrouter_api_base ?? '',
  };
}

const HTTP_URL = /^https?:\/\/[^\s/]+/;

/**
 * System administration → Models: how this deployment reaches its decision
 * model — the Jev API or OpenRouter, each with its own URL. Each workspace
 * still opts in on its own (Workspace settings → Models) with the key its
 * connection needs; an empty Jev API URL keeps the Jev connection off.
 *
 * The values are the `decision_connection`, `jev_api_base` and
 * `openrouter_api_base` system settings, read and written through the
 * engine-settings API. Hidden unless the API lets the viewer read them
 * (system admins).
 */
const DecisionModelSystemSettings: React.FC = () => {
  const { t } = useTranslation();
  const [stored, setStored] = useState<Form | undefined>(undefined);
  const [draft, setDraft] = useState<Form>({ connection: 'jev', jev: '', openrouter: '' });
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState('');
  const [saved, setSaved] = useState(false);

  useEffect(() => {
    let active = true;
    EngineConfigService.getSettings()
      .then((loaded) => {
        if (!active) return;
        setStored(formOf(loaded));
        setDraft(formOf(loaded));
      })
      .catch(() => {
        // Not a system administrator (403) or unavailable: show nothing.
      });
    return () => {
      active = false;
    };
  }, []);

  if (stored === undefined) return null;

  const connection = DECISION_CONNECTIONS[draft.connection];
  const url = draft[draft.connection].trim();
  const invalid = [draft.jev, draft.openrouter].some((v) => v.trim() !== '' && !HTTP_URL.test(v.trim()));
  const unencrypted = !invalid && url.startsWith('http://');
  const changed = draft.connection !== stored.connection
    || draft.jev.trim() !== stored.jev
    || draft.openrouter.trim() !== stored.openrouter;
  const urlLabel = t(`${K}.urlLabel`, { defaultValue: '{{name}} API URL', name: connection.urlName });

  const edit = (patch: Partial<Form>) => {
    setDraft((prev) => ({ ...prev, ...patch }));
    setSaved(false);
  };

  const save = async () => {
    setSaving(true);
    setError('');
    setSaved(false);
    try {
      const result = await EngineConfigService.updateSettings({
        decision_connection: draft.connection,
        jev_api_base: draft.jev.trim() || null,
        openrouter_api_base: draft.openrouter.trim() || null,
      });
      setStored(formOf(result));
      setDraft(formOf(result));
      setSaved(true);
      // The connection gates Auto everywhere: open chats and other tabs follow it.
      void notifyModelsChanged();
    } catch (err) {
      const detail = (err as { response?: { data?: { detail?: string } } }).response?.data?.detail;
      setError(detail || t(`${K}.systemSaveError`, { defaultValue: 'Could not save the decision model connection.' }));
    } finally {
      setSaving(false);
    }
  };

  const explanation = {
    jev: t(`${K}.connectionJevHelp`, { defaultValue: 'Jev chooses among your enabled models.' }),
    openrouter: t(`${K}.connectionOpenRouterHelp`, {
      defaultValue: "Requests go to Jev Router, which picks from OpenRouter's models; "
        + 'answers and prompts go through OpenRouter.',
    }),
  };

  return (
    <Paper variant="outlined" sx={{ p: 2.5, borderRadius: 2, mt: 3 }}>
      <Typography variant="subtitle1" fontWeight={600}>
        {t(`${K}.title`, { defaultValue: 'Decision model' })}
      </Typography>
      <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mb: 2 }}>
        {t(`${K}.systemCaption`, {
          defaultValue: 'Applies to every workspace. Each workspace admin still turns the decision model on '
            + "for their workspace in its Models settings, with the workspace's own {{key}}.",
          key: connection.apiKeyName,
        })}
      </Typography>
      {error && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {error}
        </Alert>
      )}
      {saved && (
        <Alert severity="success" sx={{ mb: 2 }}>
          {t(`${K}.systemSaved`, { defaultValue: 'Decision model connection saved.' })}
        </Alert>
      )}
      <RadioGroup
        aria-label={t(`${K}.connectionLabel`, { defaultValue: 'Connection' })}
        value={draft.connection}
        onChange={(_, value) => edit({ connection: decisionConnection(value).id })}
        sx={{ mb: 2 }}
      >
        {Object.values(DECISION_CONNECTIONS).map((option) => (
          <FormControlLabel
            key={option.id}
            value={option.id}
            control={<Radio size="small" />}
            label={(
              <span>
                <Typography variant="body2" component="span" fontWeight={500}>{option.name}</Typography>
                <Typography variant="body2" component="span" color="text.secondary">
                  {`: ${explanation[option.id]}`}
                </Typography>
              </span>
            )}
          />
        ))}
      </RadioGroup>
      <Stack direction="row" spacing={1} alignItems="flex-start">
        <TextField
          size="small"
          label={urlLabel}
          value={draft[draft.connection]}
          onChange={(e) => edit(draft.connection === 'jev' ? { jev: e.target.value } : { openrouter: e.target.value })}
          placeholder={connection.placeholder}
          error={invalid}
          color={unencrypted ? 'warning' : undefined}
          FormHelperTextProps={unencrypted ? { sx: { color: 'warning.main' } } : undefined}
          helperText={
            invalid
              ? t(`${K}.urlScheme`, { defaultValue: 'Must start with http:// or https://' })
              : unencrypted
                ? t(`${K}.urlUnencrypted`, {
                  defaultValue: 'Plain http is not encrypted: prompts and candidate content travel in clear text. '
                    + 'Use it only on a private network.',
                })
                : connection.urlRequired
                  ? t(`${K}.urlHelp`, {
                    defaultValue: 'Where the {{name}} decisions API lives. Empty keeps the decision model off for every workspace.',
                    name: 'Jev',
                  })
                  : t(`${K}.urlHelpOpenRouter`, {
                    defaultValue: "OpenRouter's API. Empty uses its public API.",
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
