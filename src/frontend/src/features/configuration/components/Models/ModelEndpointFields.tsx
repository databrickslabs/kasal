import React from 'react';
import {
  FormControl,
  FormControlLabel,
  InputLabel,
  MenuItem,
  Select,
  Stack,
  Switch,
  TextField,
} from '@mui/material';
import {
  HOSTED_OVERRIDABLE,
  LOCAL_DEFAULTS,
  SELF_HOSTED_PROVIDERS,
  endpointError,
  type ModelParams,
} from './modelEndpoint';

interface Props {
  provider: string | undefined;
  params: ModelParams | null | undefined;
  onChange: (params: ModelParams) => void;
}

/**
 * Where the model's API lives, stored on the model as `params.api_base`
 * (formerly the VLLM_BASE_URL / KAT_BASE_URL / OLLAMA_API_BASE / *_API_BASE
 * env vars), plus vLLM's tool options (formerly VLLM_SUPPORTS_TOOLS /
 * VLLM_TOOL_CHOICE).
 */
const ModelEndpointFields: React.FC<Props> = ({ provider, params, onChange }) => {
  const kind = (provider || '').toLowerCase();
  const selfHosted = SELF_HOSTED_PROVIDERS.includes(kind);
  if (!selfHosted && !HOSTED_OVERRIDABLE.includes(kind)) return null;

  const current: ModelParams = params ?? {};
  const set = (key: string, value: unknown) => {
    const next = { ...current };
    if (value === '' || value === undefined) delete next[key];
    else next[key] = value;
    onChange(next);
  };

  const apiBase = typeof current.api_base === 'string' ? current.api_base : '';
  const error = endpointError(apiBase);
  const helper = selfHosted
    ? `Where your ${kind === 'custom' ? 'server' : kind} is running. Empty uses ${LOCAL_DEFAULTS[kind]} in local development; required inside Databricks Apps.`
    : 'Optional. Empty uses the provider’s public API; set it to route through a gateway.';

  return (
    <Stack spacing={2}>
      <TextField
        label={selfHosted ? 'Endpoint URL' : 'Endpoint override'}
        value={apiBase}
        onChange={(e) => set('api_base', e.target.value.trim())}
        placeholder={selfHosted ? LOCAL_DEFAULTS[kind] : undefined}
        fullWidth
        error={!!error}
        helperText={error || helper}
        inputProps={{ 'aria-label': selfHosted ? 'Endpoint URL' : 'Endpoint override' }}
      />
      {kind === 'vllm' && (
        <>
          <FormControlLabel
            control={
              <Switch
                checked={current.supports_tools !== false}
                onChange={(e) => set('supports_tools', e.target.checked ? undefined : false)}
              />
            }
            label="Tool calling (native function calling on this server)"
          />
          <FormControl fullWidth disabled={current.supports_tools === false}>
            <InputLabel id="vllm-tool-choice-label">Tool choice</InputLabel>
            <Select
              labelId="vllm-tool-choice-label"
              label="Tool choice"
              value={typeof current.tool_choice === 'string' ? current.tool_choice : 'auto'}
              onChange={(e) => set('tool_choice', e.target.value === 'auto' ? undefined : e.target.value)}
            >
              <MenuItem value="auto">auto (the model decides)</MenuItem>
              <MenuItem value="required">required (always call a tool)</MenuItem>
              <MenuItem value="none">none (never call tools)</MenuItem>
              <MenuItem value="default">server default (send nothing)</MenuItem>
            </Select>
          </FormControl>
        </>
      )}
    </Stack>
  );
};

export default ModelEndpointFields;
