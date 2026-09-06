/**
 * Model-aware LLM controls: render only what this model actually accepts.
 *
 * Every value here is decided by the SERVER, from a registry of measured
 * per-model capability (`backend core/llm/model_capabilities.py`) that is also
 * what builds the request. Nothing about which control to show, or which options
 * it offers, is decided in the frontend — and that is the whole point:
 *
 *  - the effort scales differ per model. Five distinct ones across the
 *    catalogue: Anthropic adaptive takes low..max, gpt-5 takes minimal..high but
 *    rejects 'none', gpt-5-1 takes 'none' but rejects 'minimal', the 5-2/5-4/5-6
 *    line adds 'xhigh', Gemini takes only low/medium/high.
 *  - thinking comes in two mutually exclusive shapes: a token BUDGET (Claude
 *    4.1–4.6) or an EFFORT level (Claude 4.7+/5/Fable). Sending the wrong one is
 *    rejected outright.
 *  - ordinary sampling knobs are refused per model too. claude-opus-5 rejects
 *    `temperature`; claude-sonnet-4-5 accepts it but rejects both penalties.
 *
 * A control offered for a parameter the endpoint refuses does not degrade — the
 * run fails with a 400. So the rule is: if the server did not say it is
 * accepted, do not render it.
 *
 * Used by Edit Model (values are the workspace default) and Edit Agent (values
 * override the model's, blank inherits).
 */
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
  Typography,
} from '@mui/material';

export interface ThinkingFieldsProps {
  /** Server-derived: 'manual' (budget), 'adaptive' (effort) or null (neither). */
  thinkingMode?: 'manual' | 'adaptive' | null;
  /** Server-derived: the effort values this model accepts. Empty = no control. */
  allowedEfforts?: string[];
  /** Whether the thinking text can be shown at all (drives the helper text). */
  returnsThinkingText?: boolean;
  /** On/off. Omit to hide the toggle (agent-override mode). */
  enabled?: boolean;
  onEnabledChange?: (value: boolean) => void;
  budgetTokens?: number | null;
  onBudgetTokensChange: (value: number | null) => void;
  effort?: string | null;
  onEffortChange: (value: string | null) => void;
  /** Agent-override mode: no toggle, and blank means "inherit the model". */
  overrideMode?: boolean;
  /** Shown when the model has no thinking surface at all. */
  unsupportedHint?: string;
}

export const ThinkingFields: React.FC<ThinkingFieldsProps> = ({
  thinkingMode,
  allowedEfforts = [],
  returnsThinkingText = true,
  enabled,
  onEnabledChange,
  budgetTokens,
  onBudgetTokensChange,
  effort,
  onEffortChange,
  overrideMode = false,
  unsupportedHint,
}) => {
  // An effort control is possible whenever the model published a scale — that
  // includes GPT-5 and Gemini, which are not Anthropic and have no `thinking`
  // block but do take `reasoning_effort`.
  const hasEffortScale = allowedEfforts.length > 0;
  const showBudget = thinkingMode === 'manual';
  const showEffort = thinkingMode === 'adaptive' || (!thinkingMode && hasEffortScale);

  // Nothing to configure. Say so rather than rendering an empty section: silence
  // reads as "the feature is missing" when the truth is "this model has no knob".
  if (!showBudget && !showEffort) {
    return unsupportedHint ? (
      <Typography variant="caption" color="text.secondary">
        {unsupportedHint}
      </Typography>
    ) : null;
  }

  const showToggle =
    !overrideMode && typeof enabled === 'boolean' && Boolean(onEnabledChange) && Boolean(thinkingMode);
  // In Edit Model the depth is meaningless until thinking is on. Models with only
  // a `reasoning_effort` scale (GPT-5, Gemini) have no on/off toggle at all, so
  // their effort field is always live.
  const depthEnabled = overrideMode || !thinkingMode || enabled !== false;

  const effortLabel = overrideMode ? 'Reasoning Effort Override' : 'Reasoning Effort';

  return (
    <Stack spacing={2}>
      {showToggle && (
        <FormControlLabel
          control={
            <Switch
              checked={!!enabled}
              onChange={e => onEnabledChange?.(e.target.checked)}
              color="primary"
            />
          }
          label="Extended Thinking"
        />
      )}

      {showBudget && (
        <TextField
          fullWidth
          type="number"
          size="small"
          label={overrideMode ? 'Thinking Budget Override (tokens)' : 'Thinking Budget (tokens)'}
          value={budgetTokens ?? ''}
          disabled={!depthEnabled}
          onChange={e => {
            const raw = e.target.value.trim();
            onBudgetTokensChange(raw === '' ? null : Number(raw));
          }}
          inputProps={{ min: 1024, step: 1024 }}
          helperText={
            overrideMode
              ? 'Leave empty to inherit the model default. Minimum 1024 tokens.'
              : 'Tokens the model may spend thinking. Minimum 1024, and it must stay below Max Output Tokens — Kasal raises that to fit if needed.'
          }
        />
      )}

      {showEffort && (
        <FormControl fullWidth size="small" disabled={!depthEnabled}>
          <InputLabel>{effortLabel}</InputLabel>
          <Select
            label={effortLabel}
            value={effort ?? ''}
            onChange={e => {
              const value = e.target.value as string;
              onEffortChange(value === '' ? null : value);
            }}
          >
            <MenuItem value="">
              <em>{overrideMode ? 'Inherit model default' : 'Endpoint default'}</em>
            </MenuItem>
            {/* Options come from the server. Hardcoding them would offer values
                this model rejects — a 400, not a fallback. */}
            {allowedEfforts.map(level => (
              <MenuItem key={level} value={level}>
                {level}
              </MenuItem>
            ))}
          </Select>
          <Typography variant="caption" color="text.secondary" sx={{ mt: 0.5, ml: 1.5 }}>
            {returnsThinkingText
              ? 'This model decides how much to think per request; effort steers it.'
              : 'This model reasons but never returns the text, so effort changes depth and cost without anything to display.'}
          </Typography>
        </FormControl>
      )}
    </Stack>
  );
};

export default ThinkingFields;
