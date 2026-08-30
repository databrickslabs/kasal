import React from 'react';
import { Grid, TextField } from '@mui/material';
import ThinkingFields, { ThinkingFieldsProps } from '../Common/ThinkingFields';

/**
 * The per-agent overrides of the model row: temperature, max output tokens and
 * the thinking control. One contract for all three — blank inherits the
 * workspace default for the selected model — and one place for it, so the
 * Agent form stays JSX plus wiring.
 *
 * Which controls appear follows the SELECTED model: an override for a parameter
 * the endpoint refuses produces a failed run, not a fallback (claude-opus-5 and
 * every gpt-5* reject `temperature`), and which thinking control applies is the
 * model's property. Both come from measured capability on the model row.
 */
export type ModelOverrideField =
  | 'temperature'
  | 'max_tokens'
  | 'thinking_budget_tokens'
  | 'thinking_effort';

export interface ModelOverrideFieldsProps
  extends Pick<ThinkingFieldsProps, 'thinkingMode' | 'allowedEfforts' | 'returnsThinkingText'> {
  /** Whether the selected model accepts `temperature` at all. */
  acceptsTemperature: boolean;
  temperature?: number;
  maxTokens?: number | null;
  /** The model row's own ceiling — what "leave empty" inherits. */
  modelMaxOutputTokens?: number;
  thinkingBudgetTokens?: number;
  thinkingEffort?: string;
  onChange: (field: ModelOverrideField, value: number | string | undefined) => void;
}

const ModelOverrideFields: React.FC<ModelOverrideFieldsProps> = ({
  acceptsTemperature,
  temperature,
  maxTokens,
  modelMaxOutputTokens,
  thinkingMode,
  allowedEfforts,
  returnsThinkingText,
  thinkingBudgetTokens,
  thinkingEffort,
  onChange,
}) => (
  <>
    {acceptsTemperature && (
      <Grid item xs={12}>
        <TextField
          fullWidth
          type="number"
          label="Temperature Override (0-100)"
          value={temperature || ''}
          onChange={(e) => {
            const value = e.target.value ? parseInt(e.target.value, 10) : undefined;
            if (value === undefined || (value >= 0 && value <= 100)) {
              onChange('temperature', value);
            }
          }}
          helperText="Override the default model temperature. 0 = deterministic, 100 = creative. Leave empty to use model default."
          InputProps={{ inputProps: { min: 0, max: 100 } }}
        />
      </Grid>
    )}
    <Grid item xs={12}>
      <TextField
        fullWidth
        type="number"
        label="Max Output Tokens Override"
        value={maxTokens ?? ''}
        onChange={(e) => {
          const value = e.target.value ? parseInt(e.target.value, 10) : undefined;
          if (value === undefined || value >= 1) {
            onChange('max_tokens', value);
          }
        }}
        helperText={
          'Ceiling on the tokens this agent may write per call, reasoning included. ' +
          'Leave empty to use the model\'s max_output_tokens' +
          (modelMaxOutputTokens ? ` (${modelMaxOutputTokens})` : '') +
          '.'
        }
        InputProps={{ inputProps: { min: 1 } }}
      />
    </Grid>
    <Grid item xs={12}>
      <ThinkingFields
        overrideMode
        thinkingMode={thinkingMode}
        allowedEfforts={allowedEfforts}
        returnsThinkingText={returnsThinkingText}
        budgetTokens={thinkingBudgetTokens ?? null}
        onBudgetTokensChange={(value) => onChange('thinking_budget_tokens', value ?? undefined)}
        effort={thinkingEffort ?? null}
        onEffortChange={(value) => onChange('thinking_effort', value ?? undefined)}
      />
    </Grid>
  </>
);

export default ModelOverrideFields;
