import React from 'react';
import { Autocomplete, Box, TextField, Typography } from '@mui/material';
import { useTranslation } from 'react-i18next';
import { DATABRICKS_DECISION_MODELS, DATABRICKS_DEFAULT_DECISION_MODEL } from './decisionModelProvider';

const K = 'configuration.models.decisionModel';

/** One model id, as the backend accepts it (`engine_settings_view._MODEL_NAME`). */
export const MODEL_ID = /^[A-Za-z0-9][A-Za-z0-9._:/-]{0,199}$/;

interface Props {
  value: string;
  onChange: (value: string) => void;
}

/**
 * The Databricks connection's model: the Databricks-hosted Jev by name, or any
 * other model id typed in (for example a provider-hosted Jev once it is
 * enabled for the workspace). Empty means the default.
 */
const DecisionModelDatabricksField: React.FC<Props> = ({ value, onChange }) => {
  const { t } = useTranslation();
  const label = t(`${K}.databricksModelLabel`, { defaultValue: 'Decision model on Databricks' });
  const trimmed = value.trim();
  const invalid = trimmed !== '' && !MODEL_ID.test(trimmed);
  return (
    <Autocomplete
      freeSolo
      disableClearable
      options={DATABRICKS_DECISION_MODELS.map((m) => m.id)}
      // The presets stay listed whatever is typed: they are a choice, not a filter.
      filterOptions={(options) => options}
      inputValue={value}
      onInputChange={(_e, next) => onChange(next)}
      renderOption={(props, option) => {
        const preset = DATABRICKS_DECISION_MODELS.find((m) => m.id === option);
        return (
          <Box component="li" {...props} key={option}>
            <Box>
              <Typography variant="body2">{option}</Typography>
              {preset && (
                <Typography variant="caption" color="text.secondary">{preset.description}</Typography>
              )}
            </Box>
          </Box>
        );
      }}
      sx={{ flex: 1, maxWidth: 480 }}
      renderInput={(params) => (
        <TextField
          {...params}
          size="small"
          label={label}
          placeholder={DATABRICKS_DEFAULT_DECISION_MODEL}
          error={invalid}
          helperText={invalid
            ? t(`${K}.databricksModelInvalid`, { defaultValue: 'One model id, with no spaces.' })
            : t(`${K}.databricksModelHelp`, {
              defaultValue: 'Served by the workspace AI Gateway. Empty uses {{model}}. No API key: each '
                + "workspace uses its own Databricks credentials.",
              model: DATABRICKS_DEFAULT_DECISION_MODEL,
            })}
          inputProps={{ ...params.inputProps, 'aria-label': label }}
        />
      )}
    />
  );
};

export default DecisionModelDatabricksField;
