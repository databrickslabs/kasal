import React from 'react';
import {
  Accordion,
  AccordionDetails,
  AccordionSummary,
  Box,
  Checkbox,
  FormControlLabel,
  Switch,
  TextField,
  Typography,
} from '@mui/material';
import { ExpandMore as ExpandMoreIcon } from '@mui/icons-material';
import { SETTING_GROUPS } from './SystemSettings/settingGroups';

export type A2UIOverrides = Record<string, boolean | number>;

interface Props {
  overrides: A2UIOverrides;
  /** System values (from the UI config response); only these keys are overridable. */
  defaults: Record<string, boolean | number>;
  onChange: (next: A2UIOverrides) => void;
}

/**
 * This workspace's overrides of the system A2UI defaults (stored in the UI
 * config's settings_json, saved with the rest of Output design). A knob left on
 * "Use system default" follows whatever the system administrator sets.
 */
const A2UIRuntimeOverrides: React.FC<Props> = ({ overrides, defaults, onChange }) => {
  const fields = SETTING_GROUPS.a2ui.fields;
  const keys = Object.keys(defaults).filter((key) => key in fields);
  if (!keys.length) return null;

  const set = (key: string, value: boolean | number | undefined) => {
    const next = { ...overrides };
    if (value === undefined) delete next[key];
    else next[key] = value;
    onChange(next);
  };

  return (
    <Accordion disableGutters variant="outlined" sx={{ mt: 3 }}>
      <AccordionSummary expandIcon={<ExpandMoreIcon />}>
        <Typography variant="subtitle2">Advanced: rich answer behaviour</Typography>
      </AccordionSummary>
      <AccordionDetails>
        <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mb: 2 }}>
          Defaults come from System administration → Output design. Override one for this
          teamspace only.
        </Typography>
        {keys.map((key) => {
          const meta = fields[key];
          const systemValue = defaults[key];
          const overridden = key in overrides;
          const value = overridden ? overrides[key] : systemValue;
          const label = meta.label;
          return (
            <Box key={key} sx={{ display: 'flex', alignItems: 'center', gap: 2, mb: 1.5, flexWrap: 'wrap' }}>
              <FormControlLabel
                sx={{ minWidth: 230 }}
                control={
                  <Checkbox
                    checked={!overridden}
                    onChange={(e) => set(key, e.target.checked ? undefined : systemValue)}
                  />
                }
                label={`Use system default (${String(systemValue)})`}
              />
              {typeof systemValue === 'boolean' ? (
                <FormControlLabel
                  disabled={!overridden}
                  control={<Switch checked={Boolean(value)} onChange={(e) => set(key, e.target.checked)} />}
                  label={label}
                />
              ) : (
                <TextField
                  type="number"
                  label={label}
                  disabled={!overridden}
                  value={value}
                  onChange={(e) => {
                    const parsed = Number(e.target.value);
                    if (e.target.value !== '' && Number.isFinite(parsed)) set(key, parsed);
                  }}
                  inputProps={{ step: meta.decimal ? 'any' : 1, 'aria-label': label }}
                  sx={{ width: 260 }}
                />
              )}
            </Box>
          );
        })}
      </AccordionDetails>
    </Accordion>
  );
};

export default A2UIRuntimeOverrides;
