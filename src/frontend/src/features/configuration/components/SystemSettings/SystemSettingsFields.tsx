import React from 'react';
import { Box, FormControlLabel, Switch, Typography } from '@mui/material';
import TextSettingField from './TextSettingField';
import type { EngineSettings, EngineSettingsPatch } from '../../../../types/config/engines';
import AdvancedNumberField from '../AdvancedNumberField';
import { SETTING_GROUPS, type SettingGroupId } from './settingGroups';

interface Props {
  groups: SettingGroupId[];
  settings: EngineSettings;
  saving: boolean;
  onPatch: (body: EngineSettingsPatch) => void;
  /** Show each group's title (off when the panel title already names it). */
  titles?: boolean;
}

/** The fields of one or more setting groups; a key the API does not return is skipped. */
const SystemSettingsFields: React.FC<Props> = ({ groups, settings, saving, onPatch, titles = true }) => (
  <>
    {groups.map((id) => {
      const group = SETTING_GROUPS[id];
      return (
        <Box key={id} sx={{ mt: 2 }}>
          {titles && (
            <Typography variant="subtitle2" sx={{ mb: 1.5 }}>
              {group.title}
            </Typography>
          )}
          {Object.entries(group.fields).map(([key, meta]) => {
            const spec = settings.advanced_specs[key];
            const value = settings.advanced[key];
            if (!spec || value === undefined) return null;
            if (typeof spec.default === 'string') {
              return (
                <TextSettingField
                  key={key}
                  label={meta.label}
                  helper={meta.helper}
                  value={String(value)}
                  saving={saving}
                  onSave={(next) => onPatch({ advanced: { [key]: next } })}
                />
              );
            }
            if (typeof spec.default === 'boolean') {
              return (
                <FormControlLabel
                  key={key}
                  sx={{ display: 'flex', mb: 1 }}
                  control={
                    <Switch
                      checked={Boolean(value)}
                      disabled={saving}
                      onChange={(e) => onPatch({ advanced: { [key]: e.target.checked } })}
                    />
                  }
                  label={meta.label}
                />
              );
            }
            return (
              <AdvancedNumberField
                key={key}
                label={meta.label}
                helper={`${meta.helper} Default ${spec.default}.`}
                value={value as number}
                min={spec.minimum ?? 0}
                max={spec.maximum ?? Number.MAX_SAFE_INTEGER}
                decimal={meta.decimal}
                saving={saving}
                onSave={(next) => onPatch({ advanced: { [key]: next } })}
              />
            );
          })}
        </Box>
      );
    })}
  </>
);

export default SystemSettingsFields;
