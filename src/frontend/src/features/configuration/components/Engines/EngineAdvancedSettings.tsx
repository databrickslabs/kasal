import React from 'react';
import { Box, FormControlLabel, Switch, Typography } from '@mui/material';
import type { EngineSettings, EngineSettingsPatch } from '../../../../types/config/engines';
import AdvancedNumberField from '../AdvancedNumberField';

interface FieldMeta {
  label: string;
  helper: string;
  decimal?: boolean;
}

/** Groups in display order; a key the API returns but no group lists is not shown. */
const GROUPS: { title: string; fields: Record<string, FieldMeta> }[] = [
  {
    title: 'Memory maintenance (all workspaces)',
    fields: {
      memory_sweep_enabled: { label: 'Background memory sweep', helper: '' },
      memory_sweep_interval_hours: {
        label: 'Sweep a workspace every (hours)',
        helper: 'How long a workspace waits between background maintenance passes.',
        decimal: true,
      },
      memory_sweep_batch: {
        label: 'Workspaces per sweep',
        helper: 'Each costs up to two LLM calls; the rest wait for the next tick.',
      },
      memory_maintenance_interval: {
        label: 'Seconds between passes on one scope',
        helper: 'Throttles the pass that follows a run. 0 runs it after every run.',
      },
    },
  },
  {
    title: 'Knowledge search (all workspaces)',
    fields: {
      knowledge_min_score: {
        label: 'Minimum relevance',
        helper: 'Similarity a chunk must reach to be shown to an agent.',
        decimal: true,
      },
      knowledge_max_searches: {
        label: 'Searches per agent turn',
        helper: 'Stops an agent that keeps rephrasing the same search. 0 = unlimited.',
      },
      knowledge_ttl_days: {
        label: 'Keep uploads (days)',
        helper: 'Uploaded documents are removed after this long. 0 keeps them.',
      },
    },
  },
];

interface Props {
  settings: EngineSettings;
  saving: boolean;
  onPatch: (body: EngineSettingsPatch) => void;
}

/**
 * Server-wide settings that act across every workspace. They replaced the
 * KASAL_MEMORY_SWEEP*, KASAL_MEMORY_MAINTENANCE_INTERVAL and KNOWLEDGE_*
 * environment variables, which a Databricks App never sets.
 */
const EngineAdvancedSettings: React.FC<Props> = ({ settings, saving, onPatch }) => (
  <>
    {GROUPS.map((group) => (
      <Box key={group.title} sx={{ mt: 2 }}>
        <Typography variant="subtitle2" sx={{ mb: 1.5 }}>
          {group.title}
        </Typography>
        {Object.entries(group.fields).map(([key, meta]) => {
          const spec = settings.advanced_specs[key];
          const value = settings.advanced[key];
          if (!spec || value === undefined) return null;
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
    ))}
  </>
);

export default EngineAdvancedSettings;
