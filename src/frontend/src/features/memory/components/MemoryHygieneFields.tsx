import React from 'react';
import {
  Box,
  FormControlLabel,
  Grid,
  MenuItem,
  Slider,
  Switch,
  TextField,
  Typography,
} from '@mui/material';

import {
  MEMORY_TUNING_DEFAULTS,
  MemoryTuningConfig,
} from '../../../types/config/memoryBackend';

interface Props {
  tuning: MemoryTuningConfig;
  update: (updates: Partial<MemoryTuningConfig>) => void;
}

const D = MEMORY_TUNING_DEFAULTS;

/**
 * Write screening, recall cut-off and retention for a teamspace's memory.
 * These were the KASAL_MEMORY_* environment variables, which a Databricks App
 * never sets; they now live in Memory Tuning beside the other knobs.
 */
const MemoryHygieneFields: React.FC<Props> = ({ tuning, update }) => {
  const forgetting = tuning.forgetting_enabled ?? D.forgetting_enabled;
  const days = (key: 'superseded_retention_days' | 'episodic_ttl_days', label: string, help: string) => (
    <Grid item xs={12} md={6}>
      <TextField
        fullWidth
        type="number"
        label={label}
        value={tuning[key] ?? D[key]}
        disabled={!forgetting}
        onChange={(e) => {
          const parsed = parseInt(e.target.value, 10);
          update({ [key]: Number.isNaN(parsed) || parsed < 1 ? undefined : parsed });
        }}
        helperText={help}
        inputProps={{ min: 1, 'aria-label': label }}
      />
    </Grid>
  );

  return (
    <Box sx={{ mt: 3 }}>
      <Typography variant="subtitle2" sx={{ mb: 1.5 }}>
        Write screening and retention
      </Typography>
      <Grid container spacing={3}>
        <Grid item xs={12} md={6}>
          <TextField
            select
            fullWidth
            label="Write screening"
            value={tuning.write_screening ?? D.write_screening}
            onChange={(e) =>
              update({ write_screening: e.target.value as MemoryTuningConfig['write_screening'] })
            }
            helperText="Checks content for prompt injection before it is saved. Annotate records findings without blocking, to see what quarantine would drop."
          >
            <MenuItem value="quarantine">Quarantine (default) — drop high-severity content</MenuItem>
            <MenuItem value="annotate">Annotate — save it, record the finding</MenuItem>
            <MenuItem value="off">Off — no screening</MenuItem>
          </TextField>
        </Grid>

        <Grid item xs={12} md={6}>
          <Typography variant="body2" sx={{ mb: 1 }}>
            Recall cut-off below best match: <strong>{tuning.recall_max_drop ?? D.recall_max_drop}</strong>
          </Typography>
          <Slider
            value={tuning.recall_max_drop ?? D.recall_max_drop}
            min={0}
            max={1}
            step={0.01}
            valueLabelDisplay="auto"
            onChange={(_e, v) => update({ recall_max_drop: v as number })}
            aria-label="Recall cut-off below best match"
          />
          <Typography variant="caption" color="text.secondary">
            Recall drops memories scoring this far below the best one (default 0.12), so filler
            does not ride along with a real match.
          </Typography>
        </Grid>

        <Grid item xs={12} md={6}>
          <FormControlLabel
            control={
              <Switch
                checked={tuning.supersession_enabled ?? D.supersession_enabled}
                onChange={(e) => update({ supersession_enabled: e.target.checked })}
              />
            }
            label="Retire facts a newer one contradicts"
          />
          <FormControlLabel
            control={
              <Switch
                checked={tuning.llm_consolidation_enabled ?? D.llm_consolidation_enabled}
                onChange={(e) => update({ llm_consolidation_enabled: e.target.checked })}
              />
            }
            label="Merge near-duplicate memories between runs"
          />
        </Grid>

        <Grid item xs={12} md={6}>
          <FormControlLabel
            control={
              <Switch
                checked={forgetting}
                onChange={(e) => update({ forgetting_enabled: e.target.checked })}
              />
            }
            label="Forget expired memories"
          />
          <Typography variant="caption" color="text.secondary" display="block">
            Off by default: the only pass that deletes something a user might still want.
          </Typography>
        </Grid>

        {days(
          'superseded_retention_days',
          'Keep replaced facts (days)',
          `A fact a newer one replaced stays as history this long (default ${D.superseded_retention_days}).`,
        )}
        {days(
          'episodic_ttl_days',
          'Keep run and chat memories (days)',
          `Unimportant run and chat memories older than this are deleted (default ${D.episodic_ttl_days}).`,
        )}

        <Grid item xs={12} md={6}>
          <Typography variant="body2" sx={{ mb: 1 }}>
            Never forget at importance ≥ <strong>{tuning.importance_floor ?? D.importance_floor}</strong>
          </Typography>
          <Slider
            value={tuning.importance_floor ?? D.importance_floor}
            min={0}
            max={1}
            step={0.05}
            disabled={!forgetting}
            valueLabelDisplay="auto"
            onChange={(_e, v) => update({ importance_floor: v as number })}
            aria-label="Never forget at importance"
          />
        </Grid>
      </Grid>
    </Box>
  );
};

export default MemoryHygieneFields;
