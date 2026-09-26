import React, { useEffect, useState } from 'react';
import {
  Accordion,
  AccordionDetails,
  AccordionSummary,
  Box,
  FormControl,
  InputLabel,
  MenuItem,
  Select,
  Typography,
} from '@mui/material';
import { ExpandMore as ExpandMoreIcon } from '@mui/icons-material';
import AdvancedNumberField from './AdvancedNumberField';
import { ModelService } from '../../../api/config/ModelService';
import type { MLflowSettings, MLflowSettingsPatch } from '../../../types/config/mlflow';

/** "Not set": evaluation and optimization use the installed model inside
 * Databricks Apps; elsewhere the Optimize dialog asks for a judge. */
const NOT_SET = '';

interface Props {
  settings: MLflowSettings;
  saving: boolean;
  onPatch: (body: MLflowSettingsPatch) => Promise<void> | void;
}

/**
 * The judge model and the Advanced MLflow settings. These used to be the
 * MLFLOW_EVAL_JUDGE_MODEL / GEPA_JUDGE_MODEL / MLFLOW_EVAL_MAX_ROWS /
 * GEPA_JUDGE_SAMPLES environment variables, which a Databricks App never sets.
 */
const MLflowJudgeAndAdvanced: React.FC<Props> = ({ settings, saving, onPatch }) => {
  const [models, setModels] = useState<string[]>([]);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const enabled = await ModelService.getInstance().getEnabledModels();
        if (!cancelled) setModels(Object.keys(enabled));
      } catch {
        if (!cancelled) setModels([]);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  const judge = settings.evaluation_judge_model || NOT_SET;
  // Keep a stored judge selectable even if it is no longer an enabled model.
  const options = judge && !models.includes(judge) ? [judge, ...models] : models;

  return (
    <Box sx={{ mt: 2 }}>
      <FormControl size="small" sx={{ minWidth: 320 }} disabled={saving}>
        <InputLabel id="mlflow-judge-model-label">Judge model</InputLabel>
        <Select
          labelId="mlflow-judge-model-label"
          label="Judge model"
          value={judge}
          onChange={(e) => void onPatch({ evaluation_judge_model: String(e.target.value) })}
        >
          <MenuItem value={NOT_SET}>
            <em>Not set (installed model in Databricks Apps)</em>
          </MenuItem>
          {options.map((model) => (
            <MenuItem key={model} value={model}>
              {model}
            </MenuItem>
          ))}
        </Select>
      </FormControl>
      <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mt: 0.5, mb: 2 }}>
        Grades evaluations and is the default judge for prompt optimization. The Optimize dialog can
        still pick another one.
      </Typography>

      <Accordion disableGutters variant="outlined">
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="subtitle2">Advanced</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <AdvancedNumberField
            label="Evaluation rows"
            helper="Most traces an evaluation run scores."
            value={settings.evaluation_max_rows}
            min={1}
            max={10000}
            saving={saving}
            onSave={(value) => void onPatch({ evaluation_max_rows: value })}
          />
          <AdvancedNumberField
            label="Judge samples"
            helper="Times prompt optimization samples the judge per candidate (median). 1 turns sampling off."
            value={settings.optimization_judge_samples}
            min={1}
            max={9}
            saving={saving}
            onSave={(value) => void onPatch({ optimization_judge_samples: value })}
          />
        </AccordionDetails>
      </Accordion>
    </Box>
  );
};

export default MLflowJudgeAndAdvanced;
