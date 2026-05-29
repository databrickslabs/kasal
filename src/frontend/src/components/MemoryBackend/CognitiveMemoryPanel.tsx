/**
 * Cognitive Memory Panel — tuning knobs for CrewAI 1.10+ unified memory.
 *
 * Exposes composite-score weights (semantic / recency / importance),
 * consolidation threshold, recall-depth knobs and an optional memory-LLM
 * override. Values flow into ``config.cognitive_config`` on the
 * ``useMemoryBackendStore`` Zustand store.
 */

import React, { useState } from 'react';
import {
  Accordion,
  AccordionDetails,
  AccordionSummary,
  Box,
  Grid,
  Slider,
  TextField,
  Typography,
} from '@mui/material';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';

import {
  COGNITIVE_MEMORY_DEFAULTS,
  CognitiveMemoryConfig,
} from '../../types/memoryBackend';
import {
  useCognitiveMemoryConfig,
  useMemoryBackendStore,
} from '../../store/memoryBackend';

interface SliderSpec {
  key: keyof CognitiveMemoryConfig;
  label: string;
  min: number;
  max: number;
  step: number;
  help: string;
}

const COGNITIVE_SLIDERS: SliderSpec[] = [
  {
    key: 'semantic_weight',
    label: 'Semantic weight',
    min: 0,
    max: 1,
    step: 0.05,
    help: 'How strongly recall favors vector similarity (default 0.5).',
  },
  {
    key: 'recency_weight',
    label: 'Recency weight',
    min: 0,
    max: 1,
    step: 0.05,
    help: 'How strongly recall favors recently-created memories (default 0.3).',
  },
  {
    key: 'importance_weight',
    label: 'Importance weight',
    min: 0,
    max: 1,
    step: 0.05,
    help: 'How strongly recall favors LLM-inferred importance (default 0.2).',
  },
  {
    key: 'consolidation_threshold',
    label: 'Consolidation threshold',
    min: 0,
    max: 1,
    step: 0.05,
    help: 'Similarity above which save-time consolidation merges records (default 0.85).',
  },
];

export const CognitiveMemoryPanel: React.FC = () => {
  const cognitive: CognitiveMemoryConfig = useCognitiveMemoryConfig() || {};
  const updateCognitiveConfig = useMemoryBackendStore(
    (state) => state.updateCognitiveConfig,
  );
  const [expanded, setExpanded] = useState<boolean>(false);

  const valueOrDefault = (key: keyof CognitiveMemoryConfig): number =>
    (cognitive[key] as number | undefined) ??
    (COGNITIVE_MEMORY_DEFAULTS[
      key as keyof typeof COGNITIVE_MEMORY_DEFAULTS
    ] as number);

  return (
    <Accordion
      expanded={expanded}
      onChange={(_e, isExpanded) => setExpanded(isExpanded)}
      sx={{ mt: 3, boxShadow: 'none', border: '1px solid', borderColor: 'divider' }}
    >
      <AccordionSummary expandIcon={<ExpandMoreIcon />}>
        <Box>
          <Typography variant="subtitle1">Cognitive Memory Tuning</Typography>
          <Typography variant="caption" color="text.secondary">
            Advanced — composite-score weights, consolidation threshold, recall depth.
          </Typography>
        </Box>
      </AccordionSummary>
      <AccordionDetails>
        <Grid container spacing={3}>
          {COGNITIVE_SLIDERS.map((slider) => (
            <Grid item xs={12} md={6} key={slider.key}>
              <Typography variant="body2" sx={{ mb: 1 }}>
                {slider.label}: <strong>{valueOrDefault(slider.key)}</strong>
              </Typography>
              <Slider
                value={valueOrDefault(slider.key)}
                min={slider.min}
                max={slider.max}
                step={slider.step}
                valueLabelDisplay="auto"
                onChange={(_e, v) =>
                  updateCognitiveConfig({ [slider.key]: v as number })
                }
              />
              <Typography variant="caption" color="text.secondary">
                {slider.help}
              </Typography>
            </Grid>
          ))}

          <Grid item xs={12} md={6}>
            <TextField
              fullWidth
              type="number"
              label="Recency half-life (days)"
              value={
                cognitive.recency_half_life_days ??
                COGNITIVE_MEMORY_DEFAULTS.recency_half_life_days
              }
              onChange={(e) =>
                updateCognitiveConfig({
                  recency_half_life_days: parseInt(e.target.value, 10) || undefined,
                })
              }
              helperText="Days for the recency score to halve. Lower = memories fade faster."
              inputProps={{ min: 1 }}
            />
          </Grid>

          <Grid item xs={12} md={6}>
            <TextField
              fullWidth
              type="number"
              label="Exploration budget"
              value={
                cognitive.exploration_budget ??
                COGNITIVE_MEMORY_DEFAULTS.exploration_budget
              }
              onChange={(e) =>
                updateCognitiveConfig({
                  exploration_budget: parseInt(e.target.value, 10) || 0,
                })
              }
              helperText="LLM-driven recall rounds when confidence is low (0 = shallow only)."
              inputProps={{ min: 0 }}
            />
          </Grid>

          <Grid item xs={12}>
            <TextField
              fullWidth
              label="Memory LLM override (optional)"
              value={cognitive.memory_llm_model || ''}
              onChange={(e) =>
                updateCognitiveConfig({
                  memory_llm_model: e.target.value || undefined,
                })
              }
              placeholder="e.g. databricks-llama-4-maverick"
              helperText={
                'Overrides the LLM used for memory analysis (scope, importance, ' +
                "consolidation). Defaults to the crew's LLM so OPENAI_API_KEY isn't required."
              }
            />
          </Grid>
        </Grid>
      </AccordionDetails>
    </Accordion>
  );
};

export default CognitiveMemoryPanel;
