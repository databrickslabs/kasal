/**
 * Cognitive Memory Panel — tuning knobs for CrewAI 1.10+ unified memory.
 *
 * Exposes composite-score weights (semantic / recency / importance),
 * consolidation threshold, recall-depth knobs and an optional memory-LLM
 * override. Values flow into ``config.cognitive_config`` on the
 * ``useMemoryBackendStore`` Zustand store.
 */

import React, { useEffect, useState } from 'react';
import {
  Accordion,
  AccordionDetails,
  AccordionSummary,
  Box,
  Grid,
  MenuItem,
  Slider,
  TextField,
  Typography,
} from '@mui/material';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';

import {
  COGNITIVE_MEMORY_DEFAULTS,
  CognitiveMemoryConfig,
} from '../../types/memoryBackend';
import { Models } from '../../types/models';
import { ModelService } from '../../api/ModelService';
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

  // Populate the memory-LLM dropdown from the workspace's enabled models, so the
  // override matches the same catalog used everywhere else (agents, tasks).
  const [models, setModels] = useState<Models>({});
  useEffect(() => {
    let active = true;
    ModelService.getInstance()
      .getActiveModels()
      .then((m) => {
        if (active) setModels(m);
      })
      .catch(() => {
        /* leave list empty; the field still renders the saved value */
      });
    return () => {
      active = false;
    };
  }, []);

  const modelKeys = Object.keys(models);
  const selectedModel = cognitive.memory_llm_model;
  // Keep a previously-saved model selectable even if it was since disabled,
  // so the Select never shows an out-of-range value.
  const modelOptions =
    selectedModel && !modelKeys.includes(selectedModel)
      ? [selectedModel, ...modelKeys]
      : modelKeys;

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
            Advanced — composite-score weights, consolidation threshold, and recall
            speed (exploration budget, query-analysis threshold, memory LLM).
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
              helperText="LLM-driven recall rounds when confidence is low (0 = shallow only, fewest LLM calls)."
              inputProps={{ min: 0 }}
            />
          </Grid>

          <Grid item xs={12} md={6}>
            <TextField
              fullWidth
              type="number"
              label="Query-analysis threshold (chars)"
              value={
                cognitive.query_analysis_threshold ??
                COGNITIVE_MEMORY_DEFAULTS.query_analysis_threshold
              }
              onChange={(e) => {
                const parsed = parseInt(e.target.value, 10);
                updateCognitiveConfig({
                  query_analysis_threshold: Number.isNaN(parsed) ? undefined : parsed,
                });
              }}
              helperText={
                'Recall runs an extra LLM call to distill queries longer than this many ' +
                'characters. Task descriptions usually exceed the 200-char default, so that ' +
                'call fires on nearly every task. Raise it high (e.g. 100000) to skip it and ' +
                'save ~1–3s per recall; 0 always runs it.'
              }
              inputProps={{ min: 0 }}
            />
          </Grid>

          <Grid item xs={12}>
            <TextField
              select
              fullWidth
              label="Memory LLM override (optional)"
              value={cognitive.memory_llm_model || ''}
              onChange={(e) =>
                updateCognitiveConfig({
                  memory_llm_model: e.target.value || undefined,
                })
              }
              helperText={
                'Pick the model used for memory analysis (scope, importance, ' +
                "consolidation). Defaults to the crew's LLM — choose a fast model " +
                '(e.g. Llama 4 Maverick or Claude Haiku) to keep recall cheap.'
              }
            >
              <MenuItem value="">
                <em>Use the crew&apos;s LLM (default)</em>
              </MenuItem>
              {modelOptions.map((key) => (
                <MenuItem key={key} value={key}>
                  {models[key]?.name || key}
                  {models[key]?.provider ? ` · ${models[key].provider}` : ''}
                </MenuItem>
              ))}
            </TextField>
          </Grid>
        </Grid>
      </AccordionDetails>
    </Accordion>
  );
};

export default CognitiveMemoryPanel;
