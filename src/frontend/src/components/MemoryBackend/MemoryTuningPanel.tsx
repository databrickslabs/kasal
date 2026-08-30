/**
 * Memory Tuning Panel — the recall-scoring knobs for a teamspace's memory.
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
  MEMORY_TUNING_DEFAULTS,
  MemoryTuningConfig,
} from '../../types/config/memoryBackend';
import { Models } from '../../types/config/models';
import { ModelService } from '../../api/config/ModelService';
import {
  useMemoryTuningConfig,
  useMemoryBackendStore,
} from '../../store/memoryBackend';

interface SliderSpec {
  key: keyof MemoryTuningConfig;
  label: string;
  min: number;
  max: number;
  step: number;
  help: string;
}

const TUNING_SLIDERS: SliderSpec[] = [
  {
    key: 'semantic_weight',
    label: 'Semantic weight',
    min: 0,
    max: 1,
    step: 0.05,
    help: 'How strongly recall favors vector similarity (default 0.6).',
  },
  {
    key: 'keyword_weight',
    label: 'Keyword weight',
    min: 0,
    max: 1,
    step: 0.05,
    help: 'How strongly recall favors query terms that appear verbatim in a memory (default 0.15).',
  },
  {
    key: 'recency_weight',
    label: 'Recency weight',
    min: 0,
    max: 1,
    step: 0.05,
    help: 'How strongly recall favors recently-created memories (default 0.15).',
  },
  {
    key: 'importance_weight',
    label: 'Importance weight',
    min: 0,
    max: 1,
    step: 0.05,
    help: 'How strongly recall favors LLM-inferred importance (default 0.1).',
  },
  {
    key: 'relevance_threshold',
    label: 'Relevance threshold',
    min: 0,
    max: 1,
    step: 0.05,
    help:
      'Minimum semantic similarity for a memory to be recalled at all (default 0.35). ' +
      'Raise it to keep unrelated memories out of the context; applied before the ' +
      'recency/importance blend.',
  },
  {
    key: 'recall_min_score',
    label: 'Recall score floor',
    min: 0,
    max: 1,
    step: 0.01,
    help:
      'Blended score (similarity + keyword + recency + importance) below which a recall ' +
      'returns nothing. Left untouched it follows the embedder in use: 0.75 with the ' +
      'Databricks embedder, 0.62 with the local Ollama fallback. Lower it if reads come ' +
      'back empty for memories you know are there.',
  },
  {
    key: 'confidence_threshold_high',
    label: 'Confidence — stop exploring at',
    min: 0,
    max: 1,
    step: 0.05,
    help:
      'Best-hit score at or above which recall is satisfied and runs no exploration ' +
      'round (default 0.8).',
  },
  {
    key: 'confidence_threshold_low',
    label: 'Confidence — explore below',
    min: 0,
    max: 1,
    step: 0.05,
    help:
      'Best-hit score below which recall spends its exploration budget on alternative ' +
      'queries (default 0.5).',
  },
  {
    key: 'complex_query_threshold',
    label: 'Complex-query threshold',
    min: 0,
    max: 1,
    step: 0.05,
    help:
      'Query complexity (0–1, judged by the query-analysis call) at or above which recall ' +
      'explores even when the best hit sits between the two confidence bounds (default 0.7).',
  },
  {
    key: 'consolidation_threshold',
    label: 'Consolidation threshold',
    min: 0,
    max: 1,
    step: 0.05,
    help:
      'Similarity at or above which a new memory is merged INTO the closest existing one ' +
      'at save time instead of being stored beside it (default 0.85; 0 disables). The merge ' +
      'is a rewrite by the memory LLM, so without one the pass is skipped.',
  },
  {
    key: 'default_importance',
    label: 'Default importance',
    min: 0,
    max: 1,
    step: 0.05,
    help: 'Importance given to a memory when neither the writer nor the analysis supplies one (default 0.5).',
  },
];

export const MemoryTuningPanel: React.FC = () => {
  const tuning: MemoryTuningConfig = useMemoryTuningConfig() || {};
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
  const selectedModel = tuning.memory_llm_model;
  // Keep a previously-saved model selectable even if it was since disabled,
  // so the Select never shows an out-of-range value.
  const modelOptions =
    selectedModel && !modelKeys.includes(selectedModel)
      ? [selectedModel, ...modelKeys]
      : modelKeys;

  const valueOrDefault = (key: keyof MemoryTuningConfig): number =>
    (tuning[key] as number | undefined) ??
    (MEMORY_TUNING_DEFAULTS[
      key as keyof typeof MEMORY_TUNING_DEFAULTS
    ] as number);

  return (
    <Accordion
      expanded={expanded}
      onChange={(_e, isExpanded) => setExpanded(isExpanded)}
      sx={{ mt: 3, boxShadow: 'none', border: '1px solid', borderColor: 'divider' }}
    >
      <AccordionSummary expandIcon={<ExpandMoreIcon />}>
        <Box>
          <Typography variant="subtitle1">Memory Tuning</Typography>
          <Typography variant="caption" color="text.secondary">
            Advanced — composite-score weights and floor, recall depth (query
            distillation, exploration rounds), save-time consolidation, memory LLM.
          </Typography>
        </Box>
      </AccordionSummary>
      <AccordionDetails>
        <Grid container spacing={3}>
          {TUNING_SLIDERS.map((slider) => (
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
                tuning.recency_half_life_days ??
                MEMORY_TUNING_DEFAULTS.recency_half_life_days
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
              label="Consolidation limit"
              value={
                tuning.consolidation_limit ??
                MEMORY_TUNING_DEFAULTS.consolidation_limit
              }
              onChange={(e) =>
                updateCognitiveConfig({
                  consolidation_limit: parseInt(e.target.value, 10) || 0,
                })
              }
              helperText="How many nearest existing memories a new one is compared against at save time (0 disables consolidation)."
              inputProps={{ min: 0 }}
            />
          </Grid>

          <Grid item xs={12} md={6}>
            <TextField
              fullWidth
              type="number"
              label="Exploration budget"
              value={
                tuning.exploration_budget ??
                MEMORY_TUNING_DEFAULTS.exploration_budget
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
                tuning.query_analysis_threshold ??
                MEMORY_TUNING_DEFAULTS.query_analysis_threshold
              }
              onChange={(e) => {
                const parsed = parseInt(e.target.value, 10);
                updateCognitiveConfig({
                  query_analysis_threshold: Number.isNaN(parsed) ? undefined : parsed,
                });
              }}
              helperText={
                'Recall runs one memory-LLM call to distill queries at least this many ' +
                'characters long into a short search query (the plain search still runs too). ' +
                'Task descriptions usually exceed the 200-char default, so that call fires on ' +
                'nearly every task. Raise it high (e.g. 100000) to skip it and save ~1–3s per ' +
                'recall; 0 always runs it.'
              }
              inputProps={{ min: 0 }}
            />
          </Grid>

          <Grid item xs={12}>
            <TextField
              select
              fullWidth
              label="Memory LLM override (optional)"
              value={tuning.memory_llm_model || ''}
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

export default MemoryTuningPanel;
