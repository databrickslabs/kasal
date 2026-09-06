/**
 * Power BI Metadata Reducer Configuration Selector Component
 *
 * Provides configuration UI for the Power BI Metadata Reducer Tool (Tool 81).
 * This tool sits between the Fetcher (Tool 79) and DAX Generator (Tool 80)
 * to intelligently reduce semantic model metadata before DAX generation.
 *
 * Strategies:
 *   - fuzzy:       Deterministic fuzzy matching only (no LLM)
 *   - llm:         LLM-only selection (requires workspace URL + token)
 *   - combined:    Fuzzy pre-screening + LLM selection with hints
 *   - passthrough: No reduction — full context passed through
 *
 * No authentication needed — this tool runs entirely within Databricks.
 */

import React from 'react';
import {
  Box,
  Typography,
  TextField,
  FormControlLabel,
  Checkbox,
  Divider,
  ToggleButtonGroup,
  ToggleButton,
  Alert,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  Slider,
  Tooltip,
  IconButton,
} from '@mui/material';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import FilterAltIcon from '@mui/icons-material/FilterAlt';
import TuneIcon from '@mui/icons-material/Tune';
import AutoFixHighIcon from '@mui/icons-material/AutoFixHigh';
import SpeedIcon from '@mui/icons-material/Speed';
import SmartToyIcon from '@mui/icons-material/SmartToy';
import InfoIcon from '@mui/icons-material/Info';

export interface PowerBIMetadataReducerConfig {
  // User question (primary input for relevance scoring)
  user_question?: string;

  // Data source (for cache lookup — must match Fetcher config)
  dataset_id?: string;
  workspace_id?: string;

  // Reduction strategy
  strategy?: 'fuzzy' | 'llm' | 'combined' | 'passthrough';

  // Fuzzy matching thresholds
  synonym_threshold?: number;
  synonym_boost_min?: number;

  // Limits
  max_tables?: number;
  max_measures?: number;

  // Feature toggles
  enable_value_normalization?: boolean;

  // LLM configuration
  llm_workspace_url?: string;
  llm_token?: string;
  llm_model?: string;
  llm_temperature?: number;
  llm_confidence_threshold?: number;

  // Context enrichment
  business_mappings?: string;
  field_synonyms?: string;
  active_filters?: string;
  business_terms?: string;
  enrichment_data?: string;
  reference_dax?: string;

  // Index signature
  [key: string]: string | number | boolean | undefined;
}

interface PowerBIMetadataReducerConfigSelectorProps {
  value: PowerBIMetadataReducerConfig;
  onChange: (config: PowerBIMetadataReducerConfig) => void;
  disabled?: boolean;
}

export const PowerBIMetadataReducerConfigSelector: React.FC<PowerBIMetadataReducerConfigSelectorProps> = ({
  value = {},
  onChange,
  disabled = false
}) => {
  const handleFieldChange = (field: keyof PowerBIMetadataReducerConfig, fieldValue: string | number | boolean) => {
    onChange({
      ...value,
      [field]: fieldValue
    });
  };

  const handleStrategyChange = (_event: React.MouseEvent<HTMLElement>, newStrategy: string | null) => {
    if (newStrategy !== null) {
      onChange({
        ...value,
        strategy: newStrategy as 'fuzzy' | 'llm' | 'combined' | 'passthrough'
      });
    }
  };

  const strategy = value.strategy || 'combined';
  const usesFuzzy = strategy === 'fuzzy' || strategy === 'combined';
  const usesLLM = strategy === 'llm' || strategy === 'combined';
  const isActive = strategy !== 'passthrough';

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
      {/* User Question */}
      <TextField
        label="User Question"
        value={value.user_question || ''}
        onChange={(e) => handleFieldChange('user_question', e.target.value)}
        disabled={disabled}
        fullWidth
        multiline
        rows={2}
        placeholder="e.g. What is the revenue by country for 2024?"
        helperText="The business question used to determine which tables and measures are relevant"
        size="small"
      />

      {/* Data Source — required for cache lookup */}
      <Box sx={{ display: 'flex', gap: 2 }}>
        <TextField
          label="Dataset ID"
          value={value.dataset_id || ''}
          onChange={(e) => handleFieldChange('dataset_id', e.target.value)}
          disabled={disabled}
          fullWidth
          placeholder="e.g. 12345678-abcd-..."
          helperText="Must match the Fetcher tool's Dataset ID"
          size="small"
        />
        <TextField
          label="Workspace ID"
          value={value.workspace_id || ''}
          onChange={(e) => handleFieldChange('workspace_id', e.target.value)}
          disabled={disabled}
          fullWidth
          placeholder="e.g. abcdef12-3456-..."
          helperText="Must match the Fetcher tool's Workspace ID"
          size="small"
        />
      </Box>

      {/* Strategy Toggle */}
      <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <FilterAltIcon color="primary" sx={{ fontSize: 20 }} />
          <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>
            Reduction Strategy
          </Typography>
        </Box>
        <ToggleButtonGroup
          value={strategy}
          exclusive
          onChange={handleStrategyChange}
          disabled={disabled}
          fullWidth
          size="small"
        >
          <ToggleButton value="fuzzy">
            <Box sx={{ textAlign: 'center', py: 0.5 }}>
              <SpeedIcon sx={{ fontSize: 18, mb: 0.5 }} />
              <Typography variant="body2" sx={{ fontWeight: 600 }}>
                Fuzzy
              </Typography>
              <Typography variant="caption" color="text.secondary">
                Deterministic only
              </Typography>
            </Box>
          </ToggleButton>
          <ToggleButton value="llm">
            <Box sx={{ textAlign: 'center', py: 0.5 }}>
              <SmartToyIcon sx={{ fontSize: 18, mb: 0.5 }} />
              <Typography variant="body2" sx={{ fontWeight: 600 }}>
                LLM
              </Typography>
              <Typography variant="caption" color="text.secondary">
                LLM-only selection
              </Typography>
            </Box>
          </ToggleButton>
          <ToggleButton value="combined">
            <Box sx={{ textAlign: 'center', py: 0.5 }}>
              <AutoFixHighIcon sx={{ fontSize: 18, mb: 0.5 }} />
              <Typography variant="body2" sx={{ fontWeight: 600 }}>
                Combined
              </Typography>
              <Typography variant="caption" color="text.secondary">
                Fuzzy + LLM hints
              </Typography>
            </Box>
          </ToggleButton>
          <ToggleButton value="passthrough">
            <Box sx={{ textAlign: 'center', py: 0.5 }}>
              <Typography variant="body2" sx={{ fontWeight: 600 }}>
                Passthrough
              </Typography>
              <Typography variant="caption" color="text.secondary">
                No reduction
              </Typography>
            </Box>
          </ToggleButton>
        </ToggleButtonGroup>
      </Box>

      {/* Strategy-specific info alerts */}
      {strategy === 'passthrough' && (
        <Alert severity="warning" variant="outlined">
          <Typography variant="caption">
            No reduction will be applied. The full model context will be passed through unchanged.
            Use this for debugging or when the model is already small.
          </Typography>
        </Alert>
      )}
      {strategy === 'fuzzy' && (
        <Alert severity="info" variant="outlined">
          <Typography variant="caption">
            Deterministic fuzzy matching only — no LLM calls. Fast and predictable.
            Tables and measures are scored against the question using token-set-ratio matching.
          </Typography>
        </Alert>
      )}
      {strategy === 'llm' && (
        <Alert severity="info" variant="outlined">
          <Typography variant="caption">
            LLM selects relevant tables and measures directly from the full catalog.
            Requires Databricks workspace URL and token. No fuzzy pre-screening hints.
          </Typography>
        </Alert>
      )}
      {strategy === 'combined' && (
        <Alert severity="info" variant="outlined">
          <Typography variant="caption">
            Best of both: fuzzy scoring first, then LLM receives the results as hints
            (likely-relevant markers) to make the final selection. Falls back to fuzzy-only
            if LLM is not configured.
          </Typography>
        </Alert>
      )}

      {/* Options shown for active strategies (not passthrough) */}
      {isActive && (
        <>
          {/* Value Normalization Toggle */}
          <Divider sx={{ my: 0.5 }}>
            <Typography variant="caption" color="text.secondary">
              Options
            </Typography>
          </Divider>

          <FormControlLabel
            control={
              <Checkbox
                checked={value.enable_value_normalization !== false}
                onChange={(e) => handleFieldChange('enable_value_normalization', e.target.checked)}
                disabled={disabled}
                size="small"
              />
            }
            label={
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
                <Typography variant="body2">Normalize filter values</Typography>
                <Tooltip title="Corrects typos and demonyms in filter values (e.g., 'Austira' → 'Austria', 'Italian' → 'Italy')">
                  <IconButton size="small">
                    <InfoIcon sx={{ fontSize: 14 }} />
                  </IconButton>
                </Tooltip>
              </Box>
            }
          />

          {/* Business Terms Dictionary */}
          <TextField
            label="Business Terms Dictionary"
            value={value.business_terms || ''}
            onChange={(e) => handleFieldChange('business_terms', e.target.value)}
            disabled={disabled}
            fullWidth
            multiline
            rows={3}
            placeholder='{"BU": ["Business Unit"], "CGR": ["Complete Good Receipt"], "YoY": ["Year over Year"]}'
            helperText="Optional JSON: maps abbreviations to expansions for fuzzy matching. Each key is an abbreviation, value is a list of expansion phrases."
            size="small"
          />

          {/* Enrichment Data */}
          <Accordion>
            <AccordionSummary expandIcon={<ExpandMoreIcon />}>
              <Typography variant="subtitle2">Enrichment Data (Optional)</Typography>
            </AccordionSummary>
            <AccordionDetails>
              <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                <Alert severity="info" variant="outlined" sx={{ py: 0.5 }}>
                  <Typography variant="caption">
                    Pre-computed metadata enrichment: table purpose/grain, column synonyms/descriptions,
                    measure synonyms/descriptions. Merged into model context before scoring to improve
                    relevance matching.
                  </Typography>
                </Alert>
                <TextField
                  label="Enrichment Data"
                  value={value.enrichment_data || ''}
                  onChange={(e) => handleFieldChange('enrichment_data', e.target.value)}
                  disabled={disabled}
                  fullWidth
                  multiline
                  rows={5}
                  placeholder={'{\n  "tables": {"tbl_sizing": {"purpose": "Tracks sizing data", "grain": "One row per customer per month"}},\n  "columns": {"tbl_sizing[region]": {"synonyms": ["area", "territory"], "description": "Geographic region"}},\n  "measures": {"Total Score": {"synonyms": ["total count"], "description": "Sum of all scores"}}\n}'}
                  helperText="Optional JSON: enrichment data for tables, columns, and measures. Improves fuzzy matching accuracy."
                  size="small"
                />
              </Box>
            </AccordionDetails>
          </Accordion>

          {/* Reference DAX */}
          <TextField
            label="Reference DAX Queries"
            value={value.reference_dax || ''}
            onChange={(e) => handleFieldChange('reference_dax', e.target.value)}
            disabled={disabled}
            fullWidth
            multiline
            rows={3}
            placeholder={'EVALUATE\nSUMMARIZECOLUMNS(\n    \'DimCountry\'[Country],\n    "Revenue", [Total Revenue]\n)'}
            helperText="Optional: working DAX queries as reference. Tables and measures used here are auto-included in the reduced output."
            size="small"
          />

          {/* Thresholds & Limits — shown when fuzzy is involved */}
          {usesFuzzy && (
            <Accordion sx={{ mt: 1 }}>
              <AccordionSummary expandIcon={<ExpandMoreIcon />}>
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                  <TuneIcon sx={{ fontSize: 18 }} />
                  <Typography variant="subtitle2">Thresholds &amp; Limits</Typography>
                </Box>
              </AccordionSummary>
              <AccordionDetails>
                <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2.5, px: 1 }}>
                  {/* Synonym Threshold */}
                  <Box>
                    <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 0.5 }}>
                      <Typography variant="body2">Fuzzy Match Threshold</Typography>
                      <Typography variant="body2" color="primary" sx={{ fontWeight: 600 }}>
                        {value.synonym_threshold ?? 70}
                      </Typography>
                    </Box>
                    <Slider
                      value={value.synonym_threshold ?? 70}
                      onChange={(_e, val) => handleFieldChange('synonym_threshold', val as number)}
                      min={30}
                      max={95}
                      step={5}
                      disabled={disabled}
                      marks={[
                        { value: 30, label: '30' },
                        { value: 50, label: '50' },
                        { value: 70, label: '70' },
                        { value: 95, label: '95' },
                      ]}
                      size="small"
                    />
                    <Typography variant="caption" color="text.secondary">
                      {strategy === 'fuzzy'
                        ? 'Minimum fuzzy score to keep a table/measure. Lower = more inclusive.'
                        : 'Minimum fuzzy score to consider relevant. Lower = more inclusive.'}
                    </Typography>
                  </Box>

                  {/* Boost Min — only for combined (fuzzy hints to LLM) */}
                  {strategy === 'combined' && (
                    <Box>
                      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 0.5 }}>
                        <Typography variant="body2">LLM Hint Threshold</Typography>
                        <Typography variant="body2" color="primary" sx={{ fontWeight: 600 }}>
                          {value.synonym_boost_min ?? 60}
                        </Typography>
                      </Box>
                      <Slider
                        value={value.synonym_boost_min ?? 60}
                        onChange={(_e, val) => handleFieldChange('synonym_boost_min', val as number)}
                        min={30}
                        max={90}
                        step={5}
                        disabled={disabled}
                        marks={[
                          { value: 30, label: '30' },
                          { value: 60, label: '60' },
                          { value: 90, label: '90' },
                        ]}
                        size="small"
                      />
                      <Typography variant="caption" color="text.secondary">
                        Score above which tables are flagged as &quot;likely relevant&quot; in the LLM prompt.
                      </Typography>
                    </Box>
                  )}

                  {/* LLM Confidence Threshold — for combined strategy */}
                  {strategy === 'combined' && (
                    <Box>
                      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 0.5 }}>
                        <Typography variant="body2">LLM Confidence Threshold</Typography>
                        <Typography variant="body2" color="primary" sx={{ fontWeight: 600 }}>
                          {((value.llm_confidence_threshold ?? 0) * 100).toFixed(0)}%
                        </Typography>
                      </Box>
                      <Slider
                        value={value.llm_confidence_threshold ?? 0}
                        onChange={(_e, val) => handleFieldChange('llm_confidence_threshold', val as number)}
                        min={0}
                        max={0.95}
                        step={0.05}
                        disabled={disabled}
                        marks={[
                          { value: 0, label: '0%' },
                          { value: 0.5, label: '50%' },
                          { value: 0.95, label: '95%' },
                        ]}
                        size="small"
                      />
                      <Typography variant="caption" color="text.secondary">
                        Minimum confidence score for LLM-selected items. 0% = keep all, higher = stricter.
                      </Typography>
                    </Box>
                  )}

                  <Divider />

                  {/* Max Tables / Measures */}
                  <Box sx={{ display: 'flex', gap: 2 }}>
                    <TextField
                      label="Max Tables"
                      type="number"
                      value={value.max_tables ?? 15}
                      onChange={(e) => handleFieldChange('max_tables', parseInt(e.target.value) || 15)}
                      disabled={disabled}
                      fullWidth
                      inputProps={{ min: 1, max: 50 }}
                      helperText="Maximum tables in reduced output"
                      size="small"
                    />
                    <TextField
                      label="Max Measures"
                      type="number"
                      value={value.max_measures ?? 30}
                      onChange={(e) => handleFieldChange('max_measures', parseInt(e.target.value) || 30)}
                      disabled={disabled}
                      fullWidth
                      inputProps={{ min: 1, max: 100 }}
                      helperText="Maximum measures in reduced output"
                      size="small"
                    />
                  </Box>
                </Box>
              </AccordionDetails>
            </Accordion>
          )}

          {/* Max Tables / Measures + LLM Confidence for LLM-only (no fuzzy accordion) */}
          {strategy === 'llm' && (
            <Accordion sx={{ mt: 1 }}>
              <AccordionSummary expandIcon={<ExpandMoreIcon />}>
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                  <TuneIcon sx={{ fontSize: 18 }} />
                  <Typography variant="subtitle2">Thresholds &amp; Limits</Typography>
                </Box>
              </AccordionSummary>
              <AccordionDetails>
                <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2.5, px: 1 }}>
                  {/* LLM Confidence Threshold */}
                  <Box>
                    <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 0.5 }}>
                      <Typography variant="body2">LLM Confidence Threshold</Typography>
                      <Typography variant="body2" color="primary" sx={{ fontWeight: 600 }}>
                        {((value.llm_confidence_threshold ?? 0) * 100).toFixed(0)}%
                      </Typography>
                    </Box>
                    <Slider
                      value={value.llm_confidence_threshold ?? 0}
                      onChange={(_e, val) => handleFieldChange('llm_confidence_threshold', val as number)}
                      min={0}
                      max={0.95}
                      step={0.05}
                      disabled={disabled}
                      marks={[
                        { value: 0, label: '0%' },
                        { value: 0.5, label: '50%' },
                        { value: 0.95, label: '95%' },
                      ]}
                      size="small"
                    />
                    <Typography variant="caption" color="text.secondary">
                      Minimum confidence score for LLM-selected items. 0% = keep all, higher = stricter.
                    </Typography>
                  </Box>

                  <Divider />

                  {/* Max Tables / Measures */}
                  <Box sx={{ display: 'flex', gap: 2 }}>
                    <TextField
                      label="Max Tables"
                      type="number"
                      value={value.max_tables ?? 15}
                      onChange={(e) => handleFieldChange('max_tables', parseInt(e.target.value) || 15)}
                      disabled={disabled}
                      fullWidth
                      inputProps={{ min: 1, max: 50 }}
                      helperText="Maximum tables in reduced output"
                      size="small"
                    />
                    <TextField
                      label="Max Measures"
                      type="number"
                      value={value.max_measures ?? 30}
                      onChange={(e) => handleFieldChange('max_measures', parseInt(e.target.value) || 30)}
                      disabled={disabled}
                      fullWidth
                      inputProps={{ min: 1, max: 100 }}
                      helperText="Maximum measures in reduced output"
                      size="small"
                    />
                  </Box>
                </Box>
              </AccordionDetails>
            </Accordion>
          )}

          {/* LLM Configuration — shown for llm and combined strategies */}
          {usesLLM && (
            <Accordion>
              <AccordionSummary expandIcon={<ExpandMoreIcon />}>
                <Typography variant="subtitle2">LLM Configuration</Typography>
              </AccordionSummary>
              <AccordionDetails>
                <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                  <Alert severity="info" variant="outlined" sx={{ py: 0.5 }}>
                    <Typography variant="caption">
                      {strategy === 'llm'
                        ? 'Required: the LLM receives the full table/measure catalog and selects relevant ones.'
                        : 'The LLM receives fuzzy pre-screening hints to guide selection. Without LLM config, falls back to fuzzy-only.'}
                    </Typography>
                  </Alert>
                  <TextField
                    label="Databricks Workspace URL"
                    value={value.llm_workspace_url || ''}
                    onChange={(e) => handleFieldChange('llm_workspace_url', e.target.value)}
                    disabled={disabled}
                    fullWidth
                    placeholder="https://your-workspace.cloud.databricks.com"
                    helperText="Databricks workspace URL for LLM serving endpoint"
                    size="small"
                  />
                  <TextField
                    label="Databricks Token"
                    value={value.llm_token || ''}
                    onChange={(e) => handleFieldChange('llm_token', e.target.value)}
                    disabled={disabled}
                    type="password"
                    fullWidth
                    helperText="Personal access token or service principal token"
                    size="small"
                  />
                  <TextField
                    label="LLM Model"
                    value={value.llm_model || 'databricks-claude-sonnet-4'}
                    onChange={(e) => handleFieldChange('llm_model', e.target.value)}
                    disabled={disabled}
                    fullWidth
                    helperText="Model serving endpoint name"
                    size="small"
                  />
                </Box>
              </AccordionDetails>
            </Accordion>
          )}

        </>
      )}

      {/* Tool Info */}
      <Alert severity="info" variant="outlined" sx={{ mt: 1 }}>
        <Typography variant="body2" sx={{ fontWeight: 600, mb: 0.5 }}>
          Metadata Reduction Pipeline
        </Typography>
        <Typography variant="caption">
          Place this tool between the Fetcher and DAX Generator for optimal results:
        </Typography>
        <Box component="ol" sx={{ pl: 2, mt: 0.5, mb: 0, fontSize: '0.75rem' }}>
          <li><strong>Cache Lookup</strong> — Loads model context from DB cache using Dataset/Workspace ID</li>
          {usesFuzzy && <li><strong>Fuzzy Scoring</strong> — Scores all tables/measures against the question</li>}
          {usesLLM && <li><strong>LLM Selection</strong> — Picks relevant tables/measures{strategy === 'combined' ? ' with fuzzy hints' : ' from full catalog'}</li>}
          {!usesLLM && usesFuzzy && <li><strong>Threshold Selection</strong> — Keeps tables/measures above fuzzy threshold</li>}
          {isActive && <li><strong>Dependency Resolution</strong> — Auto-includes measures referenced in DAX</li>}
          {isActive && value.enable_value_normalization !== false && <li><strong>Value Normalization</strong> — Corrects filter value typos/demonyms</li>}
          {!isActive && <li>Full context passed through unchanged</li>}
        </Box>
        <Typography variant="caption" color="text.secondary" sx={{ mt: 0.5, display: 'block' }}>
          Typical reduction: 25 tables / 80 measures → 5-8 tables / 10-15 measures (70-80%)
        </Typography>
      </Alert>
    </Box>
  );
};
