/**
 * Perplexity Configuration Selector Component
 * 
 * A configuration form for customizing Perplexity tool settings.
 */

import React, { useState, useEffect } from 'react';
import {
  Box,
  TextField,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  FormControlLabel,
  Switch,
  Typography,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  Chip,
  OutlinedInput,
  SelectChangeEvent,
  Grid,
  Alert,
  Tooltip,
  IconButton,
} from '@mui/material';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import InfoIcon from '@mui/icons-material/Info';
import { useAPIKeysStore } from '../../store/apiKeys';

interface PerplexityConfig {
  perplexity_api_key?: string;
  model?: string;
  max_tokens?: number;
  temperature?: number;
  top_p?: number;
  top_k?: number;
  presence_penalty?: number;
  frequency_penalty?: number;
  search_recency_filter?: string;
  search_domain_filter?: string[];
  return_images?: boolean;
  return_related_questions?: boolean;
  web_search_options?: {
    search_context_size?: string;
  };
}

interface PerplexityConfigSelectorProps {
  value: PerplexityConfig;
  onChange: (config: PerplexityConfig) => void;
  label?: string;
  helperText?: string;
  fullWidth?: boolean;
  disabled?: boolean;
}

const PERPLEXITY_MODELS = [
  { value: 'sonar', label: 'Sonar (Default)' },
  { value: 'sonar-pro', label: 'Sonar Pro' },
  { value: 'sonar-deep-research', label: 'Sonar Deep Research' },
  { value: 'sonar-reasoning', label: 'Sonar Reasoning' },
  { value: 'sonar-reasoning-pro', label: 'Sonar Reasoning Pro' },
  { value: 'r1-1776', label: 'R1-1776' },
];

const RECENCY_FILTERS = [
  { value: 'day', label: 'Past Day' },
  { value: 'week', label: 'Past Week' },
  { value: 'month', label: 'Past Month' },
  { value: 'year', label: 'Past Year' },
];

const SEARCH_CONTEXT_SIZES = [
  { value: 'low', label: 'Low' },
  { value: 'medium', label: 'Medium' },
  { value: 'high', label: 'High' },
];

const DEFAULT_CONFIG: PerplexityConfig = {
  model: 'sonar',
  max_tokens: 2000,
  temperature: 0.1,
  top_p: 0.9,
  top_k: 0,
  presence_penalty: 0.0,
  frequency_penalty: 1.0,
  search_recency_filter: 'month',
  search_domain_filter: ['<any>'],
  return_images: false,
  return_related_questions: false,
  web_search_options: {
    search_context_size: 'high'
  }
};

export const PerplexityConfigSelector: React.FC<PerplexityConfigSelectorProps> = ({
  value,
  onChange,
  label = 'Perplexity Configuration',
  helperText = 'Configure Perplexity AI search parameters',
  fullWidth = true,
  disabled = false
}) => {
  const [config, setConfig] = useState<PerplexityConfig>({ ...DEFAULT_CONFIG, ...value });
  const { secrets, fetchAPIKeys } = useAPIKeysStore();
  const [hasSystemApiKey, setHasSystemApiKey] = useState(false);

  // Check for existing Perplexity API key in system
  useEffect(() => {
    fetchAPIKeys();
  }, [fetchAPIKeys]);

  useEffect(() => {
    const perplexityKey = secrets.find(key => 
      key.name?.toLowerCase().includes('perplexity') || 
      key.name === 'PERPLEXITY_API_KEY'
    );
    setHasSystemApiKey(!!perplexityKey);
  }, [secrets]);

  useEffect(() => {
    setConfig({ ...DEFAULT_CONFIG, ...value });
  }, [value]);

  const handleChange = (field: keyof PerplexityConfig, newValue: string | number | boolean | string[] | undefined) => {
    const updatedConfig = { ...config, [field]: newValue };
    setConfig(updatedConfig);
    onChange(updatedConfig);
  };

  const handleWebSearchOptionsChange = (field: string, newValue: string | number | undefined) => {
    const updatedWebSearchOptions = {
      ...config.web_search_options,
      [field]: newValue
    };
    const updatedConfig = {
      ...config,
      web_search_options: updatedWebSearchOptions
    };
    setConfig(updatedConfig);
    onChange(updatedConfig);
  };

  const handleDomainFilterChange = (event: SelectChangeEvent<string[]>) => {
    const value = event.target.value;
    const domains = typeof value === 'string' ? value.split(',') : value;
    // If user selects "<any>", clear other domains
    const filteredDomains = domains.includes('<any>') ? ['<any>'] : domains.filter(d => d !== '<any>');
    handleChange('search_domain_filter', filteredDomains);
  };

  const handleDomainInputKeyPress = (event: React.KeyboardEvent<HTMLInputElement>) => {
    if (event.key === 'Enter') {
      event.preventDefault();
      const input = event.target as HTMLInputElement;
      const newDomain = input.value.trim();
      if (newDomain && !config.search_domain_filter?.includes(newDomain)) {
        const currentDomains = config.search_domain_filter || [];
        const filteredDomains = currentDomains.filter(d => d !== '<any>');
        handleChange('search_domain_filter', [...filteredDomains, newDomain]);
        input.value = '';
      }
    }
  };

  return (
    <Box sx={{ width: fullWidth ? '100%' : 'auto' }}>
      <Typography variant="subtitle2" gutterBottom>
        {label}
      </Typography>
      <Typography variant="caption" color="text.secondary" sx={{ mb: 2, display: 'block' }}>
        {helperText}
      </Typography>

      {/* API Key with System Key Hint */}
      {hasSystemApiKey && (
        <Alert severity="info" sx={{ mb: 2 }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <Typography variant="body2">
              ✅ System API key configured - leave empty to use system default, or enter a custom key to override
            </Typography>
            <Tooltip title="A Perplexity API key is already configured in the system settings. You can override it here for this specific task/agent, or leave empty to use the system default.">
              <IconButton size="small">
                <InfoIcon fontSize="small" />
              </IconButton>
            </Tooltip>
          </Box>
        </Alert>
      )}
      <TextField
        fullWidth
        label={hasSystemApiKey ? "API Key Override (Optional)" : "API Key"}
        value={config.perplexity_api_key || ''}
        onChange={(e) => handleChange('perplexity_api_key', e.target.value)}
        disabled={disabled}
        type="password"
        sx={{ mb: 2 }}
        helperText={
          hasSystemApiKey 
            ? "Leave empty to use system default, or enter a custom key to override"
            : "Enter your Perplexity API key, or leave empty to use environment variable"
        }
        placeholder={hasSystemApiKey ? "Override system API key..." : "Enter API key..."}
      />

      {/* Model Selection */}
      <FormControl fullWidth sx={{ mb: 2 }} disabled={disabled}>
        <InputLabel>Model</InputLabel>
        <Select
          value={config.model || 'sonar'}
          onChange={(e) => handleChange('model', e.target.value)}
          label="Model"
        >
          {PERPLEXITY_MODELS.map((model) => (
            <MenuItem key={model.value} value={model.value}>
              {model.label}
            </MenuItem>
          ))}
        </Select>
      </FormControl>

      {/* Search Configuration */}
      <Accordion sx={{ mb: 2 }}>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography>Search Configuration</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Grid container spacing={2}>
            <Grid item xs={12} md={6}>
              <FormControl fullWidth disabled={disabled}>
                <InputLabel>Search Recency</InputLabel>
                <Select
                  value={config.search_recency_filter || 'month'}
                  onChange={(e) => handleChange('search_recency_filter', e.target.value)}
                  label="Search Recency"
                >
                  {RECENCY_FILTERS.map((filter) => (
                    <MenuItem key={filter.value} value={filter.value}>
                      {filter.label}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={12} md={6}>
              <FormControl fullWidth disabled={disabled}>
                <InputLabel>Search Context Size</InputLabel>
                <Select
                  value={config.web_search_options?.search_context_size || 'high'}
                  onChange={(e) => handleWebSearchOptionsChange('search_context_size', e.target.value)}
                  label="Search Context Size"
                >
                  {SEARCH_CONTEXT_SIZES.map((size) => (
                    <MenuItem key={size.value} value={size.value}>
                      {size.label}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={12}>
              <FormControl fullWidth disabled={disabled}>
                <InputLabel>Domain Filter</InputLabel>
                <Select
                  multiple
                  value={config.search_domain_filter || ['<any>']}
                  onChange={handleDomainFilterChange}
                  input={<OutlinedInput label="Domain Filter" />}
                  renderValue={(selected) => (
                    <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
                      {selected.map((value) => (
                        <Chip key={value} label={value === '<any>' ? 'Any Domain' : value} size="small" />
                      ))}
                    </Box>
                  )}
                >
                  <MenuItem value="<any>">Any Domain</MenuItem>
                  {config.search_domain_filter?.filter(d => d !== '<any>').map((domain) => (
                    <MenuItem key={domain} value={domain}>
                      {domain}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
              <TextField
                fullWidth
                placeholder="Add custom domain (press Enter)"
                onKeyPress={handleDomainInputKeyPress}
                disabled={disabled}
                sx={{ mt: 1 }}
                size="small"
                helperText="Type a domain and press Enter to add it to the filter"
              />
            </Grid>
            <Grid item xs={12} md={6}>
              <FormControlLabel
                control={
                  <Switch
                    checked={config.return_images || false}
                    onChange={(e) => handleChange('return_images', e.target.checked)}
                    disabled={disabled}
                  />
                }
                label="Return Images"
              />
            </Grid>
            <Grid item xs={12} md={6}>
              <FormControlLabel
                control={
                  <Switch
                    checked={config.return_related_questions || false}
                    onChange={(e) => handleChange('return_related_questions', e.target.checked)}
                    disabled={disabled}
                  />
                }
                label="Return Related Questions"
              />
            </Grid>
          </Grid>
        </AccordionDetails>
      </Accordion>

      {/* Advanced Parameters */}
      <Accordion sx={{ mb: 2 }}>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography>Advanced Parameters</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Grid container spacing={2}>
            <Grid item xs={12} md={6}>
              <TextField
                fullWidth
                label="Max Tokens"
                type="number"
                value={config.max_tokens || 2000}
                onChange={(e) => handleChange('max_tokens', parseInt(e.target.value) || 2000)}
                disabled={disabled}
                inputProps={{ min: 1, max: 4000 }}
                helperText="Maximum output tokens (1-4000)"
              />
            </Grid>
            <Grid item xs={12} md={6}>
              <TextField
                fullWidth
                label="Temperature"
                type="number"
                value={config.temperature || 0.1}
                onChange={(e) => handleChange('temperature', parseFloat(e.target.value) || 0.1)}
                disabled={disabled}
                inputProps={{ min: 0, max: 1, step: 0.1 }}
                helperText="Controls randomness (0.0-1.0)"
              />
            </Grid>
            <Grid item xs={12} md={6}>
              <TextField
                fullWidth
                label="Top P"
                type="number"
                value={config.top_p || 0.9}
                onChange={(e) => handleChange('top_p', parseFloat(e.target.value) || 0.9)}
                disabled={disabled}
                inputProps={{ min: 0, max: 1, step: 0.1 }}
                helperText="Nucleus sampling parameter (0.0-1.0)"
              />
            </Grid>
            <Grid item xs={12} md={6}>
              <TextField
                fullWidth
                label="Top K"
                type="number"
                value={config.top_k || 0}
                onChange={(e) => handleChange('top_k', parseInt(e.target.value) || 0)}
                disabled={disabled}
                inputProps={{ min: 0 }}
                helperText="Top-k sampling parameter (0 = disabled)"
              />
            </Grid>
            <Grid item xs={12} md={6}>
              <TextField
                fullWidth
                label="Presence Penalty"
                type="number"
                value={config.presence_penalty || 0.0}
                onChange={(e) => handleChange('presence_penalty', parseFloat(e.target.value) || 0.0)}
                disabled={disabled}
                inputProps={{ min: -2, max: 2, step: 0.1 }}
                helperText="Penalizes new topics (-2.0 to 2.0)"
              />
            </Grid>
            <Grid item xs={12} md={6}>
              <TextField
                fullWidth
                label="Frequency Penalty"
                type="number"
                value={config.frequency_penalty || 1.0}
                onChange={(e) => handleChange('frequency_penalty', parseFloat(e.target.value) || 1.0)}
                disabled={disabled}
                inputProps={{ min: -2, max: 2, step: 0.1 }}
                helperText="Penalizes repetition (-2.0 to 2.0)"
              />
            </Grid>
          </Grid>
        </AccordionDetails>
      </Accordion>
    </Box>
  );
};

export default PerplexityConfigSelector;