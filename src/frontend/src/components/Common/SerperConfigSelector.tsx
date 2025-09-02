/**
 * Serper Configuration Selector Component
 * 
 * A configuration form for customizing Serper search tool settings.
 */

import React, { useState, useEffect } from 'react';
import {
  Box,
  TextField,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Typography,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  Grid,
  Alert,
  Tooltip,
  IconButton,
} from '@mui/material';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import InfoIcon from '@mui/icons-material/Info';
import { useAPIKeysStore } from '../../store/apiKeys';

interface SerperConfig {
  serper_api_key?: string;
  n_results?: number;
  search_url?: string;
  endpoint_type?: string;
  country?: string;
  locale?: string;
  location?: string;
}

interface SerperConfigSelectorProps {
  value: SerperConfig;
  onChange: (config: SerperConfig) => void;
  label?: string;
  helperText?: string;
  fullWidth?: boolean;
  disabled?: boolean;
}

// Common country codes for search
const COUNTRY_CODES = [
  { value: 'us', label: 'United States (us)' },
  { value: 'gb', label: 'United Kingdom (gb)' },
  { value: 'ca', label: 'Canada (ca)' },
  { value: 'au', label: 'Australia (au)' },
  { value: 'de', label: 'Germany (de)' },
  { value: 'fr', label: 'France (fr)' },
  { value: 'es', label: 'Spain (es)' },
  { value: 'it', label: 'Italy (it)' },
  { value: 'jp', label: 'Japan (jp)' },
  { value: 'cn', label: 'China (cn)' },
  { value: 'in', label: 'India (in)' },
  { value: 'br', label: 'Brazil (br)' },
  { value: 'mx', label: 'Mexico (mx)' },
  { value: 'ru', label: 'Russia (ru)' },
  { value: 'kr', label: 'South Korea (kr)' },
];

// Common locales
const LOCALES = [
  { value: 'en', label: 'English (en)' },
  { value: 'es', label: 'Spanish (es)' },
  { value: 'fr', label: 'French (fr)' },
  { value: 'de', label: 'German (de)' },
  { value: 'it', label: 'Italian (it)' },
  { value: 'pt', label: 'Portuguese (pt)' },
  { value: 'ru', label: 'Russian (ru)' },
  { value: 'ja', label: 'Japanese (ja)' },
  { value: 'ko', label: 'Korean (ko)' },
  { value: 'zh', label: 'Chinese (zh)' },
  { value: 'ar', label: 'Arabic (ar)' },
  { value: 'hi', label: 'Hindi (hi)' },
];

// Serper.dev API endpoints
const SERPER_ENDPOINTS = [
  { 
    value: 'search', 
    label: 'Search', 
    url: 'https://google.serper.dev/search',
    description: 'Regular Google search results with organic results, knowledge graph, and related searches'
  },
  { 
    value: 'news', 
    label: 'News', 
    url: 'https://google.serper.dev/news',
    description: 'Google News results with recent articles from various sources'
  },
  { 
    value: 'images', 
    label: 'Images', 
    url: 'https://google.serper.dev/images',
    description: 'Google Images search results with image URLs, titles, and source information'
  },
  { 
    value: 'videos', 
    label: 'Videos', 
    url: 'https://google.serper.dev/videos',
    description: 'Google Videos search results from YouTube and other video platforms'
  },
  { 
    value: 'places', 
    label: 'Places', 
    url: 'https://google.serper.dev/places',
    description: 'Google Places/Maps results with local business information and reviews'
  },
  { 
    value: 'shopping', 
    label: 'Shopping', 
    url: 'https://google.serper.dev/shopping',
    description: 'Google Shopping results with product listings and prices'
  },
  { 
    value: 'scholar', 
    label: 'Scholar', 
    url: 'https://google.serper.dev/scholar',
    description: 'Google Scholar results for academic papers and citations'
  },
  { 
    value: 'patents', 
    label: 'Patents', 
    url: 'https://google.serper.dev/patents',
    description: 'Google Patents search results for patent documents'
  },
  { 
    value: 'autocomplete', 
    label: 'Autocomplete', 
    url: 'https://google.serper.dev/autocomplete',
    description: 'Google search suggestions and autocomplete results'
  },
];

const DEFAULT_CONFIG: SerperConfig = {
  n_results: 10,
  search_url: 'https://google.serper.dev/search',
  endpoint_type: 'search',
  country: 'us',
  locale: 'en',
  location: '',
};

export const SerperConfigSelector: React.FC<SerperConfigSelectorProps> = ({
  value,
  onChange,
  label = 'Serper Configuration',
  helperText = 'Configure Serper.dev search parameters',
  fullWidth = true,
  disabled = false
}) => {
  const [config, setConfig] = useState<SerperConfig>({ ...DEFAULT_CONFIG, ...value });
  const { secrets, fetchAPIKeys } = useAPIKeysStore();
  const [hasSystemApiKey, setHasSystemApiKey] = useState(false);

  // Check for existing Serper API key in system
  useEffect(() => {
    fetchAPIKeys();
  }, [fetchAPIKeys]);

  useEffect(() => {
    const serperKey = secrets.find(key => 
      key.name?.toLowerCase().includes('serper') || 
      key.name === 'SERPER_API_KEY'
    );
    setHasSystemApiKey(!!serperKey);
  }, [secrets]);

  useEffect(() => {
    setConfig({ ...DEFAULT_CONFIG, ...value });
  }, [value]);

  const handleChange = (field: keyof SerperConfig, newValue: string | number | undefined) => {
    const updatedConfig = { ...config, [field]: newValue };
    
    // Update search_url when endpoint_type changes
    if (field === 'endpoint_type') {
      const selectedEndpoint = SERPER_ENDPOINTS.find(endpoint => endpoint.value === newValue);
      if (selectedEndpoint) {
        updatedConfig.search_url = selectedEndpoint.url;
      }
    }
    
    setConfig(updatedConfig);
    onChange(updatedConfig);
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
            <Tooltip title="A Serper API key is already configured in the system settings. You can override it here for this specific task/agent, or leave empty to use the system default.">
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
        value={config.serper_api_key || ''}
        onChange={(e) => handleChange('serper_api_key', e.target.value)}
        disabled={disabled}
        type="password"
        sx={{ mb: 2 }}
        helperText={
          hasSystemApiKey 
            ? "Leave empty to use system default, or enter a custom key to override"
            : "Enter your Serper.dev API key, or leave empty to use environment variable"
        }
        placeholder={hasSystemApiKey ? "Override system API key..." : "Enter API key..."}
      />

      {/* Basic Configuration */}
      <Grid container spacing={2} sx={{ mb: 2 }}>
        <Grid item xs={12} md={6}>
          <TextField
            fullWidth
            label="Number of Results"
            type="number"
            value={config.n_results || 10}
            onChange={(e) => handleChange('n_results', parseInt(e.target.value) || 10)}
            disabled={disabled}
            inputProps={{ min: 1, max: 100 }}
            helperText="Number of search results to return (1-100)"
          />
        </Grid>
        <Grid item xs={12} md={6}>
          <FormControl fullWidth disabled={disabled}>
            <InputLabel>Search Endpoint</InputLabel>
            <Select
              value={config.endpoint_type || 'search'}
              onChange={(e) => handleChange('endpoint_type', e.target.value)}
              label="Search Endpoint"
            >
              {SERPER_ENDPOINTS.map((endpoint) => (
                <MenuItem key={endpoint.value} value={endpoint.value}>
                  <Box>
                    <Typography variant="body2" sx={{ fontWeight: 'medium' }}>
                      {endpoint.label}
                    </Typography>
                    <Typography variant="caption" color="text.secondary" sx={{ display: 'block', fontSize: '0.7rem' }}>
                      {endpoint.description}
                    </Typography>
                  </Box>
                </MenuItem>
              ))}
            </Select>
          </FormControl>
        </Grid>
      </Grid>

      {/* Show current endpoint URL for reference */}
      <Typography variant="caption" color="text.secondary" sx={{ mb: 2, display: 'block' }}>
        <strong>Current Endpoint:</strong> {config.search_url || 'https://google.serper.dev/search'}
      </Typography>

      {/* Geographic Configuration */}
      <Accordion sx={{ mb: 2 }}>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography>Geographic & Language Settings</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Grid container spacing={2}>
            <Grid item xs={12} md={6}>
              <FormControl fullWidth disabled={disabled}>
                <InputLabel>Country</InputLabel>
                <Select
                  value={config.country || 'us'}
                  onChange={(e) => handleChange('country', e.target.value)}
                  label="Country"
                >
                  {COUNTRY_CODES.map((country) => (
                    <MenuItem key={country.value} value={country.value}>
                      {country.label}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={12} md={6}>
              <FormControl fullWidth disabled={disabled}>
                <InputLabel>Language Locale</InputLabel>
                <Select
                  value={config.locale || 'en'}
                  onChange={(e) => handleChange('locale', e.target.value)}
                  label="Language Locale"
                >
                  {LOCALES.map((locale) => (
                    <MenuItem key={locale.value} value={locale.value}>
                      {locale.label}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={12}>
              <TextField
                fullWidth
                label="Specific Location"
                value={config.location || ''}
                onChange={(e) => handleChange('location', e.target.value)}
                disabled={disabled}
                placeholder="e.g., New York, London, Tokyo"
                helperText="Optional specific location for geographically-targeted results"
              />
            </Grid>
          </Grid>
        </AccordionDetails>
      </Accordion>

      {/* Help Information */}
      <Alert severity="info" sx={{ mt: 2 }}>
        <Typography variant="body2">
          💡 <strong>Tip:</strong> Serper.dev provides access to multiple Google search types through API. Choose from Search, News, Images, Videos, Places, Shopping, Scholar, Patents, and Autocomplete endpoints. Configure geographic settings for location-specific results.
        </Typography>
      </Alert>
    </Box>
  );
};

export default SerperConfigSelector;