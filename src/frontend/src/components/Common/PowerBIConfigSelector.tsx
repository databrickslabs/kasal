/**
 * Power BI Configuration Selector Component
 *
 * A configuration form for customizing Power BI Analysis tool settings at the task level.
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

export interface PowerBIConfig {
  tenant_id?: string;
  client_id?: string;
  workspace_id?: string;
  semantic_model_id?: string;
  auth_method?: string;
  databricks_job_id?: number;
}

interface PowerBIConfigSelectorProps {
  value: PowerBIConfig;
  onChange: (config: PowerBIConfig) => void;
  label?: string;
  helperText?: string;
  fullWidth?: boolean;
  disabled?: boolean;
}

const AUTH_METHODS = [
  { value: 'service_principal', label: 'Service Principal' },
  { value: 'device_code', label: 'Device Code' },
];

const DEFAULT_CONFIG: PowerBIConfig = {
  tenant_id: '',
  client_id: '',
  workspace_id: '',
  semantic_model_id: '',
  auth_method: 'service_principal',
  databricks_job_id: undefined,
};

export const PowerBIConfigSelector: React.FC<PowerBIConfigSelectorProps> = ({
  value,
  onChange,
  label = 'Power BI Configuration',
  helperText,
  fullWidth = true,
  disabled = false
}) => {
  const [config, setConfig] = useState<PowerBIConfig>({ ...DEFAULT_CONFIG, ...value });
  const [expanded, setExpanded] = useState<boolean>(false);
  const { secrets, fetchAPIKeys } = useAPIKeysStore();
  const [missingApiKeys, setMissingApiKeys] = useState<string[]>([]);

  // Check for required API keys
  useEffect(() => {
    fetchAPIKeys();
  }, [fetchAPIKeys]);

  useEffect(() => {
    const requiredKeys = [
      'POWERBI_CLIENT_SECRET',
      'POWERBI_USERNAME',
      'POWERBI_PASSWORD',
      'DATABRICKS_API_KEY'
    ];

    const missing = requiredKeys.filter(keyName =>
      !secrets.find(key => key.name === keyName)
    );

    setMissingApiKeys(missing);
  }, [secrets]);

  useEffect(() => {
    setConfig({ ...DEFAULT_CONFIG, ...value });
  }, [value]);

  const handleChange = (field: keyof PowerBIConfig, fieldValue: string | number | undefined) => {
    const newConfig = {
      ...config,
      [field]: fieldValue === '' ? undefined : fieldValue
    };
    setConfig(newConfig);
    onChange(newConfig);
  };

  const isConfigured = config.tenant_id && config.client_id && config.semantic_model_id;

  return (
    <Box sx={{ width: fullWidth ? '100%' : 'auto' }}>
      {!isConfigured && (
        <Alert severity="warning" sx={{ mb: 2 }}>
          Power BI configuration is required. Please configure tenant_id, client_id, and semantic_model_id.
        </Alert>
      )}

      {missingApiKeys.length > 0 && (
        <Alert severity="error" sx={{ mb: 2 }}>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
            <Typography variant="body2" fontWeight="bold">
              ⚠️ Required API Keys Missing
            </Typography>
            <Typography variant="body2">
              To use Power BI Comprehensive Analysis Tool, you MUST configure the following API Keys in Settings → API Keys:
            </Typography>
            <Box component="ul" sx={{ margin: 0, paddingLeft: 2 }}>
              {missingApiKeys.map(key => (
                <li key={key}>
                  <Typography variant="body2" component="span" fontFamily="monospace">
                    {key}
                  </Typography>
                </li>
              ))}
            </Box>
          </Box>
        </Alert>
      )}

      <Accordion
        expanded={expanded}
        onChange={(_, isExpanded) => setExpanded(isExpanded)}
        disabled={disabled}
        sx={{
          '&:before': { display: 'none' },
          boxShadow: 1,
        }}
      >
        <AccordionSummary
          expandIcon={<ExpandMoreIcon />}
          sx={{
            backgroundColor: 'rgba(0, 0, 0, 0.02)',
            borderRadius: 1,
            '&:hover': {
              backgroundColor: 'rgba(0, 0, 0, 0.04)',
            },
          }}
        >
          <Box sx={{ display: 'flex', alignItems: 'center', width: '100%' }}>
            <Typography sx={{ flexGrow: 1, fontWeight: 500 }}>
              {label}
            </Typography>
            {isConfigured && (
              <Typography variant="caption" color="success.main" sx={{ mr: 2 }}>
                ✓ Configured
              </Typography>
            )}
          </Box>
        </AccordionSummary>

        <AccordionDetails>
          <Grid container spacing={2}>
            {/* Required Fields */}
            <Grid item xs={12}>
              <Typography variant="subtitle2" color="text.secondary" gutterBottom>
                Required Configuration
              </Typography>
            </Grid>

            <Grid item xs={12} sm={6}>
              <TextField
                fullWidth
                required
                label="Tenant ID"
                value={config.tenant_id || ''}
                onChange={(e) => handleChange('tenant_id', e.target.value)}
                disabled={disabled}
                placeholder="Azure AD Tenant ID"
                helperText="Your Azure AD Tenant ID"
                InputProps={{
                  endAdornment: (
                    <Tooltip title="The Azure Active Directory Tenant ID for Power BI authentication">
                      <IconButton size="small" edge="end">
                        <InfoIcon fontSize="small" />
                      </IconButton>
                    </Tooltip>
                  ),
                }}
              />
            </Grid>

            <Grid item xs={12} sm={6}>
              <TextField
                fullWidth
                required
                label="Client ID"
                value={config.client_id || ''}
                onChange={(e) => handleChange('client_id', e.target.value)}
                disabled={disabled}
                placeholder="Azure AD Application ID"
                helperText="Your Azure AD Application/Client ID"
                InputProps={{
                  endAdornment: (
                    <Tooltip title="The Azure Active Directory Application (Client) ID">
                      <IconButton size="small" edge="end">
                        <InfoIcon fontSize="small" />
                      </IconButton>
                    </Tooltip>
                  ),
                }}
              />
            </Grid>

            <Grid item xs={12} sm={6}>
              <TextField
                fullWidth
                required
                label="Semantic Model ID"
                value={config.semantic_model_id || ''}
                onChange={(e) => handleChange('semantic_model_id', e.target.value)}
                disabled={disabled}
                placeholder="Power BI Semantic Model ID"
                helperText="The Power BI semantic model (dataset) ID to query"
                InputProps={{
                  endAdornment: (
                    <Tooltip title="The ID of the Power BI semantic model (dataset) to analyze">
                      <IconButton size="small" edge="end">
                        <InfoIcon fontSize="small" />
                      </IconButton>
                    </Tooltip>
                  ),
                }}
              />
            </Grid>

            <Grid item xs={12} sm={6}>
              <FormControl fullWidth disabled={disabled}>
                <InputLabel>Authentication Method</InputLabel>
                <Select
                  value={config.auth_method || 'service_principal'}
                  onChange={(e) => handleChange('auth_method', e.target.value)}
                  label="Authentication Method"
                >
                  {AUTH_METHODS.map((method) => (
                    <MenuItem key={method.value} value={method.value}>
                      {method.label}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>

            {/* Optional Fields */}
            <Grid item xs={12}>
              <Typography variant="subtitle2" color="text.secondary" gutterBottom sx={{ mt: 2 }}>
                Optional Configuration
              </Typography>
            </Grid>

            <Grid item xs={12} sm={6}>
              <TextField
                fullWidth
                label="Workspace ID"
                value={config.workspace_id || ''}
                onChange={(e) => handleChange('workspace_id', e.target.value)}
                disabled={disabled}
                placeholder="Power BI Workspace ID (optional)"
                helperText="Optional: Power BI workspace ID"
              />
            </Grid>

            <Grid item xs={12} sm={6}>
              <TextField
                fullWidth
                type="number"
                label="Databricks Job ID"
                value={config.databricks_job_id || ''}
                onChange={(e) => handleChange('databricks_job_id', e.target.value ? parseInt(e.target.value) : undefined)}
                disabled={disabled}
                placeholder="Databricks job ID (optional)"
                helperText="Optional: Override default Databricks job ID"
              />
            </Grid>
          </Grid>

          {helperText && (
            <Typography variant="caption" color="text.secondary" sx={{ mt: 2, display: 'block' }}>
              {helperText}
            </Typography>
          )}
        </AccordionDetails>
      </Accordion>
    </Box>
  );
};
