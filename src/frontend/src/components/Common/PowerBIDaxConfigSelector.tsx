/**
 * Power BI Semantic Model DAX Generator Configuration Selector Component
 *
 * Provides configuration UI for the Power BI Semantic Model DAX Generator Tool.
 * Generates context-aware DAX queries using LLM with semantic model metadata.
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
  Chip,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  Button,
  MenuItem,
  Select,
  FormControl,
  InputLabel
} from '@mui/material';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import LoginIcon from '@mui/icons-material/Login';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import SecurityIcon from '@mui/icons-material/Security';
import PersonIcon from '@mui/icons-material/Person';
import { usePowerBIOAuth } from '../../hooks/usePowerBIOAuth';

// Reuse auth method type
export type PowerBIAuthMethod = 'service_principal' | 'service_account' | 'user_oauth';

export interface PowerBIDaxConfig {
  mode?: 'static' | 'dynamic';
  workspace_id?: string;
  dataset_id?: string;
  report_id?: string;
  auth_method?: PowerBIAuthMethod;
  tenant_id?: string;
  client_id?: string;
  client_secret?: string;
  username?: string;
  password?: string;
  oauth_client_id?: string;
  access_token?: string;
  // LLM config
  llm_workspace_url?: string;
  llm_token?: string;
  llm_model?: string;
  // Context enrichment (JSON strings)
  business_mappings?: string;
  field_synonyms?: string;
  active_filters?: string;
  visible_tables?: string;
  // Options
  include_visual_references?: boolean;
  max_dax_retries?: string;
  output_format?: string;
  [key: string]: string | boolean | undefined;
}

interface PowerBIDaxConfigSelectorProps {
  value: PowerBIDaxConfig;
  onChange: (config: PowerBIDaxConfig) => void;
  disabled?: boolean;
}

export const PowerBIDaxConfigSelector: React.FC<PowerBIDaxConfigSelectorProps> = ({
  value = {},
  onChange,
  disabled = false
}) => {
  const { accessToken, isAuthenticated, signIn, signOut, userEmail, isLoading: oauthLoading, error: oauthError } = usePowerBIOAuth({
    clientId: value.oauth_client_id || ''
  });

  const handleFieldChange = (field: keyof PowerBIDaxConfig, fieldValue: string | boolean) => {
    onChange({
      ...value,
      [field]: fieldValue
    });
  };

  const handleAuthMethodChange = (_event: React.MouseEvent<HTMLElement>, newMethod: PowerBIAuthMethod | null) => {
    if (newMethod !== null) {
      const updatedConfig: PowerBIDaxConfig = {
        ...value,
        auth_method: newMethod
      };

      if (newMethod === 'user_oauth') {
        updatedConfig.tenant_id = undefined;
        updatedConfig.client_id = undefined;
        updatedConfig.client_secret = undefined;
        updatedConfig.username = undefined;
        updatedConfig.password = undefined;
        if (accessToken) {
          updatedConfig.access_token = accessToken;
        }
      } else if (newMethod === 'service_principal') {
        updatedConfig.access_token = undefined;
        updatedConfig.username = undefined;
        updatedConfig.password = undefined;
      } else if (newMethod === 'service_account') {
        updatedConfig.access_token = undefined;
        updatedConfig.client_secret = undefined;
      }

      onChange(updatedConfig);
    }
  };

  React.useEffect(() => {
    if (value.auth_method === 'user_oauth' && accessToken) {
      onChange({
        ...value,
        access_token: accessToken
      });
    }
  }, [accessToken, value.auth_method]);

  const handleModeChange = (_event: React.MouseEvent<HTMLElement>, newMode: 'static' | 'dynamic' | null) => {
    if (newMode !== null) {
      const updatedConfig: PowerBIDaxConfig = {
        ...value,
        mode: newMode
      };

      if (newMode === 'dynamic') {
        updatedConfig.workspace_id = '{workspace_id}';
        updatedConfig.dataset_id = '{dataset_id}';
        updatedConfig.report_id = '{report_id}';
        updatedConfig.tenant_id = '{tenant_id}';
        updatedConfig.client_id = '{client_id}';
        updatedConfig.client_secret = '{client_secret}';
        updatedConfig.access_token = '{access_token}';
        updatedConfig.llm_workspace_url = '{llm_workspace_url}';
        updatedConfig.llm_token = '{llm_token}';
        updatedConfig.include_visual_references = value.include_visual_references !== false;
        updatedConfig.max_dax_retries = value.max_dax_retries || '5';
        updatedConfig.output_format = value.output_format || 'json';
      }

      onChange(updatedConfig);
    }
  };

  const mode = value.mode || 'static';
  const authMethod = value.auth_method || 'service_principal';

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
      {/* Mode Toggle */}
      <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
        <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>
          Configuration Mode
        </Typography>
        <ToggleButtonGroup
          value={mode}
          exclusive
          onChange={handleModeChange}
          disabled={disabled}
          fullWidth
          size="small"
        >
          <ToggleButton value="static">
            <Box sx={{ textAlign: 'center', py: 0.5 }}>
              <Typography variant="body2" sx={{ fontWeight: 600 }}>
                Static
              </Typography>
              <Typography variant="caption" color="text.secondary">
                Configure all parameters in UI
              </Typography>
            </Box>
          </ToggleButton>
          <ToggleButton value="dynamic">
            <Box sx={{ textAlign: 'center', py: 0.5 }}>
              <Typography variant="body2" sx={{ fontWeight: 600 }}>
                Dynamic
              </Typography>
              <Typography variant="caption" color="text.secondary">
                Parameters from execution inputs
              </Typography>
            </Box>
          </ToggleButton>
        </ToggleButtonGroup>
      </Box>

      <Divider sx={{ my: 1 }} />

      {/* Dynamic Mode Info */}
      {mode === 'dynamic' && (
        <Alert severity="info" sx={{ mb: 1 }}>
          <Typography variant="body2" sx={{ fontWeight: 600, mb: 1 }}>
            Dynamic Parameters Mode
          </Typography>
          <Typography variant="caption" component="div">
            Parameters will be resolved from execution inputs at runtime.
            Use this when calling from external apps (e.g., Databricks Apps).
          </Typography>

          <Box sx={{ mt: 1.5, p: 1, bgcolor: 'rgba(0,0,0,0.04)', borderRadius: 1 }}>
            <Typography variant="caption" sx={{ fontWeight: 600 }}>
              Required inputs:
            </Typography>
            <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5, mt: 0.5 }}>
              <Chip label="workspace_id" size="small" color="primary" variant="outlined" />
              <Chip label="dataset_id" size="small" color="primary" variant="outlined" />
            </Box>
            <Typography variant="caption" color="text.secondary" sx={{ mt: 0.5, display: 'block' }}>
              Optional:{' '}
              <Chip label="report_id" size="small" variant="outlined" sx={{ ml: 0.5 }} />
              <Chip label="business_mappings" size="small" variant="outlined" sx={{ ml: 0.5 }} />
              <Chip label="field_synonyms" size="small" variant="outlined" sx={{ ml: 0.5 }} />
              <Chip label="active_filters" size="small" variant="outlined" sx={{ ml: 0.5 }} />
            </Typography>
          </Box>

          <Box sx={{ mt: 1.5, p: 1, bgcolor: 'rgba(0,0,0,0.04)', borderRadius: 1 }}>
            <Typography variant="caption" sx={{ fontWeight: 600 }}>
              Authentication (choose one):
            </Typography>
            <Box sx={{ mt: 0.5 }}>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 0.5 }}>
                <Chip label="access_token" size="small" color="success" />
                <Typography variant="caption">← User OAuth (recommended for user context)</Typography>
              </Box>
              <Typography variant="caption" color="text.secondary" sx={{ pl: 1, display: 'block', mb: 0.5 }}>
                <em>or</em>
              </Typography>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                <Chip label="tenant_id" size="small" variant="outlined" />
                <Chip label="client_id" size="small" variant="outlined" />
                <Chip label="client_secret" size="small" variant="outlined" />
                <Typography variant="caption">← Service Principal</Typography>
              </Box>
            </Box>
          </Box>

          <Typography variant="caption" color="text.secondary" sx={{ mt: 1, display: 'block' }}>
            <strong>Example with OAuth:</strong>{' '}
            <code style={{ fontSize: '0.7rem' }}>
              {`{ "workspace_id": "...", "dataset_id": "...", "access_token": "eyJ..." }`}
            </code>
          </Typography>
        </Alert>
      )}

      {/* Static Mode: Full Configuration Forms */}
      {mode === 'static' && (
        <>
          {/* Power BI Configuration */}
          <Typography variant="subtitle2" color="primary" sx={{ fontWeight: 600 }}>
            Power BI Workspace Configuration
          </Typography>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
            <TextField
              label="Workspace ID"
              value={value.workspace_id || ''}
              onChange={(e) => handleFieldChange('workspace_id', e.target.value)}
              disabled={disabled}
              required
              fullWidth
              helperText="Power BI workspace ID containing the semantic model"
              size="small"
            />
            <TextField
              label="Dataset ID (Semantic Model ID)"
              value={value.dataset_id || ''}
              onChange={(e) => handleFieldChange('dataset_id', e.target.value)}
              disabled={disabled}
              required
              fullWidth
              helperText="Semantic model/dataset ID to generate DAX queries for"
              size="small"
            />
            <TextField
              label="Report ID (Optional)"
              value={value.report_id || ''}
              onChange={(e) => handleFieldChange('report_id', e.target.value)}
              disabled={disabled}
              fullWidth
              helperText="Optional report ID for visual context and filter extraction"
              size="small"
            />
          </Box>

          <Divider sx={{ my: 1 }}>
            <Typography variant="caption" color="text.secondary">
              Authentication Method
            </Typography>
          </Divider>

          {/* Auth Method Toggle */}
          <ToggleButtonGroup
            value={authMethod}
            exclusive
            onChange={handleAuthMethodChange}
            disabled={disabled}
            fullWidth
            size="small"
          >
            <ToggleButton value="service_principal">
              <Box sx={{ display: 'flex', flexDirection: 'column', alignItems: 'center', py: 0.5 }}>
                <SecurityIcon sx={{ fontSize: 18, mb: 0.5 }} />
                <Typography variant="body2" sx={{ fontWeight: 600 }}>
                  Service Principal
                </Typography>
              </Box>
            </ToggleButton>
            <ToggleButton value="service_account">
              <Box sx={{ display: 'flex', flexDirection: 'column', alignItems: 'center', py: 0.5 }}>
                <PersonIcon sx={{ fontSize: 18, mb: 0.5 }} />
                <Typography variant="body2" sx={{ fontWeight: 600 }}>
                  Service Account
                </Typography>
              </Box>
            </ToggleButton>
            <ToggleButton value="user_oauth">
              <Box sx={{ display: 'flex', flexDirection: 'column', alignItems: 'center', py: 0.5 }}>
                <LoginIcon sx={{ fontSize: 18, mb: 0.5 }} />
                <Typography variant="body2" sx={{ fontWeight: 600 }}>
                  User OAuth
                </Typography>
              </Box>
            </ToggleButton>
          </ToggleButtonGroup>

          {/* Service Principal Authentication Fields */}
          {authMethod === 'service_principal' && (
            <>
              <Alert severity="warning" variant="outlined" sx={{ mb: 1 }}>
                <Typography variant="caption">
                  <strong>Important:</strong> The Service Principal must be a <strong>member of the workspace</strong> with at least read permissions.
                </Typography>
              </Alert>

              <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                <TextField
                  label="Tenant ID"
                  value={value.tenant_id || ''}
                  onChange={(e) => handleFieldChange('tenant_id', e.target.value)}
                  disabled={disabled}
                  required
                  fullWidth
                  helperText="Azure AD tenant ID"
                  size="small"
                />
                <TextField
                  label="Client ID"
                  value={value.client_id || ''}
                  onChange={(e) => handleFieldChange('client_id', e.target.value)}
                  disabled={disabled}
                  required
                  fullWidth
                  helperText="Application/Client ID (must be workspace member)"
                  size="small"
                />
                <TextField
                  label="Client Secret"
                  value={value.client_secret || ''}
                  onChange={(e) => handleFieldChange('client_secret', e.target.value)}
                  disabled={disabled}
                  required
                  type="password"
                  fullWidth
                  helperText="Client secret for service principal"
                  size="small"
                />
              </Box>
            </>
          )}

          {/* Service Account Authentication Fields */}
          {authMethod === 'service_account' && (
            <>
              <Alert severity="info" variant="outlined" sx={{ mb: 1 }}>
                <Typography variant="caption">
                  <strong>Service Account:</strong> Use a user account (username + password) instead of Service Principal.
                  Requires an Azure AD app with delegated permissions.
                </Typography>
              </Alert>

              <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                <TextField
                  label="Tenant ID"
                  value={value.tenant_id || ''}
                  onChange={(e) => handleFieldChange('tenant_id', e.target.value)}
                  disabled={disabled}
                  required
                  fullWidth
                  helperText="Azure AD tenant ID"
                  size="small"
                />
                <TextField
                  label="Client ID"
                  value={value.client_id || ''}
                  onChange={(e) => handleFieldChange('client_id', e.target.value)}
                  disabled={disabled}
                  required
                  fullWidth
                  helperText="Azure AD application Client ID (with delegated permissions)"
                  size="small"
                />
                <TextField
                  label="Username (UPN)"
                  value={value.username || ''}
                  onChange={(e) => handleFieldChange('username', e.target.value)}
                  disabled={disabled}
                  required
                  fullWidth
                  helperText="Service account email/UPN (e.g., user@domain.com)"
                  size="small"
                />
                <TextField
                  label="Password"
                  value={value.password || ''}
                  onChange={(e) => handleFieldChange('password', e.target.value)}
                  disabled={disabled}
                  required
                  type="password"
                  fullWidth
                  helperText="Service account password"
                  size="small"
                />
              </Box>
            </>
          )}

          {/* User OAuth Authentication */}
          {authMethod === 'user_oauth' && (
            <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
              <Alert severity="info" variant="outlined">
                <Typography variant="caption" component="div">
                  <strong>Option 1:</strong> If you have an Azure AD app, enter its Client ID and sign in.
                  <br />
                  <strong>Option 2:</strong> Get a token from{' '}
                  <a
                    href="https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/execute-queries?tryIt=true"
                    target="_blank"
                    rel="noopener noreferrer"
                  >
                    Microsoft's API docs (Try It)
                  </a>
                  {' '}and paste it below.
                </Typography>
              </Alert>

              <TextField
                label="OAuth Client ID (Optional)"
                value={value.oauth_client_id || ''}
                onChange={(e) => handleFieldChange('oauth_client_id', e.target.value)}
                disabled={disabled || isAuthenticated}
                fullWidth
                helperText="Your Azure AD app Client ID for OAuth sign-in"
                size="small"
              />

              <TextField
                label="Access Token (Alternative)"
                value={value.access_token || ''}
                onChange={(e) => handleFieldChange('access_token', e.target.value)}
                disabled={disabled || isAuthenticated}
                fullWidth
                type="password"
                helperText="Paste a token from Microsoft's Try It page"
                size="small"
              />

              {oauthError && (
                <Alert severity="error" variant="outlined">
                  <Typography variant="caption">{oauthError}</Typography>
                </Alert>
              )}

              {!isAuthenticated && !value.access_token ? (
                <Button
                  variant="contained"
                  onClick={signIn}
                  disabled={disabled || oauthLoading || !value.oauth_client_id}
                  startIcon={<LoginIcon />}
                  fullWidth
                >
                  {oauthLoading ? 'Signing in...' : 'Sign in with Microsoft'}
                </Button>
              ) : value.access_token && !isAuthenticated ? (
                <Alert severity="success" icon={<CheckCircleIcon />}>
                  <Typography variant="body2">Access token provided</Typography>
                </Alert>
              ) : (
                <>
                  <Alert severity="success" icon={<CheckCircleIcon />}>
                    <Typography variant="body2">
                      Signed in as <strong>{userEmail || 'User'}</strong>
                    </Typography>
                  </Alert>
                  <Button
                    variant="outlined"
                    onClick={signOut}
                    disabled={disabled}
                    size="small"
                  >
                    Sign Out
                  </Button>
                </>
              )}
            </Box>
          )}
        </>
      )}

      {/* LLM Configuration */}
      <Accordion sx={{ mt: 1 }}>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="subtitle2">LLM Configuration</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
            <TextField
              label="LLM Workspace URL"
              value={value.llm_workspace_url || ''}
              onChange={(e) => handleFieldChange('llm_workspace_url', e.target.value)}
              disabled={disabled}
              fullWidth
              helperText="Databricks workspace URL for LLM endpoint"
              size="small"
            />
            <TextField
              label="LLM Token"
              value={value.llm_token || ''}
              onChange={(e) => handleFieldChange('llm_token', e.target.value)}
              disabled={disabled}
              fullWidth
              type="password"
              helperText="Token for LLM endpoint — leave empty to use Databricks auth"
              size="small"
            />
            <TextField
              label="LLM Model"
              value={value.llm_model || 'databricks-claude-sonnet-4'}
              onChange={(e) => handleFieldChange('llm_model', e.target.value)}
              disabled={disabled}
              fullWidth
              helperText="Model name for DAX generation (e.g., databricks-claude-sonnet-4)"
              size="small"
            />
          </Box>
        </AccordionDetails>
      </Accordion>

      {/* Context Enrichment */}
      <Accordion>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="subtitle2">Context Enrichment</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
            <TextField
              label="Business Mappings"
              value={value.business_mappings || ''}
              onChange={(e) => handleFieldChange('business_mappings', e.target.value)}
              disabled={disabled}
              fullWidth
              multiline
              minRows={3}
              helperText='JSON object mapping business terms to DAX measures (e.g., {"revenue": "[Total Revenue]"})'
              size="small"
            />
            <TextField
              label="Field Synonyms"
              value={value.field_synonyms || ''}
              onChange={(e) => handleFieldChange('field_synonyms', e.target.value)}
              disabled={disabled}
              fullWidth
              multiline
              minRows={3}
              helperText='JSON object mapping synonyms to actual field names (e.g., {"sales": "Revenue"})'
              size="small"
            />
            <TextField
              label="Active Filters"
              value={value.active_filters || ''}
              onChange={(e) => handleFieldChange('active_filters', e.target.value)}
              disabled={disabled}
              fullWidth
              multiline
              minRows={2}
              helperText='JSON array of pre-applied filters (e.g., [{"table": "Date", "column": "Year", "value": "2025"}])'
              size="small"
            />
            <TextField
              label="Visible Tables"
              value={value.visible_tables || ''}
              onChange={(e) => handleFieldChange('visible_tables', e.target.value)}
              disabled={disabled}
              fullWidth
              multiline
              minRows={2}
              helperText='JSON array of tables visible in current report context (e.g., ["Sales", "Products"])'
              size="small"
            />
          </Box>
        </AccordionDetails>
      </Accordion>

      {/* Output Options */}
      <Accordion>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="subtitle2">Output Options</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
            <FormControlLabel
              control={
                <Checkbox
                  checked={value.include_visual_references !== false}
                  onChange={(e) => handleFieldChange('include_visual_references', e.target.checked)}
                  disabled={disabled}
                  size="small"
                />
              }
              label={<Typography variant="body2">Include visual references in DAX queries</Typography>}
            />
            <TextField
              label="Max DAX Retries"
              value={value.max_dax_retries || '5'}
              onChange={(e) => handleFieldChange('max_dax_retries', e.target.value)}
              disabled={disabled}
              fullWidth
              type="number"
              helperText="Maximum retry attempts for DAX query generation"
              size="small"
              inputProps={{ min: 1, max: 20 }}
            />
            <FormControl fullWidth size="small" sx={{ mt: 1 }}>
              <InputLabel>Output Format</InputLabel>
              <Select
                value={value.output_format || 'json'}
                onChange={(e) => handleFieldChange('output_format', e.target.value)}
                label="Output Format"
                disabled={disabled}
              >
                <MenuItem value="json">JSON</MenuItem>
                <MenuItem value="markdown">Markdown</MenuItem>
              </Select>
            </FormControl>
          </Box>
        </AccordionDetails>
      </Accordion>

      {/* Info about what the tool does */}
      <Alert severity="info" variant="outlined" sx={{ mt: 1 }}>
        <Typography variant="body2" sx={{ fontWeight: 600, mb: 0.5 }}>
          What this tool does:
        </Typography>
        <Typography variant="caption">
          Generates context-aware DAX queries using an LLM, grounded in the actual semantic model schema.
          Fetches model metadata via the Power BI Execute Queries API, then constructs prompts with
          table/column/measure context for accurate DAX generation.
        </Typography>
        <Box component="ul" sx={{ pl: 2, mt: 0.5, mb: 0, fontSize: '0.75rem' }}>
          <li>Auto-fetches semantic model schema for context</li>
          <li>Supports business term mappings and field synonyms</li>
          <li>Includes active filter and visible table context</li>
          <li>Validates and retries DAX queries against the model</li>
        </Box>
      </Alert>
    </Box>
  );
};
