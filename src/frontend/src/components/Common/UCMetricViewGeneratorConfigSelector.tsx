/**
 * UC Metric View Generator Configuration Selector Component
 *
 * Provides configuration UI for the UC Metric View Generator tool (Tool 86).
 * Generates Unity Catalog Metric Views from Power BI measures, M-Query, and relationships.
 * Supports both API mode (live PBI extraction) and JSON mode (paste pre-extracted data).
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
  Button
} from '@mui/material';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import LoginIcon from '@mui/icons-material/Login';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import SecurityIcon from '@mui/icons-material/Security';
import PersonIcon from '@mui/icons-material/Person';
import UploadFileIcon from '@mui/icons-material/UploadFile';
import ClearIcon from '@mui/icons-material/Clear';
import { usePowerBIOAuth } from '../../hooks/usePowerBIOAuth';

// Authentication method type
export type PowerBIAuthMethod = 'service_principal' | 'service_account' | 'user_oauth';

export interface UCMetricViewGeneratorConfig {
  // Input mode
  mode?: 'api' | 'json';
  // PBI API config (API mode)
  workspace_id?: string;
  dataset_id?: string;
  auth_method?: PowerBIAuthMethod;
  tenant_id?: string;
  client_id?: string;
  client_secret?: string;
  username?: string;
  password?: string;
  oauth_client_id?: string;
  access_token?: string;
  pbi_api_base_url?: string;
  // JSON mode (paste JSONs directly)
  measures_json?: string;
  mquery_json?: string;
  relationships_json?: string;
  scan_data_json?: string;
  config_json?: string;
  // Target
  catalog?: string;
  schema_name?: string;
  // Behavior
  inner_dim_joins?: boolean;
  unflatten_tables?: boolean;
  use_llm_fallback?: boolean;
  llm_model?: string;
  llm_workspace_url?: string;
  llm_token?: string;
  // Index signature for compatibility
  [key: string]: string | boolean | undefined;
}

interface UCMetricViewGeneratorConfigSelectorProps {
  value: UCMetricViewGeneratorConfig;
  onChange: (config: UCMetricViewGeneratorConfig) => void;
  disabled?: boolean;
}

export const UCMetricViewGeneratorConfigSelector: React.FC<UCMetricViewGeneratorConfigSelectorProps> = ({
  value = {},
  onChange,
  disabled = false
}) => {
  // OAuth hook for User OAuth authentication
  const { accessToken, isAuthenticated, signIn, signOut, userEmail, isLoading: oauthLoading, error: oauthError } = usePowerBIOAuth({
    clientId: value.oauth_client_id || ''
  });

  const handleFieldChange = (field: keyof UCMetricViewGeneratorConfig, fieldValue: string | boolean) => {
    onChange({
      ...value,
      [field]: fieldValue
    });
  };

  const handleAuthMethodChange = (_event: React.MouseEvent<HTMLElement>, newMethod: PowerBIAuthMethod | null) => {
    if (newMethod !== null) {
      const updatedConfig: UCMetricViewGeneratorConfig = {
        ...value,
        auth_method: newMethod
      };

      // Clear credentials when switching methods
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

  // Update access token when OAuth state changes
  React.useEffect(() => {
    if (value.auth_method === 'user_oauth' && accessToken) {
      onChange({
        ...value,
        access_token: accessToken
      });
    }
  }, [accessToken, value.auth_method]);

  const handleModeChange = (_event: React.MouseEvent<HTMLElement>, newMode: 'api' | 'json' | null) => {
    if (newMode !== null) {
      const updatedConfig: UCMetricViewGeneratorConfig = {
        ...value,
        mode: newMode
      };

      // Clear irrelevant fields when switching modes
      if (newMode === 'json') {
        updatedConfig.workspace_id = undefined;
        updatedConfig.dataset_id = undefined;
        updatedConfig.auth_method = undefined;
        updatedConfig.tenant_id = undefined;
        updatedConfig.client_id = undefined;
        updatedConfig.client_secret = undefined;
        updatedConfig.username = undefined;
        updatedConfig.password = undefined;
        updatedConfig.access_token = undefined;
        updatedConfig.pbi_api_base_url = undefined;
      } else if (newMode === 'api') {
        updatedConfig.measures_json = undefined;
        updatedConfig.mquery_json = undefined;
        updatedConfig.relationships_json = undefined;
        updatedConfig.scan_data_json = undefined;
        // Keep config_json — it's an override that works in both modes
        updatedConfig.auth_method = updatedConfig.auth_method || 'service_principal';
      }

      onChange(updatedConfig);
    }
  };

  const handleFileUpload = (field: keyof UCMetricViewGeneratorConfig) => (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = (e) => {
      const text = e.target?.result as string;
      try {
        // Validate it's valid JSON
        JSON.parse(text);
        handleFieldChange(field, text);
      } catch {
        // Still set it — user can fix later
        handleFieldChange(field, text);
      }
    };
    reader.readAsText(file);
    // Reset the input so re-uploading the same file works
    event.target.value = '';
  };

  const mode = value.mode || 'api';
  const authMethod = value.auth_method || 'service_principal';

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
      {/* Mode Toggle */}
      <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
        <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>
          Input Mode
        </Typography>
        <ToggleButtonGroup
          value={mode}
          exclusive
          onChange={handleModeChange}
          disabled={disabled}
          fullWidth
          size="small"
        >
          <ToggleButton value="api">
            <Box sx={{ textAlign: 'center', py: 0.5 }}>
              <Typography variant="body2" sx={{ fontWeight: 600 }}>
                API
              </Typography>
              <Typography variant="caption" color="text.secondary">
                Extract live from Power BI
              </Typography>
            </Box>
          </ToggleButton>
          <ToggleButton value="json">
            <Box sx={{ textAlign: 'center', py: 0.5 }}>
              <Typography variant="body2" sx={{ fontWeight: 600 }}>
                JSON
              </Typography>
              <Typography variant="caption" color="text.secondary">
                Paste pre-extracted JSON data
              </Typography>
            </Box>
          </ToggleButton>
        </ToggleButtonGroup>
      </Box>

      <Divider sx={{ my: 1 }} />

      {/* API Mode: PBI Authentication */}
      {mode === 'api' && (
        <>
          <Typography variant="subtitle2" sx={{ fontWeight: 600, color: 'rgb(156, 39, 176)' }}>
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
              helperText="Power BI workspace ID"
              size="small"
            />
            <TextField
              label="Dataset ID (Optional)"
              value={value.dataset_id || ''}
              onChange={(e) => handleFieldChange('dataset_id', e.target.value)}
              disabled={disabled}
              fullWidth
              helperText="Specific dataset to extract (leave empty to scan all)"
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
                helperText="Application/Client ID"
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
          )}

          {/* Service Account Authentication Fields */}
          {authMethod === 'service_account' && (
            <>
              <Alert severity="info" variant="outlined" sx={{ mb: 1 }}>
                <Typography variant="caption">
                  <strong>Service Account:</strong> Use a user account (username + password) instead of Service Principal.
                  Useful when Service Principal doesn't have sufficient permissions.
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
                    href="https://learn.microsoft.com/en-us/rest/api/power-bi/admin/workspace-info-get-scan-result?tryIt=true"
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

          <TextField
            label="PBI API Base URL (Optional)"
            value={value.pbi_api_base_url || ''}
            onChange={(e) => handleFieldChange('pbi_api_base_url', e.target.value)}
            disabled={disabled}
            fullWidth
            helperText="Custom Power BI API base URL (leave empty for default)"
            size="small"
          />
        </>
      )}

      {/* JSON Mode: Paste JSON Inputs */}
      {mode === 'json' && (
        <>
          <Typography variant="subtitle2" sx={{ fontWeight: 600, color: 'rgb(156, 39, 176)' }}>
            JSON Input Data
          </Typography>
          <Alert severity="info" variant="outlined" sx={{ mb: 1 }}>
            <Typography variant="caption">
              Paste pre-extracted JSON data from Tools 73/74/75.
              These are typically produced by the PBI extraction pipeline.
              Leave empty if provided by a prior task in the crew.
            </Typography>
          </Alert>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
            <TextField
              label="Measures JSON"
              value={value.measures_json || ''}
              onChange={(e) => handleFieldChange('measures_json', e.target.value)}
              disabled={disabled}
              fullWidth
              multiline
              rows={3}
              helperText="JSON string of extracted measures"
              size="small"
            />
            <TextField
              label="M-Query JSON"
              value={value.mquery_json || ''}
              onChange={(e) => handleFieldChange('mquery_json', e.target.value)}
              disabled={disabled}
              fullWidth
              multiline
              rows={3}
              helperText="JSON string of M-Query expressions"
              size="small"
            />
            <TextField
              label="Relationships JSON"
              value={value.relationships_json || ''}
              onChange={(e) => handleFieldChange('relationships_json', e.target.value)}
              disabled={disabled}
              fullWidth
              multiline
              rows={3}
              helperText="JSON string of table relationships"
              size="small"
            />
            <TextField
              label="Scan Data JSON"
              value={value.scan_data_json || ''}
              onChange={(e) => handleFieldChange('scan_data_json', e.target.value)}
              disabled={disabled}
              fullWidth
              multiline
              rows={3}
              helperText="JSON string of scan/schema data"
              size="small"
            />
            {/* Config JSON Override moved to dedicated accordion (visible in both modes) */}
          </Box>
        </>
      )}

      {/* Pipeline Config Override — available in BOTH modes */}
      <Accordion sx={{ mt: 1 }}>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="subtitle2">
            Pipeline Config Override
            {value.config_json ? ' (loaded)' : ''}
          </Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Alert severity="info" variant="outlined" sx={{ mb: 2 }}>
            <Typography variant="caption">
              Upload or paste a reference <code>pipeline_config.json</code> to override auto-generated config.
              This provides <strong>filter_sets</strong>, <strong>switch_decompositions</strong>, <strong>manual_overrides</strong>,
              and other keys the UCMV generator needs for complex measures.
            </Typography>
          </Alert>
          <Box sx={{ display: 'flex', gap: 1, mb: 1 }}>
            <Button
              variant="outlined"
              component="label"
              startIcon={<UploadFileIcon />}
              disabled={disabled}
              size="small"
            >
              Upload JSON
              <input
                type="file"
                accept=".json,application/json"
                hidden
                onChange={handleFileUpload('config_json')}
              />
            </Button>
            {value.config_json && (
              <Button
                variant="outlined"
                color="error"
                startIcon={<ClearIcon />}
                onClick={() => handleFieldChange('config_json', '')}
                disabled={disabled}
                size="small"
              >
                Clear
              </Button>
            )}
            {value.config_json && (
              <Typography variant="caption" color="success.main" sx={{ alignSelf: 'center' }}>
                {(() => {
                  try {
                    const parsed = JSON.parse(value.config_json);
                    const keys = Object.keys(parsed);
                    return `${keys.length} config keys loaded`;
                  } catch {
                    return 'Invalid JSON';
                  }
                })()}
              </Typography>
            )}
          </Box>
          <TextField
            label="Config JSON"
            value={value.config_json || ''}
            onChange={(e) => handleFieldChange('config_json', e.target.value)}
            disabled={disabled}
            fullWidth
            multiline
            rows={6}
            helperText="Pipeline config JSON (filter_sets, switch_decompositions, manual_overrides, etc.)"
            size="small"
            InputProps={{
              sx: { fontFamily: 'monospace', fontSize: '0.75rem' }
            }}
          />
        </AccordionDetails>
      </Accordion>

      {/* Target Configuration */}
      <Accordion sx={{ mt: 1 }} defaultExpanded>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="subtitle2">Target Configuration</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Box sx={{ display: 'flex', gap: 2 }}>
            <TextField
              label="Target Catalog"
              value={value.catalog || ''}
              onChange={(e) => handleFieldChange('catalog', e.target.value)}
              disabled={disabled}
              fullWidth
              size="small"
              helperText="Unity Catalog name (e.g., david_test_metrics)"
            />
            <TextField
              label="Target Schema"
              value={value.schema_name || ''}
              onChange={(e) => handleFieldChange('schema_name', e.target.value)}
              disabled={disabled}
              fullWidth
              size="small"
              helperText="Schema for generated metric views"
            />
          </Box>
        </AccordionDetails>
      </Accordion>

      {/* Advanced Options */}
      <Accordion sx={{ mt: 1 }}>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="subtitle2">Advanced Options</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
            {/* Behavior Toggles */}
            <Typography variant="subtitle2" color="secondary" sx={{ fontWeight: 600 }}>
              Generation Behavior
            </Typography>
            <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0 }}>
              <FormControlLabel
                control={
                  <Checkbox
                    checked={value.inner_dim_joins || false}
                    onChange={(e) => handleFieldChange('inner_dim_joins', e.target.checked)}
                    disabled={disabled}
                    size="small"
                  />
                }
                label={<Typography variant="body2">Inner dimension joins (INNER JOIN instead of LEFT)</Typography>}
              />
              <FormControlLabel
                control={
                  <Checkbox
                    checked={value.unflatten_tables || false}
                    onChange={(e) => handleFieldChange('unflatten_tables', e.target.checked)}
                    disabled={disabled}
                    size="small"
                  />
                }
                label={<Typography variant="body2">Unflatten tables (expand nested columns)</Typography>}
              />
            </Box>

            <Divider sx={{ my: 1 }} />

            {/* LLM Fallback Configuration */}
            <Typography variant="subtitle2" color="secondary" sx={{ fontWeight: 600 }}>
              LLM Fallback Settings
            </Typography>
            <FormControlLabel
              control={
                <Checkbox
                  checked={value.use_llm_fallback || false}
                  onChange={(e) => handleFieldChange('use_llm_fallback', e.target.checked)}
                  disabled={disabled}
                />
              }
              label="Use LLM fallback for complex DAX-to-SQL conversions"
            />
            {value.use_llm_fallback && (
              <>
                <TextField
                  label="LLM Model"
                  value={value.llm_model || 'databricks-claude-sonnet-4'}
                  onChange={(e) => handleFieldChange('llm_model', e.target.value)}
                  disabled={disabled}
                  fullWidth
                  helperText="Model endpoint for LLM conversion"
                  size="small"
                />
                <TextField
                  label="LLM Workspace URL (Optional)"
                  value={value.llm_workspace_url || ''}
                  onChange={(e) => handleFieldChange('llm_workspace_url', e.target.value)}
                  disabled={disabled}
                  fullWidth
                  helperText="Databricks workspace URL for LLM (uses default if empty)"
                  size="small"
                />
                <TextField
                  label="LLM Token (DAPI)"
                  value={value.llm_token || ''}
                  onChange={(e) => handleFieldChange('llm_token', e.target.value)}
                  disabled={disabled}
                  fullWidth
                  type="password"
                  helperText="Databricks API token (PAT) for LLM access (uses DATABRICKS_TOKEN env var if empty)"
                  size="small"
                />
              </>
            )}
          </Box>
        </AccordionDetails>
      </Accordion>

      {/* Info about what the tool does */}
      <Alert severity="info" variant="outlined" sx={{ mt: 1 }}>
        <Typography variant="body2" sx={{ fontWeight: 600, mb: 0.5 }}>
          What this tool does:
        </Typography>
        <Typography variant="caption">
          Generates Unity Catalog Metric Views from Power BI semantic model metadata.
          Converts DAX measures into SQL metric definitions with proper dimension/measure separation.
        </Typography>
        <Box component="ul" sx={{ pl: 2, mt: 0.5, mb: 0, fontSize: '0.75rem' }}>
          <li>Converts DAX measures to UC Metric View YAML definitions</li>
          <li>Maps PBI table relationships to metric view joins</li>
          <li>Handles dimension/fact table classification</li>
          <li>Supports LLM fallback for complex DAX expressions</li>
          <li>Generates CREATE METRIC VIEW DDL statements</li>
        </Box>
      </Alert>
    </Box>
  );
};
