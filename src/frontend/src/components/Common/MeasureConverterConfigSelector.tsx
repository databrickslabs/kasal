/**
 * Measure Converter Configuration Selector Component
 *
 * Provides FROM/TO dropdown selection for the Measure Conversion Pipeline tool.
 * Dynamically shows configuration fields based on selected inbound/outbound formats.
 */

import React from 'react';
import {
  Box,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Typography,
  TextField,
  FormControlLabel,
  Checkbox,
  SelectChangeEvent,
  Divider,
  ToggleButtonGroup,
  ToggleButton,
  Alert,
  Chip,
  Button
} from '@mui/material';
import LoginIcon from '@mui/icons-material/Login';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import SecurityIcon from '@mui/icons-material/Security';
import PersonIcon from '@mui/icons-material/Person';
import { usePowerBIOAuth } from '../../hooks/usePowerBIOAuth';

// Authentication method type
export type PowerBIAuthMethod = 'service_principal' | 'service_account' | 'user_oauth';

export interface MeasureConverterConfig {
  // Configuration mode
  mode?: 'static' | 'dynamic';
  inbound_connector?: string;
  outbound_format?: string;
  // Power BI inbound params
  powerbi_semantic_model_id?: string;
  powerbi_group_id?: string;
  // Authentication method
  powerbi_auth_method?: PowerBIAuthMethod;
  // Power BI Service Principal authentication
  powerbi_tenant_id?: string;
  powerbi_client_id?: string;
  powerbi_client_secret?: string;
  // Power BI Service Account authentication
  powerbi_username?: string;
  powerbi_password?: string;
  // Power BI User OAuth authentication
  powerbi_oauth_client_id?: string; // Azure AD app client ID for OAuth
  powerbi_access_token?: string;
  // Power BI other settings
  powerbi_include_hidden?: boolean;
  powerbi_filter_pattern?: string;
  // YAML inbound params
  yaml_content?: string;
  yaml_file_path?: string;
  // SQL outbound params
  sql_dialect?: string;
  sql_include_comments?: boolean;
  sql_process_structures?: boolean;
  // DAX outbound params
  dax_process_structures?: boolean;
  // General
  definition_name?: string;
  // Index signature for compatibility with Record<string, unknown>
  [key: string]: string | boolean | undefined;
}

interface MeasureConverterConfigSelectorProps {
  value: MeasureConverterConfig;
  onChange: (config: MeasureConverterConfig) => void;
  disabled?: boolean;
}

export const MeasureConverterConfigSelector: React.FC<MeasureConverterConfigSelectorProps> = ({
  value = {},
  onChange,
  disabled = false
}) => {
  // OAuth hook for User OAuth authentication - pass the client ID from config
  const { accessToken, isAuthenticated, signIn, signOut, userEmail, isLoading: oauthLoading, error: oauthError } = usePowerBIOAuth({
    clientId: value.powerbi_oauth_client_id || ''
  });

  const handleFieldChange = (field: keyof MeasureConverterConfig, fieldValue: string | boolean) => {
    onChange({
      ...value,
      [field]: fieldValue
    });
  };

  const handleSelectChange = (field: keyof MeasureConverterConfig) => (event: SelectChangeEvent) => {
    const newValue = event.target.value;

    // Special handling for inbound_connector
    if (field === 'inbound_connector' && newValue === 'powerbi') {
      // When Power BI is selected, ensure auth_method has a default value
      const updatedConfig = {
        ...value,
        [field]: newValue,
        powerbi_auth_method: value.powerbi_auth_method || 'service_principal'
      };
      onChange(updatedConfig);
    } else {
      handleFieldChange(field, newValue);
    }
  };

  const handleAuthMethodChange = (_event: React.MouseEvent<HTMLElement>, newMethod: PowerBIAuthMethod | null) => {
    if (newMethod !== null) {
      const updatedConfig: MeasureConverterConfig = {
        ...value,
        powerbi_auth_method: newMethod
      };

      // Clear credentials when switching methods
      if (newMethod === 'user_oauth') {
        updatedConfig.powerbi_tenant_id = undefined;
        updatedConfig.powerbi_client_id = undefined;
        updatedConfig.powerbi_client_secret = undefined;
        updatedConfig.powerbi_username = undefined;
        updatedConfig.powerbi_password = undefined;
        // Set access token if authenticated
        if (accessToken) {
          updatedConfig.powerbi_access_token = accessToken;
        }
      } else if (newMethod === 'service_principal') {
        updatedConfig.powerbi_access_token = undefined;
        updatedConfig.powerbi_username = undefined;
        updatedConfig.powerbi_password = undefined;
      } else if (newMethod === 'service_account') {
        updatedConfig.powerbi_access_token = undefined;
        updatedConfig.powerbi_client_secret = undefined;
      }

      onChange(updatedConfig);
    }
  };

  // Update access token when OAuth state changes
  React.useEffect(() => {
    if (value.powerbi_auth_method === 'user_oauth' && accessToken) {
      onChange({
        ...value,
        powerbi_access_token: accessToken
      });
    }
  }, [accessToken, value.powerbi_auth_method]);

  const handleModeChange = (_event: React.MouseEvent<HTMLElement>, newMode: 'static' | 'dynamic' | null) => {
    if (newMode !== null) {
      const updatedConfig: MeasureConverterConfig = {
        ...value,
        mode: newMode
      };

      // When switching to dynamic mode, auto-populate placeholders
      if (newMode === 'dynamic') {
        // Set inbound connector to powerbi (fixed for external app)
        updatedConfig.inbound_connector = 'powerbi';

        // Set all Power BI parameters to placeholders
        updatedConfig.powerbi_semantic_model_id = '{dataset_id}';
        updatedConfig.powerbi_group_id = '{workspace_id}';
        // Auth can be either SPN or access_token in dynamic mode
        updatedConfig.powerbi_tenant_id = '{tenant_id}';
        updatedConfig.powerbi_client_id = '{client_id}';
        updatedConfig.powerbi_client_secret = '{client_secret}';
        updatedConfig.powerbi_access_token = '{access_token}';

        // Set outbound format to placeholder
        updatedConfig.outbound_format = '{target}';

        // IMPORTANT: Clear YAML fields - not used in dynamic Power BI mode
        updatedConfig.yaml_content = undefined;
        updatedConfig.yaml_file_path = undefined;

        // Clear other Power BI fields that aren't needed
        updatedConfig.powerbi_filter_pattern = undefined;
      }

      onChange(updatedConfig);
    }
  };

  const inboundConnector = value.inbound_connector || '';
  const outboundFormat = value.outbound_format || '';
  const mode = value.mode || 'static';
  const authMethod = value.powerbi_auth_method || 'service_principal';

  // CRITICAL: Initialize auth_method on component mount if Power BI is selected but auth_method is missing
  // This handles existing crews that were created before auth_method was added
  React.useEffect(() => {
    if (inboundConnector === 'powerbi' && !value.powerbi_auth_method) {
      // Only set default on mount, not on every render
      onChange({
        ...value,
        powerbi_auth_method: 'service_principal'
      });
    }
  }, []); // Empty deps = run once on mount

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
              <Chip label="dataset_id" size="small" color="primary" variant="outlined" />
              <Chip label="workspace_id" size="small" color="primary" variant="outlined" />
              <Chip label="target" size="small" color="secondary" variant="outlined" />
            </Box>
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
              {`{ "dataset_id": "...", "workspace_id": "...", "target": "sql", "access_token": "eyJ..." }`}
            </code>
          </Typography>
        </Alert>
      )}

      {/* FROM/TO Selection - Only in Static Mode */}
      {mode === 'static' && (
        <Box sx={{ display: 'flex', gap: 2 }}>
          <FormControl fullWidth required disabled={disabled}>
            <InputLabel>FROM (Source)</InputLabel>
            <Select
              value={inboundConnector}
              onChange={handleSelectChange('inbound_connector')}
              label="FROM (Source)"
            >
              <MenuItem value="powerbi">
                <Box>
                  <Typography>Power BI</Typography>
                  <Typography variant="caption" color="text.secondary">
                    Extract measures from Power BI datasets
                  </Typography>
                </Box>
              </MenuItem>
              <MenuItem value="yaml">
                <Box>
                  <Typography>YAML</Typography>
                  <Typography variant="caption" color="text.secondary">
                    Load measures from YAML definition files
                  </Typography>
                </Box>
              </MenuItem>
            </Select>
          </FormControl>

          <FormControl fullWidth required disabled={disabled}>
            <InputLabel>TO (Target)</InputLabel>
            <Select
              value={outboundFormat}
              onChange={handleSelectChange('outbound_format')}
              label="TO (Target)"
            >
              <MenuItem value="dax">
                <Box>
                  <Typography>DAX</Typography>
                  <Typography variant="caption" color="text.secondary">
                    Power BI / Analysis Services measures
                  </Typography>
                </Box>
              </MenuItem>
              <MenuItem value="sql">
                <Box>
                  <Typography>SQL</Typography>
                  <Typography variant="caption" color="text.secondary">
                    SQL queries (multiple dialects)
                  </Typography>
                </Box>
              </MenuItem>
              <MenuItem value="uc_metrics">
                <Box>
                  <Typography>UC Metrics</Typography>
                  <Typography variant="caption" color="text.secondary">
                    Databricks Unity Catalog Metrics Store
                  </Typography>
                </Box>
              </MenuItem>
            </Select>
          </FormControl>
        </Box>
      )}

      {/* Dynamic Mode: Show FROM/TO as fixed values */}
      {mode === 'dynamic' && (
        <Alert severity="info" variant="outlined">
          <Typography variant="body2" sx={{ mb: 1 }}>
            <strong>Source & Target Configuration:</strong>
          </Typography>
          <Box sx={{ display: 'flex', gap: 2, alignItems: 'center' }}>
            <Chip label="FROM: Power BI" color="primary" />
            <Typography>→</Typography>
            <Chip label="TO: {target}" color="secondary" />
          </Box>
          <Typography variant="caption" color="text.secondary" sx={{ mt: 1, display: 'block' }}>
            Source is fixed to Power BI. Target format will be determined by the <code>target</code> execution input.
          </Typography>
        </Alert>
      )}

      {/* Inbound Configuration */}
      {inboundConnector && (
        <>
          <Divider sx={{ my: 1 }} />
          <Typography variant="subtitle2" color="primary" sx={{ fontWeight: 600 }}>
            Source Configuration ({inboundConnector.toUpperCase()})
          </Typography>

          {/* Dynamic Mode: Show simplified parameter info */}
          {mode === 'dynamic' && inboundConnector === 'powerbi' && (
            <Alert severity="success" variant="outlined">
              <Typography variant="body2" sx={{ mb: 1 }}>
                <strong>Power BI parameters configured as placeholders:</strong>
              </Typography>
              <Box component="ul" sx={{ pl: 2, mb: 0 }}>
                <li><code>powerbi_semantic_model_id</code> → <Chip label="{dataset_id}" size="small" /></li>
                <li><code>powerbi_group_id</code> → <Chip label="{workspace_id}" size="small" /></li>
              </Box>
              <Typography variant="body2" sx={{ mt: 1, mb: 0.5 }}>
                <strong>Authentication (provide one):</strong>
              </Typography>
              <Box component="ul" sx={{ pl: 2, mb: 0 }}>
                <li><code>powerbi_access_token</code> → <Chip label="{access_token}" size="small" color="success" /> (User OAuth)</li>
                <li><em>or</em> Service Principal:</li>
                <Box component="ul" sx={{ pl: 2, mb: 0 }}>
                  <li><code>powerbi_tenant_id</code> → <Chip label="{tenant_id}" size="small" /></li>
                  <li><code>powerbi_client_id</code> → <Chip label="{client_id}" size="small" /></li>
                  <li><code>powerbi_client_secret</code> → <Chip label="{client_secret}" size="small" /></li>
                </Box>
              </Box>
              <Typography variant="caption" color="text.secondary" sx={{ mt: 1, display: 'block' }}>
                These values will be provided when executing the crew via the <code>inputs</code> parameter.
              </Typography>
            </Alert>
          )}

          {/* Static Mode: Show full configuration forms */}
          {mode === 'static' && inboundConnector === 'powerbi' && (
            <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
              <TextField
                label="Dataset/Semantic Model ID"
                value={value.powerbi_semantic_model_id || ''}
                onChange={(e) => handleFieldChange('powerbi_semantic_model_id', e.target.value)}
                disabled={disabled}
                required
                fullWidth
                helperText="Power BI dataset identifier"
                size="small"
              />
              <TextField
                label="Workspace/Group ID"
                value={value.powerbi_group_id || ''}
                onChange={(e) => handleFieldChange('powerbi_group_id', e.target.value)}
                disabled={disabled}
                required
                fullWidth
                helperText="Power BI workspace identifier"
                size="small"
              />

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
                  <TextField
                    label="Tenant ID"
                    value={value.powerbi_tenant_id || ''}
                    onChange={(e) => handleFieldChange('powerbi_tenant_id', e.target.value)}
                    disabled={disabled}
                    fullWidth
                    helperText="Azure AD tenant ID"
                    size="small"
                  />
                  <TextField
                    label="Client ID"
                    value={value.powerbi_client_id || ''}
                    onChange={(e) => handleFieldChange('powerbi_client_id', e.target.value)}
                    disabled={disabled}
                    fullWidth
                    helperText="Application/Client ID"
                    size="small"
                  />
                  <TextField
                    label="Client Secret"
                    value={value.powerbi_client_secret || ''}
                    onChange={(e) => handleFieldChange('powerbi_client_secret', e.target.value)}
                    disabled={disabled}
                    type="password"
                    fullWidth
                    helperText="Client secret for service principal"
                    size="small"
                  />
                </>
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
                  <TextField
                    label="Tenant ID"
                    value={value.powerbi_tenant_id || ''}
                    onChange={(e) => handleFieldChange('powerbi_tenant_id', e.target.value)}
                    disabled={disabled}
                    fullWidth
                    helperText="Azure AD tenant ID"
                    size="small"
                  />
                  <TextField
                    label="Client ID"
                    value={value.powerbi_client_id || ''}
                    onChange={(e) => handleFieldChange('powerbi_client_id', e.target.value)}
                    disabled={disabled}
                    fullWidth
                    helperText="Azure AD application Client ID (with delegated permissions)"
                    size="small"
                  />
                  <TextField
                    label="Username (UPN)"
                    value={value.powerbi_username || ''}
                    onChange={(e) => handleFieldChange('powerbi_username', e.target.value)}
                    disabled={disabled}
                    fullWidth
                    helperText="Service account email/UPN (e.g., user@domain.com)"
                    size="small"
                  />
                  <TextField
                    label="Password"
                    value={value.powerbi_password || ''}
                    onChange={(e) => handleFieldChange('powerbi_password', e.target.value)}
                    disabled={disabled}
                    type="password"
                    fullWidth
                    helperText="Service account password"
                    size="small"
                  />
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
                        href="https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/get-dataset?tryIt=true"
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
                    value={value.powerbi_oauth_client_id || ''}
                    onChange={(e) => handleFieldChange('powerbi_oauth_client_id', e.target.value)}
                    disabled={disabled || isAuthenticated}
                    fullWidth
                    helperText="Your Azure AD app Client ID for OAuth sign-in"
                    size="small"
                  />

                  <TextField
                    label="Access Token (Alternative)"
                    value={value.powerbi_access_token || ''}
                    onChange={(e) => handleFieldChange('powerbi_access_token', e.target.value)}
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

                  {!isAuthenticated && !value.powerbi_access_token ? (
                    <Button
                      variant="contained"
                      onClick={signIn}
                      disabled={disabled || oauthLoading || !value.powerbi_oauth_client_id}
                      startIcon={<LoginIcon />}
                      fullWidth
                    >
                      {oauthLoading ? 'Signing in...' : 'Sign in with Microsoft'}
                    </Button>
                  ) : value.powerbi_access_token && !isAuthenticated ? (
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

              <Divider sx={{ my: 1 }} />

              <FormControlLabel
                control={
                  <Checkbox
                    checked={value.powerbi_include_hidden || false}
                    onChange={(e) => handleFieldChange('powerbi_include_hidden', e.target.checked)}
                    disabled={disabled}
                  />
                }
                label="Include hidden measures"
              />
              <TextField
                label="Filter Pattern (Regex)"
                value={value.powerbi_filter_pattern || ''}
                onChange={(e) => handleFieldChange('powerbi_filter_pattern', e.target.value)}
                disabled={disabled}
                fullWidth
                helperText="Optional regex pattern to filter measure names"
                size="small"
              />
            </Box>
          )}

          {/* Static Mode only for YAML (dynamic mode not typically needed for YAML) */}
          {mode === 'static' && inboundConnector === 'yaml' && (
            <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
              <TextField
                label="YAML Content"
                value={value.yaml_content || ''}
                onChange={(e) => handleFieldChange('yaml_content', e.target.value)}
                disabled={disabled}
                fullWidth
                multiline
                rows={4}
                helperText="Paste YAML content here, or specify file path below"
                size="small"
              />
              <Typography variant="caption" color="text.secondary" sx={{ textAlign: 'center' }}>
                — OR —
              </Typography>
              <TextField
                label="YAML File Path"
                value={value.yaml_file_path || ''}
                onChange={(e) => handleFieldChange('yaml_file_path', e.target.value)}
                disabled={disabled}
                fullWidth
                helperText="Path to YAML file (alternative to content)"
                size="small"
              />
            </Box>
          )}

          {/* Dynamic Mode info for YAML */}
          {mode === 'dynamic' && inboundConnector === 'yaml' && (
            <Alert severity="info" variant="outlined">
              <Typography variant="body2">
                Dynamic mode for YAML is not typically needed. Consider using static mode with YAML content.
              </Typography>
            </Alert>
          )}
        </>
      )}

      {/* Outbound Configuration - only show for formats that have config options */}
      {outboundFormat && (mode === 'dynamic' || outboundFormat === 'sql' || outboundFormat === 'dax') && (
        <>
          <Divider sx={{ my: 1 }} />
          <Typography variant="subtitle2" color="secondary" sx={{ fontWeight: 600 }}>
            Target Configuration ({outboundFormat.toUpperCase()})
          </Typography>

          {/* Dynamic Mode: Simplified outbound info */}
          {mode === 'dynamic' && (
            <Alert severity="success" variant="outlined">
              <Typography variant="body2">
                <strong>Target format:</strong> <Chip label="{target}" size="small" />
              </Typography>
              <Typography variant="caption" color="text.secondary" sx={{ mt: 1, display: 'block' }}>
                The output format will be determined by the <code>target</code> parameter in execution inputs.
                Expected values: &quot;dax&quot;, &quot;sql&quot;, &quot;uc_metrics&quot;, or &quot;yaml&quot;
              </Typography>
            </Alert>
          )}

          {/* Static Mode: Full outbound configuration */}
          {mode === 'static' && outboundFormat === 'sql' && (
            <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
              <FormControl fullWidth size="small">
                <InputLabel>SQL Dialect</InputLabel>
                <Select
                  value={value.sql_dialect || 'databricks'}
                  onChange={handleSelectChange('sql_dialect')}
                  label="SQL Dialect"
                  disabled={disabled}
                >
                  <MenuItem value="databricks">Databricks</MenuItem>
                  <MenuItem value="postgresql">PostgreSQL</MenuItem>
                  <MenuItem value="mysql">MySQL</MenuItem>
                  <MenuItem value="sqlserver">SQL Server</MenuItem>
                  <MenuItem value="snowflake">Snowflake</MenuItem>
                  <MenuItem value="bigquery">BigQuery</MenuItem>
                  <MenuItem value="standard">Standard SQL</MenuItem>
                </Select>
              </FormControl>
              <FormControlLabel
                control={
                  <Checkbox
                    checked={value.sql_include_comments !== false}
                    onChange={(e) => handleFieldChange('sql_include_comments', e.target.checked)}
                    disabled={disabled}
                  />
                }
                label="Include comments in SQL output"
              />
              <FormControlLabel
                control={
                  <Checkbox
                    checked={value.sql_process_structures !== false}
                    onChange={(e) => handleFieldChange('sql_process_structures', e.target.checked)}
                    disabled={disabled}
                  />
                }
                label="Process time intelligence structures"
              />
            </Box>
          )}

          {mode === 'static' && outboundFormat === 'dax' && (
            <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
              <FormControlLabel
                control={
                  <Checkbox
                    checked={value.dax_process_structures !== false}
                    onChange={(e) => handleFieldChange('dax_process_structures', e.target.checked)}
                    disabled={disabled}
                  />
                }
                label="Process time intelligence structures"
              />
            </Box>
          )}

          {/* Definition name - common to all outbound formats (static mode only) */}
          {mode === 'static' && (
            <TextField
              label="Definition Name (optional)"
              value={value.definition_name || ''}
              onChange={(e) => handleFieldChange('definition_name', e.target.value)}
              disabled={disabled}
              fullWidth
              helperText="Custom name for the generated definition"
              size="small"
            />
          )}
        </>
      )}
    </Box>
  );
};
