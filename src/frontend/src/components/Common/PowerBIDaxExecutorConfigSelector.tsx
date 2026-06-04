/**
 * Power BI DAX Executor Configuration Selector Component
 *
 * Provides configuration UI for the Power BI DAX Executor Tool.
 * Takes a pre-written DAX EVALUATE statement and executes it — no LLM involved.
 */

import React from 'react';
import {
  Box,
  Typography,
  TextField,
  Divider,
  ToggleButtonGroup,
  ToggleButton,
  Alert,
  Chip,
  Button,
  MenuItem,
  Select,
  FormControl,
  InputLabel
} from '@mui/material';
import LoginIcon from '@mui/icons-material/Login';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import SecurityIcon from '@mui/icons-material/Security';
import PersonIcon from '@mui/icons-material/Person';
import { usePowerBIOAuth } from '../../hooks/usePowerBIOAuth';

export type PowerBIAuthMethod = 'service_principal' | 'service_account' | 'user_oauth';

export interface PowerBIDaxExecutorConfig {
  mode?: 'static' | 'dynamic';
  workspace_id?: string;
  dataset_id?: string;
  dax_query?: string;
  auth_method?: PowerBIAuthMethod;
  tenant_id?: string;
  client_id?: string;
  client_secret?: string;
  username?: string;
  password?: string;
  oauth_client_id?: string;
  access_token?: string;
  output_format?: string;
  max_rows?: string;
  [key: string]: string | boolean | undefined;
}

interface PowerBIDaxExecutorConfigSelectorProps {
  value: PowerBIDaxExecutorConfig;
  onChange: (config: PowerBIDaxExecutorConfig) => void;
  disabled?: boolean;
}

export const PowerBIDaxExecutorConfigSelector: React.FC<PowerBIDaxExecutorConfigSelectorProps> = ({
  value = {},
  onChange,
  disabled = false
}) => {
  const { accessToken, isAuthenticated, signIn, signOut, userEmail, isLoading: oauthLoading, error: oauthError } = usePowerBIOAuth({
    clientId: value.oauth_client_id || ''
  });

  const handleFieldChange = (field: keyof PowerBIDaxExecutorConfig, fieldValue: string | boolean) => {
    onChange({
      ...value,
      [field]: fieldValue
    });
  };

  const handleAuthMethodChange = (_event: React.MouseEvent<HTMLElement>, newMethod: PowerBIAuthMethod | null) => {
    if (newMethod !== null) {
      const updatedConfig: PowerBIDaxExecutorConfig = {
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
      const updatedConfig: PowerBIDaxExecutorConfig = {
        ...value,
        mode: newMode
      };

      if (newMode === 'dynamic') {
        updatedConfig.workspace_id = '{workspace_id}';
        updatedConfig.dataset_id = '{dataset_id}';
        updatedConfig.dax_query = '{dax_query}';
        updatedConfig.tenant_id = '{tenant_id}';
        updatedConfig.client_id = '{client_id}';
        updatedConfig.client_secret = '{client_secret}';
        updatedConfig.access_token = '{access_token}';
        updatedConfig.output_format = value.output_format || 'markdown';
        updatedConfig.max_rows = value.max_rows || '1000';
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
              <Chip label="dax_query" size="small" color="primary" variant="outlined" />
            </Box>
          </Box>

          <Box sx={{ mt: 1.5, p: 1, bgcolor: 'rgba(0,0,0,0.04)', borderRadius: 1 }}>
            <Typography variant="caption" sx={{ fontWeight: 600 }}>
              Authentication (choose one):
            </Typography>
            <Box sx={{ mt: 0.5 }}>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 0.5 }}>
                <Chip label="access_token" size="small" color="success" />
                <Typography variant="caption">User OAuth (recommended)</Typography>
              </Box>
              <Typography variant="caption" color="text.secondary" sx={{ pl: 1, display: 'block', mb: 0.5 }}>
                <em>or</em>
              </Typography>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                <Chip label="tenant_id" size="small" variant="outlined" />
                <Chip label="client_id" size="small" variant="outlined" />
                <Chip label="client_secret" size="small" variant="outlined" />
                <Typography variant="caption">Service Principal</Typography>
              </Box>
            </Box>
          </Box>
        </Alert>
      )}

      {/* Static Mode: Configuration Forms */}
      {mode === 'static' && (
        <>
          {/* DAX Query — prominent, top of form */}
          <Typography variant="subtitle2" color="primary" sx={{ fontWeight: 600 }}>
            DAX Query
          </Typography>
          <TextField
            label="DAX EVALUATE Statement"
            value={value.dax_query || ''}
            onChange={(e) => handleFieldChange('dax_query', e.target.value)}
            disabled={disabled}
            required
            fullWidth
            multiline
            minRows={5}
            helperText="The DAX EVALUATE statement to execute (e.g., EVALUATE SUMMARIZECOLUMNS(...))"
            size="small"
            inputProps={{ style: { fontFamily: 'monospace', fontSize: '0.8rem' } }}
          />

          <Divider sx={{ my: 1 }}>
            <Typography variant="caption" color="text.secondary">
              Power BI Semantic Model
            </Typography>
          </Divider>

          {/* Power BI Model Identification */}
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
              helperText="Semantic model/dataset ID to execute the DAX query against"
              size="small"
            />
          </Box>

          <Divider sx={{ my: 1 }}>
            <Typography variant="caption" color="text.secondary">
              Authentication
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

          {/* Service Principal Fields */}
          {authMethod === 'service_principal' && (
            <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
              <Alert severity="info" variant="outlined" sx={{ py: 0.5 }}>
                <Typography variant="caption">
                  The Service Principal must be a <strong>member of the workspace</strong> with at least read permissions.
                </Typography>
              </Alert>
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
          )}

          {/* Service Account Fields */}
          {authMethod === 'service_account' && (
            <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
              <Alert severity="warning" variant="outlined" sx={{ py: 0.5 }}>
                <Typography variant="caption">
                  Use service account when Service Principal lacks permissions.
                  Requires an Azure AD app with delegated permissions.
                </Typography>
              </Alert>
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
          )}

          {/* User OAuth Fields */}
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
                    Microsoft&apos;s API docs (Try It)
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

          {/* Output Options */}
          <Divider sx={{ my: 1 }}>
            <Typography variant="caption" color="text.secondary">
              Output Options
            </Typography>
          </Divider>

          <Box sx={{ display: 'flex', flexDirection: 'row', gap: 2 }}>
            <FormControl size="small" sx={{ flex: 1 }}>
              <InputLabel>Output Format</InputLabel>
              <Select
                value={value.output_format || 'markdown'}
                onChange={(e) => handleFieldChange('output_format', e.target.value)}
                label="Output Format"
                disabled={disabled}
              >
                <MenuItem value="markdown">Markdown</MenuItem>
                <MenuItem value="json">JSON</MenuItem>
                <MenuItem value="table">Table</MenuItem>
              </Select>
            </FormControl>

            <TextField
              label="Max Rows"
              value={value.max_rows || '1000'}
              onChange={(e) => handleFieldChange('max_rows', e.target.value)}
              disabled={disabled}
              type="number"
              helperText="Maximum rows to return"
              size="small"
              sx={{ flex: 1 }}
              inputProps={{ min: 1, max: 100000 }}
            />
          </Box>
        </>
      )}

      {/* Info about what the tool does */}
      <Alert severity="info" variant="outlined" sx={{ mt: 1 }}>
        <Typography variant="body2" sx={{ fontWeight: 600, mb: 0.5 }}>
          What this tool does:
        </Typography>
        <Typography variant="caption">
          Executes a pre-written DAX EVALUATE statement directly against a Power BI semantic model.
          No LLM is used — provide a working DAX query and this tool runs it and returns the results.
        </Typography>
        <Box component="ul" sx={{ pl: 2, mt: 0.5, mb: 0, fontSize: '0.75rem' }}>
          <li>Runs any valid DAX EVALUATE statement</li>
          <li>Returns results as markdown table or JSON</li>
          <li>Supports Service Principal, Service Account, and User OAuth authentication</li>
          <li>Caps results at max_rows to prevent large payloads</li>
        </Box>
      </Alert>
    </Box>
  );
};
