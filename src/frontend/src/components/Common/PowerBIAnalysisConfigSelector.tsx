/**
 * Power BI Analysis Configuration Selector Component
 *
 * Provides configuration UI for the PowerBIAnalysisTool.
 * Converts business questions into DAX queries and executes them against Power BI.
 *
 * Flow:
 * 1. Extract model context (measures, relationships)
 * 2. Generate DAX using LLM based on user question
 * 3. Execute DAX via Power BI Execute Queries API
 * 4. Find visual references in reports
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
  FormControl,
  InputLabel,
  Select,
  MenuItem
} from '@mui/material';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import LoginIcon from '@mui/icons-material/Login';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import SecurityIcon from '@mui/icons-material/Security';
import PersonIcon from '@mui/icons-material/Person';
import QuestionAnswerIcon from '@mui/icons-material/QuestionAnswer';
import { usePowerBIOAuth } from '../../hooks/usePowerBIOAuth';

// Authentication method type
// - service_principal: App credentials (client_id + client_secret + tenant_id)
// - service_account: User credentials (username + password + client_id + tenant_id)
// - user_oauth: Interactive OAuth sign-in with Microsoft
export type PowerBIAuthMethod = 'service_principal' | 'service_account' | 'user_oauth';

export interface PowerBIAnalysisConfig {
  // Configuration mode
  mode?: 'static' | 'dynamic';

  // User Question (the business question to answer)
  user_question?: string;

  // Power BI Configuration
  workspace_id?: string;
  dataset_id?: string;

  // Authentication method
  auth_method?: PowerBIAuthMethod;

  // Service Principal Authentication (client_id + client_secret + tenant_id)
  tenant_id?: string;
  client_id?: string;
  client_secret?: string;

  // Service Account Authentication (username + password + client_id + tenant_id)
  // Used when Service Principal doesn't have sufficient permissions to read Power BI data
  username?: string;
  password?: string;

  // User OAuth authentication (alternative)
  oauth_client_id?: string;
  access_token?: string;

  // LLM Configuration for DAX generation
  llm_workspace_url?: string;
  llm_token?: string;
  llm_model?: string;

  // Options
  include_visual_references?: boolean;
  skip_system_tables?: boolean;
  output_format?: 'markdown' | 'json';

  // Index signature for compatibility
  [key: string]: string | boolean | undefined;
}

interface PowerBIAnalysisConfigSelectorProps {
  value: PowerBIAnalysisConfig;
  onChange: (config: PowerBIAnalysisConfig) => void;
  disabled?: boolean;
}

export const PowerBIAnalysisConfigSelector: React.FC<PowerBIAnalysisConfigSelectorProps> = ({
  value = {},
  onChange,
  disabled = false
}) => {
  // OAuth hook for User OAuth authentication
  const { accessToken, isAuthenticated, signIn, signOut, userEmail, isLoading: oauthLoading, error: oauthError } = usePowerBIOAuth({
    clientId: value.oauth_client_id || ''
  });

  const handleFieldChange = (field: keyof PowerBIAnalysisConfig, fieldValue: string | boolean) => {
    onChange({
      ...value,
      [field]: fieldValue
    });
  };

  const handleAuthMethodChange = (_event: React.MouseEvent<HTMLElement>, newMethod: PowerBIAuthMethod | null) => {
    if (newMethod !== null) {
      const updatedConfig: PowerBIAnalysisConfig = {
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
        // Set access token if authenticated
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

  const handleModeChange = (_event: React.MouseEvent<HTMLElement>, newMode: 'static' | 'dynamic' | null) => {
    if (newMode !== null) {
      const updatedConfig: PowerBIAnalysisConfig = {
        ...value,
        mode: newMode
      };

      // When switching to dynamic mode, auto-populate placeholders
      if (newMode === 'dynamic') {
        updatedConfig.user_question = '{user_question}';
        updatedConfig.workspace_id = '{workspace_id}';
        updatedConfig.dataset_id = '{dataset_id}';
        updatedConfig.tenant_id = '{tenant_id}';
        updatedConfig.client_id = '{client_id}';
        updatedConfig.client_secret = '{client_secret}';
        updatedConfig.username = '{username}';
        updatedConfig.password = '{password}';
        updatedConfig.access_token = '{access_token}';
        // Keep options as static values
        updatedConfig.include_visual_references = value.include_visual_references !== false;
        updatedConfig.skip_system_tables = value.skip_system_tables !== false;
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
              <Chip label="user_question" size="small" color="primary" />
              <Chip label="workspace_id" size="small" color="primary" variant="outlined" />
              <Chip label="dataset_id" size="small" color="primary" variant="outlined" />
            </Box>
          </Box>

          <Box sx={{ mt: 1.5, p: 1, bgcolor: 'rgba(0,0,0,0.04)', borderRadius: 1 }}>
            <Typography variant="caption" sx={{ fontWeight: 600 }}>
              Authentication (choose one):
            </Typography>
            <Box sx={{ mt: 0.5 }}>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 0.5 }}>
                <Chip label="access_token" size="small" color="success" />
                <Typography variant="caption">← User OAuth (recommended)</Typography>
              </Box>
              <Typography variant="caption" color="text.secondary" sx={{ pl: 1, display: 'block', mb: 0.5 }}>
                <em>or Service Principal:</em>
              </Typography>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5, mb: 0.5 }}>
                <Chip label="tenant_id" size="small" variant="outlined" />
                <Chip label="client_id" size="small" variant="outlined" />
                <Chip label="client_secret" size="small" variant="outlined" />
              </Box>
              <Typography variant="caption" color="text.secondary" sx={{ pl: 1, display: 'block', mb: 0.5 }}>
                <em>or Service Account:</em>
              </Typography>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
                <Chip label="tenant_id" size="small" variant="outlined" />
                <Chip label="client_id" size="small" variant="outlined" />
                <Chip label="username" size="small" variant="outlined" />
                <Chip label="password" size="small" variant="outlined" />
              </Box>
            </Box>
          </Box>
        </Alert>
      )}

      {/* Static Mode: Full Configuration Forms */}
      {mode === 'static' && (
        <>
          {/* User Question - Most Important */}
          <Box sx={{
            p: 2,
            backgroundColor: 'rgba(76, 175, 80, 0.08)',
            borderRadius: 1,
            border: '1px solid rgba(76, 175, 80, 0.3)'
          }}>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
              <QuestionAnswerIcon color="success" />
              <Typography variant="subtitle2" color="success.main" sx={{ fontWeight: 600 }}>
                Business Question
              </Typography>
            </Box>
            <TextField
              label="What do you want to know?"
              value={value.user_question || ''}
              onChange={(e) => handleFieldChange('user_question', e.target.value)}
              disabled={disabled}
              required
              fullWidth
              multiline
              rows={2}
              placeholder="e.g., What are total sales by region? Show me top 10 customers by revenue."
              helperText="The question will be converted to a DAX query and executed against the semantic model"
              size="small"
            />
          </Box>

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
              helperText="Power BI workspace ID (GUID)"
              size="small"
            />
            <TextField
              label="Dataset / Semantic Model ID"
              value={value.dataset_id || ''}
              onChange={(e) => handleFieldChange('dataset_id', e.target.value)}
              disabled={disabled}
              required
              fullWidth
              helperText="Power BI semantic model (dataset) ID to query"
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
              <Box sx={{ textAlign: 'center', py: 0.5 }}>
                <SecurityIcon sx={{ fontSize: 18, mb: 0.5 }} />
                <Typography variant="body2" sx={{ fontWeight: 600 }}>
                  Service Principal
                </Typography>
                <Typography variant="caption" color="text.secondary">
                  App credentials
                </Typography>
              </Box>
            </ToggleButton>
            <ToggleButton value="service_account">
              <Box sx={{ textAlign: 'center', py: 0.5 }}>
                <PersonIcon sx={{ fontSize: 18, mb: 0.5 }} />
                <Typography variant="body2" sx={{ fontWeight: 600 }}>
                  Service Account
                </Typography>
                <Typography variant="caption" color="text.secondary">
                  User credentials
                </Typography>
              </Box>
            </ToggleButton>
            <ToggleButton value="user_oauth">
              <Box sx={{ textAlign: 'center', py: 0.5 }}>
                <LoginIcon sx={{ fontSize: 18, mb: 0.5 }} />
                <Typography variant="body2" sx={{ fontWeight: 600 }}>
                  User OAuth
                </Typography>
                <Typography variant="caption" color="text.secondary">
                  Sign in with Microsoft
                </Typography>
              </Box>
            </ToggleButton>
          </ToggleButtonGroup>

          {/* Service Principal Authentication Fields */}
          {authMethod === 'service_principal' && (
            <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
              <Alert severity="info" variant="outlined" sx={{ py: 0.5 }}>
                <Typography variant="caption">
                  Required permission: <code>SemanticModel.ReadWrite.All</code> for model access and DAX execution
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
            <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
              <Alert severity="warning" variant="outlined" sx={{ py: 0.5 }}>
                <Typography variant="caption">
                  Use service account when Service Principal lacks permissions to read Power BI data.
                  The account must have access to the semantic model.
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
                helperText="Application/Client ID (Azure AD app registration)"
                size="small"
              />
              <TextField
                label="Username"
                value={value.username || ''}
                onChange={(e) => handleFieldChange('username', e.target.value)}
                disabled={disabled}
                required
                fullWidth
                placeholder="serviceaccount@domain.com"
                helperText="Service account UPN (email)"
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

          {/* User OAuth Authentication */}
          {authMethod === 'user_oauth' && (
            <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
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
          <Typography variant="subtitle2">LLM Configuration (DAX Generation)</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
            <Alert severity="info" variant="outlined" sx={{ py: 0.5 }}>
              <Typography variant="caption">
                LLM is used to convert your business question into a DAX query.
                Without LLM config, a simple keyword-matching fallback is used.
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

      {/* Options */}
      <Accordion>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="subtitle2">Options</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
            <FormControlLabel
              control={
                <Checkbox
                  checked={value.include_visual_references !== false}
                  onChange={(e) => handleFieldChange('include_visual_references', e.target.checked)}
                  disabled={disabled}
                  size="small"
                />
              }
              label={<Typography variant="body2">Find visual references in reports</Typography>}
            />
            <FormControlLabel
              control={
                <Checkbox
                  checked={value.skip_system_tables !== false}
                  onChange={(e) => handleFieldChange('skip_system_tables', e.target.checked)}
                  disabled={disabled}
                  size="small"
                />
              }
              label={<Typography variant="body2">Skip system tables (LocalDateTable, etc.)</Typography>}
            />

            <FormControl fullWidth size="small" disabled={disabled}>
              <InputLabel>Output Format</InputLabel>
              <Select
                value={value.output_format || 'markdown'}
                onChange={(e) => handleFieldChange('output_format', e.target.value)}
                label="Output Format"
              >
                <MenuItem value="markdown">Markdown (Readable)</MenuItem>
                <MenuItem value="json">JSON (Structured)</MenuItem>
              </Select>
            </FormControl>
          </Box>
        </AccordionDetails>
      </Accordion>

      {/* Tool Info */}
      <Alert severity="info" variant="outlined" sx={{ mt: 1 }}>
        <Typography variant="body2" sx={{ fontWeight: 600, mb: 0.5 }}>
          Power BI Analysis Pipeline
        </Typography>
        <Typography variant="caption">
          This tool converts your business questions into DAX queries and executes them:
        </Typography>
        <Box component="ol" sx={{ pl: 2, mt: 0.5, mb: 0, fontSize: '0.75rem' }}>
          <li><strong>Extract Context</strong> — Fetches measures, tables, and relationships from the semantic model</li>
          <li><strong>Generate DAX</strong> — Uses LLM to convert your question into an optimized DAX query</li>
          <li><strong>Execute Query</strong> — Runs the DAX via Power BI Execute Queries API</li>
          <li><strong>Find References</strong> — Identifies reports/visuals using the queried measures</li>
        </Box>
      </Alert>
    </Box>
  );
};
