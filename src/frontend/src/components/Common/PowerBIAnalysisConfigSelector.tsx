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
  MenuItem,
  CircularProgress,
  Tooltip,
  IconButton
} from '@mui/material';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import LoginIcon from '@mui/icons-material/Login';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import SecurityIcon from '@mui/icons-material/Security';
import PersonIcon from '@mui/icons-material/Person';
import QuestionAnswerIcon from '@mui/icons-material/QuestionAnswer';
import CloudDownloadIcon from '@mui/icons-material/CloudDownload';
import InfoIcon from '@mui/icons-material/Info';
import AutoAwesomeIcon from '@mui/icons-material/AutoAwesome';
import { usePowerBIOAuth } from '../../hooks/usePowerBIOAuth';
import { apiClient } from '../../config/api/ApiConfig';

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

  // Context Enrichment
  business_mappings?: string; // JSON string of {term: dax_expression}
  field_synonyms?: string; // JSON string of {field_name: [synonyms]}
  active_filters?: string; // JSON string of {filter_name: value}
  session_id?: string;
  visible_tables?: string; // JSON string of table names array
  conversation_history?: string; // JSON string of conversation array

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

  // State for fetching context config from database
  const [loadingContextConfig, setLoadingContextConfig] = React.useState(false);
  const [contextConfigError, setContextConfigError] = React.useState<string | null>(null);
  const [contextConfigSuccess, setContextConfigSuccess] = React.useState<string | null>(null);

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

  // Fetch context configuration from database
  const handleFetchContextConfig = async () => {
    const semanticModelId = value.dataset_id;

    if (!semanticModelId) {
      setContextConfigError('Please enter a Dataset/Semantic Model ID first');
      return;
    }

    setLoadingContextConfig(true);
    setContextConfigError(null);
    setContextConfigSuccess(null);

    try {
      // Fetch context config from backend API
      const response = await apiClient.get<{
        business_mappings: Record<string, string> | null;
        field_synonyms: Record<string, string[]> | null;
      }>(`/powerbi/models/${semanticModelId}/context-config/dict`);

      // Convert to JSON strings for storage
      const updatedConfig: PowerBIAnalysisConfig = {
        ...value,
        business_mappings: response.data.business_mappings
          ? JSON.stringify(response.data.business_mappings, null, 2)
          : undefined,
        field_synonyms: response.data.field_synonyms
          ? JSON.stringify(response.data.field_synonyms, null, 2)
          : undefined
      };

      onChange(updatedConfig);

      const mappingsCount = response.data.business_mappings ? Object.keys(response.data.business_mappings).length : 0;
      const synonymsCount = response.data.field_synonyms ? Object.keys(response.data.field_synonyms).length : 0;

      if (mappingsCount === 0 && synonymsCount === 0) {
        setContextConfigSuccess('No context configuration found in database. You can add mappings manually below.');
      } else {
        setContextConfigSuccess(`Loaded ${mappingsCount} business mappings and ${synonymsCount} field synonyms from database`);
      }
    } catch (error: any) {
      console.error('Failed to fetch context config:', error);
      setContextConfigError(error.response?.data?.detail || 'Failed to fetch context configuration from database');
    } finally {
      setLoadingContextConfig(false);
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

      {/* Context Enrichment */}
      <Accordion>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <AutoAwesomeIcon color="primary" sx={{ fontSize: 20 }} />
            <Typography variant="subtitle2">
              Context Enrichment
            </Typography>
            <Chip label="Optional" size="small" variant="outlined" />
          </Box>
        </AccordionSummary>
        <AccordionDetails>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
            <Alert severity="info" variant="outlined" sx={{ py: 1 }}>
              <Box sx={{ display: 'flex', alignItems: 'start', gap: 1 }}>
                <InfoIcon sx={{ fontSize: 18, mt: 0.2 }} />
                <Box>
                  <Typography variant="body2" sx={{ fontWeight: 600, mb: 0.5 }}>
                    Enhanced Natural Language Understanding
                  </Typography>
                  <Typography variant="caption" component="div">
                    Add business terminology mappings and field synonyms to enable simpler, more natural queries.
                    This allows users to ask questions using business language instead of technical DAX syntax.
                  </Typography>
                  <Box component="ul" sx={{ pl: 2, mt: 0.5, mb: 0, fontSize: '0.75rem' }}>
                    <li><strong>Business Mappings</strong>: Translate natural terms to DAX filters (e.g., "Complete CGR" → filter expression)</li>
                    <li><strong>Field Synonyms</strong>: Map alternative field names (e.g., "customer count" → [num_customers])</li>
                  </Box>
                </Box>
              </Box>
            </Alert>

            {/* Fetch from Database Button */}
            {mode === 'static' && value.dataset_id && (
              <Box>
                <Button
                  variant="outlined"
                  startIcon={loadingContextConfig ? <CircularProgress size={16} /> : <CloudDownloadIcon />}
                  onClick={handleFetchContextConfig}
                  disabled={disabled || loadingContextConfig || !value.dataset_id}
                  fullWidth
                  sx={{ mb: 1 }}
                >
                  {loadingContextConfig ? 'Loading from Database...' : 'Load from Database'}
                </Button>

                {contextConfigSuccess && (
                  <Alert severity="success" variant="outlined" sx={{ py: 0.5 }}>
                    <Typography variant="caption">{contextConfigSuccess}</Typography>
                  </Alert>
                )}

                {contextConfigError && (
                  <Alert severity="warning" variant="outlined" sx={{ py: 0.5 }}>
                    <Typography variant="caption">{contextConfigError}</Typography>
                  </Alert>
                )}
              </Box>
            )}

            {!value.dataset_id && mode === 'static' && (
              <Alert severity="warning" variant="outlined" sx={{ py: 0.5 }}>
                <Typography variant="caption">
                  Enter a Dataset/Semantic Model ID above to load context configuration from database
                </Typography>
              </Alert>
            )}

            {/* Business Mappings */}
            <Box>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5, mb: 1 }}>
                <Typography variant="body2" sx={{ fontWeight: 600 }}>
                  Business Term Mappings
                </Typography>
                <Tooltip title="Maps natural language business terms to DAX filter expressions">
                  <IconButton size="small">
                    <InfoIcon sx={{ fontSize: 16 }} />
                  </IconButton>
                </Tooltip>
              </Box>
              <TextField
                value={value.business_mappings || ''}
                onChange={(e) => handleFieldChange('business_mappings', e.target.value)}
                disabled={disabled}
                fullWidth
                multiline
                rows={4}
                placeholder={`{\n  "Complete CGR": "[tbl_initial_sizing_tracking][description] = 'Complete CGR'",\n  "Active customers": "[tbl_customers][status] = 'Active'"\n}`}
                helperText='JSON format: {"natural term": "DAX filter expression"}'
                size="small"
                sx={{ fontFamily: 'monospace', fontSize: '0.85rem' }}
              />
            </Box>

            {/* Field Synonyms */}
            <Box>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5, mb: 1 }}>
                <Typography variant="body2" sx={{ fontWeight: 600 }}>
                  Field Synonyms
                </Typography>
                <Tooltip title="Maps alternative field names to canonical field names in the semantic model">
                  <IconButton size="small">
                    <InfoIcon sx={{ fontSize: 16 }} />
                  </IconButton>
                </Tooltip>
              </Box>
              <TextField
                value={value.field_synonyms || ''}
                onChange={(e) => handleFieldChange('field_synonyms', e.target.value)}
                disabled={disabled}
                fullWidth
                multiline
                rows={4}
                placeholder={`{\n  "num_customers": ["number of customers", "customer count", "total customers"],\n  "revenue": ["sales", "total sales", "income"]\n}`}
                helperText='JSON format: {"field_name": ["synonym1", "synonym2"]}'
                size="small"
                sx={{ fontFamily: 'monospace', fontSize: '0.85rem' }}
              />
            </Box>

            {/* Additional Context Fields */}
            <Divider sx={{ my: 1 }}>
              <Typography variant="caption" color="text.secondary">
                Advanced Context (Optional)
              </Typography>
            </Divider>

            <TextField
              label="Active Filters"
              value={value.active_filters || ''}
              onChange={(e) => handleFieldChange('active_filters', e.target.value)}
              disabled={disabled}
              fullWidth
              multiline
              rows={2}
              placeholder='{"BU": "Italy", "Week": 1}'
              helperText="JSON format: filters currently active in the view"
              size="small"
              sx={{ fontFamily: 'monospace', fontSize: '0.85rem' }}
            />

            <TextField
              label="Session ID"
              value={value.session_id || ''}
              onChange={(e) => handleFieldChange('session_id', e.target.value)}
              disabled={disabled}
              fullWidth
              placeholder="unique-session-id"
              helperText="Track conversation context across multiple queries"
              size="small"
            />

            <TextField
              label="Visible Tables"
              value={value.visible_tables || ''}
              onChange={(e) => handleFieldChange('visible_tables', e.target.value)}
              disabled={disabled}
              fullWidth
              multiline
              rows={2}
              placeholder='["tbl_customers", "tbl_sales", "dim_country"]'
              helperText="JSON array: tables visible in current page context"
              size="small"
              sx={{ fontFamily: 'monospace', fontSize: '0.85rem' }}
            />
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
