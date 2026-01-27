/**
 * M-Query Converter Configuration Selector Component
 *
 * Provides configuration UI for the M-Query Conversion Pipeline tool.
 * Extracts M-Query expressions from Power BI and converts them to Databricks SQL.
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
  AccordionDetails
} from '@mui/material';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';

export interface MQueryConverterConfig {
  // Configuration mode
  mode?: 'static' | 'dynamic';
  // Power BI Admin API configuration
  workspace_id?: string;
  dataset_id?: string;
  // Service Principal authentication
  tenant_id?: string;
  client_id?: string;
  client_secret?: string;
  // LLM Configuration
  llm_workspace_url?: string;
  llm_token?: string;
  llm_model?: string;
  use_llm?: boolean;
  // Target Configuration
  target_catalog?: string;
  target_schema?: string;
  // Scan Options
  include_lineage?: boolean;
  include_datasource_details?: boolean;
  include_dataset_schema?: boolean;
  include_dataset_expressions?: boolean;
  include_hidden_tables?: boolean;
  skip_static_tables?: boolean;
  // Output Options
  include_relationships?: boolean;
  include_summary?: boolean;
  // Index signature for compatibility
  [key: string]: string | boolean | undefined;
}

interface MQueryConverterConfigSelectorProps {
  value: MQueryConverterConfig;
  onChange: (config: MQueryConverterConfig) => void;
  disabled?: boolean;
}

export const MQueryConverterConfigSelector: React.FC<MQueryConverterConfigSelectorProps> = ({
  value = {},
  onChange,
  disabled = false
}) => {
  const handleFieldChange = (field: keyof MQueryConverterConfig, fieldValue: string | boolean) => {
    onChange({
      ...value,
      [field]: fieldValue
    });
  };

  const handleModeChange = (_event: React.MouseEvent<HTMLElement>, newMode: 'static' | 'dynamic' | null) => {
    if (newMode !== null) {
      const updatedConfig: MQueryConverterConfig = {
        ...value,
        mode: newMode
      };

      // When switching to dynamic mode, auto-populate placeholders
      if (newMode === 'dynamic') {
        // Set all parameters to placeholders
        updatedConfig.workspace_id = '{workspace_id}';
        updatedConfig.dataset_id = '{dataset_id}';
        updatedConfig.tenant_id = '{tenant_id}';
        updatedConfig.client_id = '{client_id}';
        updatedConfig.client_secret = '{client_secret}';
        updatedConfig.target_catalog = '{target_catalog}';
        updatedConfig.target_schema = '{target_schema}';
        // Keep LLM and options as static values
        updatedConfig.use_llm = value.use_llm !== false;
        updatedConfig.llm_model = value.llm_model || 'databricks-claude-sonnet-4';
      }

      onChange(updatedConfig);
    }
  };

  const mode = value.mode || 'static';

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
          <Typography variant="caption">
            Configuration values will be resolved from execution inputs at runtime.
            Use this mode when calling the crew from external applications (e.g., Databricks Apps).
            <br /><br />
            <strong>Required execution inputs:</strong>
          </Typography>
          <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5, mt: 1 }}>
            <Chip label="workspace_id" size="small" color="primary" variant="outlined" />
            <Chip label="tenant_id" size="small" color="primary" variant="outlined" />
            <Chip label="client_id" size="small" color="primary" variant="outlined" />
            <Chip label="client_secret" size="small" color="primary" variant="outlined" />
            <Chip label="target_catalog" size="small" color="secondary" variant="outlined" />
            <Chip label="target_schema" size="small" color="secondary" variant="outlined" />
          </Box>
          <Box sx={{ mt: 1 }}>
            <Typography variant="caption" color="text.secondary">
              <strong>Optional:</strong>
            </Typography>
            <Chip label="dataset_id" size="small" variant="outlined" sx={{ ml: 0.5 }} />
          </Box>
        </Alert>
      )}

      {/* Dynamic Mode: Show parameter mapping */}
      {mode === 'dynamic' && (
        <Alert severity="success" variant="outlined">
          <Typography variant="body2" sx={{ mb: 1 }}>
            <strong>Parameter placeholders configured:</strong>
          </Typography>
          <Box component="ul" sx={{ pl: 2, mb: 0, fontSize: '0.75rem' }}>
            <li><code>workspace_id</code> → <Chip label="{workspace_id}" size="small" /></li>
            <li><code>dataset_id</code> → <Chip label="{dataset_id}" size="small" /></li>
            <li><code>tenant_id</code> → <Chip label="{tenant_id}" size="small" /></li>
            <li><code>client_id</code> → <Chip label="{client_id}" size="small" /></li>
            <li><code>client_secret</code> → <Chip label="{client_secret}" size="small" /></li>
            <li><code>target_catalog</code> → <Chip label="{target_catalog}" size="small" /></li>
            <li><code>target_schema</code> → <Chip label="{target_schema}" size="small" /></li>
          </Box>
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
              helperText="Power BI workspace ID to scan"
              size="small"
            />
            <TextField
              label="Dataset ID (Optional)"
              value={value.dataset_id || ''}
              onChange={(e) => handleFieldChange('dataset_id', e.target.value)}
              disabled={disabled}
              fullWidth
              helperText="Specific dataset to filter (leave empty to scan all)"
              size="small"
            />
          </Box>

          <Divider sx={{ my: 1 }}>
            <Typography variant="caption" color="text.secondary">
              Service Principal Authentication
            </Typography>
          </Divider>

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

          <Divider sx={{ my: 1 }}>
            <Typography variant="caption" color="text.secondary">
              Target Unity Catalog Location
            </Typography>
          </Divider>

          <Box sx={{ display: 'flex', gap: 2 }}>
            <TextField
              label="Target Catalog"
              value={value.target_catalog || 'main'}
              onChange={(e) => handleFieldChange('target_catalog', e.target.value)}
              disabled={disabled}
              fullWidth
              helperText="Unity Catalog catalog name"
              size="small"
            />
            <TextField
              label="Target Schema"
              value={value.target_schema || 'default'}
              onChange={(e) => handleFieldChange('target_schema', e.target.value)}
              disabled={disabled}
              fullWidth
              helperText="Unity Catalog schema name"
              size="small"
            />
          </Box>
        </>
      )}

      {/* Advanced Options (shown in both modes) */}
      <Accordion sx={{ mt: 1 }}>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="subtitle2">Advanced Options</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
            {/* LLM Configuration */}
            <Typography variant="subtitle2" color="secondary" sx={{ fontWeight: 600 }}>
              LLM Conversion Settings
            </Typography>
            <FormControlLabel
              control={
                <Checkbox
                  checked={value.use_llm !== false}
                  onChange={(e) => handleFieldChange('use_llm', e.target.checked)}
                  disabled={disabled}
                />
              }
              label="Use LLM for complex M-Query conversions"
            />
            {value.use_llm !== false && (
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
              </>
            )}

            <Divider sx={{ my: 1 }} />

            {/* Scan Options */}
            <Typography variant="subtitle2" color="secondary" sx={{ fontWeight: 600 }}>
              Scan Options
            </Typography>
            <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0 }}>
              <FormControlLabel
                control={
                  <Checkbox
                    checked={value.include_lineage !== false}
                    onChange={(e) => handleFieldChange('include_lineage', e.target.checked)}
                    disabled={disabled}
                    size="small"
                  />
                }
                label={<Typography variant="body2">Include lineage</Typography>}
              />
              <FormControlLabel
                control={
                  <Checkbox
                    checked={value.include_datasource_details !== false}
                    onChange={(e) => handleFieldChange('include_datasource_details', e.target.checked)}
                    disabled={disabled}
                    size="small"
                  />
                }
                label={<Typography variant="body2">Include data source details</Typography>}
              />
              <FormControlLabel
                control={
                  <Checkbox
                    checked={value.include_dataset_schema !== false}
                    onChange={(e) => handleFieldChange('include_dataset_schema', e.target.checked)}
                    disabled={disabled}
                    size="small"
                  />
                }
                label={<Typography variant="body2">Include dataset schema</Typography>}
              />
              <FormControlLabel
                control={
                  <Checkbox
                    checked={value.include_dataset_expressions !== false}
                    onChange={(e) => handleFieldChange('include_dataset_expressions', e.target.checked)}
                    disabled={disabled}
                    size="small"
                  />
                }
                label={<Typography variant="body2">Include M-Query expressions</Typography>}
              />
              <FormControlLabel
                control={
                  <Checkbox
                    checked={value.include_hidden_tables || false}
                    onChange={(e) => handleFieldChange('include_hidden_tables', e.target.checked)}
                    disabled={disabled}
                    size="small"
                  />
                }
                label={<Typography variant="body2">Include hidden tables</Typography>}
              />
              <FormControlLabel
                control={
                  <Checkbox
                    checked={value.skip_static_tables !== false}
                    onChange={(e) => handleFieldChange('skip_static_tables', e.target.checked)}
                    disabled={disabled}
                    size="small"
                  />
                }
                label={<Typography variant="body2">Skip static tables (Table.FromRows)</Typography>}
              />
            </Box>

            <Divider sx={{ my: 1 }} />

            {/* Output Options */}
            <Typography variant="subtitle2" color="secondary" sx={{ fontWeight: 600 }}>
              Output Options
            </Typography>
            <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0 }}>
              <FormControlLabel
                control={
                  <Checkbox
                    checked={value.include_relationships !== false}
                    onChange={(e) => handleFieldChange('include_relationships', e.target.checked)}
                    disabled={disabled}
                    size="small"
                  />
                }
                label={<Typography variant="body2">Include relationships (FK constraints)</Typography>}
              />
              <FormControlLabel
                control={
                  <Checkbox
                    checked={value.include_summary !== false}
                    onChange={(e) => handleFieldChange('include_summary', e.target.checked)}
                    disabled={disabled}
                    size="small"
                  />
                }
                label={<Typography variant="body2">Include summary report</Typography>}
              />
            </Box>
          </Box>
        </AccordionDetails>
      </Accordion>

      {/* Info about what the tool does */}
      <Alert severity="info" variant="outlined" sx={{ mt: 1 }}>
        <Typography variant="body2" sx={{ fontWeight: 600, mb: 0.5 }}>
          What this tool does:
        </Typography>
        <Typography variant="caption">
          Scans Power BI workspaces via the Admin API to extract M-Query (Power Query) expressions
          and converts them to Databricks SQL CREATE VIEW statements. Supports:
        </Typography>
        <Box component="ul" sx={{ pl: 2, mt: 0.5, mb: 0, fontSize: '0.75rem' }}>
          <li>Value.NativeQuery (embedded SQL)</li>
          <li>DatabricksMultiCloud.Catalogs connections</li>
          <li>Sql.Database connections</li>
          <li>Table transformations (Table.SelectRows, Table.AddColumn, etc.)</li>
          <li>Relationship extraction as FK constraints</li>
        </Box>
      </Alert>
    </Box>
  );
};
