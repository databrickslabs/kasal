/**
 * Power BI Relationships Configuration Selector Component
 *
 * Provides configuration UI for the Power BI Relationships Tool.
 * Extracts relationships from Power BI semantic models and generates Unity Catalog FK constraints.
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

export interface PowerBIRelationshipsConfig {
  // Configuration mode
  mode?: 'static' | 'dynamic';
  // Power BI Configuration
  workspace_id?: string;
  dataset_id?: string;
  // Service Principal authentication (must be workspace member)
  tenant_id?: string;
  client_id?: string;
  client_secret?: string;
  // Unity Catalog Target
  target_catalog?: string;
  target_schema?: string;
  // Output Options
  include_inactive?: boolean;
  skip_system_tables?: boolean;
  // Index signature for compatibility
  [key: string]: string | boolean | undefined;
}

interface PowerBIRelationshipsConfigSelectorProps {
  value: PowerBIRelationshipsConfig;
  onChange: (config: PowerBIRelationshipsConfig) => void;
  disabled?: boolean;
}

export const PowerBIRelationshipsConfigSelector: React.FC<PowerBIRelationshipsConfigSelectorProps> = ({
  value = {},
  onChange,
  disabled = false
}) => {
  const handleFieldChange = (field: keyof PowerBIRelationshipsConfig, fieldValue: string | boolean) => {
    onChange({
      ...value,
      [field]: fieldValue
    });
  };

  const handleModeChange = (_event: React.MouseEvent<HTMLElement>, newMode: 'static' | 'dynamic' | null) => {
    if (newMode !== null) {
      const updatedConfig: PowerBIRelationshipsConfig = {
        ...value,
        mode: newMode
      };

      // When switching to dynamic mode, auto-populate placeholders
      if (newMode === 'dynamic') {
        // Set Power BI parameters to placeholders
        updatedConfig.workspace_id = '{workspace_id}';
        updatedConfig.dataset_id = '{dataset_id}';
        updatedConfig.tenant_id = '{tenant_id}';
        updatedConfig.client_id = '{client_id}';
        updatedConfig.client_secret = '{client_secret}';
        // Unity Catalog target can also be dynamic
        updatedConfig.target_catalog = '{target_catalog}';
        updatedConfig.target_schema = '{target_schema}';
        // Keep output options as static values
        updatedConfig.include_inactive = value.include_inactive || false;
        updatedConfig.skip_system_tables = value.skip_system_tables !== false;
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
            <Chip label="dataset_id" size="small" color="primary" variant="outlined" />
            <Chip label="tenant_id" size="small" color="primary" variant="outlined" />
            <Chip label="client_id" size="small" color="primary" variant="outlined" />
            <Chip label="client_secret" size="small" color="primary" variant="outlined" />
          </Box>
          <Box sx={{ mt: 1 }}>
            <Typography variant="caption" color="text.secondary">
              <strong>Optional:</strong>
            </Typography>
            <Chip label="target_catalog" size="small" variant="outlined" sx={{ ml: 0.5 }} />
            <Chip label="target_schema" size="small" variant="outlined" sx={{ ml: 0.5 }} />
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
              helperText="Power BI workspace ID containing the semantic model"
              size="small"
            />
            <TextField
              label="Dataset ID"
              value={value.dataset_id || ''}
              onChange={(e) => handleFieldChange('dataset_id', e.target.value)}
              disabled={disabled}
              required
              fullWidth
              helperText="Semantic model/dataset ID to extract relationships from"
              size="small"
            />
          </Box>

          <Divider sx={{ my: 1 }}>
            <Typography variant="caption" color="text.secondary">
              Service Principal Authentication
            </Typography>
          </Divider>

          <Alert severity="warning" variant="outlined" sx={{ mb: 1 }}>
            <Typography variant="caption">
              <strong>Important:</strong> The Service Principal must be a <strong>member of the workspace</strong> with at least read permissions.
              This is different from the Admin API which requires admin-level permissions.
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

          <Divider sx={{ my: 1 }}>
            <Typography variant="caption" color="text.secondary">
              Unity Catalog Target
            </Typography>
          </Divider>

          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
            <TextField
              label="Target Catalog"
              value={value.target_catalog || 'main'}
              onChange={(e) => handleFieldChange('target_catalog', e.target.value)}
              disabled={disabled}
              fullWidth
              helperText="Unity Catalog catalog name for FK statements"
              size="small"
            />
            <TextField
              label="Target Schema"
              value={value.target_schema || 'default'}
              onChange={(e) => handleFieldChange('target_schema', e.target.value)}
              disabled={disabled}
              fullWidth
              helperText="Unity Catalog schema name for FK statements"
              size="small"
            />
          </Box>
        </>
      )}

      {/* Advanced Options (shown in both modes) */}
      <Accordion sx={{ mt: 1 }}>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="subtitle2">Output Options</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
            <FormControlLabel
              control={
                <Checkbox
                  checked={value.include_inactive || false}
                  onChange={(e) => handleFieldChange('include_inactive', e.target.checked)}
                  disabled={disabled}
                  size="small"
                />
              }
              label={<Typography variant="body2">Include inactive relationships</Typography>}
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
          </Box>
        </AccordionDetails>
      </Accordion>

      {/* Info about what the tool does */}
      <Alert severity="info" variant="outlined" sx={{ mt: 1 }}>
        <Typography variant="body2" sx={{ fontWeight: 600, mb: 0.5 }}>
          What this tool does:
        </Typography>
        <Typography variant="caption">
          Extracts relationships from Power BI semantic models using the Execute Queries API
          with <code>INFO.VIEW.RELATIONSHIPS()</code> DAX function. Generates Unity Catalog
          Foreign Key constraint statements (<code>NOT ENFORCED</code>).
        </Typography>
        <Box component="ul" sx={{ pl: 2, mt: 0.5, mb: 0, fontSize: '0.75rem' }}>
          <li>Extracts all relationships (1:1, 1:*, *:1, *:*)</li>
          <li>Generates ALTER TABLE ADD CONSTRAINT FOREIGN KEY statements</li>
          <li>Includes cardinality and cross-filtering metadata</li>
          <li>Filters system tables and inactive relationships</li>
        </Box>
      </Alert>
    </Box>
  );
};
