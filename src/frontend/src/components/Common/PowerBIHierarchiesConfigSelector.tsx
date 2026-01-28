/**
 * Power BI Hierarchies Configuration Selector Component
 *
 * Provides configuration UI for the Power BI Hierarchies Tool.
 * Extracts hierarchies from Power BI/Fabric semantic models using the Fabric API getDefinition
 * endpoint (TMDL format) and generates Unity Catalog dimension views.
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

export interface PowerBIHierarchiesConfig {
  // Configuration mode
  mode?: 'static' | 'dynamic';
  // Power BI / Fabric Configuration
  workspace_id?: string;
  dataset_id?: string;
  // Service Principal authentication (must have SemanticModel.ReadWrite.All permission)
  tenant_id?: string;
  client_id?: string;
  client_secret?: string;
  // Unity Catalog Target
  target_catalog?: string;
  target_schema?: string;
  // Output Options
  skip_system_tables?: boolean;
  include_hidden?: boolean;
  // Index signature for compatibility
  [key: string]: string | boolean | undefined;
}

interface PowerBIHierarchiesConfigSelectorProps {
  value: PowerBIHierarchiesConfig;
  onChange: (config: PowerBIHierarchiesConfig) => void;
  disabled?: boolean;
}

export const PowerBIHierarchiesConfigSelector: React.FC<PowerBIHierarchiesConfigSelectorProps> = ({
  value = {},
  onChange,
  disabled = false
}) => {
  const handleFieldChange = (field: keyof PowerBIHierarchiesConfig, fieldValue: string | boolean) => {
    onChange({
      ...value,
      [field]: fieldValue
    });
  };

  const handleModeChange = (_event: React.MouseEvent<HTMLElement>, newMode: 'static' | 'dynamic' | null) => {
    if (newMode !== null) {
      const updatedConfig: PowerBIHierarchiesConfig = {
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
        updatedConfig.skip_system_tables = value.skip_system_tables !== false;
        updatedConfig.include_hidden = value.include_hidden || false;
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
          {/* Power BI / Fabric Configuration */}
          <Typography variant="subtitle2" color="primary" sx={{ fontWeight: 600 }}>
            Power BI / Fabric Configuration
          </Typography>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
            <TextField
              label="Workspace ID"
              value={value.workspace_id || ''}
              onChange={(e) => handleFieldChange('workspace_id', e.target.value)}
              disabled={disabled}
              required
              fullWidth
              helperText="Power BI/Fabric workspace ID containing the semantic model"
              size="small"
            />
            <TextField
              label="Dataset ID (Semantic Model ID)"
              value={value.dataset_id || ''}
              onChange={(e) => handleFieldChange('dataset_id', e.target.value)}
              disabled={disabled}
              required
              fullWidth
              helperText="Semantic model/dataset ID to extract hierarchies from"
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
              <strong>Important:</strong> The Service Principal must have <strong>SemanticModel.ReadWrite.All</strong> permissions
              or be a <strong>member of the workspace</strong>. This tool uses the Fabric API <code>getDefinition</code> endpoint
              which requires appropriate access to fetch TMDL definitions.
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
              helperText="Application/Client ID with SemanticModel.ReadWrite.All permission"
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
              helperText="Unity Catalog catalog name for dimension views"
              size="small"
            />
            <TextField
              label="Target Schema"
              value={value.target_schema || 'default'}
              onChange={(e) => handleFieldChange('target_schema', e.target.value)}
              disabled={disabled}
              fullWidth
              helperText="Unity Catalog schema name for dimension views"
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
                  checked={value.skip_system_tables !== false}
                  onChange={(e) => handleFieldChange('skip_system_tables', e.target.checked)}
                  disabled={disabled}
                  size="small"
                />
              }
              label={<Typography variant="body2">Skip system tables (LocalDateTable, DateTableTemplate, etc.)</Typography>}
            />
            <FormControlLabel
              control={
                <Checkbox
                  checked={value.include_hidden || false}
                  onChange={(e) => handleFieldChange('include_hidden', e.target.checked)}
                  disabled={disabled}
                  size="small"
                />
              }
              label={<Typography variant="body2">Include hidden hierarchies</Typography>}
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
          Extracts hierarchies from Power BI/Fabric semantic models using the Fabric API
          <code>getDefinition</code> endpoint which returns TMDL format. Generates Unity Catalog
          dimension views with <code>hierarchy_path</code> column and metadata tables.
        </Typography>
        <Box component="ul" sx={{ pl: 2, mt: 0.5, mb: 0, fontSize: '0.75rem' }}>
          <li>Extracts all hierarchy definitions (levels, columns, ordinals)</li>
          <li>Generates CREATE VIEW statements with CONCAT-based hierarchy_path</li>
          <li>Creates _metadata_hierarchies table DDL and INSERT statements</li>
          <li>Supports drill-down path visualization</li>
          <li>Filters system date hierarchies by default</li>
        </Box>
      </Alert>
    </Box>
  );
};
