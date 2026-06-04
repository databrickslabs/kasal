/**
 * Pipeline Config Generator Configuration Selector Component
 *
 * Provides configuration UI for the Pipeline Config Generator tool (Tool 90).
 * Calls 4 PBI APIs directly — no LLM intermediation.
 * Requires two Service Principals: non-admin (Execute Queries) and admin (Admin Scanner).
 */

import React from 'react';
import {
  Box,
  Typography,
  TextField,
  Alert,
  Accordion,
  AccordionSummary,
  AccordionDetails
} from '@mui/material';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';

export interface PipelineConfigGeneratorConfig {
  // PBI Configuration
  workspace_id?: string;
  dataset_id?: string;
  report_id?: string;
  // Non-Admin SP (Execute Queries API)
  tenant_id?: string;
  client_id?: string;
  client_secret?: string;
  // Admin SP (Admin Scanner API)
  admin_client_id?: string;
  admin_client_secret?: string;
  // Target
  catalog?: string;
  schema_name?: string;
  // Index signature for compatibility
  [key: string]: string | undefined;
}

interface PipelineConfigGeneratorConfigSelectorProps {
  value: PipelineConfigGeneratorConfig;
  onChange: (config: PipelineConfigGeneratorConfig) => void;
  disabled?: boolean;
}

export const PipelineConfigGeneratorConfigSelector: React.FC<PipelineConfigGeneratorConfigSelectorProps> = ({
  value = {},
  onChange,
  disabled = false
}) => {
  const handleFieldChange = (field: keyof PipelineConfigGeneratorConfig, fieldValue: string) => {
    onChange({
      ...value,
      [field]: fieldValue
    });
  };

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
      {/* Info note */}
      <Alert severity="info" variant="outlined">
        <Typography variant="caption">
          This tool calls 4 Power BI APIs directly to generate <code>pipeline_config.json</code> with
          all 26 config keys. No LLM intermediation &mdash; no data truncation.
          Requires two Service Principals with different permission levels.
        </Typography>
      </Alert>

      {/* PBI Configuration */}
      <Typography variant="subtitle2" sx={{ fontWeight: 600, color: 'rgb(25, 118, 210)' }}>
        Power BI Configuration
      </Typography>
      <Box sx={{ display: 'flex', gap: 2 }}>
        <TextField
          label="Workspace ID"
          value={value.workspace_id || ''}
          onChange={(e) => handleFieldChange('workspace_id', e.target.value)}
          disabled={disabled}
          fullWidth
          size="small"
          required
          helperText="PBI Workspace GUID"
        />
        <TextField
          label="Dataset ID"
          value={value.dataset_id || ''}
          onChange={(e) => handleFieldChange('dataset_id', e.target.value)}
          disabled={disabled}
          fullWidth
          size="small"
          required
          helperText="PBI Dataset / Semantic Model GUID"
        />
      </Box>

      {/* Non-Admin SP */}
      <Typography variant="subtitle2" sx={{ fontWeight: 600, color: 'rgb(76, 175, 80)' }}>
        Non-Admin Service Principal (Execute Queries API)
      </Typography>
      <TextField
        label="Tenant ID"
        value={value.tenant_id || ''}
        onChange={(e) => handleFieldChange('tenant_id', e.target.value)}
        disabled={disabled}
        fullWidth
        size="small"
        required
        helperText="Azure AD Tenant ID (shared by both SPs)"
      />
      <Box sx={{ display: 'flex', gap: 2 }}>
        <TextField
          label="Client ID"
          value={value.client_id || ''}
          onChange={(e) => handleFieldChange('client_id', e.target.value)}
          disabled={disabled}
          fullWidth
          size="small"
          required
          helperText="Workspace member with SemanticModel.ReadWrite.All"
        />
        <TextField
          label="Client Secret"
          value={value.client_secret || ''}
          onChange={(e) => handleFieldChange('client_secret', e.target.value)}
          disabled={disabled}
          fullWidth
          size="small"
          required
          type="password"
          helperText="Non-admin SP secret"
        />
      </Box>

      {/* Admin SP */}
      <Typography variant="subtitle2" sx={{ fontWeight: 600, color: 'rgb(255, 152, 0)' }}>
        Admin Service Principal (Admin Scanner API)
      </Typography>
      <Box sx={{ display: 'flex', gap: 2 }}>
        <TextField
          label="Admin Client ID"
          value={value.admin_client_id || ''}
          onChange={(e) => handleFieldChange('admin_client_id', e.target.value)}
          disabled={disabled}
          fullWidth
          size="small"
          required
          helperText="Power BI Admin with Tenant.Read.All"
        />
        <TextField
          label="Admin Client Secret"
          value={value.admin_client_secret || ''}
          onChange={(e) => handleFieldChange('admin_client_secret', e.target.value)}
          disabled={disabled}
          fullWidth
          size="small"
          required
          type="password"
          helperText="Admin SP secret"
        />
      </Box>

      {/* Target Configuration */}
      <Typography variant="subtitle2" sx={{ fontWeight: 600, color: 'rgb(76, 175, 80)' }}>
        Target Configuration
      </Typography>
      <Box sx={{ display: 'flex', gap: 2 }}>
        <TextField
          label="Target Catalog"
          value={value.catalog || ''}
          onChange={(e) => handleFieldChange('catalog', e.target.value)}
          disabled={disabled}
          fullWidth
          size="small"
          helperText="Unity Catalog name (default: main)"
        />
        <TextField
          label="Target Schema"
          value={value.schema_name || ''}
          onChange={(e) => handleFieldChange('schema_name', e.target.value)}
          disabled={disabled}
          fullWidth
          size="small"
          helperText="UC Schema name (default: default)"
        />
      </Box>

      {/* Optional: Report ID */}
      <Accordion sx={{ mt: 1 }}>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="subtitle2">Optional: Report Metadata</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <TextField
            label="Report ID"
            value={value.report_id || ''}
            onChange={(e) => handleFieldChange('report_id', e.target.value)}
            disabled={disabled}
            fullWidth
            size="small"
            helperText="PBIR Report GUID for visual display names and dimension ordering (optional)"
          />
        </AccordionDetails>
      </Accordion>

      {/* Info about what the tool does */}
      <Alert severity="info" variant="outlined" sx={{ mt: 1 }}>
        <Typography variant="body2" sx={{ fontWeight: 600, mb: 0.5 }}>
          What this tool does:
        </Typography>
        <Typography variant="caption" component="div">
          Calls 4 PBI APIs directly and produces all 26 pipeline_config keys:
        </Typography>
        <Typography variant="caption" component="div" sx={{ mt: 0.5 }}>
          <strong>API 1</strong>: INFO.VIEW.RELATIONSHIPS() &rarr; join_key_map, enrichment_joins, dim_alias_map<br />
          <strong>API 2</strong>: $SYSTEM.MDSCHEMA_MEASURES &rarr; switch_decompositions, filter_sets, measure_resolutions<br />
          <strong>API 3</strong>: Admin Scanner &rarr; column_metadata, dimension_exclusions, period_dim_priority<br />
          <strong>API 4</strong>: Report Definition (optional) &rarr; measure_metadata, dimension_metadata
        </Typography>
      </Alert>
    </Box>
  );
};
