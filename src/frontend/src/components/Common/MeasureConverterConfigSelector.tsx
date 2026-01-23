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
  Chip
} from '@mui/material';

export interface MeasureConverterConfig {
  // Configuration mode
  mode?: 'static' | 'dynamic';
  inbound_connector?: string;
  outbound_format?: string;
  // Power BI inbound params
  powerbi_semantic_model_id?: string;
  powerbi_group_id?: string;
  // Power BI Service Principal authentication
  powerbi_tenant_id?: string;
  powerbi_client_id?: string;
  powerbi_client_secret?: string;
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
  // UC Metrics outbound params
  uc_catalog?: string;
  uc_schema?: string;
  uc_process_structures?: boolean;
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
  const handleFieldChange = (field: keyof MeasureConverterConfig, fieldValue: string | boolean) => {
    onChange({
      ...value,
      [field]: fieldValue
    });
  };

  const handleSelectChange = (field: keyof MeasureConverterConfig) => (event: SelectChangeEvent) => {
    handleFieldChange(field, event.target.value);
  };

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
        updatedConfig.powerbi_tenant_id = '{tenant_id}';
        updatedConfig.powerbi_client_id = '{client_id}';
        updatedConfig.powerbi_client_secret = '{client_secret}';

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
            Use this mode when calling the crew from external applications.
            <br /><br />
            <strong>Required execution inputs:</strong>
          </Typography>
          <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5, mt: 1 }}>
            {inboundConnector === 'powerbi' && (
              <>
                <Chip label="dataset_id" size="small" color="primary" variant="outlined" />
                <Chip label="workspace_id" size="small" color="primary" variant="outlined" />
                <Chip label="tenant_id" size="small" color="primary" variant="outlined" />
                <Chip label="client_id" size="small" color="primary" variant="outlined" />
                <Chip label="client_secret" size="small" color="primary" variant="outlined" />
              </>
            )}
            {outboundFormat && (
              <Chip label="target" size="small" color="secondary" variant="outlined" />
            )}
          </Box>
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
                <li><code>powerbi_tenant_id</code> → <Chip label="{tenant_id}" size="small" /></li>
                <li><code>powerbi_client_id</code> → <Chip label="{client_id}" size="small" /></li>
                <li><code>powerbi_client_secret</code> → <Chip label="{client_secret}" size="small" /></li>
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
                  Service Principal Authentication
                </Typography>
              </Divider>
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

      {/* Outbound Configuration */}
      {outboundFormat && (
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

          {mode === 'static' && outboundFormat === 'uc_metrics' && (
            <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
              <TextField
                label="Unity Catalog Catalog"
                value={value.uc_catalog || 'main'}
                onChange={(e) => handleFieldChange('uc_catalog', e.target.value)}
                disabled={disabled}
                fullWidth
                size="small"
              />
              <TextField
                label="Unity Catalog Schema"
                value={value.uc_schema || 'default'}
                onChange={(e) => handleFieldChange('uc_schema', e.target.value)}
                disabled={disabled}
                fullWidth
                size="small"
              />
              <FormControlLabel
                control={
                  <Checkbox
                    checked={value.uc_process_structures !== false}
                    onChange={(e) => handleFieldChange('uc_process_structures', e.target.checked)}
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
