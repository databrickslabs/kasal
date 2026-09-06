/**
 * Config Generator Configuration Selector Component
 *
 * Provides configuration UI for the Config Generator tool (Tool 89).
 * Auto-proposes pipeline_config.json from PBI extraction output.
 * Typically used in a crew after Tools 73/74/75 have already extracted the JSONs.
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

export interface ConfigGeneratorConfig {
  // Input data (typically from prior crew tasks)
  measures_json?: string;
  mquery_json?: string;
  relationships_json?: string;
  scan_data_json?: string;
  // Target
  catalog?: string;
  schema_name?: string;
  // Index signature for compatibility
  [key: string]: string | undefined;
}

interface ConfigGeneratorConfigSelectorProps {
  value: ConfigGeneratorConfig;
  onChange: (config: ConfigGeneratorConfig) => void;
  disabled?: boolean;
}

export const ConfigGeneratorConfigSelector: React.FC<ConfigGeneratorConfigSelectorProps> = ({
  value = {},
  onChange,
  disabled = false
}) => {
  const handleFieldChange = (field: keyof ConfigGeneratorConfig, fieldValue: string) => {
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
          This tool auto-proposes <code>pipeline_config.json</code> from PBI extraction output.
          Typically used in a crew after Tools 73/74/75 have already extracted the JSONs.
          The input data fields below are optional &mdash; they are usually provided by prior tasks in the pipeline.
        </Typography>
      </Alert>

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
          helperText="Unity Catalog name (e.g., david_test_metrics)"
        />
        <TextField
          label="Target Schema"
          value={value.schema_name || ''}
          onChange={(e) => handleFieldChange('schema_name', e.target.value)}
          disabled={disabled}
          fullWidth
          size="small"
          helperText="Schema for generated config output"
        />
      </Box>

      {/* Input Data (Accordion - collapsed by default) */}
      <Accordion sx={{ mt: 1 }}>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="subtitle2">Input Data (Optional)</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
            <Alert severity="warning" variant="outlined" sx={{ mb: 1 }}>
              <Typography variant="caption">
                Only paste JSON here if running this tool standalone.
                In a crew pipeline, these values are automatically passed from upstream extraction tasks.
              </Typography>
            </Alert>
            <TextField
              label="Measures JSON"
              value={value.measures_json || ''}
              onChange={(e) => handleFieldChange('measures_json', e.target.value)}
              disabled={disabled}
              fullWidth
              multiline
              rows={3}
              helperText="JSON string of extracted measures (from Tool 73)"
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
              helperText="JSON string of M-Query expressions (from Tool 74)"
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
              helperText="JSON string of table relationships (from Tool 75)"
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
          </Box>
        </AccordionDetails>
      </Accordion>

      {/* Info about what the tool does */}
      <Alert severity="info" variant="outlined" sx={{ mt: 1 }}>
        <Typography variant="body2" sx={{ fontWeight: 600, mb: 0.5 }}>
          What this tool does:
        </Typography>
        <Typography variant="caption">
          Analyzes PBI extraction output (measures, M-Query, relationships, scan data) and
          generates an optimized <code>pipeline_config.json</code> that controls how UC Metric Views
          are generated. Handles table classification, join strategy, and conversion settings.
        </Typography>
      </Alert>
    </Box>
  );
};
