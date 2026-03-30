/**
 * Manual configuration form for Databricks Vector Search
 */

import React from 'react';
import {
  Box,
  TextField,
  Button,
  Alert,
  Typography,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  CircularProgress,
} from '@mui/material';
import { Save as SaveIcon } from '@mui/icons-material';
import { ManualConfig } from '../../types/memoryBackend';
import { EMBEDDING_MODELS } from './constants';
import { validateVectorSearchIndexName } from './databricksVectorSearchUtils';

interface ManualConfigurationFormProps {
  manualConfig: ManualConfig;
  isSettingUp: boolean;
  error: string;
  onConfigChange: (config: ManualConfig) => void;
  onSave: () => void;
}

export const ManualConfigurationForm: React.FC<ManualConfigurationFormProps> = ({
  manualConfig,
  isSettingUp,
  error,
  onConfigChange,
  onSave,
}) => {
  const handleChange = (field: keyof ManualConfig) => (
    event: React.ChangeEvent<HTMLInputElement | { value: unknown }>
  ) => {
    onConfigChange({
      ...manualConfig,
      [field]: event.target.value as string,
    });
  };

  const isInvalidIndex = (indexValue: string) => {
    return indexValue !== '' && !validateVectorSearchIndexName(indexValue);
  };

  const hasValidationErrors = () => {
    return (
      !manualConfig.workspace_url ||
      !manualConfig.endpoint_name ||
      !manualConfig.document_endpoint_name ||
      !manualConfig.short_term_index ||
      !manualConfig.long_term_index ||
      !manualConfig.entity_index ||
      !manualConfig.document_index ||
      isInvalidIndex(manualConfig.short_term_index) ||
      isInvalidIndex(manualConfig.long_term_index) ||
      isInvalidIndex(manualConfig.entity_index) ||
      isInvalidIndex(manualConfig.document_index)
    );
  };

  return (
    <Box>
      <Alert severity="info" sx={{ mb: 3 }}>
        <Typography variant="body2" sx={{ mb: 1 }}>
          Enter your existing Databricks Vector Search configuration. This will connect to your existing endpoints and indexes.
        </Typography>
        <Typography variant="body2">
          <strong>Format for indexes:</strong> catalog.schema.index_name (e.g., ml.agents.short_term_memory)
        </Typography>
      </Alert>

      {error && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {error}
        </Alert>
      )}

      <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
        <TextField
          fullWidth
          label="Databricks Workspace URL"
          value={manualConfig.workspace_url}
          onChange={handleChange('workspace_url')}
          placeholder="https://your-workspace.databricks.com"
          required
          disabled={isSettingUp}
        />

        <Typography variant="subtitle2" sx={{ mt: 2 }}>
          Vector Search Endpoints
        </Typography>

        <TextField
          fullWidth
          label="Memory Endpoint Name"
          value={manualConfig.endpoint_name}
          onChange={handleChange('endpoint_name')}
          placeholder="kasal_memory_endpoint"
          required
          disabled={isSettingUp}
        />

        <TextField
          fullWidth
          label="Document Endpoint Name"
          value={manualConfig.document_endpoint_name}
          onChange={handleChange('document_endpoint_name')}
          placeholder="kasal_docs_endpoint"
          required
          disabled={isSettingUp}
        />

        <FormControl fullWidth>
          <InputLabel>Embedding Model</InputLabel>
          <Select
            value={manualConfig.embedding_model}
            onChange={(e) => handleChange('embedding_model')(e as React.ChangeEvent<HTMLInputElement | { value: unknown }>)}
            label="Embedding Model"
            disabled={isSettingUp}
          >
            {EMBEDDING_MODELS.map((model) => (
              <MenuItem key={model.value} value={model.value}>
                {model.name} ({model.dimension} dimensions)
              </MenuItem>
            ))}
          </Select>
        </FormControl>

        <Typography variant="subtitle2" sx={{ mt: 2 }}>
          Memory Indexes (catalog.schema.index_name)
        </Typography>

        <TextField
          fullWidth
          label="Short-term Memory Index"
          value={manualConfig.short_term_index}
          onChange={handleChange('short_term_index')}
          placeholder="ml.agents.short_term_memory"
          required
          error={isInvalidIndex(manualConfig.short_term_index)}
          helperText={
            isInvalidIndex(manualConfig.short_term_index)
              ? 'Invalid format. Use: catalog.schema.index_name'
              : ''
          }
          disabled={isSettingUp}
        />

        <TextField
          fullWidth
          label="Long-term Memory Index"
          value={manualConfig.long_term_index}
          onChange={handleChange('long_term_index')}
          placeholder="ml.agents.long_term_memory"
          required
          error={isInvalidIndex(manualConfig.long_term_index)}
          helperText={
            isInvalidIndex(manualConfig.long_term_index)
              ? 'Invalid format. Use: catalog.schema.index_name'
              : ''
          }
          disabled={isSettingUp}
        />

        <TextField
          fullWidth
          label="Entity Memory Index"
          value={manualConfig.entity_index}
          onChange={handleChange('entity_index')}
          placeholder="ml.agents.entity_memory"
          required
          error={isInvalidIndex(manualConfig.entity_index)}
          helperText={
            isInvalidIndex(manualConfig.entity_index)
              ? 'Invalid format. Use: catalog.schema.index_name'
              : ''
          }
          disabled={isSettingUp}
        />

        <TextField
          fullWidth
          label="Document Embeddings Index"
          value={manualConfig.document_index}
          onChange={handleChange('document_index')}
          placeholder="ml.agents.document_embeddings"
          required
          error={isInvalidIndex(manualConfig.document_index)}
          helperText={
            isInvalidIndex(manualConfig.document_index)
              ? 'Invalid format. Use: catalog.schema.index_name'
              : ''
          }
          disabled={isSettingUp}
        />

        <Button
          variant="contained"
          color="primary"
          startIcon={isSettingUp ? <CircularProgress size={20} /> : <SaveIcon />}
          onClick={onSave}
          disabled={isSettingUp || hasValidationErrors()}
          sx={{ mt: 3 }}
          fullWidth
        >
          {isSettingUp ? 'Saving Configuration...' : 'Save Configuration'}
        </Button>
      </Box>
    </Box>
  );
};