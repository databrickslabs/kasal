/**
 * Automatic setup form for Databricks Vector Search
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
  Grid,
} from '@mui/material';
import { CloudSync as CloudSyncIcon } from '@mui/icons-material';
import { EMBEDDING_MODELS } from './constants';

interface AutomaticSetupFormProps {
  detectedWorkspaceUrl?: string | null;
  catalog: string;
  schema: string;
  embeddingModel: string;
  isSettingUp: boolean;
  error: string;
  onCatalogChange: (catalog: string) => void;
  onSchemaChange: (schema: string) => void;
  onEmbeddingModelChange: (model: string) => void;
  onSetup: () => void;
}

export const AutomaticSetupForm: React.FC<AutomaticSetupFormProps> = ({
  detectedWorkspaceUrl,
  catalog,
  schema,
  embeddingModel,
  isSettingUp,
  error,
  onCatalogChange,
  onSchemaChange,
  onEmbeddingModelChange,
  onSetup,
}) => {
  return (
    <Box>
      <Alert severity="info" sx={{ mb: 3 }}>
        <Typography variant="body2" sx={{ mb: 1 }}>
          <strong>One-click setup will automatically:</strong>
        </Typography>
        <Typography variant="body2" component="ul" sx={{ m: 0, pl: 2 }}>
          <li>Create two Vector Search endpoints (one for memory, one for documents)</li>
          <li>Create four indexes (short-term, long-term, entity memory, and documents)</li>
          <li>Configure optimal settings for agent memory persistence</li>
          <li>Enable Databricks as the default memory backend</li>
        </Typography>
      </Alert>

      {detectedWorkspaceUrl && (
        <Alert severity="success" sx={{ mb: 2 }}>
          Workspace detected: {detectedWorkspaceUrl}
        </Alert>
      )}

      {!detectedWorkspaceUrl && (
        <Alert severity="warning" sx={{ mb: 2 }}>
          No Databricks workspace detected. Please set DATABRICKS_HOST environment variable.
        </Alert>
      )}

      {error && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {error}
        </Alert>
      )}

      <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>

        <Grid container spacing={2}>
          <Grid item xs={6}>
            <TextField
              fullWidth
              label="Catalog"
              value={catalog}
              onChange={(e) => onCatalogChange(e.target.value)}
              placeholder="ml"
              required
              disabled={isSettingUp}
              helperText="Unity Catalog name for indexes"
            />
          </Grid>
          <Grid item xs={6}>
            <TextField
              fullWidth
              label="Schema"
              value={schema}
              onChange={(e) => onSchemaChange(e.target.value)}
              placeholder="agents"
              required
              disabled={isSettingUp}
              helperText="Schema name for indexes"
            />
          </Grid>
        </Grid>

        <FormControl fullWidth>
          <InputLabel>Embedding Model</InputLabel>
          <Select
            value={embeddingModel}
            onChange={(e) => onEmbeddingModelChange(e.target.value)}
            label="Embedding Model"
            disabled={isSettingUp}
          >
            {EMBEDDING_MODELS.map((model) => (
              <MenuItem key={model.value} value={model.value}>
                {model.name} ({model.dimension} dimensions)
              </MenuItem>
            ))}
          </Select>
          <Typography variant="caption" color="text.secondary" sx={{ mt: 1 }}>
            Choose the embedding model for document and memory vectorization
          </Typography>
        </FormControl>

        <Alert severity="warning" sx={{ mt: 2 }}>
          <Typography variant="body2">
            <strong>Important:</strong> This will create resources in your Databricks workspace. 
            Ensure you have the necessary permissions to create endpoints and indexes.
          </Typography>
        </Alert>

        <Button
          variant="contained"
          color="primary"
          size="large"
          startIcon={isSettingUp ? <CircularProgress size={20} /> : <CloudSyncIcon />}
          onClick={onSetup}
          disabled={!detectedWorkspaceUrl || !catalog || !schema || isSettingUp}
          fullWidth
          sx={{ mt: 3, py: 1.5 }}
        >
          {isSettingUp ? 'Setting up Databricks Vector Search...' : 'Setup Databricks Vector Search'}
        </Button>
      </Box>
    </Box>
  );
};