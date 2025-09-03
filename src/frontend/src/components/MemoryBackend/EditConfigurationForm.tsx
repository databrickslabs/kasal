/**
 * Edit configuration form for Databricks Vector Search
 */

import React from 'react';
import {
  Box,
  TextField,
  Grid,
  Typography,
} from '@mui/material';
import { SavedConfigInfo } from '../../types/memoryBackend';

interface EditConfigurationFormProps {
  editedConfig: SavedConfigInfo | null;
  onEditChange: (field: string, value: string | undefined) => void;
}

export const EditConfigurationForm: React.FC<EditConfigurationFormProps> = ({
  editedConfig,
  onEditChange,
}) => {
  return (
    <Box>
      {/* Endpoints Section */}
      <Box sx={{ mb: 1.5 }}>
        <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mb: 0.5 }}>
          Endpoints:
        </Typography>
        <Box sx={{ display: 'flex', gap: 0.5, flexWrap: 'wrap', alignItems: 'center' }}>
          <TextField
            size="small"
            label="Memory Endpoint"
            value={editedConfig?.endpoints?.memory?.name || ''}
            onChange={(e) => onEditChange('endpoints.memory.name', e.target.value || undefined)}
            placeholder="kasal_memory_endpoint"
            sx={{ width: 250 }}
          />
          <TextField
            size="small"
            label="Document Endpoint"
            value={editedConfig?.endpoints?.document?.name || ''}
            onChange={(e) => onEditChange('endpoints.document.name', e.target.value || undefined)}
            placeholder="kasal_docs_endpoint"
            sx={{ width: 250 }}
          />
        </Box>
      </Box>

      {/* Indexes Section */}
      <Box>
        <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mb: 0.5 }}>
          Indexes:
        </Typography>
        
        <Grid container spacing={1}>
          <Grid item xs={12} sm={6}>
            <TextField
              size="small"
              label="Short-term Index"
              value={editedConfig?.indexes?.short_term?.name || ''}
              onChange={(e) => onEditChange('indexes.short_term.name', e.target.value || undefined)}
              placeholder="catalog.schema.short_term_memory"
              fullWidth
            />
          </Grid>
          <Grid item xs={12} sm={6}>
            <TextField
              size="small"
              label="Long-term Index"
              value={editedConfig?.indexes?.long_term?.name || ''}
              onChange={(e) => onEditChange('indexes.long_term.name', e.target.value || undefined)}
              placeholder="catalog.schema.long_term_memory"
              fullWidth
            />
          </Grid>
          <Grid item xs={12} sm={6}>
            <TextField
              size="small"
              label="Entity Index"
              value={editedConfig?.indexes?.entity?.name || ''}
              onChange={(e) => onEditChange('indexes.entity.name', e.target.value || undefined)}
              placeholder="catalog.schema.entity_memory"
              fullWidth
            />
          </Grid>
          <Grid item xs={12} sm={6}>
            <TextField
              size="small"
              label="Document Index"
              value={editedConfig?.indexes?.document?.name || ''}
              onChange={(e) => onEditChange('indexes.document.name', e.target.value || undefined)}
              placeholder="catalog.schema.document_embeddings"
              fullWidth
            />
          </Grid>
        </Grid>
      </Box>
    </Box>
  );
};