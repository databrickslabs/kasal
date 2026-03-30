/**
 * Display endpoints with status chips
 */

import React from 'react';
import {
  Box,
  Chip,
  Typography,
} from '@mui/material';
import {
  Memory as MemoryIcon,
  Storage as StorageIcon,
  Delete as DeleteIcon,
} from '@mui/icons-material';
import { SavedConfigInfo } from '../../types/memoryBackend';
import { buildVectorSearchEndpointUrl, hasActiveVectorSearchIndexes } from './databricksVectorSearchUtils';

interface EndpointsDisplayProps {
  savedConfig: SavedConfigInfo;
  endpointStatuses: Record<string, { state?: string; can_delete_indexes?: boolean }>;
  onDeleteEndpoint: (type: 'memory' | 'document') => void;
}

export const EndpointsDisplay: React.FC<EndpointsDisplayProps> = ({
  savedConfig,
  endpointStatuses,
  onDeleteEndpoint,
}) => {
  return (
    <Box sx={{ mb: 1.5 }}>
      <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mb: 0.5 }}>
        Endpoints:
      </Typography>
      <Box sx={{ display: 'flex', gap: 0.5, flexWrap: 'wrap', alignItems: 'center' }}>
        {savedConfig.endpoints?.memory && (
          <Chip
            size="small"
            label={`Memory Endpoint${endpointStatuses.memory?.state ? ` (${endpointStatuses.memory.state})` : ''}`}
            clickable
            icon={<MemoryIcon />}
            color={
              endpointStatuses.memory?.state === 'ONLINE' ? 'success' : 
              endpointStatuses.memory?.state === 'PROVISIONING' ? 'warning' : 
              endpointStatuses.memory?.state === 'NOT_FOUND' ? 'error' : 
              'default'
            }
            onClick={() => window.open(buildVectorSearchEndpointUrl(savedConfig.workspace_url || '', savedConfig.endpoints?.memory?.name || ''), '_blank')}
            onDelete={
              hasActiveVectorSearchIndexes(savedConfig, 'memory') && endpointStatuses.memory?.state !== 'NOT_FOUND' 
                ? undefined 
                : () => onDeleteEndpoint('memory')
            }
            deleteIcon={<DeleteIcon />}
          />
        )}
        {savedConfig.endpoints?.document && (
          <Chip
            size="small"
            label={`Document Endpoint${endpointStatuses.document?.state ? ` (${endpointStatuses.document.state})` : ''}`}
            clickable
            icon={<StorageIcon />}
            color={
              endpointStatuses.document?.state === 'ONLINE' ? 'success' : 
              endpointStatuses.document?.state === 'PROVISIONING' ? 'warning' : 
              endpointStatuses.document?.state === 'NOT_FOUND' ? 'error' : 
              'default'
            }
            onClick={() => window.open(buildVectorSearchEndpointUrl(savedConfig.workspace_url || '', savedConfig.endpoints?.document?.name || ''), '_blank')}
            onDelete={
              hasActiveVectorSearchIndexes(savedConfig, 'document') && endpointStatuses.document?.state !== 'NOT_FOUND' 
                ? undefined 
                : () => onDeleteEndpoint('document')
            }
            deleteIcon={<DeleteIcon />}
          />
        )}
      </Box>
    </Box>
  );
};