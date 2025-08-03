/**
 * Component for managing Databricks Vector Search indexes in a table format
 */

import React from 'react';
import {
  Box,
  Typography,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  Link,
  IconButton,
  Tooltip,
  CircularProgress,
} from '@mui/material';
import {
  Refresh as RefreshIcon,
  ClearAll as ClearAllIcon,
  Delete as DeleteIcon,
  Info as InfoIcon,
} from '@mui/icons-material';
import { SavedConfigInfo, IndexInfoState } from '../../types/memoryBackend';
import { buildVectorSearchIndexUrl, buildVectorSearchEndpointUrl } from './databricksVectorSearchUtils';
import { INDEX_DESCRIPTIONS } from './constants';

interface IndexManagementTableProps {
  title: string;
  subtitle?: string;
  savedConfig: SavedConfigInfo;
  endpointName?: string;
  endpointType: 'memory' | 'document';
  indexes: Array<{
    type: 'short_term' | 'long_term' | 'entity' | 'document';
    name?: string;
  }>;
  indexInfoMap: Record<string, IndexInfoState>;
  endpointStatuses: Record<string, { state?: string; can_delete_indexes?: boolean }>;
  isSettingUp: boolean;
  onRefresh?: () => void;
  onEmpty: (indexType: string) => void | Promise<void>;
  onDelete: (indexType: string) => void | Promise<void>;
  showEndpointLink?: boolean;
}

export const IndexManagementTable: React.FC<IndexManagementTableProps> = ({
  title,
  subtitle,
  savedConfig,
  endpointName,
  endpointType,
  indexes,
  indexInfoMap,
  endpointStatuses,
  isSettingUp,
  onRefresh,
  onEmpty,
  onDelete,
  showEndpointLink = true,
}) => {
  const renderIndexStatus = (indexName: string) => {
    const info = indexInfoMap[indexName];
    if (!info) return 'UNKNOWN';
    
    if (info.loading) {
      return <CircularProgress size={16} />;
    }
    
    return (
      <Typography 
        variant="body2" 
        sx={{ 
          fontSize: '0.875rem',
          color: info.index_type === 'DELETED' ? 'error.main' : 'inherit'
        }}
      >
        {info.index_type === 'DELETED' ? 'NOT FOUND' : (info.index_type || 'UNKNOWN')}
      </Typography>
    );
  };

  const renderDocumentCount = (indexName: string) => {
    const info = indexInfoMap[indexName];
    if (!info || info.loading) {
      return <CircularProgress size={16} />;
    }
    return info.doc_count || 0;
  };

  const isDeleteDisabled = (indexName: string) => {
    const info = indexInfoMap[indexName];
    const endpoint = endpointStatuses[endpointType];
    
    return isSettingUp || 
           endpoint?.can_delete_indexes === false || 
           info?.index_type === 'DELETED';
  };

  return (
    <Box sx={{ mb: 4 }}>
      <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
        <Typography variant="subtitle1" sx={{ fontWeight: 'medium' }}>
          {title}
        </Typography>
        {subtitle && (
          <Tooltip 
            title={subtitle}
            placement="top"
            arrow
          >
            <IconButton size="small" sx={{ ml: 1, p: 0.5 }}>
              <InfoIcon fontSize="small" sx={{ fontSize: 18 }} />
            </IconButton>
          </Tooltip>
        )}
      </Box>
      
      {showEndpointLink && endpointName && (
        <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
          Endpoint: 
          <Link
            href={buildVectorSearchEndpointUrl(savedConfig.workspace_url || '', endpointName)}
            target="_blank"
            rel="noopener noreferrer"
            sx={{ ml: 1 }}
          >
            {endpointName}
          </Link>
        </Typography>
      )}
      
      <TableContainer component={Paper} variant="outlined">
        <Table size="small" sx={{ minWidth: 650 }}>
          <TableHead>
            <TableRow>
              <TableCell>Name</TableCell>
              <TableCell align="center">Type</TableCell>
              <TableCell align="center">Documents</TableCell>
              <TableCell>Description</TableCell>
              <TableCell align="center">Actions</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {indexes.map(({ type, name }) => {
              if (!name) return null;
              const description = INDEX_DESCRIPTIONS[type] || { brief: '', detailed: '' };
              
              return (
                <TableRow key={type}>
                  <TableCell>
                    <Link
                      href={buildVectorSearchIndexUrl(savedConfig.workspace_url || '', name)}
                      target="_blank"
                      rel="noopener noreferrer"
                      sx={{ fontSize: '0.875rem' }}
                    >
                      {name.split('.').pop()}
                    </Link>
                  </TableCell>
                  <TableCell align="center">
                    {renderIndexStatus(name)}
                  </TableCell>
                  <TableCell align="center">
                    {renderDocumentCount(name)}
                  </TableCell>
                  <TableCell>
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
                      {description.brief}
                      <Tooltip 
                        title={description.detailed}
                        placement="top"
                        arrow
                        sx={{ maxWidth: 400 }}
                      >
                        <IconButton size="small" sx={{ p: 0.5 }}>
                          <InfoIcon fontSize="small" sx={{ fontSize: 16 }} />
                        </IconButton>
                      </Tooltip>
                    </Box>
                  </TableCell>
                  <TableCell align="center">
                    {type === 'document' && onRefresh && (
                      <Tooltip title="Re-seed Documentation">
                        <IconButton
                          size="small"
                          onClick={onRefresh}
                          disabled={isSettingUp}
                        >
                          <RefreshIcon fontSize="small" />
                        </IconButton>
                      </Tooltip>
                    )}
                    <Tooltip title="Empty Index">
                      <IconButton
                        size="small"
                        onClick={() => onEmpty(type)}
                        disabled={isSettingUp}
                      >
                        <ClearAllIcon fontSize="small" />
                      </IconButton>
                    </Tooltip>
                    <Tooltip title="Delete Index">
                      <IconButton
                        size="small"
                        onClick={() => onDelete(type)}
                        disabled={isDeleteDisabled(name)}
                      >
                        <DeleteIcon fontSize="small" />
                      </IconButton>
                    </Tooltip>
                  </TableCell>
                </TableRow>
              );
            })}
          </TableBody>
        </Table>
      </TableContainer>
    </Box>
  );
};