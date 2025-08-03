/**
 * Component for displaying saved Databricks Vector Search configuration
 */

import React from 'react';
import {
  Box,
  Typography,
  Chip,
  Button,
  Alert,
} from '@mui/material';
import {
  CheckCircle as CheckCircleIcon,
  Edit as EditIcon,
  Refresh as RefreshIcon,
  Save as SaveIcon,
  Cancel as CancelIcon,
} from '@mui/icons-material';
import { SavedConfigInfo } from '../../types/memoryBackend';

interface ConfigurationDisplayProps {
  savedConfig: SavedConfigInfo;
  isEditingConfig: boolean;
  isSettingUp: boolean;
  onStartEdit: () => void;
  onSaveEdit: () => void;
  onCancelEdit: () => void;
  onRefresh: () => void;
  children?: React.ReactNode;
}

export const ConfigurationDisplay: React.FC<ConfigurationDisplayProps> = ({
  savedConfig,
  isEditingConfig,
  isSettingUp,
  onStartEdit,
  onSaveEdit,
  onCancelEdit,
  onRefresh,
  children,
}) => {
  return (
    <Box sx={{ mt: 2, p: 2, bgcolor: (theme) => theme.palette.mode === 'light' ? 'rgba(76, 175, 80, 0.08)' : 'rgba(102, 187, 106, 0.16)', borderRadius: 2, border: '1px solid', borderColor: 'success.main' }}>
      <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 1 }}>
        <Box sx={{ display: 'flex', alignItems: 'center' }}>
          <CheckCircleIcon sx={{ mr: 0.5, color: 'success.main', fontSize: 18 }} />
          <Typography variant="subtitle2" color="success.main">
            Active Configuration
          </Typography>
        </Box>
        <Box sx={{ display: 'flex', gap: 0.5 }}>
          {!isEditingConfig && (
            <>
              <Button
                size="small"
                startIcon={<EditIcon />}
                onClick={onStartEdit}
                disabled={isSettingUp}
                sx={{ minWidth: 'auto', py: 0.5 }}
              >
                Edit
              </Button>
              <Button
                size="small"
                startIcon={<RefreshIcon />}
                onClick={onRefresh}
                disabled={isSettingUp}
                sx={{ minWidth: 'auto', py: 0.5 }}
              >
                Refresh
              </Button>
            </>
          )}
          {isEditingConfig && (
            <>
              <Button
                size="small"
                startIcon={<SaveIcon />}
                onClick={onSaveEdit}
                disabled={isSettingUp}
                sx={{ minWidth: 'auto', py: 0.5 }}
                color="primary"
              >
                Save
              </Button>
              <Button
                size="small"
                startIcon={<CancelIcon />}
                onClick={onCancelEdit}
                disabled={isSettingUp}
                sx={{ minWidth: 'auto', py: 0.5 }}
              >
                Cancel
              </Button>
            </>
          )}
        </Box>
      </Box>
      
      <Box sx={{ display: 'flex', gap: 1, mb: 1, flexWrap: 'wrap' }}>
        <Chip size="small" label={`Workspace: ${savedConfig.workspace_url?.replace('https://', '').split('?')[0]}`} />
        <Chip size="small" label={`Catalog: ${savedConfig.catalog}`} />
        <Chip size="small" label={`Schema: ${savedConfig.schema}`} />
      </Box>

      {isEditingConfig && (
        <Alert severity="info" sx={{ mb: 1 }}>
          Enter the names of your existing Databricks Vector Search endpoints and indexes. Leave empty to remove.
        </Alert>
      )}

      {children}
    </Box>
  );
};