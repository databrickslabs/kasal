/**
 * Simplified Memory Backend Selector Component
 * 
 * This component provides a compact interface for selecting and configuring
 * memory backends within agent or workflow configuration forms.
 */

import React, { useState } from 'react';
import {
  Box,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Button,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Chip,
  Typography,
  IconButton,
  Tooltip,
} from '@mui/material';
import {
  Settings as SettingsIcon,
  CheckCircle as CheckCircleIcon,
  Warning as WarningIcon,
} from '@mui/icons-material';

import { MemoryBackendConfig } from './MemoryBackendConfig';
import { 
  MemoryBackendType, 
  getBackendDisplayName,
  MemoryBackendConfig as MemoryBackendConfigType,
  DEFAULT_MEMORY_BACKEND_CONFIG,
} from '../../types/memoryBackend';

interface MemoryBackendSelectorProps {
  value: MemoryBackendConfigType | undefined;
  onChange: (config: MemoryBackendConfigType) => void;
  required?: boolean;
  disabled?: boolean;
  error?: boolean;
  helperText?: string;
}

export const MemoryBackendSelector: React.FC<MemoryBackendSelectorProps> = ({
  value = DEFAULT_MEMORY_BACKEND_CONFIG,
  onChange,
  required = false,
  disabled = false,
  error = false,
  helperText,
}) => {
  const [dialogOpen, setDialogOpen] = useState(false);
  const [tempConfig, setTempConfig] = useState<MemoryBackendConfigType>(value);
  const [isValid, setIsValid] = useState(true);

  const handleOpenDialog = () => {
    setTempConfig(value);
    setDialogOpen(true);
  };

  const handleCloseDialog = () => {
    setDialogOpen(false);
  };

  const handleSaveConfig = () => {
    if (isValid) {
      onChange(tempConfig);
      setDialogOpen(false);
    }
  };

  const getStatusChip = () => {
    if (value.backend_type === MemoryBackendType.DEFAULT) {
      return (
        <Chip
          icon={<CheckCircleIcon />}
          label="Ready"
          color="success"
          size="small"
        />
      );
    }

    if (value.backend_type === MemoryBackendType.DATABRICKS) {
      const isConfigured = value.databricks_config?.endpoint_name && 
                          value.databricks_config?.short_term_index;
      
      if (isConfigured) {
        return (
          <Chip
            icon={<CheckCircleIcon />}
            label="Configured"
            color="success"
            size="small"
          />
        );
      } else {
        return (
          <Chip
            icon={<WarningIcon />}
            label="Not Configured"
            color="warning"
            size="small"
          />
        );
      }
    }

    return null;
  };

  const getQuickSummary = () => {
    if (value.backend_type === MemoryBackendType.DATABRICKS && value.databricks_config) {
      const parts = [];
      if (value.databricks_config.endpoint_name) {
        parts.push(`Endpoint: ${value.databricks_config.endpoint_name}`);
      }
      if (value.databricks_config.short_term_index) {
        parts.push(`Index: ${value.databricks_config.short_term_index}`);
      }
      return parts.join(' | ');
    }
    return '';
  };

  return (
    <>
      <Box>
        <FormControl fullWidth required={required} disabled={disabled} error={error}>
          <InputLabel>Memory Backend</InputLabel>
          <Select
            value={value.backend_type}
            onChange={(e) => {
              const newType = e.target.value as MemoryBackendType;
              if (newType === MemoryBackendType.DEFAULT) {
                onChange({ ...DEFAULT_MEMORY_BACKEND_CONFIG, backend_type: newType });
              } else {
                handleOpenDialog();
              }
            }}
            label="Memory Backend"
            endAdornment={
              value.backend_type !== MemoryBackendType.DEFAULT && (
                <Tooltip title="Configure Backend">
                  <IconButton
                    size="small"
                    onClick={(e) => {
                      e.stopPropagation();
                      handleOpenDialog();
                    }}
                    sx={{ mr: 2 }}
                  >
                    <SettingsIcon />
                  </IconButton>
                </Tooltip>
              )
            }
          >
            {Object.values(MemoryBackendType).map((type) => (
              <MenuItem key={type} value={type}>
                <Box sx={{ display: 'flex', alignItems: 'center', width: '100%' }}>
                  <Typography sx={{ flexGrow: 1 }}>
                    {getBackendDisplayName(type)}
                  </Typography>
                  {value.backend_type === type && getStatusChip()}
                </Box>
              </MenuItem>
            ))}
          </Select>
        </FormControl>
        
        {helperText && (
          <Typography variant="caption" color={error ? 'error' : 'text.secondary'} sx={{ mt: 0.5, ml: 1.75, display: 'block' }}>
            {helperText}
          </Typography>
        )}

        {value.backend_type !== MemoryBackendType.DEFAULT && getQuickSummary() && (
          <Typography variant="caption" color="text.secondary" sx={{ mt: 0.5, ml: 1.75, display: 'block' }}>
            {getQuickSummary()}
          </Typography>
        )}
      </Box>

      {/* Configuration Dialog */}
      <Dialog
        open={dialogOpen}
        onClose={handleCloseDialog}
        maxWidth="md"
        fullWidth
      >
        <DialogTitle>
          Configure {getBackendDisplayName(tempConfig.backend_type)}
        </DialogTitle>
        <DialogContent>
          <Box sx={{ pt: 2 }}>
            <MemoryBackendConfig
              embedded
              onConfigChange={setIsValid}
            />
          </Box>
        </DialogContent>
        <DialogActions>
          <Button onClick={handleCloseDialog}>Cancel</Button>
          <Button 
            onClick={handleSaveConfig} 
            variant="contained"
            disabled={!isValid}
          >
            Save Configuration
          </Button>
        </DialogActions>
      </Dialog>
    </>
  );
};