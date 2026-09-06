import React, { useState, useEffect } from 'react';
import {
  Box,
  TextField,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  FormControlLabel,
  Switch,
  Typography,
  Alert,
  SelectChangeEvent,
  InputAdornment
} from '@mui/material';

export interface DatabricksVolumeConfig {
  volume_path: string;
  file_format: 'json' | 'csv' | 'txt';
  create_date_dirs: boolean;
  max_file_size_mb: number;
  workspace_url?: string;
  token?: string;
}

interface DatabricksVolumeConfigProps {
  config: DatabricksVolumeConfig | null;
  onChange: (config: DatabricksVolumeConfig) => void;
}

const DEFAULT_CONFIG: DatabricksVolumeConfig = {
  volume_path: 'main.default.task_outputs',  // Using catalog.schema.volume format
  file_format: 'json',
  create_date_dirs: true,
  max_file_size_mb: 50.0  // Default 50MB, sufficient for model outputs
};

export const DatabricksVolumeConfigComponent: React.FC<DatabricksVolumeConfigProps> = ({
  config,
  onChange
}) => {
  const [localConfig, setLocalConfig] = useState<DatabricksVolumeConfig>(() => {
    console.log('DatabricksVolumeConfig - initial config:', config);
    return config || DEFAULT_CONFIG;
  });

  useEffect(() => {
    console.log('DatabricksVolumeConfig - received config:', config);
    if (config) {
      console.log('DatabricksVolumeConfig - updating localConfig with:', config);
      setLocalConfig(config);
    } else {
      console.log('DatabricksVolumeConfig - no config provided, using default');
      setLocalConfig(DEFAULT_CONFIG);
    }
  }, [config]);

  const handleChange = (field: keyof DatabricksVolumeConfig, value: string | number | boolean | undefined) => {
    const updatedConfig = {
      ...localConfig,
      [field]: value
    };
    setLocalConfig(updatedConfig);
    onChange(updatedConfig);
  };

  const handleFormatChange = (event: SelectChangeEvent<string>) => {
    handleChange('file_format', event.target.value as 'json' | 'csv' | 'txt');
  };

  return (
    <Box sx={{ mt: 2, p: 2, border: '1px solid', borderColor: 'divider', borderRadius: 1 }}>
      <Typography variant="subtitle2" color="primary" sx={{ mb: 2 }}>
        Databricks Volume Configuration
      </Typography>

      <Alert severity="info" sx={{ mb: 2 }}>
        Task outputs will be uploaded to Databricks Volumes. Ensure DATABRICKS_HOST and DATABRICKS_TOKEN 
        environment variables are set, or configure them below.
      </Alert>

      <TextField
        fullWidth
        label="Volume Path"
        value={localConfig.volume_path}
        onChange={(e) => handleChange('volume_path', e.target.value)}
        helperText="Format: catalog.schema.volume (e.g., catalog.schema.volume)"
        placeholder="catalog.schema.volume"
        sx={{ mb: 2 }}
        required
      />

      <FormControl fullWidth sx={{ mb: 2 }}>
        <InputLabel>File Format</InputLabel>
        <Select
          value={localConfig.file_format}
          onChange={handleFormatChange}
          label="File Format"
        >
          <MenuItem value="json">JSON</MenuItem>
          <MenuItem value="csv">CSV</MenuItem>
          <MenuItem value="txt">Text</MenuItem>
        </Select>
      </FormControl>

      <FormControlLabel
        control={
          <Switch
            checked={localConfig.create_date_dirs}
            onChange={(e) => handleChange('create_date_dirs', e.target.checked)}
          />
        }
        label="Create Date-based Directories"
        sx={{ mb: 2 }}
      />



      <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mt: 1 }}>
        Optional: Override environment variables (leave empty to use defaults)
      </Typography>

      <TextField
        fullWidth
        label="Workspace URL (Optional)"
        value={localConfig.workspace_url || ''}
        onChange={(e) => handleChange('workspace_url', e.target.value ? e.target.value : undefined)}
        placeholder="https://your-workspace.databricks.com"
        sx={{ mb: 2, mt: 1 }}
        InputProps={{
          startAdornment: <InputAdornment position="start">https://</InputAdornment>,
        }}
      />

      <TextField
        fullWidth
        label="Access Token (Optional)"
        value={localConfig.token || ''}
        onChange={(e) => handleChange('token', e.target.value ? e.target.value : undefined)}
        type="password"
        placeholder="Leave empty to use DATABRICKS_TOKEN env var"
        sx={{ mb: 1 }}
      />

      <Alert severity="warning" sx={{ mt: 2 }}>
        <Typography variant="caption">
          <strong>Note:</strong> Files will be organized as: {localConfig.volume_path}/
          {localConfig.create_date_dirs ? 'YYYY/MM/DD/' : ''}
          task_key_timestamp.{localConfig.file_format}
        </Typography>
      </Alert>
    </Box>
  );
};