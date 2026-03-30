/**
 * MCP Server Selector Component
 * 
 * A multi-select dropdown for selecting MCP servers configured in the system.
 * Follows the same pattern as GenieSpaceSelector and other tool selectors.
 */

import React, { useState, useEffect } from 'react';
import {
  Autocomplete,
  TextField,
  CircularProgress,
  Box,
  Typography,
  Chip,
  FormHelperText,
  InputAdornment
} from '@mui/material';
import StorageIcon from '@mui/icons-material/Storage';
import { MCPService } from '../../api/MCPService';
import { MCPServerConfig } from '../Configuration/MCP/MCPConfiguration';

interface MCPServerSelectorProps {
  value: string | string[] | null;
  onChange: (value: string | string[] | null) => void;
  multiple?: boolean;
  label?: string;
  placeholder?: string;
  disabled?: boolean;
  required?: boolean;
  helperText?: string;
  error?: boolean;
  fullWidth?: boolean;
}

export const MCPServerSelector: React.FC<MCPServerSelectorProps> = ({
  value,
  onChange,
  multiple = true,
  label = 'MCP Servers',
  placeholder = 'Select MCP servers...',
  disabled = false,
  required = false,
  helperText,
  error = false,
  fullWidth = true,
}) => {
  const [open, setOpen] = useState(false);
  const [options, setOptions] = useState<MCPServerConfig[]>([]);
  const [selectedOptions, setSelectedOptions] = useState<MCPServerConfig | MCPServerConfig[] | null>(null);
  const [loading, setLoading] = useState(false);
  const [mcpError, setMcpError] = useState<string | null>(null);

  // Fetch MCP servers when component mounts or opens
  useEffect(() => {
    if (open && options.length === 0) {
      fetchMCPServers();
    }
  }, [open, options.length]);

  // Update selected options when value prop changes
  useEffect(() => {
    if (!value) {
      setSelectedOptions(multiple ? [] : null);
      return;
    }

    if (options.length > 0) {
      if (multiple) {
        const valueArray = Array.isArray(value) ? value : [value];
        const selected = options.filter(server => 
          valueArray.includes(server.name) // Use name instead of id
        );
        setSelectedOptions(selected);
      } else {
        const selected = options.find(server => server.name === value); // Use name instead of id
        setSelectedOptions(selected || null);
      }
    } else {
      // If options haven't loaded yet, set to empty state
      setSelectedOptions(multiple ? [] : null);
    }
  }, [value, options, multiple]);

  const fetchMCPServers = async () => {
    try {
      setLoading(true);
      setMcpError(null);
      
      const mcpService = MCPService.getInstance();
      const response = await mcpService.getMcpServers();
      
      // Only show enabled servers
      const enabledServers = response.servers.filter(server => server.enabled);
      setOptions(enabledServers);
      
      console.log('Fetched MCP servers for selector:', enabledServers);
    } catch (error) {
      console.error('Error fetching MCP servers:', error);
      setMcpError('Failed to load MCP servers');
      setOptions([]);
    } finally {
      setLoading(false);
    }
  };

  const handleSelectionChange = (_: React.SyntheticEvent, newValue: MCPServerConfig | MCPServerConfig[] | null) => {
    setSelectedOptions(newValue);

    // Convert back to names for the parent component (consistent with backend expectation)
    if (!newValue) {
      onChange(multiple ? [] : null);
    } else if (multiple) {
      const servers = Array.isArray(newValue) ? newValue : [newValue];
      const names = servers.map(server => server.name);
      onChange(names);
    } else {
      const server = Array.isArray(newValue) ? newValue[0] : newValue;
      onChange(server?.name || null);
    }
  };

  const getOptionLabel = (option: MCPServerConfig) => {
    return `${option.name} (${option.server_type})`;
  };

  const renderOption = (props: React.HTMLAttributes<HTMLLIElement>, option: MCPServerConfig) => (
    <Box component="li" {...props}>
      <StorageIcon sx={{ mr: 2, color: 'primary.main' }} />
      <Box>
        <Typography variant="body2" fontWeight="medium">
          {option.name}
        </Typography>
        <Typography variant="caption" color="text.secondary">
          {option.server_type} • {option.server_url || 'No URL'}
        </Typography>
      </Box>
    </Box>
  );

  const renderTags = (tagValue: readonly MCPServerConfig[], getTagProps: (params: { index: number }) => object) =>
    tagValue.map((option, index) => (
      <Chip
        {...getTagProps({ index })}
        key={option.name}
        label={option.name}
        size="small"
        color="primary"
        variant="outlined"
      />
    ));

  return (
    <Box>
      <Autocomplete
        multiple={multiple}
        open={open}
        onOpen={() => setOpen(true)}
        onClose={() => setOpen(false)}
        value={multiple ? (selectedOptions as MCPServerConfig[] || []) : (selectedOptions as MCPServerConfig || null)}
        onChange={handleSelectionChange}
        options={options}
        getOptionLabel={getOptionLabel}
        renderOption={renderOption}
        renderTags={multiple ? renderTags : undefined}
        loading={loading}
        disabled={disabled}
        fullWidth={fullWidth}
        isOptionEqualToValue={(option, val) => option.name === val.name}
        renderInput={(params) => (
          <TextField
            {...params}
            label={label}
            placeholder={selectedOptions && (multiple 
              ? (selectedOptions as MCPServerConfig[]).length > 0 
              : selectedOptions) ? '' : placeholder}
            required={required}
            error={error || !!mcpError}
            InputProps={{
              ...params.InputProps,
              startAdornment: (
                <InputAdornment position="start">
                  <StorageIcon color="action" />
                </InputAdornment>
              ),
              endAdornment: (
                <>
                  {loading ? <CircularProgress color="inherit" size={20} /> : null}
                  {params.InputProps.endAdornment}
                </>
              ),
            }}
          />
        )}
        noOptionsText={
          mcpError ? (
            <Box sx={{ p: 2, textAlign: 'center' }}>
              <Typography color="error" variant="body2">
                {mcpError}
              </Typography>
            </Box>
          ) : loading ? (
            <Box sx={{ p: 2, textAlign: 'center' }}>
              <CircularProgress size={20} />
              <Typography variant="body2" sx={{ ml: 1 }}>
                Loading MCP servers...
              </Typography>
            </Box>
          ) : (
            <Box sx={{ p: 2, textAlign: 'center' }}>
              <Typography variant="body2" color="text.secondary">
                No enabled MCP servers found
              </Typography>
              <Typography variant="caption" color="text.secondary">
                Configure MCP servers in Settings → Configuration → MCP
              </Typography>
            </Box>
          )
        }
      />
      {(helperText || mcpError) && (
        <FormHelperText error={error || !!mcpError}>
          {mcpError || helperText}
        </FormHelperText>
      )}
    </Box>
  );
};

export default MCPServerSelector;