/**
 * AgentBricks Endpoint Selector Component
 *
 * A searchable dropdown for selecting AgentBricks endpoints.
 */

import React, { useState, useEffect, useRef } from 'react';
import {
  Autocomplete,
  TextField,
  CircularProgress,
  Box,
  Typography,
  Chip,
  InputAdornment,
  IconButton,
  Tooltip
} from '@mui/material';
import SearchIcon from '@mui/icons-material/Search';
import ClearIcon from '@mui/icons-material/Clear';
import PushPinIcon from '@mui/icons-material/PushPin';
import PushPinOutlinedIcon from '@mui/icons-material/PushPinOutlined';
import { AgentBricksService, AgentBricksEndpoint, AgentBricksEndpointsResponse } from '../../api/AgentBricksService';

interface AgentBricksEndpointSelectorProps {
  value: string | string[] | null;
  onChange: (value: string | string[] | null, endpointName?: string) => void;
  multiple?: boolean;
  label?: string;
  placeholder?: string;
  disabled?: boolean;
  required?: boolean;
  helperText?: string;
  error?: boolean;
  fullWidth?: boolean;
  toolId?: number;
}

export const AgentBricksEndpointSelector: React.FC<AgentBricksEndpointSelectorProps> = ({
  value,
  onChange,
  multiple = false,
  label = 'AgentBricks Endpoint',
  placeholder = 'Search for AgentBricks endpoints...',
  disabled = false,
  required = false,
  helperText,
  error = false,
  fullWidth = true,
  toolId
}) => {
  const [open, setOpen] = useState(false);
  const [options, setOptions] = useState<AgentBricksEndpoint[]>([]);
  const [selectedOptions, setSelectedOptions] = useState<AgentBricksEndpoint | AgentBricksEndpoint[] | null>(null);
  const [inputValue, setInputValue] = useState('');
  const [loading, setLoading] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');
  const [pinnedEndpoints, setPinnedEndpoints] = useState<string[]>([]);
  const searchTimeoutRef = useRef<NodeJS.Timeout | null>(null);

  // Load pinned endpoints from localStorage
  useEffect(() => {
    const saved = localStorage.getItem('pinnedAgentBricksEndpoints');
    if (saved) {
      try {
        setPinnedEndpoints(JSON.parse(saved));
      } catch (e) {
        console.error('Failed to load pinned endpoints:', e);
      }
    }
  }, []);

  // Save pinned endpoints to localStorage
  const savePinnedEndpoints = (endpoints: string[]) => {
    setPinnedEndpoints(endpoints);
    localStorage.setItem('pinnedAgentBricksEndpoints', JSON.stringify(endpoints));
  };

  // Toggle pin for an endpoint
  const togglePin = (endpointName: string) => {
    if (pinnedEndpoints.includes(endpointName)) {
      savePinnedEndpoints(pinnedEndpoints.filter(name => name !== endpointName));
    } else {
      savePinnedEndpoints([...pinnedEndpoints, endpointName]);
    }
  };

  // Clear selected endpoint
  const handleClear = () => {
    setSelectedOptions(null);
    onChange(null);
    setInputValue('');
  };

  // Convert value (name) to selected option(s)
  useEffect(() => {
    const loadSelectedEndpoint = async () => {
      if (value) {
        if (multiple && Array.isArray(value)) {
          const selected = options.filter(opt => value.includes(opt.name));
          setSelectedOptions(selected.length > 0 ? selected : null);
        } else if (!multiple && typeof value === 'string') {
          // First check if we already have this endpoint in options
          let selected = options.find(opt => opt.name === value);

          // If not found in options and we have a value, try to fetch it
          if (!selected && value) {
            try {
              selected = await AgentBricksService.getEndpointByName(value);

              if (selected) {
                // Add to options if not already there
                setOptions(prev => {
                  const exists = prev.some(opt => opt.name === selected?.name);
                  if (!exists && selected) {
                    return [...prev, selected];
                  }
                  return prev;
                });
              }
            } catch (error) {
              console.error('Failed to load selected endpoint:', error);
            }
          }

          setSelectedOptions(selected || null);
        }
      } else {
        setSelectedOptions(null);
      }
    };

    loadSelectedEndpoint();
  }, [value, multiple, options]);

  // Load endpoints function
  const loadEndpoints = async (search?: string) => {
    try {
      setLoading(true);

      let response: AgentBricksEndpointsResponse;
      if (search) {
        response = await AgentBricksService.searchEndpoints({
          search_query: search,
          ready_only: true
        });
      } else {
        response = await AgentBricksService.getEndpoints(true);
      }

      setOptions(response.endpoints);
    } catch (error) {
      console.error('Error loading AgentBricks endpoints:', error);
    } finally {
      setLoading(false);
    }
  };

  // Handle input change for search with manual debouncing
  const handleInputChange = (_event: React.SyntheticEvent, newInputValue: string) => {
    setInputValue(newInputValue);

    if (open) {
      // Clear existing timeout
      if (searchTimeoutRef.current) {
        clearTimeout(searchTimeoutRef.current);
      }

      // Set new timeout
      searchTimeoutRef.current = setTimeout(() => {
        setSearchQuery(newInputValue);
        loadEndpoints(newInputValue);
      }, 300);
    }
  };

  // Load initial data when dropdown opens or when we have a value
  useEffect(() => {
    // Load immediately if we have a value but no options
    if (value && options.length === 0 && !loading) {
      loadEndpoints();
    }
    // Also load when dropdown opens
    if (open && options.length === 0 && !loading) {
      loadEndpoints();
    }
  }, [open, options.length, value, loading]);

  // Cleanup timeout on unmount
  useEffect(() => {
    return () => {
      if (searchTimeoutRef.current) {
        clearTimeout(searchTimeoutRef.current);
      }
    };
  }, []);

  // Handle selection change
  const handleChange = (_event: React.SyntheticEvent, newValue: AgentBricksEndpoint | AgentBricksEndpoint[] | null) => {
    setSelectedOptions(newValue);

    if (multiple && Array.isArray(newValue)) {
      onChange(newValue.map(endpoint => endpoint.name));
    } else if (!multiple && newValue && !Array.isArray(newValue)) {
      onChange(newValue.name, newValue.name);
    } else {
      onChange(null);
    }
  };

  // Sort options to show pinned endpoints first
  const sortedOptions = React.useMemo(() => {
    return [...options].sort((a, b) => {
      const aPinned = pinnedEndpoints.includes(a.name);
      const bPinned = pinnedEndpoints.includes(b.name);
      if (aPinned && !bPinned) return -1;
      if (!aPinned && bPinned) return 1;
      return 0;
    });
  }, [options, pinnedEndpoints]);

  // Custom option rendering with pin button
  const renderOption = (props: React.HTMLAttributes<HTMLLIElement>, option: AgentBricksEndpoint) => {
    const isPinned = pinnedEndpoints.includes(option.name);
    const isReady = AgentBricksService.isEndpointReady(option);
    return (
      <Box component="li" {...props} sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
        <Box sx={{ flex: 1 }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <Typography variant="body2">{option.name}</Typography>
            {isReady && (
              <Chip label="Ready" size="small" color="success" sx={{ height: 20, fontSize: '0.7rem' }} />
            )}
          </Box>
          {option.creator && (
            <Typography variant="caption" color="text.secondary">
              Created by {option.creator}
            </Typography>
          )}
        </Box>
        <Tooltip title={isPinned ? "Unpin endpoint" : "Pin endpoint"}>
          <IconButton
            size="small"
            onClick={(e) => {
              e.stopPropagation();
              togglePin(option.name);
            }}
            sx={{ ml: 1 }}
          >
            {isPinned ? <PushPinIcon fontSize="small" /> : <PushPinOutlinedIcon fontSize="small" />}
          </IconButton>
        </Tooltip>
      </Box>
    );
  };

  return (
    <Autocomplete
      multiple={multiple}
      open={open}
      onOpen={() => setOpen(true)}
      onClose={() => setOpen(false)}
      value={selectedOptions}
      onChange={handleChange}
      inputValue={inputValue}
      onInputChange={handleInputChange}
      options={sortedOptions}
      loading={loading}
      loadingText="Loading endpoints..."
      noOptionsText={
        loading ? "Loading endpoints..." :
        inputValue ? `No endpoints found matching "${inputValue}"` :
        "Start typing to search for endpoints"
      }
      getOptionLabel={(option) => option.name}
      isOptionEqualToValue={(option, value) => option.name === value.name}
      renderOption={renderOption}
      disabled={disabled}
      fullWidth={fullWidth}
      renderInput={(params) => (
        <TextField
          {...params}
          label={label}
          placeholder={placeholder}
          required={required}
          error={error}
          helperText={helperText}
          InputProps={{
            ...params.InputProps,
            startAdornment: (
              <>
                <InputAdornment position="start">
                  <SearchIcon />
                </InputAdornment>
                {params.InputProps.startAdornment}
              </>
            ),
            endAdornment: (
              <>
                {loading ? <CircularProgress color="inherit" size={20} /> : null}
                {!multiple && selectedOptions && !Array.isArray(selectedOptions) && (
                  <Box sx={{ display: 'flex', alignItems: 'center', mr: 1 }}>
                    <Tooltip title={pinnedEndpoints.includes(selectedOptions.name) ? "Unpin endpoint" : "Pin endpoint"}>
                      <IconButton
                        size="small"
                        onClick={(e) => {
                          e.stopPropagation();
                          togglePin(selectedOptions.name);
                        }}
                      >
                        {pinnedEndpoints.includes(selectedOptions.name) ?
                          <PushPinIcon fontSize="small" color="primary" /> :
                          <PushPinOutlinedIcon fontSize="small" />
                        }
                      </IconButton>
                    </Tooltip>
                    <Tooltip title="Clear selection">
                      <IconButton
                        size="small"
                        onClick={(e) => {
                          e.stopPropagation();
                          handleClear();
                        }}
                      >
                        <ClearIcon fontSize="small" />
                      </IconButton>
                    </Tooltip>
                  </Box>
                )}
                {params.InputProps.endAdornment}
              </>
            ),
          }}
        />
      )}
      renderTags={(value, getTagProps) =>
        value.map((option, index) => {
          const { key, ...tagProps } = getTagProps({ index });
          return (
            <Chip
              key={key || option.name}
              label={option.name}
              {...tagProps}
              size="small"
            />
          );
        })
      }
    />
  );
};

export default AgentBricksEndpointSelector;
