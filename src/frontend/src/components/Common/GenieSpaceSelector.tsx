/**
 * Genie Space Selector Component
 * 
 * A searchable dropdown with infinite scrolling for selecting Genie spaces.
 */

import React, { useState, useEffect, useRef } from 'react';
import {
  Autocomplete,
  TextField,
  CircularProgress,
  Box,
  Typography,
  Chip,
  Paper,
  InputAdornment,
  IconButton,
  Tooltip
} from '@mui/material';
import SearchIcon from '@mui/icons-material/Search';
import ClearIcon from '@mui/icons-material/Clear';
import PushPinIcon from '@mui/icons-material/PushPin';
import PushPinOutlinedIcon from '@mui/icons-material/PushPinOutlined';
import { GenieService, GenieSpace, GenieSpacesResponse } from '../../api/GenieService';

interface GenieSpaceSelectorProps {
  value: string | string[] | null;
  onChange: (value: string | string[] | null, spaceName?: string) => void;
  multiple?: boolean;
  label?: string;
  placeholder?: string;
  disabled?: boolean;
  required?: boolean;
  helperText?: string;
  error?: boolean;
  fullWidth?: boolean;
  toolId?: number;  // Optional tool ID to update config when space is selected
}

export const GenieSpaceSelector: React.FC<GenieSpaceSelectorProps> = ({
  value,
  onChange,
  multiple = false,
  label = 'Genie Space',
  placeholder = 'Search for Genie spaces...',
  disabled = false,
  required = false,
  helperText,
  error = false,
  fullWidth = true,
  toolId
}) => {
  const [open, setOpen] = useState(false);
  const [options, setOptions] = useState<GenieSpace[]>([]);
  const [selectedOptions, setSelectedOptions] = useState<GenieSpace | GenieSpace[] | null>(null);
  const [inputValue, setInputValue] = useState('');
  const [loading, setLoading] = useState(false);
  const [hasMore, setHasMore] = useState(true);
  const [nextPageToken, setNextPageToken] = useState<string | undefined>(undefined);
  const [searchQuery, setSearchQuery] = useState('');
  const [pinnedSpaces, setPinnedSpaces] = useState<string[]>([]);
  const isLoadingMore = useRef(false);
  const searchTimeoutRef = useRef<NodeJS.Timeout | null>(null);

  // Load pinned spaces from localStorage
  useEffect(() => {
    const saved = localStorage.getItem('pinnedGenieSpaces');
    if (saved) {
      try {
        setPinnedSpaces(JSON.parse(saved));
      } catch (e) {
        console.error('Failed to load pinned spaces:', e);
      }
    }
  }, []);

  // Save pinned spaces to localStorage
  const savePinnedSpaces = (spaces: string[]) => {
    setPinnedSpaces(spaces);
    localStorage.setItem('pinnedGenieSpaces', JSON.stringify(spaces));
  };

  // Toggle pin for a space
  const togglePin = (spaceId: string) => {
    if (pinnedSpaces.includes(spaceId)) {
      savePinnedSpaces(pinnedSpaces.filter(id => id !== spaceId));
    } else {
      savePinnedSpaces([...pinnedSpaces, spaceId]);
    }
  };

  // Clear selected space
  const handleClear = () => {
    setSelectedOptions(null);
    onChange(null);
    setInputValue('');
  };

  // Convert value (ID) to selected option(s)
  useEffect(() => {
    const loadSelectedSpace = async () => {
      if (value) {
        if (multiple && Array.isArray(value)) {
          const selected = options.filter(opt => value.includes(opt.id));
          setSelectedOptions(selected.length > 0 ? selected : null);
        } else if (!multiple && typeof value === 'string') {
          // First check if we already have this space in options
          let selected = options.find(opt => opt.id === value);

          // If not found in options and we have a value, try to fetch it
          if (!selected && value) {
            try {
              // Search for the specific space by ID
              const response = await GenieService.searchSpaces({
                search_query: value,
                page_size: 10,
                enabled_only: false
              });

              // Find the exact match by ID
              selected = response.spaces.find(space => space.id === value);

              if (selected) {
                // Add to options if not already there
                setOptions(prev => {
                  const exists = prev.some(opt => opt.id === selected?.id);
                  if (!exists && selected) {
                    return [...prev, selected];
                  }
                  return prev;
                });
              }
            } catch (error) {
              console.error('Failed to load selected space:', error);
            }
          }

          setSelectedOptions(selected || null);
        }
      } else {
        setSelectedOptions(null);
      }
    };

    loadSelectedSpace();
  }, [value, multiple, options]); // Added options back to dependencies

  // Load spaces function
  const loadSpaces = async (search?: string, pageToken?: string, append = false) => {
    if (isLoadingMore.current && append) return;
    
    try {
      isLoadingMore.current = append;
      if (!append) setLoading(true);
      
      let response: GenieSpacesResponse;
      if (search) {
        response = await GenieService.searchSpaces({
          search_query: search,
          page_token: pageToken,
          page_size: 50,  // Increased for better performance
          enabled_only: true
        });
      } else {
        response = await GenieService.getSpaces(pageToken, 50);  // Increased for better performance
      }
      
      if (append) {
        setOptions(prev => [...prev, ...response.spaces]);
      } else {
        setOptions(response.spaces);
      }
      
      setNextPageToken(response.next_page_token);
      setHasMore(response.has_more || false);
    } catch (error) {
      console.error('Error loading Genie spaces:', error);
    } finally {
      setLoading(false);
      isLoadingMore.current = false;
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
        setNextPageToken(undefined);
        loadSpaces(newInputValue);
      }, 300);
    }
  };

  // Handle scroll for infinite loading
  const handleScroll = (event: React.UIEvent<HTMLUListElement>) => {
    const listbox = event.currentTarget;
    const scrollTop = listbox.scrollTop;
    const scrollHeight = listbox.scrollHeight;
    const clientHeight = listbox.clientHeight;
    
    // Load more when scrolled to bottom
    if (scrollHeight - scrollTop <= clientHeight * 1.5 && hasMore && !isLoadingMore.current) {
      loadSpaces(searchQuery, nextPageToken, true);
    }
  };

  // Load initial data when dropdown opens or when we have a value
  useEffect(() => {
    // Load immediately if we have a value but no options
    if (value && options.length === 0 && !loading) {
      loadSpaces();
    }
    // Also load when dropdown opens
    if (open && options.length === 0 && !loading) {
      loadSpaces();
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
  const handleChange = (_event: React.SyntheticEvent, newValue: GenieSpace | GenieSpace[] | null) => {
    setSelectedOptions(newValue);

    if (multiple && Array.isArray(newValue)) {
      onChange(newValue.map(space => space.id));
    } else if (!multiple && newValue && !Array.isArray(newValue)) {
      onChange(newValue.id, newValue.name);
    } else {
      onChange(null);
    }
  };

  // Sort options to show pinned spaces first
  const sortedOptions = React.useMemo(() => {
    return [...options].sort((a, b) => {
      const aPinned = pinnedSpaces.includes(a.id);
      const bPinned = pinnedSpaces.includes(b.id);
      if (aPinned && !bPinned) return -1;
      if (!aPinned && bPinned) return 1;
      return 0;
    });
  }, [options, pinnedSpaces]);

  // Custom option rendering with pin button
  const renderOption = (props: React.HTMLAttributes<HTMLLIElement>, option: GenieSpace) => {
    const isPinned = pinnedSpaces.includes(option.id);
    return (
      <Box component="li" {...props} sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
        <Box sx={{ flex: 1 }}>
          <Typography variant="body2">{option.name}</Typography>
          {option.description && (
            <Typography variant="caption" color="text.secondary">
              {option.description}
            </Typography>
          )}
        </Box>
        <Tooltip title={isPinned ? "Unpin space" : "Pin space"}>
          <IconButton
            size="small"
            onClick={(e) => {
              e.stopPropagation();
              togglePin(option.id);
            }}
            sx={{ ml: 1 }}
          >
            {isPinned ? <PushPinIcon fontSize="small" /> : <PushPinOutlinedIcon fontSize="small" />}
          </IconButton>
        </Tooltip>
      </Box>
    );
  };

  // Custom listbox component with scroll handler
  const ListboxComponent = React.forwardRef<HTMLUListElement, React.HTMLAttributes<HTMLUListElement>>((props, ref) => (
    <ul
      {...props}
      ref={ref}
      onScroll={handleScroll}
      style={{ maxHeight: 300, overflow: 'auto' }}
    />
  ));
  ListboxComponent.displayName = 'ListboxComponent';

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
      loadingText="Loading spaces..."
      noOptionsText={
        loading ? "Loading spaces..." : 
        inputValue ? `No spaces found matching "${inputValue}"` : 
        "Start typing to search for spaces"
      }
      getOptionLabel={(option) => option.name}
      isOptionEqualToValue={(option, value) => option.id === value.id}
      renderOption={renderOption}
      ListboxComponent={ListboxComponent}
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
                    <Tooltip title={pinnedSpaces.includes(selectedOptions.id) ? "Unpin space" : "Pin space"}>
                      <IconButton
                        size="small"
                        onClick={(e) => {
                          e.stopPropagation();
                          togglePin(selectedOptions.id);
                        }}
                      >
                        {pinnedSpaces.includes(selectedOptions.id) ?
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
              key={key || option.id}
              label={option.name}
              {...tagProps}
              size="small"
            />
          );
        })
      }
      PaperComponent={(props) => (
        <Paper {...props}>
          {props.children}
          {isLoadingMore.current && (
            <Box display="flex" justifyContent="center" p={1}>
              <CircularProgress size={20} />
            </Box>
          )}
        </Paper>
      )}
    />
  );
};

export default GenieSpaceSelector;