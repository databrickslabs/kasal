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
  InputAdornment
} from '@mui/material';
import SearchIcon from '@mui/icons-material/Search';
import { GenieService, GenieSpace, GenieSpacesResponse } from '../../api/GenieService';

interface GenieSpaceSelectorProps {
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
  const isLoadingMore = useRef(false);
  const searchTimeoutRef = useRef<NodeJS.Timeout | null>(null);

  // Convert value (ID) to selected option(s)
  useEffect(() => {
    if (value) {
      if (multiple && Array.isArray(value)) {
        const selected = options.filter(opt => value.includes(opt.id));
        setSelectedOptions(selected.length > 0 ? selected : null);
      } else if (!multiple && typeof value === 'string') {
        const selected = options.find(opt => opt.id === value);
        setSelectedOptions(selected || null);
      }
    } else {
      setSelectedOptions(null);
    }
  }, [value, options, multiple]);

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

  // Load initial data when dropdown opens
  useEffect(() => {
    if (open && options.length === 0) {
      loadSpaces();
    }
  }, [open, options.length]);

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
      onChange(newValue.id);
    } else {
      onChange(null);
    }
  };

  // Custom option rendering
  const renderOption = (props: React.HTMLAttributes<HTMLLIElement>, option: GenieSpace) => (
    <Box component="li" {...props}>
      <Box>
        <Typography variant="body2">{option.name}</Typography>
        {option.description && (
          <Typography variant="caption" color="text.secondary">
            {option.description}
          </Typography>
        )}
      </Box>
    </Box>
  );

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
      options={options}
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