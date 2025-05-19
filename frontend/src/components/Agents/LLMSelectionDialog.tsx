import React, { useState, useCallback, KeyboardEvent, useEffect, useRef } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  IconButton,
  Typography,
  Divider,
  Box,
  CircularProgress,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  SelectChangeEvent
} from '@mui/material';
import CloseIcon from '@mui/icons-material/Close';
import { ModelService } from '../../api/ModelService';
import { Models } from '../../types/models';

// Default fallback model when API is down
const DEFAULT_FALLBACK_MODEL = {
  'gpt-4o-mini': {
    name: 'gpt-4o-mini',
    temperature: 0.7,
    context_window: 128000,
    max_output_tokens: 4096,
    enabled: true
  }
};

export interface LLMSelectionDialogProps {
  open: boolean;
  onClose: () => void;
  onSelectLLM: (model: string) => void;
  isUpdating?: boolean;
}

const LLMSelectionDialog: React.FC<LLMSelectionDialogProps> = ({
  open,
  onClose,
  onSelectLLM,
  isUpdating = false
}) => {
  const [selectedModel, setSelectedModel] = useState<string>('');
  const [models, setModels] = useState<Models>(DEFAULT_FALLBACK_MODEL);
  const [isLoading, setIsLoading] = useState<boolean>(false);
  const selectRef = useRef<HTMLInputElement>(null);

  // Fetch models when the dialog opens
  useEffect(() => {
    if (open) {
      const fetchModels = async () => {
        setIsLoading(true);
        try {
          const modelService = ModelService.getInstance();
          const fetchedModels = await modelService.getActiveModels();
          
          // Only update models if we got at least one model back
          if (Object.keys(fetchedModels).length > 0) {
            setModels(fetchedModels);
          } else {
            console.warn('No models returned from API, using fallback model');
          }
        } catch (error) {
          console.error('Error fetching models:', error);
          // Fallback to synchronous method if async fails
          try {
            const modelService = ModelService.getInstance();
            const fallbackModels = modelService.getActiveModelsSync();
            if (Object.keys(fallbackModels).length > 0) {
              setModels(fallbackModels);
            } else {
              console.warn('No fallback models available, using default model');
            }
          } catch (fallbackError) {
            console.error('Error fetching fallback models:', fallbackError);
          }
        } finally {
          setIsLoading(false);
          // Focus on the select when loading completes
          setTimeout(() => {
            if (selectRef.current) {
              selectRef.current.focus();
            }
          }, 100);
        }
      };

      fetchModels();
    } else {
      // Reset selection when dialog closes
      setSelectedModel('');
    }
  }, [open]);

  const handleSelectModel = (event: SelectChangeEvent<string>) => {
    setSelectedModel(event.target.value);
  };

  // Get a valid select value to avoid MUI errors
  const getValidSelectValue = (): string => {
    // During loading or if no models available, return empty string
    if (isLoading || Object.keys(models).length === 0) {
      return '';
    }
    
    // If selectedModel is valid, return it
    if (selectedModel && models[selectedModel]) {
      return selectedModel;
    }
    
    // Otherwise return empty string
    return '';
  };

  const handleClose = () => {
    setSelectedModel('');
    onClose();
  };

  const handleApply = useCallback(() => {
    if (selectedModel && models[selectedModel]) {
      onSelectLLM(selectedModel);
      // Close the dialog after applying
      setSelectedModel('');
      onClose();
    }
  }, [selectedModel, onSelectLLM, models, onClose]);

  // Handle Enter key press
  const handleKeyDown = (event: KeyboardEvent<HTMLDivElement>) => {
    if (event.key === 'Enter' && selectedModel && !isUpdating && !isLoading) {
      event.preventDefault();
      handleApply();
    }
  };

  // Convert the models object to an array of model keys
  const modelKeys = Object.keys(models);

  return (
    <Dialog 
      open={open} 
      onClose={handleClose} 
      maxWidth="sm" 
      fullWidth 
      onKeyDown={handleKeyDown}
    >
      <DialogTitle>
        <Typography variant="h6" component="div">
          Select LLM
        </Typography>
        <IconButton
          aria-label="close"
          onClick={handleClose}
          sx={{
            position: 'absolute',
            right: 8,
            top: 8,
          }}
        >
          <CloseIcon />
        </IconButton>
      </DialogTitle>
      <Divider />
      <DialogContent>
        <Box sx={{ mt: 2 }}>
          {isLoading ? (
            <Box display="flex" justifyContent="center" alignItems="center" py={4}>
              <CircularProgress />
            </Box>
          ) : (
            <FormControl fullWidth>
              <InputLabel>Select Model</InputLabel>
              <Select
                value={getValidSelectValue()}
                onChange={handleSelectModel}
                label="Select Model"
                inputRef={selectRef}
              >
                {Object.keys(models).length === 0 ? (
                  // Always provide an empty option if no models are available
                  <MenuItem value="">No models available</MenuItem>
                ) : (
                  modelKeys.map((key) => (
                    <MenuItem key={key} value={key}>
                      <Box display="flex" justifyContent="space-between" width="100%">
                        <Typography>{models[key].name}</Typography>
                        <Typography variant="caption" color="text.secondary">
                          {models[key].provider}
                        </Typography>
                      </Box>
                    </MenuItem>
                  ))
                )}
              </Select>
            </FormControl>
          )}
        </Box>
      </DialogContent>
      <DialogActions>
        <Button onClick={handleClose}>Cancel</Button>
        <Button 
          onClick={handleApply} 
          color="primary" 
          disabled={!getValidSelectValue() || isUpdating || isLoading}
        >
          {isUpdating ? <CircularProgress size={24} /> : 'Select'}
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default LLMSelectionDialog; 