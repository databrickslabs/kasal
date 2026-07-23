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
import { useModelConfigStore } from '../../store/modelConfig';
import { ModelService } from '../../api/ModelService';

export interface LLMSelectionDialogProps {
  open: boolean;
  onClose: () => void;
  onSelectLLM: (model: string) => void;
  currentLLM?: string;
  isUpdating?: boolean;
}

const LLMSelectionDialog: React.FC<LLMSelectionDialogProps> = ({
  open,
  onClose,
  onSelectLLM,
  currentLLM = '',
  isUpdating = false
}) => {
  const [selectedModel, setSelectedModel] = useState<string>('');
  const [isLoading, setIsLoading] = useState<boolean>(false);
  const selectRef = useRef<HTMLInputElement>(null);

  // Read models from the Zustand store
  const storeModels = useModelConfigStore(state => state.models);
  const setStoreModels = useModelConfigStore(state => state.setModels);

  // Ensure store has active models; refresh if empty or stale
  useEffect(() => {
    if (open) {
      setSelectedModel(currentLLM);

      const ensureModels = async () => {
        // If store already has models beyond defaults, use them directly
        if (Object.keys(storeModels).length > 1) {
          return;
        }
        setIsLoading(true);
        try {
          const modelService = ModelService.getInstance();
          const fetched = await modelService.getActiveModels();
          if (Object.keys(fetched).length > 0) {
            setStoreModels(fetched);
          }
        } catch (error) {
          console.error('Error fetching models:', error);
          try {
            const modelService = ModelService.getInstance();
            const fallback = modelService.getActiveModelsSync();
            if (Object.keys(fallback).length > 0) {
              setStoreModels(fallback);
            }
          } catch (fallbackError) {
            console.error('Error fetching fallback models:', fallbackError);
          }
        } finally {
          setIsLoading(false);
          setTimeout(() => selectRef.current?.focus(), 100);
        }
      };

      void ensureModels();
    }
  }, [open, currentLLM, storeModels, setStoreModels]);

  const models = storeModels;
  const modelKeys = Object.keys(models);

  const handleSelectModel = (event: SelectChangeEvent<string>) => {
    setSelectedModel(event.target.value);
  };

  // Get a valid select value to avoid MUI errors
  const getValidSelectValue = (): string => {
    if (isLoading || modelKeys.length === 0) return '';
    if (selectedModel && models[selectedModel]) return selectedModel;
    return '';
  };

  const handleClose = () => {
    onClose();
  };

  const handleApply = useCallback(() => {
    if (selectedModel && models[selectedModel]) {
      onSelectLLM(selectedModel);
      onClose();
    }
  }, [selectedModel, onSelectLLM, models, onClose]);

  const handleKeyDown = (event: KeyboardEvent<HTMLDivElement>) => {
    if (event.key === 'Enter' && selectedModel && !isUpdating && !isLoading) {
      event.preventDefault();
      handleApply();
    }
  };

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
                {modelKeys.length === 0 ? (
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
