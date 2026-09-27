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
import { useEnabledModels, useRefreshModelsWhen } from '../../../../hooks/global/useEnabledModels';

export interface LLMSelectionDialogProps {
  open: boolean;
  embedded?: boolean;
  onClose: () => void;
  onSelectLLM: (model: string) => void;
  currentLLM?: string;
  isUpdating?: boolean;
}

const LLMSelectionDialog: React.FC<LLMSelectionDialogProps> = ({
  open,
  embedded = false,
  onClose,
  onSelectLLM,
  currentLLM = '',
  isUpdating = false
}) => {
  const [selectedModel, setSelectedModel] = useState<string>('');
  const selectRef = useRef<HTMLInputElement>(null);

  // The shared live list of ENABLED models (store/models.ts). It updates in
  // place when an admin changes models; opening the dialog refreshes it when
  // stale. (It used to read the Configuration page's store, which holds every
  // model — disabled ones included — once that page has been opened.)
  const { models, loading: isLoading } = useEnabledModels();
  useRefreshModelsWhen(open);

  useEffect(() => {
    if (!open) return;
    setSelectedModel(currentLLM);
    const focus = setTimeout(() => selectRef.current?.focus(), 100);
    return () => clearTimeout(focus);
  }, [open, currentLLM]);

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

  if (!open) return null;
  const content = (
    <>
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
    </>
  );
  return embedded ? <Box onKeyDown={handleKeyDown} sx={{ display: 'flex', flexDirection: 'column', flex: 1, minHeight: 0, overflow: 'hidden' }}> {content} </Box>
    : <Dialog open onClose={handleClose} maxWidth="sm" fullWidth onKeyDown={handleKeyDown}> {content} </Dialog>;
};

export default LLMSelectionDialog;
