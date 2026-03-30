import React, { useState, useCallback, KeyboardEvent } from 'react';
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
  SelectChangeEvent,
  Paper
} from '@mui/material';
import CloseIcon from '@mui/icons-material/Close';

// Define common RPM values for selection
const rpmOptions = [
  { value: '1', label: '1 RPM' },
  { value: '3', label: '3 RPM' },
  { value: '10', label: '10 RPM' },
  { value: '20', label: '20 RPM' },
  { value: '30', label: '30 RPM' },
  { value: '40', label: '40 RPM' },
  { value: '50', label: '50 RPM' },
  { value: '60', label: '60 RPM (1 request per second)' },
  { value: '120', label: '120 RPM (2 requests per second)' },
  { value: '300', label: '300 RPM (5 requests per second)' },
  { value: '600', label: '600 RPM (10 requests per second)' },
  { value: '1200', label: '1200 RPM (20 requests per second)' }
];

export interface MaxRPMSelectionDialogProps {
  open: boolean;
  onClose: () => void;
  onSelectMaxRPM: (rpm: string) => void;
  isUpdating?: boolean;
}

const MaxRPMSelectionDialog: React.FC<MaxRPMSelectionDialogProps> = ({
  open,
  onClose,
  onSelectMaxRPM,
  isUpdating = false
}) => {
  const [selectedRPM, setSelectedRPM] = useState<string>('');

  const handleSelectRPM = (event: SelectChangeEvent<string>) => {
    setSelectedRPM(event.target.value);
  };

  const handleClose = useCallback(() => {
    setSelectedRPM('');
    onClose();
  }, [onClose]);

  const handleApply = useCallback(() => {
    if (selectedRPM) {
      onSelectMaxRPM(selectedRPM);
      handleClose();
    }
  }, [selectedRPM, onSelectMaxRPM, handleClose]);

  // Handle Enter key press
  const handleKeyDown = (event: KeyboardEvent<HTMLDivElement>) => {
    if (event.key === 'Enter' && selectedRPM && !isUpdating) {
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
      PaperComponent={Paper}
    >
      <DialogTitle>
        Set Max RPM for All Agents
        <IconButton
          aria-label="close"
          onClick={handleClose}
          sx={{ position: 'absolute', right: 8, top: 8 }}
        >
          <CloseIcon />
        </IconButton>
      </DialogTitle>
      <DialogContent sx={{ pb: 1 }}>
        <Typography variant="body2" sx={{ mb: 2 }}>
          Select a Max RPM (Requests Per Minute) value to apply to all agents on the canvas. Press Enter to apply.
        </Typography>
        <Divider sx={{ my: 2 }} />
        
        <FormControl fullWidth sx={{ my: 2 }}>
          <InputLabel id="max-rpm-select-label">Max RPM</InputLabel>
          <Select
            labelId="max-rpm-select-label"
            id="max-rpm-select"
            value={selectedRPM}
            onChange={handleSelectRPM}
            label="Max RPM"
            autoFocus
          >
            {rpmOptions.map((option) => (
              <MenuItem key={option.value} value={option.value}>
                <Box>
                  <Typography variant="body1">{option.label}</Typography>
                  <Typography variant="caption" color="textSecondary">
                    Maximum requests per minute limit
                  </Typography>
                </Box>
              </MenuItem>
            ))}
          </Select>
        </FormControl>
      </DialogContent>
      <DialogActions sx={{ px: 3, py: 2 }}>
        <Button onClick={handleClose}>Cancel</Button>
        <Box sx={{ position: 'relative' }}>
          <Button
            variant="contained"
            onClick={handleApply}
            disabled={!selectedRPM || isUpdating}
          >
            Apply to All Agents
          </Button>
          {isUpdating && (
            <CircularProgress
              size={24}
              sx={{
                position: 'absolute',
                top: '50%',
                left: '50%',
                marginTop: '-12px',
                marginLeft: '-12px',
              }}
            />
          )}
        </Box>
      </DialogActions>
    </Dialog>
  );
};

export default MaxRPMSelectionDialog; 