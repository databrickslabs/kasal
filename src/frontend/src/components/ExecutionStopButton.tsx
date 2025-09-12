import React, { useState } from 'react';
import {
  Button,
  IconButton,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogContentText,
  DialogActions,
  CircularProgress,
  Tooltip,
  Box,
  Typography,
  Alert,
} from '@mui/material';
import {
  Stop as StopIcon,
  Warning as WarningIcon,
} from '@mui/icons-material';
import { apiClient } from '../config/api/ApiConfig';
import { toast } from 'react-hot-toast';

interface ExecutionStopButtonProps {
  executionId: string;
  status: string;
  variant?: 'button' | 'icon';
  size?: 'small' | 'medium' | 'large';
  onStopComplete?: (result: any) => void;
  onStatusChange?: (newStatus: string) => void;
}

const ExecutionStopButton: React.FC<ExecutionStopButtonProps> = ({
  executionId,
  status,
  variant = 'button',
  size = 'medium',
  onStopComplete,
  onStatusChange,
}) => {
  const [isStopping, setIsStopping] = useState(false);
  const [confirmDialogOpen, setConfirmDialogOpen] = useState(false);

  // Determine if the button should be shown and enabled
  const isStoppable = ['RUNNING', 'PREPARING'].includes(status?.toUpperCase());
  const isCurrentlyStopping = status?.toUpperCase() === 'STOPPING' || isStopping;

  const handleStopClick = () => {
    setConfirmDialogOpen(true);
  };

  const handleConfirmStop = async (selectedStopType: 'graceful' | 'force') => {
    setConfirmDialogOpen(false);
    setIsStopping(true);

    try {
      const endpoint = selectedStopType === 'force' 
        ? `/executions/${executionId}/force-stop`
        : `/executions/${executionId}/stop`;

      const requestData = selectedStopType === 'graceful' 
        ? {
            stop_type: 'graceful',
            reason: 'User requested stop',
            preserve_partial_results: true
          }
        : {};

      const response = await apiClient.post(endpoint, requestData);
      
      if (response.data) {
        toast.success(
          `Execution ${selectedStopType === 'force' ? 'forcefully' : 'gracefully'} stopped`
        );
        
        // Update status
        if (onStatusChange) {
          onStatusChange(response.data.status);
        }
        
        // Call completion callback with partial results
        if (onStopComplete) {
          onStopComplete(response.data);
        }
        
        // Dispatch event to clear execution state in chat panel
        if (response.data.status === 'STOPPED') {
          window.dispatchEvent(new CustomEvent('jobStopped', { 
            detail: { 
              jobId: executionId,
              status: response.data.status,
              partialResults: response.data.partial_results
            } 
          }));
        }
      }
    } catch (error: any) {
      console.error('Error stopping execution:', error);
      toast.error(
        error.response?.data?.detail || 'Failed to stop execution'
      );
    } finally {
      setIsStopping(false);
    }
  };

  const handleCancelDialog = () => {
    setConfirmDialogOpen(false);
  };

  // Don't render if execution is not in a stoppable state
  if (!isStoppable && !isCurrentlyStopping) {
    return null;
  }

  return (
    <>
      {variant === 'icon' ? (
        <Tooltip title={isCurrentlyStopping ? 'Stopping execution...' : 'Stop execution'}>
          <span>
            <IconButton
              color="error"
              size={size}
              onClick={handleStopClick}
              disabled={isCurrentlyStopping}
            >
              {isCurrentlyStopping ? <CircularProgress size={20} /> : <StopIcon />}
            </IconButton>
          </span>
        </Tooltip>
      ) : (
        <Button
          variant="contained"
          color="error"
          size={size}
          startIcon={isCurrentlyStopping ? <CircularProgress size={16} /> : <StopIcon />}
          onClick={handleStopClick}
          disabled={isCurrentlyStopping}
        >
          {isCurrentlyStopping ? 'Stopping...' : 'Stop Execution'}
        </Button>
      )}

      {/* Confirmation Dialog */}
      <Dialog
        open={confirmDialogOpen}
        onClose={handleCancelDialog}
        maxWidth="sm"
        fullWidth
      >
        <DialogTitle>
          <Box display="flex" alignItems="center">
            <WarningIcon color="warning" sx={{ mr: 1 }} />
            Stop Execution?
          </Box>
        </DialogTitle>
        <DialogContent>
          <DialogContentText component="div">
            <Typography variant="body1" gutterBottom>
              Choose how you want to stop the execution:
            </Typography>
            
            <Box sx={{ mt: 2 }}>
              <Alert severity="info" sx={{ mb: 2 }}>
                <Typography variant="subtitle2" fontWeight="bold">
                  Graceful Stop
                </Typography>
                <Typography variant="body2">
                  Completes the current task before stopping. Preserves all partial results.
                </Typography>
              </Alert>
              
              <Alert severity="warning">
                <Typography variant="subtitle2" fontWeight="bold">
                  Force Stop
                </Typography>
                <Typography variant="body2">
                  Immediately terminates the execution. May lose data from the current task.
                </Typography>
              </Alert>
            </Box>
          </DialogContentText>
        </DialogContent>
        <DialogActions>
          <Button onClick={handleCancelDialog} color="inherit">
            Cancel
          </Button>
          <Button
            onClick={() => handleConfirmStop('graceful')}
            color="warning"
            variant="outlined"
          >
            Graceful Stop
          </Button>
          <Button
            onClick={() => handleConfirmStop('force')}
            color="error"
            variant="contained"
          >
            Force Stop
          </Button>
        </DialogActions>
      </Dialog>
    </>
  );
};

export default ExecutionStopButton;