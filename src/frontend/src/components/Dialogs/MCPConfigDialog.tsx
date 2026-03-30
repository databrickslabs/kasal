import React from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  IconButton,
  Typography,
  Box
} from '@mui/material';
import CloseIcon from '@mui/icons-material/Close';
import CloudIcon from '@mui/icons-material/Cloud';
import MCPConfiguration from '../Configuration/MCP/MCPConfiguration';

interface MCPConfigDialogProps {
  open: boolean;
  onClose: () => void;
}

/**
 * Dialog component for MCP configuration
 * Used to enable or disable MCP servers
 */
const MCPConfigDialog: React.FC<MCPConfigDialogProps> = ({
  open,
  onClose
}) => {
  return (
    <Dialog
      open={open}
      onClose={onClose}
      fullWidth
      maxWidth="lg"
      PaperProps={{ 
        sx: { 
          borderRadius: 2,
          boxShadow: '0 8px 32px rgba(0,0,0,0.12)'
        } 
      }}
    >
      <DialogTitle
        sx={{
          borderBottom: '1px solid',
          borderColor: 'divider',
          p: 3,
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between'
        }}
      >
        <Box sx={{ display: 'flex', alignItems: 'center' }}>
          <CloudIcon sx={{ mr: 1.5, color: 'primary.main', fontSize: '1.5rem' }} />
          <Typography variant="h5">MCP Configuration</Typography>
        </Box>
        <IconButton onClick={onClose} aria-label="close">
          <CloseIcon />
        </IconButton>
      </DialogTitle>
      <DialogContent sx={{ p: 3 }}>
        <MCPConfiguration />
      </DialogContent>
      <DialogActions sx={{ p: 2, borderTop: '1px solid', borderColor: 'divider' }}>
        <Button onClick={onClose} color="primary">
          Close
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default MCPConfigDialog; 