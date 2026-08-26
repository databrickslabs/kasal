import React from 'react';
import {
  Dialog,
  DialogContent,
  DialogTitle,
  IconButton,
  Stack,
} from '@mui/material';
import CloseIcon from '@mui/icons-material/Close';

import TriggersPanel from './TriggersPanel';

interface TriggersDialogProps {
  open: boolean;
  onClose: () => void;
}

/**
 * Dialog wrapper around TriggersPanel, opened from the workflow canvas's right
 * sidebar (next to Schedules).
 */
const TriggersDialog: React.FC<TriggersDialogProps> = ({ open, onClose }) => (
  <Dialog open={open} onClose={onClose} maxWidth="md" fullWidth>
    <DialogTitle>
      <Stack
        direction="row"
        alignItems="center"
        justifyContent="space-between"
      >
        Event Triggers
        <IconButton onClick={onClose} size="small" aria-label="close">
          <CloseIcon fontSize="small" />
        </IconButton>
      </Stack>
    </DialogTitle>
    <DialogContent dividers>
      <TriggersPanel embedded />
    </DialogContent>
  </Dialog>
);

export default TriggersDialog;
