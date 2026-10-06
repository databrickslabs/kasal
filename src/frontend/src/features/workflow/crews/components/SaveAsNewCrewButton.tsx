/**
 * "Save as new crew" — clone the CURRENTLY-SAVED crew into a new, independent one
 * under a name the user types, instead of overwriting it.
 *
 * The clone goes through the backend deep-clone endpoint (CrewService.cloneCrew →
 * POST /crews/{id}/clone), which duplicates the crew's agents and tasks and
 * rebuilds the canvas with the clone's IDs. So editing the copy never changes the
 * original — the whole point of "save new, don't overwrite".
 *
 * It clones the crew as last SAVED (unsaved canvas edits are not included); the
 * dialog says so. `renderTrigger` lets the host toolbar supply its own button
 * element so this matches the surrounding action cluster, while this component
 * owns the rename dialog + the clone call.
 */

import React, { useState } from 'react';
import {
  Button,
  CircularProgress,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  IconButton,
  TextField,
  Tooltip,
} from '@mui/material';
import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import axios from 'axios';
import { CrewService } from '../../../../api/workflow/CrewService';

export interface SaveAsNewCrewButtonProps {
  /** The currently-loaded SAVED crew to clone. Button is disabled when absent. */
  crewId?: string | null;
  /** Name of the loaded crew — seeds the dialog with "<name> (copy)". */
  crewName?: string | null;
  /** Called with the new crew after a successful clone. */
  onCloned?: (crew: { id: string; name: string }) => void;
  /** Supply the host toolbar's own trigger element; falls back to an IconButton. */
  renderTrigger?: (open: () => void, disabled: boolean) => React.ReactNode;
  disabled?: boolean;
}

const SaveAsNewCrewButton: React.FC<SaveAsNewCrewButtonProps> = ({
  crewId,
  crewName,
  onCloned,
  renderTrigger,
  disabled = false,
}) => {
  const [open, setOpen] = useState(false);
  const [name, setName] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);

  const isDisabled = disabled || !crewId;

  const handleOpen = () => {
    if (isDisabled) return;
    setName(crewName ? `${crewName} (copy)` : '');
    setError(null);
    setOpen(true);
  };

  const handleClose = () => {
    if (saving) return;
    setOpen(false);
    setError(null);
  };

  const handleSave = async (e?: React.FormEvent) => {
    if (e) e.preventDefault();
    if (!crewId) return;
    const trimmed = name.trim();
    if (!trimmed) {
      setError('Crew name is required');
      return;
    }
    setSaving(true);
    try {
      const cloned = await CrewService.cloneCrew(crewId, trimmed);
      onCloned?.({ id: String(cloned.id), name: cloned.name });
      setOpen(false);
    } catch (err) {
      let message = 'Failed to save as new crew';
      if (axios.isAxiosError(err) && err.response?.data) {
        const data = err.response.data;
        if (typeof data === 'string') message = data;
        else if (data.detail) message = String(data.detail);
        else if (data.message) message = String(data.message);
      } else if (err instanceof Error) {
        message = err.message;
      }
      // 409 → name clash: keep the dialog open so the user can rename.
      setError(message);
    } finally {
      setSaving(false);
    }
  };

  return (
    <>
      {renderTrigger ? (
        renderTrigger(handleOpen, isDisabled)
      ) : (
        <Tooltip
          title={isDisabled ? 'Save the crew first to clone it' : 'Save as new crew'}
        >
          <span>
            <IconButton
              onClick={handleOpen}
              disabled={isDisabled}
              size="small"
              aria-label="Save as new crew"
            >
              <ContentCopyIcon fontSize="small" />
            </IconButton>
          </span>
        </Tooltip>
      )}
      <Dialog
        open={open}
        onClose={handleClose}
        maxWidth="sm"
        fullWidth
        component="form"
        onSubmit={handleSave}
      >
        <DialogTitle>Save as new crew</DialogTitle>
        <DialogContent>
          <TextField
            autoFocus
            margin="dense"
            label="New crew name"
            type="text"
            fullWidth
            value={name}
            onChange={(e) => setName(e.target.value)}
            error={!!error}
            helperText={
              error ?? 'Creates an independent copy — the original is untouched.'
            }
          />
        </DialogContent>
        <DialogActions>
          <Button onClick={handleClose} disabled={saving}>
            Cancel
          </Button>
          <Button
            type="submit"
            variant="contained"
            color="primary"
            disabled={saving}
            startIcon={saving ? <CircularProgress size={16} /> : undefined}
          >
            {saving ? 'Saving…' : 'Save as new'}
          </Button>
        </DialogActions>
      </Dialog>
    </>
  );
};

export default SaveAsNewCrewButton;
