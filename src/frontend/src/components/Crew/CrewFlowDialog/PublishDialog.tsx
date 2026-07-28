import React, { useCallback, useEffect, useState } from 'react';
import {
  Alert,
  Box,
  Button,
  Checkbox,
  Chip,
  CircularProgress,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  FormControlLabel,
  Stack,
  TextField,
  Typography,
} from '@mui/material';
import PublicIcon from '@mui/icons-material/Public';

import { PublicationService } from '../../../api/workflow/PublicationService';
import {
  ExternalProtocol,
  PublicationResponse,
  PublishableEntity,
} from '../../../types/workflow/publication';

interface PublishDialogProps {
  open: boolean;
  onClose: () => void;
  entityType: PublishableEntity;
  entityId: string;
  /** The crew or flow name, used to seed the external name and description. */
  entityName: string;
  /** Notifies the catalog so its Published chip updates without a refetch. */
  onChanged?: (published: boolean) => void;
}

/** A display name -> the lowercase identifier external clients will pin. */
const toExternalName = (name: string): string =>
  name
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '')
    .replace(/^([^a-z])/, 'x$1')
    .slice(0, 64);

/**
 * Publish a crew or flow to external agents.
 *
 * Its own component rather than more surface on CrewFlowDialog, which is already
 * far past the file-size ceiling — and because publication is one self-contained
 * decision with its own state, validation and failure modes.
 */
const PublishDialog: React.FC<PublishDialogProps> = ({
  open,
  onClose,
  entityType,
  entityId,
  entityName,
  onChanged,
}) => {
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [existing, setExisting] = useState<PublicationResponse | null>(null);

  const [externalName, setExternalName] = useState('');
  const [description, setDescription] = useState('');
  const [protocols, setProtocols] = useState<ExternalProtocol[]>(['mcp', 'a2a']);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const publication = await PublicationService.get(entityType, entityId);
      setExisting(publication);
      // Seed from the existing publication when there is one, so editing does
      // not silently rewrite a name external clients have already pinned.
      setExternalName(publication?.external_name ?? toExternalName(entityName));
      setDescription(publication?.description ?? '');
      setProtocols(publication?.protocols ?? ['mcp', 'a2a']);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Could not load publication');
    } finally {
      setLoading(false);
    }
  }, [entityType, entityId, entityName]);

  useEffect(() => {
    if (open) void load();
  }, [open, load]);

  const toggleProtocol = (protocol: ExternalProtocol) => {
    setProtocols((current) =>
      current.includes(protocol)
        ? current.filter((p) => p !== protocol)
        : [...current, protocol],
    );
  };

  const handlePublish = async () => {
    setSaving(true);
    setError(null);
    try {
      await PublicationService.publish(entityType, entityId, {
        external_name: externalName,
        description,
        protocols,
      });
      onChanged?.(true);
      onClose();
    } catch (e) {
      const detail = (e as { response?: { data?: { detail?: string } } })?.response
        ?.data?.detail;
      setError(detail ?? (e instanceof Error ? e.message : 'Could not publish'));
    } finally {
      setSaving(false);
    }
  };

  const handleUnpublish = async () => {
    setSaving(true);
    setError(null);
    try {
      await PublicationService.unpublish(entityType, entityId);
      onChanged?.(false);
      onClose();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Could not unpublish');
    } finally {
      setSaving(false);
    }
  };

  const nameIsValid = /^[a-z][a-z0-9_]{0,63}$/.test(externalName);
  const canPublish =
    nameIsValid && description.trim().length > 0 && protocols.length > 0 && !saving;

  return (
    <Dialog
      open={open}
      onClose={onClose}
      maxWidth="sm"
      fullWidth
      // The dialog is rendered from inside a catalogue card, and that card has
      // onClick={loadCrew} / onKeyDown={cardNavigation}. MUI portals the dialog
      // out of the DOM, but React's synthetic events still travel the REACT
      // tree — so clicking the Description field loaded the crew onto the canvas
      // and closed the catalogue, and typing drove the card's keyboard nav.
      // (The feedback block a few hundred lines away carries the same guard for
      // the same reason.)
      onClick={(event) => event.stopPropagation()}
      onKeyDown={(event) => event.stopPropagation()}>
      <DialogTitle sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
        <PublicIcon fontSize="small" />
        Publish {entityType} externally
        {existing && <Chip size="small" color="success" label="Published" />}
      </DialogTitle>

      <DialogContent>
        {loading ? (
          <Box sx={{ display: 'flex', justifyContent: 'center', py: 4 }}>
            <CircularProgress size={24} />
          </Box>
        ) : (
          <Stack spacing={2} sx={{ mt: 1 }}>
            <Typography variant="body2" color="text.secondary">
              Publishing makes <strong>{entityName}</strong> callable by agents
              outside this workspace — Claude Code, Cursor or any MCP/A2A client.
              Nothing is exposed until you publish it.
            </Typography>

            {error && <Alert severity="error">{error}</Alert>}

            <TextField
              label="External name"
              value={externalName}
              onChange={(e) => setExternalName(e.target.value)}
              fullWidth
              size="small"
              error={externalName.length > 0 && !nameIsValid}
              helperText={
                externalName.length > 0 && !nameIsValid
                  ? 'Lowercase letters, digits and underscores; must start with a letter.'
                  : 'The tool name external clients will use. Changing it breaks callers that pinned the old one.'
              }
            />

            <TextField
              label="Description"
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              fullWidth
              multiline
              minRows={3}
              size="small"
              helperText={
                'What it does AND when to use it. This is the only thing a calling ' +
                'agent matches on — a vague description means it is never chosen.'
              }
            />

            <Box>
              <Typography variant="subtitle2" gutterBottom>
                Expose over
              </Typography>
              <FormControlLabel
                control={
                  <Checkbox
                    checked={protocols.includes('mcp')}
                    onChange={() => toggleProtocol('mcp')}
                  />
                }
                label="MCP — Claude Code, Cursor, other MCP clients"
              />
              <FormControlLabel
                control={
                  <Checkbox
                    checked={protocols.includes('a2a')}
                    onChange={() => toggleProtocol('a2a')}
                  />
                }
                label="A2A — other agent platforms"
              />
              {protocols.length === 0 && (
                <Typography variant="caption" color="warning.main" display="block">
                  With no protocol selected this is not exposed anywhere.
                </Typography>
              )}
            </Box>
          </Stack>
        )}
      </DialogContent>

      <DialogActions sx={{ px: 3, pb: 2 }}>
        {existing && (
          <Button color="error" onClick={handleUnpublish} disabled={saving}>
            Unpublish
          </Button>
        )}
        <Box sx={{ flex: 1 }} />
        <Button onClick={onClose} disabled={saving}>
          Cancel
        </Button>
        <Button variant="contained" onClick={handlePublish} disabled={!canPublish}>
          {existing ? 'Update' : 'Publish'}
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default PublishDialog;
