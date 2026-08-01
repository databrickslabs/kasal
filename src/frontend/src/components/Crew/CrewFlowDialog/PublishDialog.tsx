import React, { useCallback, useEffect, useMemo, useState } from 'react';
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
import { useAppStore as useChatAppStore } from '../../ChatMode/store/appStore';
import {
  PublicationProtocol,
  PublicationResponse,
  PublishableEntity,
} from '../../../types/workflow/publication';
import PublishInputSchema from './PublishInputSchema';
import PublishFlowOutcomes from './PublishFlowOutcomes';
import { FlowService } from '../../../api/workflow/FlowService';
import {
  PublicationInputField,
  buildInputSchema,
  deriveCrewInputFields,
  deriveFlowInputFields,
  fieldsFromSchema,
} from './publicationInputFields';

/**
 * What a brand-new publication is exposed over.
 *
 * All three: publishing is an explicit act, and the surfaces are individually
 * untickable right there in the dialog. The alternative — defaulting to the
 * external pair, as this did before `chat` existed — meant a crew the publisher
 * wanted reachable from their own chat box arrived exposed to MCP and A2A and
 * NOT to chat, which is the opposite of what they asked for on both counts.
 */
const DEFAULT_PROTOCOLS: PublicationProtocol[] = ['mcp', 'a2a', 'chat'];

/**
 * Reload the chat-mode catalog after a publication changes.
 *
 * That catalog is now filtered to what is published TO CHAT, so publishing is
 * what puts something in the rail and enables the composer's "Use existing"
 * control. Without this the change is only visible after a reload — the user
 * publishes, goes to chat, and finds their crew missing.
 *
 * Same seam SaveCrew uses for the same reason (`components/Crew/SaveCrew.tsx`);
 * everything downstream is subscribed to that store, so one call updates the
 * rail and the pill together.
 */
const refreshChatCatalog = () => {
  void useChatAppStore.getState().loadCatalog();
};

interface PublishDialogProps {
  open: boolean;
  onClose: () => void;
  entityType: PublishableEntity;
  entityId: string;
  /** The crew or flow name, used to seed the external name and description. */
  entityName: string;
  /**
   * The crew's or flow's nodes, used to derive the declared input fields.
   * Omitted, the dialog still publishes — the publisher just adds fields by
   * hand, and a capability with no schema makes every consumer treat all its
   * placeholders as required.
   */
  nodes?: unknown[];
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
  nodes,
  onChanged,
}) => {
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [existing, setExisting] = useState<PublicationResponse | null>(null);

  const [externalName, setExternalName] = useState('');
  const [description, setDescription] = useState('');
  // What each crew in this FLOW delivers. Kept on the flow, not on the
  // publication: a conversational flow uses it to pick which crew a follow-up
  // needs, published or not.
  const [outcomes, setOutcomes] = useState<Record<string, string>>({});
  const [protocols, setProtocols] = useState<PublicationProtocol[]>(DEFAULT_PROTOCOLS);
  const [inputFields, setInputFields] = useState<PublicationInputField[]>([]);

  // The placeholders actually written into this crew's or flow's text. Derived
  // from the nodes rather than from the saved schema, because the point is to
  // compare the two: a declared field with no placeholder behind it is passed to
  // every run and read by nothing.
  const usedPlaceholders = useMemo(
    () =>
      (entityType === 'flow'
        ? deriveFlowInputFields(nodes ?? [])
        : deriveCrewInputFields(nodes ?? [])
      ).map((f) => f.name),
    [entityType, nodes],
  );

  // The crews on this flow's canvas, and which of them nothing else listens to.
  // Read from the nodes rather than the saved config so the list matches what
  // the author is looking at, including crews added since the last save.
  const flowCrews = useMemo(() => {
    if (entityType !== 'flow') return [];
    const names = (nodes ?? [])
      .filter((n) => (n as { type?: string }).type === 'crewNode')
      .map((n) => {
        const data = (n as { data?: Record<string, unknown> }).data || {};
        return String(data.crewName || data.label || '');
      })
      .filter(Boolean);
    return Array.from(new Set(names));
  }, [entityType, nodes]);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const publication = await PublicationService.get(entityType, entityId);
      setExisting(publication);
      if (entityType === 'flow') {
        const flow = await FlowService.getFlow(entityId);
        const config = (flow?.flowConfig ?? flow?.flow_config ?? {}) as {
          outcomes?: Record<string, string>;
        };
        setOutcomes(config.outcomes ?? {});
      }
      // Seed from the existing publication when there is one, so editing does
      // not silently rewrite a name external clients have already pinned.
      setExternalName(publication?.external_name ?? toExternalName(entityName));
      setDescription(publication?.description ?? '');
      setProtocols(publication?.protocols ?? DEFAULT_PROTOCOLS);
      // A publication saved before this editor existed has no schema at all, so
      // fall through to deriving one rather than showing an empty list — the
      // whole back catalogue is in that state.
      setInputFields(
        fieldsFromSchema(publication?.input_schema) ??
          (entityType === 'flow'
            ? deriveFlowInputFields(nodes ?? [])
            : deriveCrewInputFields(nodes ?? [])),
      );
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Could not load publication');
    } finally {
      setLoading(false);
    }
  }, [entityType, entityId, entityName, nodes]);

  useEffect(() => {
    if (open) void load();
  }, [open, load]);

  const toggleProtocol = (protocol: PublicationProtocol) => {
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
        input_schema: buildInputSchema(inputFields),
      });
      // Saved to the FLOW, not the publication: the selection that uses these
      // runs for any conversational flow, whether or not it is published.
      if (entityType === 'flow') {
        await FlowService.updateFlowOutcomes(entityId, outcomes);
      }
      refreshChatCatalog();
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
      refreshChatCatalog();
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
        Publish {entityType}
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
              Publishing makes <strong>{entityName}</strong> reachable by something
              other than the canvas. Pick where below — publishing to chat alone
              exposes nothing outside this workspace.
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
                'What it does AND when to use it. This is the only thing a caller ' +
                'matches on — a vague description means it is never chosen. Chat ' +
                'routing has the least context of any caller, so name the phrases ' +
                'someone would actually type.'
              }
            />

            <Box>
              <Typography variant="subtitle2" gutterBottom>
                Reachable from
              </Typography>
              <FormControlLabel
                control={
                  <Checkbox
                    checked={protocols.includes('chat')}
                    onChange={() => toggleProtocol('chat')}
                  />
                }
                label="Chat — let this be picked in Use existing mode"
              />
              <Typography
                variant="caption"
                color="text.secondary"
                display="block"
                sx={{ ml: 4, mt: -0.5, mb: 0.5 }}
              >
                Inside this workspace only. Nothing is exposed outside it.
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
                  With nothing selected this is not reachable from anywhere.
                </Typography>
              )}
            </Box>

            <PublishInputSchema
              fields={inputFields}
              onChange={setInputFields}
              entityLabel={entityType}
              usedPlaceholders={usedPlaceholders}
            />

            {entityType === 'flow' && (
              <PublishFlowOutcomes
                crews={flowCrews}
                outcomes={outcomes}
                onChange={setOutcomes}
              />
            )}
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
