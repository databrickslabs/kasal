import React, { useCallback, useEffect, useMemo, useState } from 'react';
import {
  Alert,
  Box,
  Button,
  Checkbox,
  Chip,
  CircularProgress,
  DialogActions,
  DialogContent,
  DialogTitle,
  FormControlLabel,
  Stack,
  TextField,
  Typography,
} from '@mui/material';
import PublicIcon from '@mui/icons-material/Public';
import { Bot, MessageCircle, Plug } from 'lucide-react';
import CatalogActionPane from './CatalogActionPane';

import { PublicationService } from '../../../../../api/workflow/PublicationService';
import { useAppStore as useChatAppStore } from '../../../../chat/store/appStore';
import {
  PublicationProtocol,
  PublicationResponse,
  PublishableEntity,
} from '../../../../../types/workflow/publication';
import PublishInputSchema from './PublishInputSchema';
import PublishFlowOutcomes from './PublishFlowOutcomes';
import { FlowService } from '../../../../../api/workflow/FlowService';
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
const destinations = [
  { id: 'chat', title: 'Workspace chat', description: 'Available in Use existing, inside this workspace.', icon: MessageCircle },
  { id: 'mcp', title: 'MCP clients', description: 'Let connected tools such as Claude Code and Cursor run it.', icon: Plug },
  { id: 'a2a', title: 'Agent platforms', description: 'Let other agents discover and run it through A2A.', icon: Bot },
] as const;

const sectionSx = { p: 2, border: 1, borderColor: 'divider', borderRadius: 3, minWidth: 0 };


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
  // Only a crew needs the switch: a flow holds a conversation when its state
  // declares it, which the flow's own editor owns.
  const [conversational, setConversational] = useState(false);
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
      setConversational(Boolean(publication?.conversational));
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
        ...(entityType === 'crew' ? { conversational } : {}),
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
    nameIsValid && description.trim().length > 0 && protocols.length > 0 && !saving && !loading;

  return (
    <CatalogActionPane open={open}
      label={`Publish · ${entityName}`} onClose={() => { if (!saving) onClose(); }}>
      <DialogTitle component="div">
        <Stack direction="row" spacing={1.5} alignItems="center">
          <Box sx={{ display: 'flex', p: 1.25, borderRadius: 3, bgcolor: 'action.hover' }}><PublicIcon fontSize="small" /></Box>
          <Box sx={{ flex: 1, minWidth: 0 }}>
            <Typography component="h2" sx={{ fontSize: 20, fontWeight: 600 }}>Publish {entityType}</Typography>
            <Typography variant="body2" color="text.secondary" sx={{ overflowWrap: 'anywhere' }}>{entityName}</Typography>
          </Box>
          <Chip size="small" variant="outlined" color={existing ? 'success' : 'default'} label={existing ? 'Published' : 'Not published'} />
        </Stack>
      </DialogTitle>

      <DialogContent>
        {loading ? (
          <Box sx={{ display: 'flex', justifyContent: 'center', py: 6 }}><CircularProgress size={24} aria-label="Loading publication" /></Box>
        ) : (
          <Stack spacing={2.5}>
            {error && <Alert severity="error">{error}</Alert>}
            <Box component="section" aria-label="Publication details" sx={sectionSx}>
              <Typography component="h3" variant="subtitle2" sx={{ mb: 0.5 }}>Details</Typography>
              <Typography variant="body2" color="text.secondary" sx={{ mb: 2.5 }}>Help people and agents recognize when to use this {entityType}.</Typography>
              <Stack spacing={2.5}>
                <TextField label="External name" value={externalName} onChange={e => setExternalName(e.target.value)}
                  fullWidth size="small" disabled={saving} error={externalName.length > 0 && !nameIsValid}
                  helperText={externalName.length > 0 && !nameIsValid
                    ? 'Use lowercase letters, digits and underscores, starting with a letter.'
                    : existing ? 'Changing this name affects clients using the current name.' : 'The identifier connected clients use to run this workload.'} />
                <TextField label="Description" value={description} onChange={e => setDescription(e.target.value)}
                  fullWidth multiline minRows={3} size="small" disabled={saving}
                  placeholder="What does it deliver, and when should someone use it?"
                  helperText="Describe the result and include phrases someone might use to ask for it." />
              </Stack>
            </Box>

            <Box component="section" aria-label="Publication availability" sx={sectionSx}>
              <Typography component="h3" variant="subtitle2" sx={{ mb: 0.5 }}>Where it is available</Typography>
              <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>Choose one or more places that can run this {entityType}.</Typography>
              <Stack spacing={1}>
                {destinations.map(({ id, title, description: detail, icon: Icon }) => {
                  const selected = protocols.includes(id);
                  return <FormControlLabel key={id} sx={{ m: 0, p: 1.25, gap: 1, borderRadius: 2.5,
                    border: 1, borderColor: selected ? 'text.secondary' : 'divider', bgcolor: selected ? 'action.hover' : 'transparent',
                    alignItems: 'flex-start', '& .MuiFormControlLabel-label': { flex: 1, minWidth: 0 } }}
                    control={<Checkbox size="small" color="default" checked={selected} disabled={saving}
                      inputProps={{ 'aria-label': title }} onChange={() => toggleProtocol(id)} sx={{ p: 0.25, mt: 0.25 }} />}
                    label={<Box sx={{ display: 'flex', gap: 1.25, alignItems: 'flex-start' }}>
                      <Box sx={{ display: 'flex', mt: 0.5, color: 'text.secondary' }}><Icon size={17} /></Box>
                      <Box><Typography variant="body2" sx={{ fontWeight: 600 }}>{title}</Typography>
                        <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mt: 0.25 }}>{detail}</Typography></Box>
                    </Box>} />;
                })}
              </Stack>
              {protocols.length === 0 && <Typography role="status" variant="caption" color="warning.main" sx={{ display: 'block', mt: 1 }}>Select at least one destination to publish.</Typography>}
            </Box>

            {entityType === 'crew' && <Box component="section" aria-label="Conversation behavior" sx={sectionSx}>
              <Typography component="h3" variant="subtitle2" sx={{ mb: 1 }}>Conversation</Typography>
              <FormControlLabel sx={{ m: 0, alignItems: 'flex-start' }}
                control={<Checkbox color="default" checked={conversational} disabled={saving}
                  onChange={e => setConversational(e.target.checked)} inputProps={{ 'aria-label': 'Holds a conversation' }} sx={{ pl: 0, pt: 0.25 }} />}
                label={<Box><Typography variant="body2" sx={{ fontWeight: 500 }}>Continue this crew on follow-ups</Typography>
                  <Typography variant="caption" color="text.secondary">Run it again with recent conversation context. Leave this off for a single request.</Typography></Box>} />
            </Box>}

            <Box component="section" aria-label="Publication inputs" sx={sectionSx}>
              <PublishInputSchema fields={inputFields} onChange={setInputFields} entityLabel={entityType} usedPlaceholders={usedPlaceholders} />
            </Box>
            {entityType === 'flow' && flowCrews.length > 0 && <Box component="section" aria-label="Flow outcomes" sx={sectionSx}>
              <PublishFlowOutcomes crews={flowCrews} outcomes={outcomes} onChange={setOutcomes} />
            </Box>}
          </Stack>
        )}
      </DialogContent>

      <DialogActions>
        {existing && <Button color="error" onClick={handleUnpublish} disabled={saving || loading}>Unpublish</Button>}
        <Box sx={{ flex: 1 }} />
        <Button color="inherit" onClick={onClose} disabled={saving}>Cancel</Button>
        <Button variant="contained" onClick={handlePublish} disabled={!canPublish}
          startIcon={saving ? <CircularProgress size={16} color="inherit" /> : undefined}>
          {saving ? 'Saving…' : existing ? 'Update publication' : 'Publish'}
        </Button>
      </DialogActions>
    </CatalogActionPane>
  );
};

export default PublishDialog;
