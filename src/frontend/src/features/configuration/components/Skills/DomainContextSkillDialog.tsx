/**
 * The UC Metric View domain context, as a skill — a form shaped for it.
 *
 * Same content the generator's old "Domain context" field took (the greyed
 * example shows the shape), with the learn-from-deployed-YAMLs flywheel beside
 * it, saved as a skill tagged `kasal-tool-param: domain_context`. Opened from
 * the generator's task config (optionally attaching the skill to that task's
 * agent in the same step) and from Settings → Skills.
 */

import React, { useEffect, useState } from 'react';
import {
  Alert,
  Box,
  Button,
  Checkbox,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Divider,
  FormControlLabel,
  Stack,
  TextField,
  Typography,
} from '@mui/material';
import { Skill, SkillService } from '../../../../api/tools/SkillService';
import { UCMVLearningPanel } from '../../../tools/components/configuration/UCMVLearningPanel';
import {
  buildDomainContextSkill,
  DOMAIN_CONTEXT_EXAMPLE,
  domainContextDescription,
  domainContextSkillName,
} from './skillTemplates';
import { attachSkillToAgent } from './attachSkill';

export interface DomainContextAgent {
  id: string | number;
  name?: string;
}

interface Props {
  open: boolean;
  onClose: () => void;
  /** Edit this skill; omit to create one. */
  skill?: Skill | null;
  /** Pre-fill the text — e.g. moving an older task's inline domain context. */
  initialBody?: string;
  /** Offer to attach the new skill to this agent (the task's agent). */
  attachTo?: DomainContextAgent;
  onSaved?: (skill: Skill, attached: boolean) => void;
}

const DomainContextSkillDialog: React.FC<Props> = ({
  open,
  onClose,
  skill,
  initialBody,
  attachTo,
  onSaved,
}) => {
  const [model, setModel] = useState('');
  const [name, setName] = useState('');
  const [nameEdited, setNameEdited] = useState(false);
  const [description, setDescription] = useState('');
  const [descriptionEdited, setDescriptionEdited] = useState(false);
  const [body, setBody] = useState('');
  const [attach, setAttach] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!open) return;
    setModel('');
    setName(skill?.name ?? domainContextSkillName(''));
    setNameEdited(Boolean(skill));
    setDescription(skill?.description ?? domainContextDescription(''));
    setDescriptionEdited(Boolean(skill));
    setBody(skill?.body ?? initialBody ?? '');
    setAttach(true);
    setError(null);
  }, [open, skill, initialBody]);

  // Name and description follow the model name until the author edits them.
  const changeModel = (value: string) => {
    setModel(value);
    if (!nameEdited) setName(domainContextSkillName(value));
    if (!descriptionEdited) setDescription(domainContextDescription(value));
  };

  const handleSave = async () => {
    if (!body.trim()) {
      setError('Write the domain context, or generate it from deployed UCMV YAMLs below.');
      return;
    }
    setSaving(true);
    setError(null);
    try {
      const input = buildDomainContextSkill(name.trim(), description.trim(), body);
      const saved = skill
        ? await SkillService.update(skill.id, input)
        : await SkillService.create(input);
      let attached = false;
      if (!skill && attachTo && attach) {
        attached = Boolean(await attachSkillToAgent(attachTo.id, saved.name));
      }
      onSaved?.(saved, attached);
      onClose();
    } catch (err) {
      const detail =
        (err as { response?: { data?: { detail?: string } } })?.response?.data?.detail;
      setError(detail ?? (err instanceof Error ? err.message : 'Could not save.'));
    } finally {
      setSaving(false);
    }
  };

  return (
    <Dialog open={open} onClose={onClose} maxWidth="md" fullWidth>
      <DialogTitle>{skill ? `Edit ${skill.name}` : 'New domain context skill'}</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ mt: 1 }}>
          <Typography variant="body2" color="text.secondary">
            Your model&apos;s domain knowledge — business/metric vocabulary, naming conventions,
            fiscal-calendar quirks, cost-accounting terms. It&apos;s fed verbatim into the UC Metric
            View Generator&apos;s DAX→SQL translation (LLM fallback on) for every agent this skill is
            attached to.
          </Typography>

          {error && <Alert severity="error">{error}</Alert>}

          {!skill && (
            <TextField
              label="Model / report name"
              value={model}
              onChange={(e) => changeModel(e.target.value)}
              size="small"
              fullWidth
              placeholder="e.g. Total Supply Chain"
            />
          )}
          <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2}>
            <TextField
              label="Skill name"
              value={name}
              onChange={(e) => {
                setName(e.target.value);
                setNameEdited(true);
              }}
              size="small"
              disabled={Boolean(skill)}
              sx={{ minWidth: 260 }}
              helperText="Lowercase, hyphenated. Cannot change later."
            />
            <TextField
              label="When to use this skill"
              value={description}
              onChange={(e) => {
                setDescription(e.target.value);
                setDescriptionEdited(true);
              }}
              size="small"
              fullWidth
              multiline
              maxRows={3}
            />
          </Stack>

          <TextField
            label="Domain context"
            value={body}
            onChange={(e) => setBody(e.target.value)}
            fullWidth
            multiline
            rows={14}
            size="small"
            placeholder={DOMAIN_CONTEXT_EXAMPLE}
            helperText="The greyed text is an example showing the shape — type over it."
            InputProps={{ sx: { fontFamily: 'monospace', fontSize: '0.75rem' } }}
          />

          <Divider>
            <Typography variant="caption" color="text.secondary">
              or learn it from deployed UCMVs
            </Typography>
          </Divider>
          <Box>
            <UCMVLearningPanel
              applyLabel="Use as domain context"
              onApplyDomainContext={(readme) => setBody(readme)}
              disabled={saving}
            />
          </Box>

          {!skill && attachTo && (
            <FormControlLabel
              control={<Checkbox checked={attach} onChange={(e) => setAttach(e.target.checked)} />}
              label={`Attach to agent “${attachTo.name || attachTo.id}” (runs this task)`}
            />
          )}
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Cancel</Button>
        <Button onClick={handleSave} variant="contained" disabled={saving || !name.trim()}>
          {saving ? 'Saving…' : skill ? 'Save' : attachTo && attach ? 'Create & attach' : 'Create skill'}
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default DomainContextSkillDialog;
