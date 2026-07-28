import React, { useEffect, useState } from 'react';
import {
  Alert,
  Box,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  FormControlLabel,
  Stack,
  Switch,
  TextField,
  Typography,
} from '@mui/material';
import { Skill, SkillInput, SkillService } from '../../../api/tools/SkillService';

interface Props {
  open: boolean;
  skill: Skill | null;
  onClose: () => void;
  onSave: (input: SkillInput) => Promise<void>;
}

const EMPTY: SkillInput = {
  name: '',
  description: '',
  body: '',
  enabled: true,
  global_enabled: false,
};

/**
 * Write or edit a skill.
 *
 * The **description** is treated as the important field rather than an
 * afterthought, because it is the only thing the model sees when deciding
 * whether the skill applies. A vague one never activates; a greedy one activates
 * always. That is a content problem the editor can help with and code cannot
 * fix, so the field says what it is for and is checked for length.
 *
 * Validation runs against the backend, which runs the Agent Skills reference
 * validator — so what the editor accepts is exactly what every other client
 * accepts, and the messages are the ones an author can search for.
 */
const SkillEditor: React.FC<Props> = ({ open, skill, onClose, onSave }) => {
  const [form, setForm] = useState<SkillInput>(EMPTY);
  const [saving, setSaving] = useState(false);
  const [errors, setErrors] = useState<string[]>([]);
  const [warnings, setWarnings] = useState<string[]>([]);

  useEffect(() => {
    if (!open) return;
    setErrors([]);
    setWarnings([]);
    setForm(
      skill
        ? {
            name: skill.name,
            description: skill.description,
            body: skill.body,
            license: skill.license,
            compatibility: skill.compatibility,
            enabled: skill.enabled,
            global_enabled: skill.global_enabled,
          }
        : EMPTY,
    );
  }, [open, skill]);

  const set = <K extends keyof SkillInput>(key: K, value: SkillInput[K]) =>
    setForm((prev) => ({ ...prev, [key]: value }));

  const check = async () => {
    try {
      const result = await SkillService.validate(form);
      setErrors(result.errors);
      setWarnings(result.warnings);
      return result.valid;
    } catch {
      setErrors(['Could not reach the validator.']);
      return false;
    }
  };

  const handleSave = async () => {
    if (!(await check())) return;
    setSaving(true);
    try {
      await onSave(form);
      onClose();
    } catch (err) {
      const detail =
        (err as { response?: { data?: { detail?: string } } })?.response?.data?.detail;
      setErrors([detail ?? (err instanceof Error ? err.message : 'Could not save.')]);
    } finally {
      setSaving(false);
    }
  };

  return (
    <Dialog open={open} onClose={onClose} maxWidth="md" fullWidth>
      <DialogTitle>{skill ? `Edit ${skill.name}` : 'New skill'}</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ mt: 1 }}>
          {errors.length > 0 && (
            <Alert severity="error">
              {errors.map((e) => (
                <Box key={e}>{e}</Box>
              ))}
            </Alert>
          )}
          {warnings.length > 0 && (
            <Alert severity="info">
              {warnings.map((w) => (
                <Box key={w}>{w}</Box>
              ))}
            </Alert>
          )}

          <TextField
            label="Name"
            value={form.name}
            onChange={(e) => set('name', e.target.value)}
            fullWidth
            size="small"
            disabled={Boolean(skill)}
            helperText="Lowercase, hyphenated — for example quarterly-review. This is the folder name it exports to, so it cannot change later."
          />

          <TextField
            label="When to use this skill"
            value={form.description}
            onChange={(e) => set('description', e.target.value)}
            fullWidth
            multiline
            minRows={2}
            size="small"
            helperText={
              `${form.description.length}/1024 — the only thing the agent sees when ` +
              'deciding whether this applies. Say what it does AND when to use it.'
            }
          />

          <TextField
            label="Instructions"
            value={form.body ?? ''}
            onChange={(e) => set('body', e.target.value)}
            fullWidth
            multiline
            minRows={12}
            size="small"
            InputProps={{ style: { fontFamily: 'monospace', fontSize: 13 } }}
            helperText="Markdown. Loaded only once the agent decides the skill applies, so length costs nothing until then — but keep it under ~500 lines and push detail into reference files."
          />

          <FormControlLabel
            control={
              <Switch
                checked={form.enabled ?? true}
                onChange={(e) => set('enabled', e.target.checked)}
              />
            }
            label="Enabled"
          />

          <FormControlLabel
            control={
              <Switch
                checked={form.global_enabled ?? false}
                onChange={(e) => set('global_enabled', e.target.checked)}
              />
            }
            label="Give this to every agent without attaching it"
          />

          <Typography variant="caption" color="text.secondary">
            Reference files are added by importing a skill folder — this editor
            writes the SKILL.md.
          </Typography>
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={() => void check()}>Check</Button>
        <Box sx={{ flex: 1 }} />
        <Button onClick={onClose}>Cancel</Button>
        <Button onClick={handleSave} variant="contained" disabled={saving}>
          {saving ? 'Saving…' : 'Save'}
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default SkillEditor;
