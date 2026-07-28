import React, { useState } from 'react';
import {
  Alert,
  Box,
  Button,
  IconButton,
  Stack,
  TextField,
  Tooltip,
  Typography,
} from '@mui/material';
import AddIcon from '@mui/icons-material/Add';
import DeleteIcon from '@mui/icons-material/Delete';
import { SkillFileInput } from '../../../api/tools/SkillService';

interface Props {
  files: SkillFileInput[];
  onChange: (files: SkillFileInput[]) => void;
}

/** The path rule the backend enforces, restated here only to catch it early. */
const REFUSED = 'scripts/';

/**
 * Add and edit the files bundled with a skill.
 *
 * This is tier 3 of progressive disclosure, and the reason to use it is
 * concrete: the SKILL.md body stays short and always-loadable, while the detail
 * sits in files the agent reads only when the instructions send it there. A
 * skill whose body is 500 lines costs those tokens on every activation; the
 * same content split across two reference files costs them only when needed.
 *
 * `scripts/` is refused — Kasal does not execute bundled code, and being able
 * to save a script implies it will run.
 */
const SkillFilesEditor: React.FC<Props> = ({ files, onChange }) => {
  const [error, setError] = useState<string | null>(null);

  const update = (index: number, patch: Partial<SkillFileInput>) => {
    onChange(files.map((f, i) => (i === index ? { ...f, ...patch } : f)));
  };

  const add = () => {
    const base = 'references/notes.md';
    const taken = new Set(files.map((f) => f.path));
    let path = base;
    let n = 2;
    while (taken.has(path)) path = `references/notes-${n++}.md`;
    onChange([...files, { path, content: '' }]);
    setError(null);
  };

  const remove = (index: number) => onChange(files.filter((_, i) => i !== index));

  const validate = (path: string) => {
    if (path.startsWith(REFUSED)) {
      setError('Kasal does not run bundled scripts, so scripts/ cannot be saved.');
      return;
    }
    if (path.includes('..') || path.startsWith('/')) {
      setError('A file path must stay inside the skill.');
      return;
    }
    if (path.split('/').filter(Boolean).length > 2) {
      setError('Keep files one level deep, as the spec recommends.');
      return;
    }
    setError(null);
  };

  return (
    <Box>
      <Stack direction="row" justifyContent="space-between" alignItems="center">
        <Box>
          <Typography variant="subtitle2">Reference files</Typography>
          <Typography variant="caption" color="text.secondary">
            Loaded only when the instructions point at them — so detail here
            costs nothing until it is needed. Refer to one from the instructions
            as, for example, <code>references/pricing.md</code>.
          </Typography>
        </Box>
        <Button size="small" startIcon={<AddIcon />} onClick={add}>
          Add file
        </Button>
      </Stack>

      {error && (
        <Alert severity="error" sx={{ mt: 1 }}>
          {error}
        </Alert>
      )}

      <Stack spacing={2} sx={{ mt: 2 }}>
        {files.length === 0 && (
          <Typography variant="caption" color="text.secondary">
            No reference files. Add one when the instructions grow past what
            belongs in the body.
          </Typography>
        )}

        {files.map((file, index) => (
          <Box
            key={index}
            sx={{ border: 1, borderColor: 'divider', borderRadius: 1, p: 1.5 }}
          >
            <Stack direction="row" spacing={1} alignItems="center">
              <TextField
                label="Path"
                value={file.path}
                onChange={(e) => {
                  update(index, { path: e.target.value });
                  validate(e.target.value);
                }}
                size="small"
                fullWidth
                placeholder="references/pricing.md"
              />
              <Tooltip title="Remove this file">
                <IconButton size="small" color="error" onClick={() => remove(index)}>
                  <DeleteIcon fontSize="small" />
                </IconButton>
              </Tooltip>
            </Stack>
            <TextField
              value={file.content}
              onChange={(e) => update(index, { content: e.target.value })}
              fullWidth
              multiline
              minRows={6}
              size="small"
              sx={{ mt: 1 }}
              InputProps={{ style: { fontFamily: 'monospace', fontSize: 13 } }}
              placeholder="Markdown the agent reads when the instructions send it here."
            />
          </Box>
        ))}
      </Stack>
    </Box>
  );
};

export default SkillFilesEditor;
