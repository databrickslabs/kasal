import React, { useEffect, useState } from 'react';
import { Box, Button, TextField } from '@mui/material';

interface Props {
  label: string;
  helper: string;
  value: string;
  saving: boolean;
  /** null clears the setting (back to its default). */
  onSave: (value: string | null) => void;
}

/** A text setting with Save and Clear. */
const TextSettingField: React.FC<Props> = ({ label, helper, value, saving, onSave }) => {
  const [draft, setDraft] = useState(value);
  useEffect(() => setDraft(value), [value]);
  const trimmed = draft.trim();
  return (
    <Box sx={{ display: 'flex', gap: 1, alignItems: 'flex-start', mb: 2 }}>
      <TextField
        size="small"
        label={label}
        value={draft}
        onChange={(e) => setDraft(e.target.value)}
        helperText={helper}
        inputProps={{ 'aria-label': label }}
        sx={{ flex: 1, maxWidth: 420 }}
      />
      <Button
        size="small"
        variant="outlined"
        disabled={saving || trimmed === value}
        onClick={() => onSave(trimmed || null)}
        sx={{ mt: 0.5 }}
      >
        Save
      </Button>
      <Button size="small" disabled={saving || !value} onClick={() => onSave(null)} sx={{ mt: 0.5 }}>
        Clear
      </Button>
    </Box>
  );
};

export default TextSettingField;
