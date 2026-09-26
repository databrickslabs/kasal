import { useEffect, useState } from 'react';
import { Box, Button, TextField } from '@mui/material';

export interface AdvancedNumberFieldProps {
  label: string;
  helper: string;
  value: number | undefined;
  min: number;
  max: number;
  saving: boolean;
  onSave: (value: number | null) => void;
  /** Accept decimals (default: whole numbers only). */
  decimal?: boolean;
}

/** A bounded number with Save and "Reset to default" (sends null). */
function AdvancedNumberField({ label, helper, value, min, max, saving, onSave, decimal = false }: AdvancedNumberFieldProps) {
  const [draft, setDraft] = useState(value === undefined ? '' : String(value));
  useEffect(() => setDraft(value === undefined ? '' : String(value)), [value]);

  const parsed = Number(draft);
  const valid =
    draft.trim() !== '' && (decimal ? Number.isFinite(parsed) : Number.isInteger(parsed)) && parsed >= min && parsed <= max;
  const changed = valid && parsed !== value;

  return (
    <Box sx={{ display: 'flex', gap: 1, alignItems: 'flex-start', mb: 2 }}>
      <TextField
        size="small"
        type="number"
        label={label}
        value={draft}
        onChange={(e) => setDraft(e.target.value)}
        error={draft.trim() !== '' && !valid}
        helperText={draft.trim() !== '' && !valid ? `${decimal ? 'Number' : 'Whole number'} from ${min} to ${max}.` : helper}
        inputProps={{ min, max, step: decimal ? 'any' : 1, 'aria-label': label }}
        sx={{ width: 260 }}
      />
      <Button size="small" variant="outlined" disabled={!changed || saving} onClick={() => onSave(parsed)} sx={{ mt: 0.5 }}>
        Save
      </Button>
      <Button size="small" disabled={saving} onClick={() => onSave(null)} sx={{ mt: 0.5 }}>
        Reset to default
      </Button>
    </Box>
  );
}

export default AdvancedNumberField;
