/**
 * KeyEditor — per-key editor that renders different UIs based on value type.
 */

import React, { useState, useCallback } from 'react';
import {
  Box,
  Typography,
  TextField,
  IconButton,
  Chip,
  Paper,
  Alert,
  Button,
  Tooltip,
  useTheme,
  alpha,
  Divider,
} from '@mui/material';
import {
  Add as AddIcon,
  Delete as DeleteIcon,
  Code as CodeIcon,
  Edit as EditIcon,
} from '@mui/icons-material';
import {
  CONFIG_KEY_LABELS,
  classifyValueType,
  getKeyStatus,
  countTodos,
} from '../../types/configEditor';

interface KeyEditorProps {
  configKey: string;
  value: unknown;
  onChange: (key: string, newValue: unknown) => void;
}

const KeyEditor: React.FC<KeyEditorProps> = ({ configKey, value, onChange }) => {
  const theme = useTheme();
  const [rawMode, setRawMode] = useState(false);
  const [rawText, setRawText] = useState('');
  const [rawError, setRawError] = useState<string | null>(null);

  const valueType = classifyValueType(configKey, value);
  const status = getKeyStatus(value);
  const todoCount = countTodos(value);

  const handleRawToggle = useCallback(() => {
    if (!rawMode) {
      setRawText(JSON.stringify(value, null, 2));
      setRawError(null);
    } else if (rawText) {
      try {
        const parsed = JSON.parse(rawText);
        onChange(configKey, parsed);
        setRawError(null);
      } catch (e) {
        setRawError(`Invalid JSON: ${(e as Error).message}`);
        return; // Don't close raw mode on error
      }
    }
    setRawMode(!rawMode);
  }, [rawMode, rawText, value, configKey, onChange]);

  const handleRawSave = useCallback(() => {
    try {
      const parsed = JSON.parse(rawText);
      onChange(configKey, parsed);
      setRawError(null);
      setRawMode(false);
    } catch (e) {
      setRawError(`Invalid JSON: ${(e as Error).message}`);
    }
  }, [rawText, configKey, onChange]);

  // ── Header ──
  const header = (
    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
      <Typography variant="h6" sx={{ fontWeight: 600, flex: 1 }}>
        {CONFIG_KEY_LABELS[configKey] || configKey}
      </Typography>
      <Chip
        label={status.toUpperCase()}
        size="small"
        sx={{
          backgroundColor: alpha(
            status === 'auto' ? '#4caf50' : status === 'todo' ? '#ff9800' : status === 'empty' ? '#f44336' : '#9e9e9e',
            0.12,
          ),
          color: status === 'auto' ? '#4caf50' : status === 'todo' ? '#ff9800' : status === 'empty' ? '#f44336' : '#9e9e9e',
          fontWeight: 600,
          fontSize: '0.7rem',
        }}
      />
      {todoCount > 0 && (
        <Chip label={`${todoCount} TODO`} size="small" color="warning" variant="outlined" />
      )}
      <Tooltip title={rawMode ? 'Switch to form view' : 'Switch to raw JSON'}>
        <IconButton size="small" onClick={handleRawToggle}>
          {rawMode ? <EditIcon fontSize="small" /> : <CodeIcon fontSize="small" />}
        </IconButton>
      </Tooltip>
    </Box>
  );

  // ── Raw JSON mode ──
  if (rawMode) {
    return (
      <Box>
        {header}
        <Typography variant="caption" color="text.secondary" sx={{ mb: 1, display: 'block' }}>
          Key: <code>{configKey}</code> &middot; Type: {valueType}
        </Typography>
        {rawError && <Alert severity="error" sx={{ mb: 1 }}>{rawError}</Alert>}
        <TextField
          fullWidth
          multiline
          minRows={10}
          maxRows={30}
          value={rawText}
          onChange={(e) => setRawText(e.target.value)}
          sx={{
            '& .MuiInputBase-input': {
              fontFamily: 'monospace',
              fontSize: '0.85rem',
            },
          }}
        />
        <Box sx={{ display: 'flex', gap: 1, mt: 1 }}>
          <Button variant="contained" size="small" onClick={handleRawSave}>
            Apply JSON
          </Button>
          <Button variant="text" size="small" onClick={() => { setRawMode(false); setRawError(null); }}>
            Cancel
          </Button>
        </Box>
      </Box>
    );
  }

  // ── String / nullable string editor ──
  if (valueType === 'string' || valueType === 'string-nullable') {
    const isTodo = typeof value === 'string' && value.includes('TODO');
    return (
      <Box>
        {header}
        <Typography variant="caption" color="text.secondary" sx={{ mb: 1, display: 'block' }}>
          Key: <code>{configKey}</code>
        </Typography>
        <TextField
          fullWidth
          value={value === null ? '' : String(value)}
          onChange={(e) => {
            const v = e.target.value;
            onChange(configKey, valueType === 'string-nullable' && v === '' ? null : v);
          }}
          placeholder={value === null ? '(null — click to set a value)' : ''}
          sx={{
            '& .MuiOutlinedInput-root': isTodo ? {
              backgroundColor: alpha('#ff9800', 0.06),
              borderColor: '#ff9800',
            } : {},
          }}
          helperText={isTodo ? 'This value contains a TODO marker — please fill in the correct value' : undefined}
        />
        {valueType === 'string-nullable' && value !== null && (
          <Button size="small" color="warning" sx={{ mt: 0.5 }} onClick={() => onChange(configKey, null)}>
            Set to null
          </Button>
        )}
      </Box>
    );
  }

  // ── List editor (chip list + add) ──
  if (valueType === 'list') {
    const items = Array.isArray(value) ? value : [];
    return (
      <Box>
        {header}
        <Typography variant="caption" color="text.secondary" sx={{ mb: 1, display: 'block' }}>
          Key: <code>{configKey}</code> &middot; {items.length} item{items.length !== 1 ? 's' : ''}
        </Typography>
        <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5, mb: 1 }}>
          {items.map((item, idx) => {
            const isTodo = typeof item === 'string' && item.includes('TODO');
            return (
              <Chip
                key={idx}
                label={String(item)}
                onDelete={() => {
                  const newItems = [...items];
                  newItems.splice(idx, 1);
                  onChange(configKey, newItems);
                }}
                sx={isTodo ? {
                  backgroundColor: alpha('#ff9800', 0.12),
                  color: '#ff9800',
                  '& .MuiChip-deleteIcon': { color: '#ff9800' },
                } : {}}
                onClick={() => {
                  const newVal = prompt('Edit value:', String(item));
                  if (newVal !== null) {
                    const newItems = [...items];
                    newItems[idx] = newVal;
                    onChange(configKey, newItems);
                  }
                }}
              />
            );
          })}
        </Box>
        <AddItemInput onAdd={(val) => onChange(configKey, [...items, val])} />
      </Box>
    );
  }

  // ── Dict editors (dict-of-dict, dict-of-list) ──
  const dictValue = (value && typeof value === 'object' && !Array.isArray(value))
    ? value as Record<string, unknown>
    : {};
  const dictKeys = Object.keys(dictValue);

  return (
    <Box>
      {header}
      <Typography variant="caption" color="text.secondary" sx={{ mb: 1, display: 'block' }}>
        Key: <code>{configKey}</code> &middot; {dictKeys.length} entr{dictKeys.length !== 1 ? 'ies' : 'y'}
      </Typography>

      {dictKeys.length === 0 && (
        <Alert severity="info" variant="outlined">
          Empty object — add entries or switch to raw JSON to paste data.
        </Alert>
      )}

      {dictKeys.map((dk) => {
        const dv = dictValue[dk];
        const entryStr = JSON.stringify(dv, null, 2);
        const entryTodo = typeof entryStr === 'string' && entryStr.includes('TODO');

        return (
          <Paper
            key={dk}
            variant="outlined"
            sx={{
              p: 1.5,
              mb: 1,
              borderColor: entryTodo ? alpha('#ff9800', 0.5) : theme.palette.divider,
              backgroundColor: entryTodo ? alpha('#ff9800', 0.03) : 'transparent',
            }}
          >
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 0.5 }}>
              <Typography variant="subtitle2" sx={{ fontFamily: 'monospace', flex: 1 }}>
                {dk}
              </Typography>
              {entryTodo && <Chip label="TODO" size="small" color="warning" sx={{ height: 18, fontSize: '0.6rem' }} />}
              <Tooltip title="Delete entry">
                <IconButton
                  size="small"
                  onClick={() => {
                    const newDict = { ...dictValue };
                    delete newDict[dk];
                    onChange(configKey, newDict);
                  }}
                >
                  <DeleteIcon fontSize="small" />
                </IconButton>
              </Tooltip>
            </Box>
            <Divider sx={{ mb: 1 }} />

            {/* Simple string value */}
            {typeof dv === 'string' ? (
              <TextField
                fullWidth
                size="small"
                value={dv}
                onChange={(e) => onChange(configKey, { ...dictValue, [dk]: e.target.value })}
                sx={{
                  '& .MuiInputBase-input': { fontFamily: 'monospace', fontSize: '0.85rem' },
                }}
              />
            ) : (
              /* Complex value — editable JSON textarea */
              <DictEntryEditor
                entryKey={dk}
                entryValue={dv}
                onChange={(newVal) => onChange(configKey, { ...dictValue, [dk]: newVal })}
              />
            )}
          </Paper>
        );
      })}

      <AddDictEntryInput
        onAdd={(key, val) => onChange(configKey, { ...dictValue, [key]: val })}
      />
    </Box>
  );
};

// ── Sub-components ──────────────────────────────────────────────

const AddItemInput: React.FC<{ onAdd: (value: string) => void }> = ({ onAdd }) => {
  const [value, setValue] = useState('');

  return (
    <Box sx={{ display: 'flex', gap: 1, alignItems: 'center' }}>
      <TextField
        size="small"
        placeholder="Add item..."
        value={value}
        onChange={(e) => setValue(e.target.value)}
        onKeyDown={(e) => {
          if (e.key === 'Enter' && value.trim()) {
            onAdd(value.trim());
            setValue('');
          }
        }}
        sx={{ flex: 1 }}
      />
      <IconButton
        size="small"
        color="primary"
        disabled={!value.trim()}
        onClick={() => { onAdd(value.trim()); setValue(''); }}
      >
        <AddIcon />
      </IconButton>
    </Box>
  );
};

const AddDictEntryInput: React.FC<{
  onAdd: (key: string, value: unknown) => void;
}> = ({ onAdd }) => {
  const [newKey, setNewKey] = useState('');

  return (
    <Box sx={{ display: 'flex', gap: 1, alignItems: 'center', mt: 1 }}>
      <TextField
        size="small"
        placeholder="New key name..."
        value={newKey}
        onChange={(e) => setNewKey(e.target.value)}
        onKeyDown={(e) => {
          if (e.key === 'Enter' && newKey.trim()) {
            onAdd(newKey.trim(), '');
            setNewKey('');
          }
        }}
        sx={{ flex: 1 }}
      />
      <Button
        size="small"
        startIcon={<AddIcon />}
        disabled={!newKey.trim()}
        onClick={() => { onAdd(newKey.trim(), ''); setNewKey(''); }}
      >
        Add Entry
      </Button>
    </Box>
  );
};

const DictEntryEditor: React.FC<{
  entryKey: string;
  entryValue: unknown;
  onChange: (newValue: unknown) => void;
}> = ({ entryValue, onChange }) => {
  const [text, setText] = useState(JSON.stringify(entryValue, null, 2));
  const [error, setError] = useState<string | null>(null);
  const [dirty, setDirty] = useState(false);

  const handleBlur = () => {
    if (!dirty) return;
    try {
      const parsed = JSON.parse(text);
      onChange(parsed);
      setError(null);
      setDirty(false);
    } catch (e) {
      setError(`Invalid JSON: ${(e as Error).message}`);
    }
  };

  return (
    <Box>
      {error && <Alert severity="error" sx={{ mb: 0.5, py: 0 }}><Typography variant="caption">{error}</Typography></Alert>}
      <TextField
        fullWidth
        multiline
        size="small"
        minRows={2}
        maxRows={12}
        value={text}
        onChange={(e) => { setText(e.target.value); setDirty(true); setError(null); }}
        onBlur={handleBlur}
        sx={{
          '& .MuiInputBase-input': { fontFamily: 'monospace', fontSize: '0.8rem' },
        }}
      />
      {dirty && (
        <Button
          size="small"
          variant="text"
          sx={{ mt: 0.5 }}
          onClick={handleBlur}
        >
          Apply
        </Button>
      )}
    </Box>
  );
};

export default KeyEditor;
