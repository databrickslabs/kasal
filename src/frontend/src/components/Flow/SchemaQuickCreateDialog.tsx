import React, { useState } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  TextField,
  Box,
  Typography,
  FormControl,
  Select,
  MenuItem,
  IconButton,
  Alert,
} from '@mui/material';
import { Delete as DeleteIcon, Add as AddIcon } from '@mui/icons-material';
import { SchemaService } from '../../api/SchemaService';
import { Schema } from '../../types/schema';

interface SchemaField {
  name: string;
  type: string;
}

interface SchemaQuickCreateDialogProps {
  open: boolean;
  onClose: () => void;
  onCreated: (schema: Schema) => void;
}

const FIELD_TYPES = ['string', 'number', 'integer', 'boolean', 'array', 'object'];

/**
 * Minimal schema authoring dialog for use inside the flow router config. Creates a
 * `data_model` schema via SchemaService so the router can route on its fields. Mirrors
 * the payload shape used by ObjectManagement, but kept lean for the inline flow.
 */
const SchemaQuickCreateDialog: React.FC<SchemaQuickCreateDialogProps> = ({ open, onClose, onCreated }) => {
  const [name, setName] = useState('');
  const [description, setDescription] = useState('');
  const [fields, setFields] = useState<SchemaField[]>([{ name: '', type: 'string' }]);
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);

  const reset = () => {
    setName('');
    setDescription('');
    setFields([{ name: '', type: 'string' }]);
    setError(null);
    setSaving(false);
  };

  const handleClose = () => {
    reset();
    onClose();
  };

  const handleFieldChange = (index: number, key: keyof SchemaField, value: string) => {
    setFields((prev) => prev.map((f, i) => (i === index ? { ...f, [key]: value } : f)));
  };

  const handleAddField = () => setFields((prev) => [...prev, { name: '', type: 'string' }]);
  const handleRemoveField = (index: number) => setFields((prev) => prev.filter((_, i) => i !== index));

  const validFields = fields.filter((f) => f.name.trim());

  const handleCreate = async () => {
    setError(null);
    if (!name.trim()) {
      setError('Schema name is required.');
      return;
    }
    if (validFields.length === 0) {
      setError('Add at least one field.');
      return;
    }

    const properties: Record<string, { type: string }> = {};
    validFields.forEach((f) => {
      properties[f.name.trim()] = { type: f.type };
    });

    const payload = {
      name: name.trim(),
      description: description.trim() || `Output schema for ${name.trim()}`,
      schema_type: 'data_model',
      schema_definition: {
        type: 'object',
        properties,
        required: validFields.map((f) => f.name.trim()),
      },
    };

    try {
      setSaving(true);
      const created = await SchemaService.getInstance().createSchema(payload);
      if (created) {
        // The create response may not echo a parsed schema_definition; hand back a
        // normalized object so the caller can immediately read its properties.
        onCreated({ ...created, schema_definition: payload.schema_definition });
        reset();
      } else {
        setError('Failed to create schema.');
      }
    } catch (e) {
      const message = e instanceof Error ? e.message : 'Failed to create schema.';
      setError(message.includes('409') || message.toLowerCase().includes('exist')
        ? `A schema named "${name.trim()}" already exists.`
        : message);
    } finally {
      setSaving(false);
    }
  };

  return (
    <Dialog open={open} onClose={handleClose} maxWidth="sm" fullWidth>
      <DialogTitle sx={{ pb: 1 }}>New Output Schema</DialogTitle>
      <DialogContent sx={{ pt: 1 }}>
        <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
          <Typography variant="caption" color="text.secondary">
            Define the structured output the source crew should produce. The router can then route
            on any of these fields.
          </Typography>

          {error && <Alert severity="error" sx={{ fontSize: '0.8rem', py: 0.5 }}>{error}</Alert>}

          <TextField
            size="small"
            label="Schema name"
            placeholder="e.g. ResearchResult"
            value={name}
            onChange={(e) => setName(e.target.value)}
            fullWidth
            autoFocus
          />
          <TextField
            size="small"
            label="Description (optional)"
            value={description}
            onChange={(e) => setDescription(e.target.value)}
            fullWidth
          />

          <Box>
            <Typography variant="subtitle2" sx={{ fontSize: '0.8rem', fontWeight: 600, mb: 1 }}>
              Fields
            </Typography>
            <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
              {fields.map((field, index) => (
                <Box key={index} sx={{ display: 'flex', gap: 1, alignItems: 'center' }}>
                  <TextField
                    size="small"
                    placeholder="field name"
                    value={field.name}
                    onChange={(e) => handleFieldChange(index, 'name', e.target.value)}
                    sx={{ flex: 1, '& input': { fontSize: '0.85rem' } }}
                  />
                  <FormControl size="small" sx={{ minWidth: 120 }}>
                    <Select
                      value={field.type}
                      onChange={(e) => handleFieldChange(index, 'type', e.target.value)}
                      sx={{ fontSize: '0.85rem' }}
                    >
                      {FIELD_TYPES.map((t) => (
                        <MenuItem key={t} value={t} sx={{ fontSize: '0.85rem' }}>{t}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                  <IconButton
                    size="small"
                    onClick={() => handleRemoveField(index)}
                    disabled={fields.length === 1}
                    sx={{ color: 'error.main', p: 0.5 }}
                  >
                    <DeleteIcon fontSize="small" />
                  </IconButton>
                </Box>
              ))}
              <Button size="small" startIcon={<AddIcon />} onClick={handleAddField} sx={{ alignSelf: 'flex-start', fontSize: '0.75rem' }}>
                Add field
              </Button>
            </Box>
          </Box>
        </Box>
      </DialogContent>
      <DialogActions sx={{ px: 3, py: 1.5 }}>
        <Button onClick={handleClose} size="small" disabled={saving}>Cancel</Button>
        <Button onClick={handleCreate} variant="contained" size="small" disabled={saving}>
          {saving ? 'Creating…' : 'Create & use'}
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default SchemaQuickCreateDialog;
