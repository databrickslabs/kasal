import React, { useEffect, useState } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  TextField,
  Box,
  Typography,
  Alert,
} from '@mui/material';
import { SchemaService } from '../../api/workflow/SchemaService';
import { Schema } from '../../types/workflow/schema';
import SchemaFieldsEditor from './SchemaFieldsEditor';
import {
  SchemaFieldModel,
  emptyField,
  fieldsToSchema,
  schemaToFields,
} from '../../utils/schemaModel';

interface SchemaDialogProps {
  open: boolean;
  onClose: () => void;
  /** The schema being edited. Omit to create a new one. */
  schema?: Schema | null;
  /** Called with the created/updated schema after a successful save. */
  onSaved: (schema: Schema) => void;
  /** `schema_type` to stamp on a newly created schema. */
  schemaType?: string;
}

/**
 * The one dialog for defining an output schema.
 *
 * There used to be two — a create-only one in the flow builder and a separate
 * create and edit pair in Object Management. They had already drifted apart
 * (different type lists, only one of them offered "required"), and a schema
 * looked different depending on which door you opened it through. Since they
 * describe exactly the same thing, they are now the same dialog: one code path
 * for create and edit, in both places.
 *
 * Creating and editing differ in three small ways, all handled here: the title,
 * whether the name is editable (renaming is how the API identifies a schema, so
 * it stays fixed once saved), and which service call runs.
 */
const SchemaDialog: React.FC<SchemaDialogProps> = ({
  open,
  onClose,
  schema = null,
  onSaved,
  schemaType = 'data_model',
}) => {
  const isEdit = Boolean(schema);
  const [name, setName] = useState('');
  const [description, setDescription] = useState('');
  const [fields, setFields] = useState<SchemaFieldModel[]>([emptyField()]);
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    if (!open) return;
    setError(null);
    setSaving(false);
    if (schema) {
      setName(schema.name);
      setDescription(schema.description || '');
      const parsed = schemaToFields(schema.schema_definition);
      setFields(parsed.length > 0 ? parsed : [emptyField()]);
    } else {
      setName('');
      setDescription('');
      setFields([emptyField()]);
    }
  }, [open, schema]);

  const namedFields = fields.filter((f) => f.name.trim());

  const handleSave = async () => {
    setError(null);
    if (!name.trim()) {
      setError('Schema name is required.');
      return;
    }
    if (namedFields.length === 0) {
      setError('Add at least one field.');
      return;
    }

    const definition = fieldsToSchema(fields);
    const service = SchemaService.getInstance();

    try {
      setSaving(true);
      const saved = isEdit
        ? await service.updateSchema(schema!.name, {
            name: schema!.name,
            description: description.trim() || schema!.description,
            schema_type: schema!.schema_type,
            schema_definition: definition,
          })
        : await service.createSchema({
            name: name.trim(),
            description: description.trim() || `Output schema for ${name.trim()}`,
            schema_type: schemaType,
            schema_definition: definition,
          });

      if (!saved) {
        setError(isEdit ? 'Failed to save schema.' : 'Failed to create schema.');
        return;
      }
      // The response may not echo a parsed schema_definition; hand back a
      // normalized object so the caller can read its properties immediately.
      onSaved({ ...saved, schema_definition: definition });
    } catch (e) {
      const fallback = isEdit ? 'Failed to save schema.' : 'Failed to create schema.';
      const message = e instanceof Error ? e.message : fallback;
      setError(
        !isEdit && (message.includes('409') || message.toLowerCase().includes('exist'))
          ? `A schema named "${name.trim()}" already exists.`
          : message
      );
    } finally {
      setSaving(false);
    }
  };

  return (
    <Dialog open={open} onClose={onClose} maxWidth="sm" fullWidth>
      <DialogTitle sx={{ pb: 1 }}>
        {isEdit ? `Edit: ${schema?.name}` : 'New Output Schema'}
      </DialogTitle>
      <DialogContent sx={{ pt: 1 }}>
        <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
          <Typography variant="caption" color="text.secondary">
            Define what this step produces. A router can then route on any of these
            values — including a value inside a list, using &ldquo;List of items&rdquo;.
          </Typography>

          {error && (
            <Alert severity="error" sx={{ fontSize: '0.8rem', py: 0.5 }}>
              {error}
            </Alert>
          )}

          <TextField
            size="small"
            label="Schema name"
            placeholder="e.g. ResearchResult"
            value={name}
            onChange={(e) => setName(e.target.value)}
            fullWidth
            autoFocus={!isEdit}
            // The API identifies a schema by name, so renaming would create a
            // second one and orphan every task pointing at the first.
            disabled={isEdit}
            helperText={isEdit ? 'A schema cannot be renamed once saved.' : undefined}
          />
          <TextField
            size="small"
            label="Description (optional)"
            value={description}
            onChange={(e) => setDescription(e.target.value)}
            fullWidth
          />

          <Box>
            <Typography
              variant="subtitle2"
              sx={{ fontSize: '0.8rem', fontWeight: 600, mb: 1 }}
            >
              Fields
            </Typography>
            <SchemaFieldsEditor fields={fields} onChange={setFields} showRequired />
          </Box>
        </Box>
      </DialogContent>
      <DialogActions sx={{ px: 3, py: 1.5 }}>
        <Button onClick={onClose} size="small" disabled={saving}>
          Cancel
        </Button>
        <Button onClick={handleSave} variant="contained" size="small" disabled={saving}>
          {saving ? 'Saving…' : isEdit ? 'Save' : 'Create & use'}
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default SchemaDialog;
