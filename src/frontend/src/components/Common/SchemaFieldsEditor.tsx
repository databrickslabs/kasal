import React from 'react';
import {
  Box,
  FormControl,
  Select,
  MenuItem,
  TextField,
  IconButton,
  Typography,
  Checkbox,
  Tooltip,
  Button,
} from '@mui/material';
import DeleteIcon from '@mui/icons-material/Delete';
import AddIcon from '@mui/icons-material/Add';
import {
  FieldKind,
  KIND_OPTIONS,
  MAX_NESTING,
  SchemaFieldModel,
  emptyField,
  holdsChildren,
} from '../../utils/schemaModel';

interface SchemaFieldsEditorProps {
  fields: SchemaFieldModel[];
  onChange: (fields: SchemaFieldModel[]) => void;
  /** Show the per-field required checkbox. */
  showRequired?: boolean;
  /** 0 at the top. Nesting stops at MAX_NESTING. */
  depth?: number;
  /** Wording for this level's add button. */
  addLabel?: string;
}

const FIELD_NAME_HINT = 'field name';

/**
 * The field list behind both schema editors, recursive.
 *
 * A field is either one value, a group, or a list of things — and a list or
 * group expands inline to say what it holds. That is the shape a model returns
 * when it classifies a batch (`articles[].category`) or reports structure
 * (`orders[].lines[].sku`), and neither editor could express it before, so the
 * router had nothing to offer and Save stayed disabled.
 *
 * Deliberately never says "array", "object", "items" or "properties".
 */
const SchemaFieldsEditor: React.FC<SchemaFieldsEditorProps> = ({
  fields,
  onChange,
  showRequired = false,
  depth = 0,
  addLabel = 'Add field',
}) => {
  // Below the nesting limit a field can only be a plain value. The limit exists
  // so the router's field list stays readable and the model stands a chance of
  // producing the shape — not because deeper paths cannot be resolved.
  const kindOptions =
    depth >= MAX_NESTING ? KIND_OPTIONS.filter((o) => !holdsChildren(o.kind)) : KIND_OPTIONS;

  const updateAt = (index: number, patch: Partial<SchemaFieldModel>) => {
    onChange(fields.map((f, i) => (i === index ? { ...f, ...patch } : f)));
  };

  const changeKind = (index: number, kind: FieldKind) => {
    const current = fields[index];
    // Opening a list or group for the first time gives it one blank sub-field so
    // there is something to type into rather than an empty region.
    const children = holdsChildren(kind)
      ? current.children && current.children.length > 0
        ? current.children
        : [emptyField()]
      : undefined;
    updateAt(index, { kind, children });
  };

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', gap: depth === 0 ? 1 : 0.5 }}>
      {fields.map((field, index) => (
        <Box key={index}>
          <Box sx={{ display: 'flex', gap: 1, alignItems: 'center' }}>
            <TextField
              size="small"
              placeholder={FIELD_NAME_HINT}
              value={field.name}
              onChange={(e) => updateAt(index, { name: e.target.value })}
              sx={{ flex: 1, '& input': { fontSize: depth === 0 ? '0.85rem' : '0.8rem' } }}
            />

            {field.kind === 'advanced' ? (
              <Tooltip title="This field uses an advanced definition. It is kept exactly as it is.">
                <Typography
                  variant="caption"
                  color="text.secondary"
                  sx={{ minWidth: 130, fontSize: '0.75rem', fontStyle: 'italic' }}
                >
                  Advanced (kept as-is)
                </Typography>
              </Tooltip>
            ) : (
              <FormControl size="small" sx={{ minWidth: 130 }}>
                <Select
                  value={
                    kindOptions.some((o) => o.kind === field.kind) ? field.kind : 'text'
                  }
                  onChange={(e) => changeKind(index, e.target.value as FieldKind)}
                  sx={{ fontSize: depth === 0 ? '0.85rem' : '0.8rem' }}
                >
                  {kindOptions.map((option) => (
                    <MenuItem
                      key={option.kind}
                      value={option.kind}
                      sx={{ fontSize: '0.85rem' }}
                    >
                      {option.label}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
            )}

            {showRequired && (
              <Tooltip title="Required">
                <Checkbox
                  checked={field.required}
                  onChange={(e) => updateAt(index, { required: e.target.checked })}
                  size="small"
                />
              </Tooltip>
            )}

            <IconButton
              size="small"
              onClick={() => onChange(fields.filter((_, i) => i !== index))}
              sx={{ color: 'error.main' }}
            >
              <DeleteIcon fontSize="small" />
            </IconButton>
          </Box>

          {holdsChildren(field.kind) && (
            <Box
              sx={{
                ml: 2,
                mt: 0.5,
                pl: 1.5,
                borderLeft: '2px solid',
                borderColor: 'divider',
                display: 'flex',
                flexDirection: 'column',
                gap: 0.5,
              }}
            >
              <Typography
                variant="caption"
                color="text.secondary"
                sx={{ fontSize: '0.75rem' }}
              >
                {field.kind === 'list' ? 'Each item has:' : 'This group has:'}
              </Typography>

              <SchemaFieldsEditor
                fields={field.children ?? []}
                onChange={(children) => updateAt(index, { children })}
                showRequired={showRequired}
                depth={depth + 1}
                addLabel={field.kind === 'list' ? 'Add field to item' : 'Add field to group'}
              />
            </Box>
          )}
        </Box>
      ))}

      <Button
        size="small"
        startIcon={<AddIcon />}
        onClick={() => onChange([...fields, emptyField()])}
        sx={{ alignSelf: 'flex-start', fontSize: '0.75rem' }}
      >
        {addLabel}
      </Button>
    </Box>
  );
};

export default SchemaFieldsEditor;
