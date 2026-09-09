import React, { useState } from 'react';
import {
  Box,
  Checkbox,
  IconButton,
  Stack,
  TextField,
  Tooltip,
  Typography,
} from '@mui/material';
import AddIcon from '@mui/icons-material/Add';
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutline';

import { PublicationInputField } from './publicationInputFields';

interface PublishInputSchemaProps {
  fields: PublicationInputField[];
  onChange: (fields: PublicationInputField[]) => void;
  entityLabel: string;
  /**
   * The `{placeholder}` names actually present in this crew's or flow's text.
   * A declared field that is not among them is passed to the run and then used
   * by nothing — see the warning below.
   */
  usedPlaceholders?: string[];
}

/**
 * The declared-inputs editor in the publish dialog.
 *
 * This is the ONLY point in the product where a human can say "`quarter` is
 * required, `format` is not". Nothing downstream can infer it, so a capability
 * published without passing through here makes every consumer treat all of its
 * placeholders as required and interrogate the user for each one.
 */
const PublishInputSchema: React.FC<PublishInputSchemaProps> = ({
  fields,
  onChange,
  entityLabel,
  usedPlaceholders,
}) => {
  const [newField, setNewField] = useState('');
  const used = usedPlaceholders ? new Set(usedPlaceholders) : null;

  const update = (index: number, patch: Partial<PublicationInputField>) =>
    onChange(fields.map((f, i) => (i === index ? { ...f, ...patch } : f)));

  const remove = (index: number) => onChange(fields.filter((_, i) => i !== index));

  const add = () => {
    const name = newField.trim();
    if (!name || fields.some((f) => f.name === name)) return;
    onChange([...fields, { name, required: true, description: '' }]);
    setNewField('');
  };

  return (
    <Box>
      <Typography variant="subtitle2" gutterBottom>
        Inputs
      </Typography>
      <Typography variant="caption" color="text.secondary" display="block" sx={{ mb: 1 }}>
        Values this {entityLabel} needs to run. Mark only the fields a caller must provide as required.
      </Typography>

      <Stack spacing={1}>
        {fields.map((field, index) => {
          // A field nothing references is the quietest failure this form has:
          // the caller is asked for it, the value is passed to the run, and no
          // agent or task ever reads it — so the run ignores what they said and
          // looks like it simply got the answer wrong.
          const unused = used !== null && !used.has(field.name);
          return (
            <Box key={field.name} sx={{ p: 1.5, borderRadius: 2.5, bgcolor: 'action.hover', minWidth: 0 }}>
              <Stack direction="row" spacing={0.5} alignItems="center" sx={{ mb: 1 }}>
              <Tooltip title={field.required ? 'Required' : 'Optional'}>
                <Checkbox
                  size="small"
                  checked={field.required}
                  onChange={() => update(index, { required: !field.required })}
                  inputProps={{ 'aria-label': `${field.name} is required` }}
                />
              </Tooltip>
              <Tooltip
                title={
                  unused
                    ? `No {${field.name}} appears in this ${entityLabel}'s agents or tasks, so the value will be passed but never used. Add {${field.name}} to a task description.`
                    : ''
                }
              >
                <Typography
                  variant="body2"
                  sx={{
                    fontFamily: 'monospace',
                    flex: 1,
                    minWidth: 0,
                    overflowWrap: 'anywhere',
                    color: unused ? 'warning.main' : undefined,
                  }}
                >
                  {field.name}
                  {unused && ' ⚠'}
                </Typography>
              </Tooltip>
              <Typography variant="caption" color="text.secondary">{field.required ? 'Required' : 'Optional'}</Typography>
              <IconButton
                size="small"
                onClick={() => remove(index)}
                aria-label={`Remove ${field.name}`}
              >
                <DeleteOutlineIcon fontSize="small" />
              </IconButton>
              </Stack>
              <TextField
                value={field.description}
                onChange={(e) => update(index, { description: e.target.value })}
                label={`Description for ${field.name}`}
                placeholder="Help the caller provide the right value"
                size="small"
                multiline
                fullWidth
              />
            </Box>
          );
        })}

        {used !== null && fields.some((f) => !used.has(f.name)) && (
          <Typography variant="caption" color="warning.main">
            Fields marked ⚠ do not appear as {'{placeholders}'} in this{' '}
            {entityLabel}. Add them to a task description, or the caller will be
            asked for a value that changes nothing.
          </Typography>
        )}

        {fields.length === 0 && (
          <Typography variant="caption" color="text.secondary">
            No {'{placeholders}'} found. Add any values a caller should supply.
          </Typography>
        )}

        <Stack direction="row" spacing={1} alignItems="center">
          <TextField
            value={newField}
            onChange={(e) => setNewField(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'Enter') {
                e.preventDefault();
                add();
              }
            }}
            placeholder="Add a field"
            inputProps={{ 'aria-label': 'New input field' }}
            size="small"
            fullWidth
          />
          <IconButton size="small" onClick={add} aria-label="Add field">
            <AddIcon fontSize="small" />
          </IconButton>
        </Stack>
      </Stack>
    </Box>
  );
};

export default PublishInputSchema;
