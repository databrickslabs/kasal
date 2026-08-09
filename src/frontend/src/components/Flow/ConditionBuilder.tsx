import React from 'react';
import {
  Box,
  FormControl,
  Select,
  MenuItem,
  TextField,
  IconButton,
  Typography,
  Button,
  Paper,
} from '@mui/material';
import DeleteIcon from '@mui/icons-material/Delete';
import AddIcon from '@mui/icons-material/Add';
import { RoutableField } from '../../utils/schemaFields';
import {
  ConditionGroup,
  ConditionOperator,
  ConditionTerm,
  RESULT_SUBJECT,
  defaultSubject,
  emptyGroup,
  fieldsForSubject,
  subjectsFor,
} from '../../utils/conditionGroups';

interface ConditionBuilderProps {
  groups: ConditionGroup[];
  onChange: (groups: ConditionGroup[]) => void;
  label?: string;
  helperText?: string;
  /**
   * The values a condition may be built on, from `schemaToRoutableFields`.
   * Each carries the `path` written into the condition and the human `label`
   * that is the only thing shown.
   */
  fields?: RoutableField[];
}

/**
 * Operator wording. Only `!=` changes, and only for a value drawn across a
 * list: the field already reads "Any tag", so "is not" would suggest "some tag
 * differs" when it means NO tag matches. Inside a group whose subject is one
 * item there is a single value, so the plain wording is right there.
 */
const BASE_OPERATORS = [
  { value: '=', label: 'is' },
  { value: '>', label: 'is more than' },
  { value: '<', label: 'is less than' },
  { value: '>=', label: 'is at least' },
  { value: '<=', label: 'is at most' },
  { value: 'contains', label: 'contains' },
  { value: 'starts_with', label: 'starts with' },
  { value: 'ends_with', label: 'ends with' },
];

function operatorsFor(anyElement: boolean) {
  const negation = anyElement
    ? { value: '!=', label: 'is never' }
    : { value: '!=', label: 'is not' };
  return [BASE_OPERATORS[0], negation, ...BASE_OPERATORS.slice(1)];
}

function operatorLabel(operator: string, anyElement: boolean): string {
  return operatorsFor(anyElement).find((o) => o.value === operator)?.label ?? operator;
}

/**
 * Build a router condition by naming a SUBJECT and then describing it.
 *
 * The subject is the whole result or one item of a list, and every term in a
 * group is asserted about that same subject. Flat rows could not express that:
 * each row independently said "Any article -> category", so
 *
 *   Any article -> category is Sports  AND  Any article -> score > 5
 *
 * was satisfied by a Sports article and a DIFFERENT high-scoring one, and
 * nothing on screen distinguished that from "a Sports article scoring over 5".
 * Both readings are things people mean, so neither could be assumed. One group
 * is now one item; two groups are independent.
 *
 * A schema with no lists has only one possible subject, so the picker is hidden
 * and this renders the flat rows it always did. The idea appears only when it
 * can change the answer.
 */
const ConditionBuilder: React.FC<ConditionBuilderProps> = ({
  groups,
  onChange,
  label = 'Conditions',
  helperText,
  fields,
}) => {
  const available = fields ?? [];
  const subjects = subjectsFor(available);
  const hasSubjectChoice = subjects.length > 1;

  const updateGroup = (index: number, patch: Partial<ConditionGroup>) => {
    onChange(groups.map((group, i) => (i === index ? { ...group, ...patch } : group)));
  };

  const changeSubject = (index: number, subject: string) => {
    // The chosen fields belong to the old subject, so they cannot carry over.
    updateGroup(index, { subject, terms: [{ field: '', operator: '=', value: '' }] });
  };

  const updateTerm = (
    index: number,
    termIndex: number,
    patch: Partial<ConditionTerm>
  ) => {
    updateGroup(index, {
      terms: groups[index].terms.map((term, i) =>
        i === termIndex ? { ...term, ...patch } : term
      ),
    });
  };

  const addTerm = (index: number) =>
    updateGroup(index, {
      terms: [...groups[index].terms, { field: '', operator: '=', value: '' }],
    });

  const removeTerm = (index: number, termIndex: number) => {
    const terms = groups[index].terms.filter((_, i) => i !== termIndex);
    if (terms.length === 0) {
      onChange(groups.filter((_, i) => i !== index));
      return;
    }
    updateGroup(index, { terms });
  };

  const addGroup = () =>
    onChange([
      ...groups,
      { ...emptyGroup(defaultSubject(available)), connector: 'AND' as const },
    ]);

  const renderTerm = (
    group: ConditionGroup,
    index: number,
    term: ConditionTerm,
    termIndex: number
  ) => {
    const choices = fieldsForSubject(available, group.subject);
    const selected = choices.find((choice) => choice.value === term.field);
    const anyElement = Boolean(selected?.isList);

    return (
      <Box key={termIndex}>
        <Box sx={{ display: 'flex', gap: 1, alignItems: 'center' }}>
          {termIndex > 0 && (
            <Typography
              variant="caption"
              color="text.secondary"
              sx={{ minWidth: 28, fontSize: '0.75rem' }}
            >
              and
            </Typography>
          )}
          <FormControl size="small" sx={{ flex: 1 }}>
            <Select
              value={selected ? term.field : ''}
              displayEmpty
              onChange={(e) => updateTerm(index, termIndex, { field: e.target.value })}
              sx={{ fontSize: '0.85rem' }}
            >
              <MenuItem value="" disabled sx={{ fontSize: '0.85rem' }}>
                <em>Choose a value</em>
              </MenuItem>
              {choices.map((choice) => (
                <MenuItem
                  key={choice.value}
                  value={choice.value}
                  sx={{ fontSize: '0.85rem' }}
                >
                  {choice.label}
                </MenuItem>
              ))}
            </Select>
          </FormControl>

          <FormControl size="small" sx={{ minWidth: 140 }}>
            <Select
              value={term.operator}
              onChange={(e) =>
                updateTerm(index, termIndex, {
                  operator: e.target.value as ConditionOperator,
                })
              }
              sx={{ fontSize: '0.85rem' }}
            >
              {operatorsFor(anyElement).map((op) => (
                <MenuItem key={op.value} value={op.value} sx={{ fontSize: '0.85rem' }}>
                  {op.label}
                </MenuItem>
              ))}
            </Select>
          </FormControl>

          {selected?.type === 'boolean' ? (
            <FormControl size="small" sx={{ flex: 1 }}>
              <Select
                value={
                  ['true', 'false'].includes((term.value || '').toLowerCase())
                    ? (term.value || '').toLowerCase()
                    : ''
                }
                displayEmpty
                onChange={(e) => updateTerm(index, termIndex, { value: e.target.value })}
                sx={{ fontSize: '0.85rem' }}
              >
                <MenuItem value="" disabled sx={{ fontSize: '0.85rem' }}>
                  <em>Value</em>
                </MenuItem>
                <MenuItem value="true" sx={{ fontSize: '0.85rem' }}>
                  true
                </MenuItem>
                <MenuItem value="false" sx={{ fontSize: '0.85rem' }}>
                  false
                </MenuItem>
              </Select>
            </FormControl>
          ) : (
            <TextField
              size="small"
              placeholder="Value"
              value={term.value}
              onChange={(e) => updateTerm(index, termIndex, { value: e.target.value })}
              sx={{ flex: 1, '& input': { fontSize: '0.85rem' } }}
            />
          )}

          <IconButton
            size="small"
            onClick={() => removeTerm(index, termIndex)}
            sx={{ color: 'error.main' }}
          >
            <DeleteIcon fontSize="small" />
          </IconButton>
        </Box>

        {/* Plain-language echo. The condition is generated, never typed, so this
            is the only place the author can check they picked what they meant. */}
        {term.field && term.value !== '' && (
          <Typography
            variant="caption"
            color="text.secondary"
            sx={{
              mt: 0.25,
              ml: termIndex > 0 ? 4.5 : 0,
              display: 'block',
              fontSize: '0.75rem',
              fontStyle: 'italic',
            }}
          >
            {selected?.label ?? term.field} {operatorLabel(term.operator, anyElement)}{' '}
            {term.value}
          </Typography>
        )}
      </Box>
    );
  };

  return (
    <Box>
      <Typography
        variant="subtitle2"
        sx={{ mb: 1, fontWeight: 600, fontSize: '0.875rem' }}
      >
        {label}
      </Typography>

      {groups.length === 0 ? (
        <Box
          sx={{
            border: '1px dashed',
            borderColor: 'divider',
            borderRadius: 1,
            p: 2,
            textAlign: 'center',
            cursor: 'pointer',
            '&:hover': { borderColor: 'primary.main', bgcolor: 'action.hover' },
          }}
          onClick={() => onChange([emptyGroup(defaultSubject(available))])}
        >
          <Typography variant="body2" color="text.secondary" sx={{ fontSize: '0.8rem' }}>
            Click to add a condition
          </Typography>
        </Box>
      ) : (
        <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
          {groups.map((group, index) => (
            <Box key={index}>
              {index > 0 && (
                <Box sx={{ display: 'flex', alignItems: 'center', mb: 0.5 }}>
                  <FormControl size="small" sx={{ minWidth: 80 }}>
                    <Select
                      value={group.connector ?? 'AND'}
                      onChange={(e) =>
                        updateGroup(index, { connector: e.target.value as 'AND' | 'OR' })
                      }
                      sx={{ fontSize: '0.75rem', height: 28 }}
                    >
                      <MenuItem value="AND" sx={{ fontSize: '0.75rem' }}>
                        AND
                      </MenuItem>
                      <MenuItem value="OR" sx={{ fontSize: '0.75rem' }}>
                        OR
                      </MenuItem>
                    </Select>
                  </FormControl>
                </Box>
              )}

              <Paper
                variant="outlined"
                sx={{ p: 1.5, display: 'flex', flexDirection: 'column', gap: 1 }}
              >
                {/* Shown only when there is a choice — a schema with no lists has
                    one possible subject, and naming it would be noise. */}
                {hasSubjectChoice && (
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                    <FormControl size="small" sx={{ minWidth: 180 }}>
                      <Select
                        value={
                          subjects.some((s) => s.subject === group.subject)
                            ? group.subject
                            : (subjects[0]?.subject ?? RESULT_SUBJECT)
                        }
                        displayEmpty
                        onChange={(e) => changeSubject(index, e.target.value)}
                        sx={{ fontSize: '0.85rem', fontWeight: 600 }}
                      >
                        {subjects.map((subject) => (
                          <MenuItem
                            key={subject.subject || 'result'}
                            value={subject.subject}
                            sx={{ fontSize: '0.85rem' }}
                          >
                            {subject.label}
                          </MenuItem>
                        ))}
                      </Select>
                    </FormControl>
                    <Typography
                      variant="caption"
                      color="text.secondary"
                      sx={{ fontSize: '0.75rem' }}
                    >
                      {group.subject === RESULT_SUBJECT
                        ? 'where'
                        : 'where, all on the same one'}
                    </Typography>
                  </Box>
                )}

                {group.terms.map((term, termIndex) =>
                  renderTerm(group, index, term, termIndex)
                )}

                <Button
                  size="small"
                  startIcon={<AddIcon />}
                  onClick={() => addTerm(index)}
                  sx={{ alignSelf: 'flex-start', fontSize: '0.75rem' }}
                >
                  and
                </Button>
              </Paper>
            </Box>
          ))}

          <Box
            sx={{
              border: '1px dashed',
              borderColor: 'divider',
              borderRadius: 1,
              p: 1,
              textAlign: 'center',
              cursor: 'pointer',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              gap: 0.5,
              '&:hover': { borderColor: 'primary.main', bgcolor: 'action.hover' },
            }}
            onClick={addGroup}
          >
            <AddIcon fontSize="small" />
            <Typography variant="body2" sx={{ fontSize: '0.8rem' }}>
              Add condition
            </Typography>
          </Box>
        </Box>
      )}

      {helperText && (
        <Typography
          variant="caption"
          color="text.secondary"
          sx={{ mt: 0.5, display: 'block', fontSize: '0.75rem' }}
        >
          {helperText}
        </Typography>
      )}
    </Box>
  );
};

export default ConditionBuilder;
