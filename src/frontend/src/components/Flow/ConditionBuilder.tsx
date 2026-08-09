import React from 'react';
import {
  Box,
  FormControl,
  Select,
  MenuItem,
  TextField,
  IconButton,
  Typography
} from '@mui/material';
import DeleteIcon from '@mui/icons-material/Delete';
import { RoutableField, findFieldByPath } from '../../utils/schemaFields';
import AddIcon from '@mui/icons-material/Add';

export interface Condition {
  field: string;
  operator: '>' | '<' | '=' | '!=' | '>=' | '<=' | 'contains' | 'starts_with' | 'ends_with';
  value: string;
  connector?: 'AND' | 'OR';
}

interface ConditionBuilderProps {
  conditions: Condition[];
  onChange: (conditions: Condition[]) => void;
  label?: string;
  helperText?: string;
  /**
   * The values a condition may be built on, derived from the declared output
   * schema by `schemaToRoutableFields`. Each carries the `path` that is written
   * into the condition and the human `label` that is the only thing shown.
   * When omitted the field becomes a free-text input.
   */
  fields?: RoutableField[];
}

/**
 * Operator wording. Only `!=` differs between a single value and a list.
 *
 * On a list the field already reads "Any article → category", so "is" correctly
 * means "at least one is". But "is not" would read as "some item differs", when
 * it actually means NO item matches — the one genuinely counter-intuitive part
 * of any-match semantics. "is never" says what it does.
 */
const BASE_OPERATORS = [
  { value: '=', label: 'is' },
  { value: '>', label: 'is more than' },
  { value: '<', label: 'is less than' },
  { value: '>=', label: 'is at least' },
  { value: '<=', label: 'is at most' },
  { value: 'contains', label: 'contains' },
  { value: 'starts_with', label: 'starts with' },
  { value: 'ends_with', label: 'ends with' }
];

function operatorsFor(isList: boolean) {
  const negation = isList
    ? { value: '!=', label: 'is never' }
    : { value: '!=', label: 'is not' };
  return [BASE_OPERATORS[0], negation, ...BASE_OPERATORS.slice(1)];
}

function operatorLabel(operator: string, isList: boolean): string {
  return operatorsFor(isList).find((o) => o.value === operator)?.label ?? operator;
}

const ConditionBuilder: React.FC<ConditionBuilderProps> = ({
  conditions,
  onChange,
  label = 'Conditions',
  helperText,
  fields
}) => {
  const hasFieldOptions = Array.isArray(fields) && fields.length > 0;
  const handleAddCondition = () => {
    onChange([
      ...conditions,
      { field: '', operator: '=', value: '', connector: conditions.length > 0 ? 'AND' : undefined }
    ]);
  };

  const handleRemoveCondition = (index: number) => {
    const updated = conditions.filter((_, i) => i !== index);
    // Remove connector from first condition if it exists
    if (updated.length > 0 && updated[0].connector) {
      updated[0] = { ...updated[0], connector: undefined };
    }
    onChange(updated);
  };

  const handleUpdateCondition = (index: number, updates: Partial<Condition>) => {
    const updated = conditions.map((cond, i) =>
      i === index ? { ...cond, ...updates } : cond
    );
    onChange(updated);
  };

  return (
    <Box>
      <Typography variant="subtitle2" sx={{ mb: 1, fontWeight: 600, fontSize: '0.875rem' }}>
        {label}
      </Typography>

      {conditions.length === 0 ? (
        <Box
          sx={{
            border: '1px dashed',
            borderColor: 'divider',
            borderRadius: 1,
            p: 2,
            textAlign: 'center',
            cursor: 'pointer',
            '&:hover': { borderColor: 'primary.main', bgcolor: 'action.hover' }
          }}
          onClick={handleAddCondition}
        >
          <Typography variant="body2" color="text.secondary" sx={{ fontSize: '0.8rem' }}>
            Click to add a condition
          </Typography>
        </Box>
      ) : (
        <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
          {conditions.map((condition, index) => {
            const selected = hasFieldOptions
              ? findFieldByPath(fields!, condition.field)
              : undefined;
            return (
            <Box key={index}>
              {/* Connector (AND/OR) - only show for conditions after the first */}
              {index > 0 && condition.connector && (
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 0.5 }}>
                  <FormControl size="small" sx={{ minWidth: 80 }}>
                    <Select
                      value={condition.connector}
                      onChange={(e) =>
                        handleUpdateCondition(index, { connector: e.target.value as 'AND' | 'OR' })
                      }
                      sx={{ fontSize: '0.75rem', height: 28 }}
                    >
                      <MenuItem value="AND" sx={{ fontSize: '0.75rem' }}>AND</MenuItem>
                      <MenuItem value="OR" sx={{ fontSize: '0.75rem' }}>OR</MenuItem>
                    </Select>
                  </FormControl>
                </Box>
              )}

              {/* Condition Row */}
              <Box sx={{ display: 'flex', gap: 1, alignItems: 'center' }}>
                {/* Field — dropdown of schema variables when provided, else free text */}
                {hasFieldOptions ? (
                  <FormControl size="small" sx={{ flex: 1 }}>
                    <Select
                      // Only show the schema's routable fields. If a stored field is no
                      // longer in the schema, fall back to the empty placeholder rather
                      // than surfacing a stale/bogus option.
                      value={findFieldByPath(fields!, condition.field) ? condition.field : ''}
                      displayEmpty
                      onChange={(e) => handleUpdateCondition(index, { field: e.target.value })}
                      sx={{ fontSize: '0.85rem' }}
                    >
                      <MenuItem value="" disabled sx={{ fontSize: '0.85rem' }}>
                        <em>Choose a value</em>
                      </MenuItem>
                      {fields!.map((opt) => (
                        <MenuItem key={opt.path} value={opt.path} sx={{ fontSize: '0.85rem' }}>
                          {opt.label}
                        </MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                ) : (
                  <TextField
                    size="small"
                    placeholder="Field name"
                    value={condition.field}
                    onChange={(e) => handleUpdateCondition(index, { field: e.target.value })}
                    sx={{ flex: 1, '& input': { fontSize: '0.85rem' } }}
                  />
                )}

                {/* Operator */}
                <FormControl size="small" sx={{ minWidth: 140 }}>
                  <Select
                    value={condition.operator}
                    onChange={(e) =>
                      handleUpdateCondition(index, { operator: e.target.value as Condition['operator'] })
                    }
                    sx={{ fontSize: '0.85rem' }}
                  >
                    {operatorsFor(selected?.isList ?? false).map((op) => (
                      <MenuItem key={op.value} value={op.value} sx={{ fontSize: '0.85rem' }}>
                        {op.label}
                      </MenuItem>
                    ))}
                  </Select>
                </FormControl>

                {/* Value — boolean fields get a true/false dropdown, others free text */}
                {selected?.type === 'boolean' ? (
                  <FormControl size="small" sx={{ flex: 1 }}>
                    <Select
                      value={['true', 'false'].includes((condition.value || '').toLowerCase())
                        ? (condition.value || '').toLowerCase()
                        : ''}
                      displayEmpty
                      onChange={(e) => handleUpdateCondition(index, { value: e.target.value })}
                      sx={{ fontSize: '0.85rem' }}
                    >
                      <MenuItem value="" disabled sx={{ fontSize: '0.85rem' }}>
                        <em>Value</em>
                      </MenuItem>
                      <MenuItem value="true" sx={{ fontSize: '0.85rem' }}>true</MenuItem>
                      <MenuItem value="false" sx={{ fontSize: '0.85rem' }}>false</MenuItem>
                    </Select>
                  </FormControl>
                ) : (
                  <TextField
                    size="small"
                    placeholder="Value"
                    value={condition.value}
                    onChange={(e) => handleUpdateCondition(index, { value: e.target.value })}
                    sx={{ flex: 1, '& input': { fontSize: '0.85rem' } }}
                  />
                )}

                {/* Delete Button */}
                <IconButton
                  size="small"
                  onClick={() => handleRemoveCondition(index)}
                  sx={{ color: 'error.main' }}
                >
                  <DeleteIcon fontSize="small" />
                </IconButton>
              </Box>

              {/* Plain-language echo of what this row will do. The condition is
                  generated, never typed, so this is the only place the author
                  can check they picked what they meant. */}
              {condition.field && condition.value !== '' && (
                <Typography
                  variant="caption"
                  color="text.secondary"
                  sx={{ mt: 0.5, display: 'block', fontSize: '0.75rem', fontStyle: 'italic' }}
                >
                  {selected?.label ?? condition.field}{' '}
                  {operatorLabel(condition.operator, selected?.isList ?? false)}{' '}
                  {condition.value}
                </Typography>
              )}
            </Box>
            );
          })}

          {/* Add Condition Button */}
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
              '&:hover': { borderColor: 'primary.main', bgcolor: 'action.hover' }
            }}
            onClick={handleAddCondition}
          >
            <AddIcon fontSize="small" />
            <Typography variant="body2" sx={{ fontSize: '0.8rem' }}>
              Add condition
            </Typography>
          </Box>
        </Box>
      )}

      {helperText && (
        <Typography variant="caption" color="text.secondary" sx={{ mt: 0.5, display: 'block', fontSize: '0.75rem' }}>
          {helperText}
        </Typography>
      )}
    </Box>
  );
};

export default ConditionBuilder;

/**
 * Helper function to convert conditions to Python expression
 */
export function conditionsToPython(conditions: Condition[]): string {
  if (conditions.length === 0) return '';

  return conditions
    .map((cond, index) => {
      let expr = '';

      // Add connector for subsequent conditions
      if (index > 0 && cond.connector) {
        expr += ` ${cond.connector.toLowerCase()} `;
      }

      // Build the condition expression
      const field = `state.get("${cond.field}", "")`;
      // Emit real Python booleans for true/false so boolean fields compare correctly
      // (state.get(...) == True), not against the string "true".
      const lowered = cond.value.trim().toLowerCase();
      let value: string;
      if (lowered === 'true') {
        value = 'True';
      } else if (lowered === 'false') {
        value = 'False';
      } else {
        value = isNaN(Number(cond.value)) ? `"${cond.value}"` : cond.value;
      }

      switch (cond.operator) {
        case 'contains':
          expr += `${value} in ${field}`;
          break;
        case 'starts_with':
          expr += `${field}.startswith(${value})`;
          break;
        case 'ends_with':
          expr += `${field}.endswith(${value})`;
          break;
        case '=':
          // Map '=' to '==' for Python equality comparison
          expr += `${field} == ${value}`;
          break;
        default:
          expr += `${field} ${cond.operator} ${value}`;
      }

      return expr;
    })
    .join('');
}

const unquote = (raw: string): string => {
  const trimmed = raw.trim();
  // An UNQUOTED True/False is the Python boolean conditionsToPython emits; the
  // builder stores booleans lowercase, so send it back the way it was stored.
  // A quoted "True" is a string value and is left alone.
  if (trimmed === 'True' || trimmed === 'False') return trimmed.toLowerCase();
  return trimmed.replace(/^["']|["']$/g, '');
};

/**
 * Parse a stored condition back into rows for the builder.
 *
 * Must round-trip EVERY form `conditionsToPython` emits. It previously matched
 * only `[><=!]+`, so the builder's own `contains` / `starts with` / `ends with`
 * output failed to re-parse: reopening that edge produced zero rows, which
 * disabled Save and left the whole edge — description, checkpoint, HITL —
 * uneditable.
 */
export function pythonToConditions(expression: string): Condition[] {
  if (!expression.trim()) return [];

  try {
    const conditions: Condition[] = [];

    // Split by AND/OR (case insensitive)
    const parts = expression.split(/\s+(and|or)\s+/i);

    for (let i = 0; i < parts.length; i += 2) {
      const part = parts[i].trim();
      // The connector at parts[n-1] sits BEFORE the condition at parts[n], which
      // is where conditionsToPython puts it back. Reading parts[i + 1] attached
      // each connector to the condition before it, so three-row conditions
      // round-tripped with their AND/OR shifted by one.
      const connector = i > 0 ? (parts[i - 1].toUpperCase() as 'AND' | 'OR') : undefined;

      // state.get("field", ...) == value
      const comparison = part.match(
        /^state\.get\("([^"]+)",\s*[^)]*\)\s*(==|!=|>=|<=|>|<)\s*(.+)$/
      );
      if (comparison) {
        const [, field, operator, value] = comparison;
        conditions.push({
          field,
          operator: (operator === '==' ? '=' : operator) as Condition['operator'],
          value: unquote(value),
          connector
        });
        continue;
      }

      // value in state.get("field", ...)
      const contains = part.match(/^(.+?)\s+in\s+state\.get\("([^"]+)",\s*[^)]*\)$/);
      if (contains) {
        const [, value, field] = contains;
        conditions.push({ field, operator: 'contains', value: unquote(value), connector });
        continue;
      }

      // state.get("field", ...).startswith(value)
      const method = part.match(
        /^state\.get\("([^"]+)",\s*[^)]*\)\.(startswith|endswith)\((.+)\)$/
      );
      if (method) {
        const [, field, name, value] = method;
        conditions.push({
          field,
          operator: name === 'startswith' ? 'starts_with' : 'ends_with',
          value: unquote(value),
          connector
        });
      }
    }

    return conditions;
  } catch {
    return [];
  }
}
