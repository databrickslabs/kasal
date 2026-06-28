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
   * When provided, the condition "field" becomes a dropdown of these variable names
   * (e.g. the properties of a declared output schema) instead of a free-text input.
   */
  fieldOptions?: string[];
  /**
   * Optional map of variable name → JSON-schema type (string/number/integer/boolean).
   * Used to tailor the value input — e.g. boolean fields get a true/false dropdown.
   */
  fieldTypes?: Record<string, string>;
}

const operators = [
  { value: '=', label: 'equals' },
  { value: '!=', label: 'not equals' },
  { value: '>', label: 'greater than' },
  { value: '<', label: 'less than' },
  { value: '>=', label: 'greater or equal' },
  { value: '<=', label: 'less or equal' },
  { value: 'contains', label: 'contains' },
  { value: 'starts_with', label: 'starts with' },
  { value: 'ends_with', label: 'ends with' }
];

const ConditionBuilder: React.FC<ConditionBuilderProps> = ({
  conditions,
  onChange,
  label = 'Conditions',
  helperText,
  fieldOptions,
  fieldTypes
}) => {
  const hasFieldOptions = Array.isArray(fieldOptions) && fieldOptions.length > 0;
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
          {conditions.map((condition, index) => (
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
                      value={fieldOptions!.includes(condition.field) ? condition.field : ''}
                      displayEmpty
                      onChange={(e) => handleUpdateCondition(index, { field: e.target.value })}
                      sx={{ fontSize: '0.85rem' }}
                    >
                      <MenuItem value="" disabled sx={{ fontSize: '0.85rem' }}>
                        <em>Variable</em>
                      </MenuItem>
                      {fieldOptions!.map((opt) => (
                        <MenuItem key={opt} value={opt} sx={{ fontSize: '0.85rem' }}>
                          {opt}
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
                    {operators.map((op) => (
                      <MenuItem key={op.value} value={op.value} sx={{ fontSize: '0.85rem' }}>
                        {op.label}
                      </MenuItem>
                    ))}
                  </Select>
                </FormControl>

                {/* Value — boolean fields get a true/false dropdown, others free text */}
                {fieldTypes?.[condition.field] === 'boolean' ? (
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
            </Box>
          ))}

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

/**
 * Helper function to parse Python expression back to conditions
 * Basic parser for simple conditions
 */
export function pythonToConditions(expression: string): Condition[] {
  if (!expression.trim()) return [];

  // This is a simplified parser - handles basic cases
  // For complex expressions, we'll just return empty and let users rebuild
  try {
    const conditions: Condition[] = [];

    // Split by AND/OR (case insensitive)
    const parts = expression.split(/\s+(and|or)\s+/i);

    for (let i = 0; i < parts.length; i += 2) {
      const part = parts[i].trim();
      const connector = parts[i + 1] ? (parts[i + 1].toUpperCase() as 'AND' | 'OR') : undefined;

      // Try to parse the condition
      // Pattern: state.get("field", ...) operator value
      const match = part.match(/state\.get\("([^"]+)",\s*[^)]*\)\s*([><=!]+)\s*(.+)/);

      if (match) {
        const [, field, operator, value] = match;
        // Normalize '==' back to '=' for UI (Python uses ==, but UI uses =)
        const normalizedOperator = operator === '==' ? '=' : operator;
        conditions.push({
          field,
          operator: normalizedOperator as Condition['operator'],
          value: value.replace(/['"]/g, '').trim(),
          connector: i > 0 ? connector : undefined
        });
      }
    }

    return conditions;
  } catch {
    return [];
  }
}
