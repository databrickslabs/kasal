import React, { useState, useEffect, useMemo } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  TextField,
  Typography,
  Box,
  Chip,
  Alert,
  IconButton,
  Switch,
  FormControlLabel,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  InputAdornment,
  Tooltip,
} from '@mui/material';
import {
  Add as AddIcon,
  Close as CloseIcon,
  ExpandMore as ExpandMoreIcon,
  Search as SearchIcon,
  Clear as ClearIcon,
} from '@mui/icons-material';
import { Node } from 'reactflow';

interface InputVariable {
  name: string;
  value: string;
  description?: string;
  required: boolean;
}

interface InputVariablesDialogProps {
  open: boolean;
  onClose: () => void;
  onConfirm: (inputs: Record<string, string>) => void;
  nodes: Node[];
}

const ITEMS_PER_SECTION = 20;

export const InputVariablesDialog: React.FC<InputVariablesDialogProps> = ({
  open,
  onClose,
  onConfirm,
  nodes
}) => {
  const [variables, setVariables] = useState<InputVariable[]>([]);
  const [detectedVariables, setDetectedVariables] = useState<string[]>([]);
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [searchQuery, setSearchQuery] = useState('');
  const [requiredExpanded, setRequiredExpanded] = useState(true);
  const [optionalExpanded, setOptionalExpanded] = useState(true);
  const [showAllRequired, setShowAllRequired] = useState(false);
  const [showAllOptional, setShowAllOptional] = useState(false);

  // Extract variables from agent and task nodes
  useEffect(() => {
    if (!open) return;

    const variablePattern = /\{([a-zA-Z_][a-zA-Z0-9_-]*)\}/g;
    const foundVariables = new Set<string>();

    nodes.forEach(node => {
      if (node.type === 'agentNode' || node.type === 'taskNode') {
        const data = node.data as Record<string, unknown>;

        // Check various fields for variables
        const fieldsToCheck = [
          data.role,
          data.goal,
          data.backstory,
          data.description,
          data.expected_output,
          data.label
        ];

        fieldsToCheck.forEach(field => {
          if (field && typeof field === 'string') {
            let match;
            while ((match = variablePattern.exec(field)) !== null) {
              foundVariables.add(match[1]);
            }
          }
        });
      }
    });

    const detectedVars = Array.from(foundVariables);
    setDetectedVariables(detectedVars);

    // Initialize variables if they don't exist yet
    if (variables.length === 0 && detectedVars.length > 0) {
      setVariables(detectedVars.map(name => ({ name, value: '', required: true })));
    }
  }, [open, nodes, variables.length]);

  // Reset state when dialog closes
  useEffect(() => {
    if (!open) {
      setSearchQuery('');
      setShowAllRequired(false);
      setShowAllOptional(false);
    }
  }, [open]);

  // Memoized filtered and grouped variables
  const { requiredVariables, optionalVariables, filteredRequired, filteredOptional } = useMemo(() => {
    const required = variables.filter(v => v.required);
    const optional = variables.filter(v => !v.required);

    const filterFn = (v: InputVariable) =>
      v.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
      v.value.toLowerCase().includes(searchQuery.toLowerCase());

    return {
      requiredVariables: required,
      optionalVariables: optional,
      filteredRequired: searchQuery ? required.filter(filterFn) : required,
      filteredOptional: searchQuery ? optional.filter(filterFn) : optional,
    };
  }, [variables, searchQuery]);

  // Variables to display (with pagination)
  const displayedRequired = showAllRequired
    ? filteredRequired
    : filteredRequired.slice(0, ITEMS_PER_SECTION);

  const displayedOptional = showAllOptional
    ? filteredOptional
    : filteredOptional.slice(0, ITEMS_PER_SECTION);

  const handleVariableChange = (index: number, field: keyof InputVariable, newValue: string | boolean) => {
    const updatedVariables = [...variables];
    updatedVariables[index] = { ...updatedVariables[index], [field]: newValue };
    setVariables(updatedVariables);

    // Clear error for this variable
    if (field === 'value' && newValue) {
      const newErrors = { ...errors };
      delete newErrors[updatedVariables[index].name];
      setErrors(newErrors);
    }
  };

  const handleToggleRequired = (variableName: string) => {
    const index = variables.findIndex(v => v.name === variableName);
    if (index !== -1) {
      const updatedVariables = [...variables];
      updatedVariables[index] = {
        ...updatedVariables[index],
        required: !updatedVariables[index].required
      };
      setVariables(updatedVariables);

      // Clear error if making optional
      if (!updatedVariables[index].required) {
        const newErrors = { ...errors };
        delete newErrors[variableName];
        setErrors(newErrors);
      }
    }
  };

  const handleAddVariable = () => {
    setVariables([...variables, { name: '', value: '', required: false }]);
    setOptionalExpanded(true);
  };

  const handleRemoveVariable = (variableName: string) => {
    const updatedVariables = variables.filter(v => v.name !== variableName);
    setVariables(updatedVariables);
  };

  const handleClearAllValues = () => {
    setVariables(variables.map(v => ({ ...v, value: '' })));
    setErrors({});
  };

  const handleConfirm = () => {
    // Validate that all required variables have values
    const newErrors: Record<string, string> = {};
    let hasErrors = false;

    variables.forEach(variable => {
      if (variable.required && !variable.value) {
        newErrors[variable.name] = 'This variable is required';
        hasErrors = true;
      }
    });

    if (hasErrors) {
      setErrors(newErrors);
      setRequiredExpanded(true); // Expand required section to show errors
      return;
    }

    // Convert to record format
    const inputs: Record<string, string> = {};
    variables.forEach(variable => {
      if (variable.name && variable.value) {
        inputs[variable.name] = variable.value;
      }
    });

    onConfirm(inputs);
  };

  const renderVariableRow = (variable: InputVariable, isDetected: boolean) => {
    const index = variables.findIndex(v => v.name === variable.name);

    return (
      <Box
        key={variable.name}
        sx={{
          display: 'flex',
          gap: 1,
          mb: 1.5,
          alignItems: 'flex-start',
          p: 1,
          borderRadius: 1,
          bgcolor: errors[variable.name] ? 'error.lighter' : 'transparent',
          '&:hover': { bgcolor: 'action.hover' }
        }}
      >
        <TextField
          label={variable.required ? "Variable Name *" : "Variable Name"}
          value={variable.name}
          onChange={(e) => handleVariableChange(index, 'name', e.target.value)}
          size="small"
          sx={{ flex: 1, minWidth: 120 }}
          error={!!errors[variable.name]}
          disabled={isDetected}
        />
        <TextField
          label={variable.required ? "Value *" : "Value"}
          value={variable.value}
          onChange={(e) => handleVariableChange(index, 'value', e.target.value)}
          size="small"
          sx={{ flex: 2, minWidth: 200 }}
          error={!!errors[variable.name]}
          helperText={errors[variable.name]}
          placeholder={variable.required ? "Required" : "Optional"}
        />
        <Tooltip title={variable.required ? "Mark as optional" : "Mark as required"}>
          <FormControlLabel
            control={
              <Switch
                checked={variable.required}
                onChange={() => handleToggleRequired(variable.name)}
                size="small"
                color="primary"
              />
            }
            label={
              <Typography variant="caption" sx={{ minWidth: 55 }}>
                {variable.required ? 'Required' : 'Optional'}
              </Typography>
            }
            sx={{ mx: 0, minWidth: 110 }}
          />
        </Tooltip>
        {!isDetected && (
          <IconButton
            onClick={() => handleRemoveVariable(variable.name)}
            size="small"
            color="error"
            sx={{ mt: 0.5 }}
          >
            <CloseIcon fontSize="small" />
          </IconButton>
        )}
      </Box>
    );
  };

  const totalVariables = variables.length;
  const hasLargeDataset = totalVariables > 20;

  return (
    <Dialog
      open={open}
      onClose={onClose}
      maxWidth="md"
      fullWidth
      PaperProps={{
        sx: { maxHeight: '90vh' }
      }}
    >
      <DialogTitle>
        <Box display="flex" alignItems="center" justifyContent="space-between">
          <Box display="flex" alignItems="center" gap={1}>
            <Typography variant="h6">Input Variables</Typography>
            {totalVariables > 0 && (
              <Chip
                label={`${totalVariables} total`}
                size="small"
                variant="outlined"
              />
            )}
          </Box>
          <IconButton onClick={onClose} size="small">
            <CloseIcon />
          </IconButton>
        </Box>
      </DialogTitle>

      <Box
        component="form"
        onSubmit={(e) => {
          e.preventDefault();
          handleConfirm();
        }}
      >
        <DialogContent sx={{ pt: 1 }}>
          {/* Summary and Search Bar */}
          <Box sx={{ mb: 2 }}>
            <Box display="flex" alignItems="center" gap={1} mb={1.5}>
              <Chip
                label={`${requiredVariables.length} Required`}
                size="small"
                color="error"
                variant={requiredVariables.length > 0 ? "filled" : "outlined"}
              />
              <Chip
                label={`${optionalVariables.length} Optional`}
                size="small"
                color="default"
                variant="outlined"
              />
              {totalVariables > 0 && (
                <Button
                  size="small"
                  onClick={handleClearAllValues}
                  startIcon={<ClearIcon />}
                  sx={{ ml: 'auto' }}
                >
                  Clear All Values
                </Button>
              )}
            </Box>

            {/* Search - show for large datasets */}
            {hasLargeDataset && (
              <TextField
                fullWidth
                size="small"
                placeholder="Search variables..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                InputProps={{
                  startAdornment: (
                    <InputAdornment position="start">
                      <SearchIcon fontSize="small" />
                    </InputAdornment>
                  ),
                  endAdornment: searchQuery && (
                    <InputAdornment position="end">
                      <IconButton size="small" onClick={() => setSearchQuery('')}>
                        <CloseIcon fontSize="small" />
                      </IconButton>
                    </InputAdornment>
                  ),
                }}
                sx={{ mb: 1 }}
              />
            )}

            {searchQuery && (
              <Typography variant="body2" color="text.secondary" sx={{ mb: 1 }}>
                Showing {filteredRequired.length + filteredOptional.length} of {totalVariables} variables
              </Typography>
            )}
          </Box>

          {/* Required Variables Section */}
          {(filteredRequired.length > 0 || !searchQuery) && (
            <Accordion
              expanded={requiredExpanded}
              onChange={() => setRequiredExpanded(!requiredExpanded)}
              sx={{ mb: 1 }}
            >
              <AccordionSummary expandIcon={<ExpandMoreIcon />}>
                <Box display="flex" alignItems="center" gap={1}>
                  <Typography fontWeight="medium">Required Variables</Typography>
                  <Chip
                    label={filteredRequired.length}
                    size="small"
                    color="error"
                  />
                </Box>
              </AccordionSummary>
              <AccordionDetails sx={{ pt: 0 }}>
                {filteredRequired.length === 0 ? (
                  <Typography variant="body2" color="text.secondary" sx={{ py: 1 }}>
                    No required variables. Toggle the switch to mark variables as required.
                  </Typography>
                ) : (
                  <>
                    {displayedRequired.map(variable =>
                      renderVariableRow(variable, detectedVariables.includes(variable.name))
                    )}
                    {filteredRequired.length > ITEMS_PER_SECTION && !showAllRequired && (
                      <Button
                        size="small"
                        onClick={() => setShowAllRequired(true)}
                        sx={{ mt: 1 }}
                      >
                        Show all ({filteredRequired.length - ITEMS_PER_SECTION} more)
                      </Button>
                    )}
                    {showAllRequired && filteredRequired.length > ITEMS_PER_SECTION && (
                      <Button
                        size="small"
                        onClick={() => setShowAllRequired(false)}
                        sx={{ mt: 1 }}
                      >
                        Show less
                      </Button>
                    )}
                  </>
                )}
              </AccordionDetails>
            </Accordion>
          )}

          {/* Optional Variables Section */}
          {(filteredOptional.length > 0 || !searchQuery) && (
            <Accordion
              expanded={optionalExpanded}
              onChange={() => setOptionalExpanded(!optionalExpanded)}
              sx={{ mb: 2 }}
            >
              <AccordionSummary expandIcon={<ExpandMoreIcon />}>
                <Box display="flex" alignItems="center" gap={1}>
                  <Typography fontWeight="medium">Optional Variables</Typography>
                  <Chip
                    label={filteredOptional.length}
                    size="small"
                    variant="outlined"
                  />
                </Box>
              </AccordionSummary>
              <AccordionDetails sx={{ pt: 0 }}>
                {filteredOptional.length === 0 ? (
                  <Typography variant="body2" color="text.secondary" sx={{ py: 1 }}>
                    No optional variables. Add custom variables or toggle required variables to optional.
                  </Typography>
                ) : (
                  <>
                    {displayedOptional.map(variable =>
                      renderVariableRow(variable, detectedVariables.includes(variable.name))
                    )}
                    {filteredOptional.length > ITEMS_PER_SECTION && !showAllOptional && (
                      <Button
                        size="small"
                        onClick={() => setShowAllOptional(true)}
                        sx={{ mt: 1 }}
                      >
                        Show all ({filteredOptional.length - ITEMS_PER_SECTION} more)
                      </Button>
                    )}
                    {showAllOptional && filteredOptional.length > ITEMS_PER_SECTION && (
                      <Button
                        size="small"
                        onClick={() => setShowAllOptional(false)}
                        sx={{ mt: 1 }}
                      >
                        Show less
                      </Button>
                    )}
                  </>
                )}
              </AccordionDetails>
            </Accordion>
          )}

          <Button
            startIcon={<AddIcon />}
            onClick={handleAddVariable}
            variant="outlined"
            size="small"
          >
            Add Custom Variable
          </Button>

          <Alert severity="info" sx={{ mt: 2 }}>
            <Typography variant="body2">
              <strong>How to use variables:</strong>
            </Typography>
            <Typography variant="body2" component="ul" sx={{ mt: 0.5, pl: 2, mb: 0 }}>
              <li>Use {'{variable_name}'} syntax in agent roles, goals, backstories, and task descriptions</li>
              <li><strong>Required</strong> variables must have a value before execution</li>
              <li><strong>Optional</strong> variables can be left empty</li>
            </Typography>
          </Alert>
        </DialogContent>
        <DialogActions>
          <Button onClick={onClose}>Cancel</Button>
          <Button type="submit" variant="contained" color="primary" autoFocus>
            Execute with Variables
          </Button>
        </DialogActions>
      </Box>
    </Dialog>
  );
};
