import React, { useState, useEffect } from 'react';
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
} from '@mui/material';
import { Add as AddIcon, Close as CloseIcon } from '@mui/icons-material';
import { Node } from 'reactflow';

interface InputVariable {
  name: string;
  value: string;
  description?: string;
}

interface InputVariablesDialogProps {
  open: boolean;
  onClose: () => void;
  onConfirm: (inputs: Record<string, string>) => void;
  nodes: Node[];
}

export const InputVariablesDialog: React.FC<InputVariablesDialogProps> = ({
  open,
  onClose,
  onConfirm,
  nodes
}) => {
  const [variables, setVariables] = useState<InputVariable[]>([]);
  const [detectedVariables, setDetectedVariables] = useState<string[]>([]);
  const [errors, setErrors] = useState<Record<string, string>>({});

  // Extract variables from agent and task nodes
  useEffect(() => {
    if (!open) return;

    const variablePattern = /\{([^}]+)\}/g;
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
      setVariables(detectedVars.map(name => ({ name, value: '' })));
    }
  }, [open, nodes, variables.length]);

  const handleVariableChange = (index: number, field: 'name' | 'value', newValue: string) => {
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

  const handleAddVariable = () => {
    setVariables([...variables, { name: '', value: '' }]);
  };

  const handleRemoveVariable = (index: number) => {
    const updatedVariables = variables.filter((_, i) => i !== index);
    setVariables(updatedVariables);
  };

  const handleConfirm = () => {
    // Validate that all detected variables have values
    const newErrors: Record<string, string> = {};
    let hasErrors = false;

    detectedVariables.forEach(varName => {
      const variable = variables.find(v => v.name === varName);
      if (!variable || !variable.value) {
        newErrors[varName] = 'This variable is required';
        hasErrors = true;
      }
    });

    if (hasErrors) {
      setErrors(newErrors);
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

  return (
    <Dialog open={open} onClose={onClose} maxWidth="md" fullWidth>
      <DialogTitle>
        <Box display="flex" alignItems="center" justifyContent="space-between">
          <Typography variant="h6">Input Variables</Typography>
          <IconButton onClick={onClose} size="small">
            <CloseIcon />
          </IconButton>
        </Box>
      </DialogTitle>
      <DialogContent>
        {detectedVariables.length > 0 && (
          <Alert severity="info" sx={{ mb: 2 }}>
            <Box display="flex" alignItems="center" gap={1}>
              <Typography variant="body2">
                Detected variables in your workflow:
              </Typography>
              {detectedVariables.map(varName => (
                <Chip key={varName} label={`{${varName}}`} size="small" color="primary" />
              ))}
            </Box>
          </Alert>
        )}

        <Box sx={{ mt: 2 }}>
          <Typography variant="subtitle2" gutterBottom>
            Define values for variables used in your agents and tasks:
          </Typography>
          
          {variables.map((variable, index) => (
            <Box key={index} sx={{ display: 'flex', gap: 1, mb: 2, alignItems: 'flex-start' }}>
              <TextField
                label="Variable Name"
                value={variable.name}
                onChange={(e) => handleVariableChange(index, 'name', e.target.value)}
                size="small"
                sx={{ flex: 1 }}
                error={!!errors[variable.name]}
                disabled={detectedVariables.includes(variable.name)}
              />
              <TextField
                label="Value"
                value={variable.value}
                onChange={(e) => handleVariableChange(index, 'value', e.target.value)}
                size="small"
                sx={{ flex: 2 }}
                error={!!errors[variable.name]}
                helperText={errors[variable.name]}
                required={detectedVariables.includes(variable.name)}
              />
              {!detectedVariables.includes(variable.name) && (
                <IconButton
                  onClick={() => handleRemoveVariable(index)}
                  size="small"
                  color="error"
                >
                  <CloseIcon />
                </IconButton>
              )}
            </Box>
          ))}

          <Button
            startIcon={<AddIcon />}
            onClick={handleAddVariable}
            variant="outlined"
            size="small"
            sx={{ mt: 1 }}
          >
            Add Custom Variable
          </Button>
        </Box>

        <Alert severity="info" sx={{ mt: 3 }}>
          <Typography variant="body2">
            <strong>How to use variables:</strong>
          </Typography>
          <Typography variant="body2" component="ul" sx={{ mt: 1, pl: 2 }}>
            <li>Use {'{variable_name}'} syntax in agent roles, goals, backstories, and task descriptions</li>
            <li>Variables will be replaced with the values you provide here during execution</li>
            <li>Example: {'"Analyze {topic} trends in {industry}"'} → {'"Analyze AI trends in Healthcare"'}</li>
          </Typography>
        </Alert>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Cancel</Button>
        <Button onClick={handleConfirm} variant="contained" color="primary">
          Execute with Variables
        </Button>
      </DialogActions>
    </Dialog>
  );
};