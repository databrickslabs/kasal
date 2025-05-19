import React, { useState } from 'react';
import {
  Box,
  Button,
  Card,
  CardContent,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  TextField,
  Typography,
  Divider,
  Chip,
  SelectChangeEvent,
  DialogActions,
  FormHelperText,
} from '@mui/material';
import CompareArrowsIcon from '@mui/icons-material/CompareArrows';
import CallSplitIcon from '@mui/icons-material/CallSplit';
import RouterIcon from '@mui/icons-material/Router';
import { FlowFormData } from '../../types/flow';

interface ConditionEditFormProps {
  initialData: {
    conditionId?: string;
    label: string;
    conditionType: 'and' | 'or' | 'router';
    routerCondition?: string;
    inputs?: string[];
    crewRef?: string;
    taskRef?: string;
  };
  availableNodes: { id: string; label: string }[];
  onCancel: () => void;
  onSubmit: (data: FlowFormData) => void;
}

const ConditionEditForm: React.FC<ConditionEditFormProps> = ({
  initialData,
  availableNodes,
  onCancel,
  onSubmit
}) => {
  const [formData, setFormData] = useState({
    label: initialData.label || '',
    conditionType: initialData.conditionType || 'and',
    routerCondition: initialData.routerCondition || '',
    inputs: initialData.inputs || [],
    crewRef: initialData.crewRef || '',
    taskRef: initialData.taskRef || ''
  });

  const handleChange = (field: string, value: string | string[]) => {
    setFormData(prev => ({
      ...prev,
      [field]: value
    }));
  };

  const handleInputsChange = (event: SelectChangeEvent<string[]>) => {
    const selectedInputs = Array.isArray(event.target.value) 
      ? event.target.value 
      : [event.target.value];
    
    handleChange('inputs', selectedInputs);
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    
    // Convert to the format needed by the parent component
    const result: FlowFormData = {
      name: formData.label,
      crewName: '',  // Not used but required by interface
      conditionType: formData.conditionType,
      routerCondition: formData.routerCondition,
      listenTo: formData.inputs,
      crewRef: formData.crewRef,
      taskRef: formData.taskRef,
      // Set the type based on the condition type
      type: formData.conditionType === 'router' ? 'router' : (
        formData.conditionType === 'and' || formData.conditionType === 'or' ? 'listen' : 'normal'
      ),
      // Add condition data for the parent component
      conditionData: {
        conditionType: formData.conditionType,
        targetNodes: formData.inputs,
        routerCondition: formData.routerCondition
      }
    };
    
    onSubmit(result);
  };

  // Icons and descriptions for condition types
  const getConditionIcon = (type: string) => {
    switch (type) {
      case 'router':
        return <RouterIcon sx={{ fontSize: '1.5rem', color: 'warning.main' }} />;
      case 'or':
        return <CallSplitIcon sx={{ fontSize: '1.5rem', color: 'secondary.main' }} />;
      case 'and':
      default:
        return <CompareArrowsIcon sx={{ fontSize: '1.5rem', color: 'primary.main' }} />;
    }
  };

  const getConditionDescription = (type: string) => {
    switch (type) {
      case 'and':
        return 'Executes when ALL input conditions are met';
      case 'or':
        return 'Executes when ANY input condition is met';
      case 'router':
        return 'Routes flow based on condition evaluation';
      default:
        return '';
    }
  };

  return (
    <Box component="form" onSubmit={handleSubmit}>
      <Card>
        <CardContent>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
            <Typography variant="h6" gutterBottom>
              Condition Properties
            </Typography>
            
            <TextField
              fullWidth
              label="Condition Name"
              value={formData.label}
              onChange={(e) => handleChange('label', e.target.value)}
              required
            />
            
            <FormControl fullWidth>
              <InputLabel id="condition-type-label">Condition Type</InputLabel>
              <Select
                labelId="condition-type-label"
                value={formData.conditionType}
                label="Condition Type"
                onChange={(e) => handleChange('conditionType', e.target.value)}
              >
                <MenuItem value="and" sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                  {getConditionIcon('and')} AND
                </MenuItem>
                <MenuItem value="or" sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                  {getConditionIcon('or')} OR
                </MenuItem>
                <MenuItem value="router" sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                  {getConditionIcon('router')} ROUTER
                </MenuItem>
              </Select>
              <FormHelperText>
                {getConditionDescription(formData.conditionType)}
              </FormHelperText>
            </FormControl>
            
            <Divider />
            
            {['and', 'or'].includes(formData.conditionType) && (
              <FormControl fullWidth>
                <InputLabel id="inputs-label">Input Nodes</InputLabel>
                <Select
                  labelId="inputs-label"
                  multiple
                  value={formData.inputs}
                  label="Input Nodes"
                  onChange={handleInputsChange}
                  renderValue={(selected) => (
                    <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
                      {(selected as string[]).map((value) => (
                        <Chip key={value} label={value} size="small" />
                      ))}
                    </Box>
                  )}
                >
                  {availableNodes.map(node => (
                    <MenuItem key={node.id} value={node.id}>{node.label}</MenuItem>
                  ))}
                </Select>
                <FormHelperText>
                  {formData.conditionType === 'and'
                    ? 'Select nodes that ALL must complete before continuing'
                    : 'Select nodes where ANY completion will continue the flow'}
                </FormHelperText>
              </FormControl>
            )}
            
            {formData.conditionType === 'router' && (
              <TextField
                fullWidth
                label="Router Condition"
                value={formData.routerCondition}
                onChange={(e) => handleChange('routerCondition', e.target.value)}
                placeholder="Define the routing condition or select a predefined option"
                select
              >
                <MenuItem value="success">Success</MenuItem>
                <MenuItem value="failure">Failure</MenuItem>
                <MenuItem value="custom">Custom (defined in code)</MenuItem>
              </TextField>
            )}
          </Box>
        </CardContent>
      </Card>
      
      <DialogActions sx={{ mt: 2 }}>
        <Button onClick={onCancel}>
          Cancel
        </Button>
        <Button type="submit" variant="contained" color="primary">
          Save Condition
        </Button>
      </DialogActions>
    </Box>
  );
};

export default ConditionEditForm; 