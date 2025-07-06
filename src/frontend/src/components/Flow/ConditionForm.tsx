import React, { useState } from 'react';
import {
  Box,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Typography,
  Chip,
  SelectChangeEvent,
  FormHelperText,
  Card,
  CardContent,
  Divider,
  Link,
} from '@mui/material';

export interface ConditionFormData {
  conditionType?: 'none' | 'and' | 'or' | 'router';
  targetNodes?: string[];
  routerCondition?: string;
}

interface ConditionFormProps {
  availableNodes?: { id: string; label: string }[];
  initialData?: ConditionFormData;
  onChange?: (data: ConditionFormData) => void;
}

const ConditionForm: React.FC<ConditionFormProps> = ({
  availableNodes = [],
  initialData = {
    conditionType: 'none',
    targetNodes: [],
    routerCondition: '',
  },
  onChange
}) => {
  const [formData, setFormData] = useState<ConditionFormData>(initialData);

  const handleConditionTypeChange = (value: 'none' | 'and' | 'or' | 'router') => {
    const newData = {
      ...formData,
      conditionType: value,
      // Reset other values if condition type changes
      ...(value === 'none' && { targetNodes: [], routerCondition: '' }),
      ...(value === 'router' && { targetNodes: [] }),
      ...((['and', 'or'].includes(value)) && { routerCondition: '' }),
    };
    
    setFormData(newData);
    if (onChange) onChange(newData);
  };

  const handleNodesChange = (event: SelectChangeEvent<string[]>) => {
    const selectedNodes = Array.isArray(event.target.value) 
      ? event.target.value 
      : [event.target.value];
    
    const newData = {
      ...formData,
      targetNodes: selectedNodes
    };
    
    setFormData(newData);
    if (onChange) onChange(newData);
  };

  const handleRouterConditionChange = (value: string) => {
    const newData = {
      ...formData,
      routerCondition: value
    };
    
    setFormData(newData);
    if (onChange) onChange(newData);
  };

  const getConditionTypeHelp = (type: string) => {
    switch(type) {
      case 'and':
        return (
          <>
            <Typography variant="body2" sx={{ mt: 1 }}>
              The <code>and_</code> function in Flows allows you to listen to multiple methods and trigger the listener method 
              only when <strong>all</strong> the specified methods emit an output.
            </Typography>
            <Typography variant="body2" sx={{ mt: 1, fontStyle: 'italic' }}>
              Example: <code>@listen(and_(start_method, validate_data))</code>
            </Typography>
          </>
        );
      case 'or':
        return (
          <>
            <Typography variant="body2" sx={{ mt: 1 }}>
              The <code>or_</code> function in Flows allows you to listen to multiple methods and trigger the listener method 
              when <strong>any</strong> of the specified methods emit an output.
            </Typography>
            <Typography variant="body2" sx={{ mt: 1, fontStyle: 'italic' }}>
              Example: <code>@listen(or_(check_email, check_sms))</code>
            </Typography>
          </>
        );
      case 'router':
        return (
          <>
            <Typography variant="body2" sx={{ mt: 1 }}>
              The <code>@router()</code> decorator allows conditional routing based on the output of a method. 
              Routes can be defined based on return values like &quot;success&quot; or &quot;failure&quot;.
            </Typography>
            <Typography variant="body2" sx={{ mt: 1, fontStyle: 'italic' }}>
              Example: 
              <pre style={{ 
                backgroundColor: '#f5f5f5', 
                padding: '8px', 
                borderRadius: '4px',
                fontSize: '0.8rem',
                overflow: 'auto' 
              }}>
{`@router(start_method)
def route_method(self):
    if self.state.success:
        return "success"
    else:
        return "failure"

@listen("success")
def success_handler(self):
    # Handle success case

@listen("failure") 
def failure_handler(self):
    # Handle failure case`}
              </pre>
            </Typography>
          </>
        );
      default:
        return null;
    }
  };

  return (
    <Card>
      <CardContent>
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Typography variant="subtitle1">Flow Conditions</Typography>
          <Link 
            href="https://docs.crewai.com/concepts/flows" 
            target="_blank" 
            rel="noopener"
            sx={{ fontSize: '0.8rem' }}
          >
            View crewAI Docs
          </Link>
        </Box>
        <Divider sx={{ my: 2 }} />
        
        <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
          <FormControl fullWidth>
            <InputLabel id="condition-type-label">Condition Type</InputLabel>
            <Select
              labelId="condition-type-label"
              value={formData.conditionType || 'none'}
              label="Condition Type"
              onChange={(e) => handleConditionTypeChange(e.target.value as 'none' | 'and' | 'or' | 'router')}
              inputProps={{
                'aria-label': 'Condition Type',
                'title': '' // Ensure no tooltip is shown on hover
              }}
            >
              <MenuItem value="none">None</MenuItem>
              <MenuItem value="and">AND (all conditions must be true)</MenuItem>
              <MenuItem value="or">OR (any condition can be true)</MenuItem>
              <MenuItem value="router">Router (conditional routing)</MenuItem>
            </Select>
            {getConditionTypeHelp(formData.conditionType || '')}
          </FormControl>

          {['and', 'or'].includes(formData.conditionType || '') && (
            <FormControl fullWidth>
              <InputLabel id="target-nodes-label">Target Nodes</InputLabel>
              <Select
                labelId="target-nodes-label"
                multiple
                value={formData.targetNodes || []}
                label="Target Nodes"
                onChange={handleNodesChange}
                inputProps={{
                  'aria-label': 'Target Nodes',
                  'title': '' // Ensure no tooltip is shown on hover
                }}
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
                  ? 'Select the nodes that ALL must be completed before continuing' 
                  : 'Select the nodes where ANY completion will continue the flow'}
              </FormHelperText>
            </FormControl>
          )}

          {formData.conditionType === 'router' && (
            <FormControl fullWidth>
              <InputLabel id="router-condition-label">Router Condition</InputLabel>
              <Select
                labelId="router-condition-label"
                value={formData.routerCondition || ''}
                label="Router Condition"
                onChange={(e) => handleRouterConditionChange(e.target.value)}
                inputProps={{
                  'aria-label': 'Router Condition',
                  'title': '' // Ensure no tooltip is shown on hover
                }}
              >
                <MenuItem value="success">Success</MenuItem>
                <MenuItem value="failure">Failure</MenuItem>
                <MenuItem value="custom">Custom (defined in code)</MenuItem>
              </Select>
              <FormHelperText>
                Define the routing condition that will determine the flow path
              </FormHelperText>
            </FormControl>
          )}
        </Box>
      </CardContent>
    </Card>
  );
};

export default ConditionForm; 