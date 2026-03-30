import React, { useState } from 'react';
import { 
  Box, 
  Button, 
  FormControl, 
  Grid,
  InputLabel,
  MenuItem,
  Select,
  TextField, 
  Typography
} from '@mui/material';
import { FlowEdgeFormData } from '../../types/flow';

interface EdgeStateFormProps {
  initialData?: {
    stateType?: 'structured' | 'unstructured';
    stateDefinition?: string;
    stateData?: Record<string, unknown>;
  };
  onCancel?: () => void;
  onSubmit?: (stateData: FlowEdgeFormData) => void;
}

const EdgeStateForm: React.FC<EdgeStateFormProps> = ({
  initialData,
  onCancel,
  onSubmit
}) => {
  const [stateType, setStateType] = useState<FlowEdgeFormData['stateType']>(initialData?.stateType || 'unstructured');
  const [stateDefinition, setStateDefinition] = useState(initialData?.stateDefinition || '');
  const [stateData, setStateData] = useState<string>(
    initialData?.stateData ? JSON.stringify(initialData.stateData, null, 2) : '{}'
  );

  const handleStateTypeChange = (newType: FlowEdgeFormData['stateType']) => {
    setStateType(newType);
    if (onSubmit) {
      onSubmit({
        stateType: newType,
        stateDefinition,
        stateData: tryParseJSON(stateData) || {}
      });
    }
  };

  const handleStateDefinitionChange = (newDefinition: string) => {
    setStateDefinition(newDefinition);
    if (onSubmit) {
      onSubmit({
        stateType,
        stateDefinition: newDefinition,
        stateData: tryParseJSON(stateData) || {}
      });
    }
  };

  const handleStateDataChange = (newData: string) => {
    setStateData(newData);
    if (onSubmit) {
      onSubmit({
        stateType,
        stateDefinition,
        stateData: tryParseJSON(newData) || {}
      });
    }
  };

  const tryParseJSON = (jsonString: string): Record<string, unknown> | null => {
    try {
      return JSON.parse(jsonString);
    } catch (e) {
      return null;
    }
  };

  return (
    <Box component="form" onSubmit={(e) => {
      e.preventDefault();
      if (onSubmit) {
        onSubmit({
          stateType,
          stateDefinition,
          stateData: tryParseJSON(stateData) || {}
        });
      }
    }} sx={{ mt: 2 }}>
      <Typography variant="h6" gutterBottom>
        Edge State Configuration
      </Typography>
      <Typography variant="body2" sx={{ mb: 3 }}>
        Configure how state is managed and passed along this edge.
      </Typography>

      <FormControl fullWidth sx={{ mb: 3 }}>
        <InputLabel id="state-type-label">State Type</InputLabel>
        <Select
          labelId="state-type-label"
          value={stateType}
          onChange={(e) => handleStateTypeChange(e.target.value as FlowEdgeFormData['stateType'])}
          label="State Type"
        >
          <MenuItem value="unstructured">
            <Box>
              <Typography variant="body2">Unstructured</Typography>
              <Typography variant="caption" color="text.secondary">
                Free-form state handling with no schema validation
              </Typography>
            </Box>
          </MenuItem>
          <MenuItem value="structured">
            <Box>
              <Typography variant="body2">Structured</Typography>
              <Typography variant="caption" color="text.secondary">
                Schema-based state handling with validation
              </Typography>
            </Box>
          </MenuItem>
        </Select>
      </FormControl>

      {stateType === 'structured' && (
        <Box sx={{ mb: 3 }}>
          <Typography variant="subtitle2" sx={{ mb: 1 }}>
            State Definition
          </Typography>
          <TextField
            fullWidth
            multiline
            rows={5}
            value={stateDefinition}
            onChange={(e) => handleStateDefinitionChange(e.target.value)}
            placeholder="class ExampleState(BaseModel):
    counter: int = 0
    message: str = ''"
            sx={{ '& .MuiInputBase-input': { fontFamily: 'monospace' } }}
          />
        </Box>
      )}

      <Box sx={{ mb: 3 }}>
        <Typography variant="subtitle2" sx={{ mb: 1 }}>
          Initial State Data
        </Typography>
        <TextField
          fullWidth
          multiline
          rows={5}
          value={stateData}
          onChange={(e) => handleStateDataChange(e.target.value)}
          error={!tryParseJSON(stateData)}
          helperText={tryParseJSON(stateData) ? 'Valid JSON' : 'Invalid JSON'}
          placeholder={`{
  "key": "value"
}`}
          sx={{ '& .MuiInputBase-input': { fontFamily: 'monospace' } }}
        />
      </Box>

      <Grid container spacing={2} justifyContent="flex-end">
        <Grid item>
          <Button onClick={onCancel} variant="outlined">
            Cancel
          </Button>
        </Grid>
        <Grid item>
          <Button type="submit" variant="contained" color="primary">
            Save
          </Button>
        </Grid>
      </Grid>
    </Box>
  );
};

export { EdgeStateForm }; 