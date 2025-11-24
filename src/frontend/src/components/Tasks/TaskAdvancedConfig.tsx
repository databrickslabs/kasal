/* eslint-disable react/prop-types, react/display-name */
import React, { useState, useCallback, memo, useEffect } from 'react';
import {
  Box,
  TextField,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  FormControlLabel,
  Switch,
  Typography,
  Divider,
  Tooltip,
  IconButton,
  CircularProgress,
} from '@mui/material';
import { type TaskAdvancedConfigProps, type LLMGuardrailConfig } from '../../types/task';
import { TASK_CALLBACKS, type TaskCallbackOption } from '../../types/taskCallbacks';
import HelpOutlineIcon from '@mui/icons-material/HelpOutline';
import { SchemaService } from '../../api/SchemaService';
import { ModelService } from '../../api/ModelService';
import { type ModelConfig } from '../../types/models';
import { DatabricksVolumeConfigComponent, type DatabricksVolumeConfig } from './DatabricksVolumeConfig';

export type ConditionType = 'is_data_missing';

// Create the memoized component
const TaskAdvancedConfigComponent: React.FC<TaskAdvancedConfigProps> = ({
  advancedConfig,
  onConfigChange,
  availableTasks: _availableTasks,
}) => {
  const [selectedCallback, setSelectedCallback] = useState<TaskCallbackOption | null>(
    advancedConfig.callback 
      ? TASK_CALLBACKS.find(cb => cb.value === advancedConfig.callback) || null 
      : null
  );
  
  const [pydanticModels, setPydanticModels] = useState<Array<{ value: string, label: string }>>([]);
  const [databricksConfig, setDatabricksConfig] = useState<DatabricksVolumeConfig | null>(null);

  // LLM Guardrail state
  const [llmModels, setLlmModels] = useState<Array<{ name: string; config: ModelConfig }>>([]);
  const [llmModelsLoading, setLlmModelsLoading] = useState(false);
  
  // Fetch Pydantic models from SchemaService
  useEffect(() => {
    const fetchPydanticModels = async () => {
      try {
        const schemaService = SchemaService.getInstance();
        const schemas = await schemaService.getSchemas();
        
        // Map schemas to the format needed for the dropdown
        const models = schemas.map(schema => ({
          value: schema.name,
          label: schema.name
        }));
        
        setPydanticModels(models);
      } catch (error) {
        console.error('Error fetching Pydantic models:', error);
      }
    };
    
    void fetchPydanticModels();
  }, []);

  // Fetch LLM models for guardrail validation
  useEffect(() => {
    const fetchLlmModels = async () => {
      setLlmModelsLoading(true);
      try {
        const modelService = ModelService.getInstance();
        const modelsDict = await modelService.getModels();
        // Convert Models dictionary to array format for dropdown
        const modelsArray = Object.entries(modelsDict).map(([name, config]) => ({
          name,
          config
        }));
        setLlmModels(modelsArray);
      } catch (error) {
        console.error('Error fetching LLM models:', error);
      } finally {
        setLlmModelsLoading(false);
      }
    };

    void fetchLlmModels();
  }, []);

  const handleConditionChange = useCallback((field: string, value: string | number | boolean | null) => {
    if (field === 'type') {
      onConfigChange('condition', value === '' ? null : 'is_data_missing');
      return;
    }
  }, [onConfigChange]);

  const handleCallbackChange = useCallback((value: string) => {
    const callback = TASK_CALLBACKS.find(cb => cb.value === value);
    setSelectedCallback(callback || null);
    onConfigChange('callback', value || null);
    
    // Clear output file if new callback doesn't require it
    if (!callback?.requiresPath) {
      onConfigChange('output_file', null);
    }
    
    // Initialize Databricks config if DatabricksVolumeCallback is selected
    if (value === 'DatabricksVolumeCallback') {
      // Only set default config if we don't already have one
      if (!databricksConfig) {
        const defaultConfig: DatabricksVolumeConfig = {
          volume_path: 'main.default.task_outputs',  // Using new format
          file_format: 'json',
          create_date_dirs: true,
          max_file_size_mb: 50.0  // 50MB is sufficient for model outputs
        };
        setDatabricksConfig(defaultConfig);
        onConfigChange('callback_config', {...defaultConfig});
      }
    } else {
      setDatabricksConfig(null);
      onConfigChange('callback_config', null);
    }
  }, [onConfigChange, databricksConfig]);

  // Initialize DatabricksVolumeConfig when component mounts or advancedConfig changes
  useEffect(() => {
    console.log('TaskAdvancedConfig useEffect triggered');
    console.log('  - callback:', advancedConfig.callback);
    console.log('  - callback_config:', JSON.stringify(advancedConfig.callback_config, null, 2));
    console.log('  - current databricksConfig state:', databricksConfig);
    
    if (advancedConfig.callback === 'DatabricksVolumeCallback') {
      if (advancedConfig.callback_config) {
        // Restore the databricks configuration from saved callback_config
        const savedConfig = advancedConfig.callback_config as unknown as DatabricksVolumeConfig;
        console.log('Restoring saved databricksConfig:', savedConfig);
        console.log('  - volume_path from saved:', savedConfig.volume_path);
        
        // Create a properly typed config object
        const restoredConfig: DatabricksVolumeConfig = {
          volume_path: savedConfig.volume_path || 'main.default.task_outputs',
          file_format: (savedConfig.file_format || 'json') as 'json' | 'csv' | 'txt',
          create_date_dirs: savedConfig.create_date_dirs !== undefined ? savedConfig.create_date_dirs : true,
          max_file_size_mb: savedConfig.max_file_size_mb || 50.0,
          workspace_url: savedConfig.workspace_url,
          token: savedConfig.token
        };
        
        console.log('Setting databricksConfig to restored config:', restoredConfig);
        setDatabricksConfig(restoredConfig);
      } else if (!databricksConfig) {
        // No saved config but callback is selected, set default
        console.log('No saved config, setting default databricksConfig');
        const defaultConfig: DatabricksVolumeConfig = {
          volume_path: 'main.default.task_outputs',
          file_format: 'json',
          create_date_dirs: true,
          max_file_size_mb: 50.0
        };
        setDatabricksConfig(defaultConfig);
      }
      
      // Ensure the selected callback is set
      const callback = TASK_CALLBACKS.find(cb => cb.value === 'DatabricksVolumeCallback');
      if (callback && !selectedCallback) {
        console.log('Setting selectedCallback to DatabricksVolumeCallback');
        setSelectedCallback(callback);
      }
    } else {
      // Different callback selected, clear databricks config
      if (databricksConfig) {
        console.log('Clearing databricksConfig - different callback selected');
        setDatabricksConfig(null);
      }
    }
  }, [advancedConfig.callback, advancedConfig.callback_config, databricksConfig, selectedCallback]);

  // Handle guardrail type changes
  const handleGuardrailTypeChange = useCallback((value: string) => {
    if (value === '') {
      // Clear the guardrail if "None" is selected
      onConfigChange('guardrail', null);
    } else {
      // Set default values based on the guardrail type
      if (value === 'company_count') {
        // For company_count guardrail, set the type and default minimum companies
        const guardrailConfig = {
          type: 'company_count',
          min_companies: 50
        };
        // Use a string field to store the config as JSON
        onConfigChange('guardrail', JSON.stringify(guardrailConfig) as unknown as string);
      } else if (value === 'data_processing') {
        // For data_processing guardrail, set the type without a specific che_number
        const guardrailConfig = {
          type: 'data_processing'
        };
        // Use a string field to store the config as JSON
        onConfigChange('guardrail', JSON.stringify(guardrailConfig) as unknown as string);
      } else if (value === 'empty_data_processing') {
        // For empty_data_processing guardrail, set the type
        const guardrailConfig = {
          type: 'empty_data_processing'
        };
        // Use a string field to store the config as JSON
        onConfigChange('guardrail', JSON.stringify(guardrailConfig) as unknown as string);
      } else if (value === 'data_processing_count') {
        // For data_processing_count guardrail, set the type and default minimum count
        const guardrailConfig = {
          type: 'data_processing_count',
          minimum_count: 0
        };
        // Use a string field to store the config as JSON
        onConfigChange('guardrail', JSON.stringify(guardrailConfig) as unknown as string);
      } else if (value === 'company_name_not_null') {
        // For company_name_not_null guardrail, set the type
        const guardrailConfig = {
          type: 'company_name_not_null'
        };
        // Use a string field to store the config as JSON
        onConfigChange('guardrail', JSON.stringify(guardrailConfig) as unknown as string);
      } else if (value === 'minimum_number') {
        // For minimum_number guardrail, set the type and default configuration
        const guardrailConfig = {
          type: 'minimum_number',
          min_value: 5,
          field_name: 'total_count'
        };
        // Use a string field to store the config as JSON
        onConfigChange('guardrail', JSON.stringify(guardrailConfig) as unknown as string);
      } else {
        // For other guardrail types, just set the type
        onConfigChange('guardrail', JSON.stringify({ type: value }) as unknown as string);
      }
    }
  }, [onConfigChange]);

  // Handle guardrail option changes
  const handleGuardrailOptionChange = useCallback((option: string, value: number | string | boolean) => {
    try {
      // Parse the current guardrail config
      const currentGuardrail = advancedConfig.guardrail ? 
        JSON.parse(advancedConfig.guardrail as string) : 
        { type: 'company_count' };
      
      // Update the option
      currentGuardrail[option] = value;
      
      // Stringify the updated config and cast to appropriate type
      onConfigChange('guardrail', JSON.stringify(currentGuardrail) as unknown as string);
    } catch (error) {
      console.error('Error updating guardrail option:', error);
      // In case of error, create a new config with default values
      const defaultConfig = { type: 'company_count', [option]: value };
      onConfigChange('guardrail', JSON.stringify(defaultConfig) as unknown as string);
    }
  }, [advancedConfig.guardrail, onConfigChange]);

  // Parse guardrail config from string
  const guardrailConfig = React.useMemo(() => {
    try {
      return advancedConfig.guardrail ?
        JSON.parse(advancedConfig.guardrail as string) :
        null;
    } catch (error) {
      console.error('Error parsing guardrail config:', error);
      return null;
    }
  }, [advancedConfig.guardrail]);

  // Handle LLM guardrail toggle
  const handleLlmGuardrailToggle = useCallback((enabled: boolean) => {
    if (enabled) {
      const defaultConfig: LLMGuardrailConfig = {
        description: 'Validate the task output for accuracy and completeness',
        llm_model: 'databricks-claude-sonnet-4-5'
      };
      onConfigChange('llm_guardrail', defaultConfig as unknown as Record<string, unknown>);
    } else {
      onConfigChange('llm_guardrail', null);
    }
  }, [onConfigChange]);

  // Handle LLM guardrail field changes
  const handleLlmGuardrailChange = useCallback((field: keyof LLMGuardrailConfig, value: string) => {
    const currentConfig = advancedConfig.llm_guardrail || {
      description: 'Validate the task output for accuracy and completeness',
      llm_model: 'databricks-claude-sonnet-4-5'
    };
    const updatedConfig = {
      ...currentConfig,
      [field]: value
    };
    onConfigChange('llm_guardrail', updatedConfig as unknown as Record<string, unknown>);
  }, [advancedConfig.llm_guardrail, onConfigChange]);

  // Automatically enable retry on failure when data_processing guardrail is selected
  useEffect(() => {
    if ((guardrailConfig?.type === 'data_processing' || 
         guardrailConfig?.type === 'empty_data_processing' ||
         guardrailConfig?.type === 'company_name_not_null') && 
        !advancedConfig.retry_on_fail) {
      // Enable retry on failure and set max retries to 5 by default for data processing
      onConfigChange('retry_on_fail', true);
      onConfigChange('max_retries', 5);
    }
  }, [guardrailConfig?.type, advancedConfig.retry_on_fail, onConfigChange]);

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
      <Typography variant="subtitle2" color="text.secondary">Execution Settings</Typography>
      <FormControlLabel
        control={
          <Switch
            checked={Boolean(advancedConfig.async_execution)}
            onChange={(e) => onConfigChange('async_execution', e.target.checked)}
          />
        }
        label="Async Execution"
      />
      <FormControlLabel
        control={
          <Switch
            checked={advancedConfig.human_input}
            onChange={(e) => onConfigChange('human_input', e.target.checked)}
          />
        }
        label="Require Human Input"
      />
      <TextField
        type="number"
        label="Priority"
        value={advancedConfig.priority}
        onChange={(e) => onConfigChange('priority', parseInt(e.target.value))}
        fullWidth
        inputProps={{ min: 1, max: 10 }}
      />
      <TextField
        type="number"
        label="Timeout (seconds)"
        value={advancedConfig.timeout || ''}
        onChange={(e) => onConfigChange('timeout', e.target.value ? parseInt(e.target.value) : null)}
        fullWidth
        inputProps={{ min: 0 }}
      />

      <Divider sx={{ my: 2 }} />
      <Typography variant="subtitle2" color="text.secondary">Caching & Output</Typography>
      <FormControlLabel
        control={
          <Switch
            checked={advancedConfig.cache_response}
            onChange={(e) => onConfigChange('cache_response', e.target.checked)}
          />
        }
        label="Cache Response"
      />
      <TextField
        type="number"
        label="Cache TTL (seconds)"
        value={advancedConfig.cache_ttl}
        onChange={(e) => onConfigChange('cache_ttl', parseInt(e.target.value))}
        fullWidth
        disabled={!advancedConfig.cache_response}
        inputProps={{ min: 0 }}
      />
      <TextField
        label="Output JSON Schema"
        value={advancedConfig.output_json || ''}
        onChange={(e) => onConfigChange('output_json', e.target.value || null)}
        fullWidth
        multiline
        rows={3}
        helperText="JSON schema for task output"
      />
      <TextField
        label="Output File Path"
        value={advancedConfig.output_file || ''}
        onChange={(e) => onConfigChange('output_file', e.target.value || null)}
        fullWidth
      />
      <FormControlLabel
        control={
          <Switch
            checked={advancedConfig.markdown}
            onChange={(e) => onConfigChange('markdown', e.target.checked)}
          />
        }
        label="Enable Markdown Output"
      />

      <Divider sx={{ my: 2 }} />
      <Typography variant="subtitle2" color="text.secondary">Error Handling</Typography>
      <FormControlLabel
        control={
          <Switch
            checked={advancedConfig.retry_on_fail}
            onChange={(e) => onConfigChange('retry_on_fail', e.target.checked)}
          />
        }
        label="Retry on Failure"
      />
      <TextField
        type="number"
        label="Max Retries"
        value={advancedConfig.max_retries}
        onChange={(e) => onConfigChange('max_retries', parseInt(e.target.value))}
        fullWidth
        disabled={!advancedConfig.retry_on_fail}
        inputProps={{ min: 1, max: 10 }}
      />
      <FormControl fullWidth>
        <InputLabel>Error Handling Strategy</InputLabel>
        <Select
          value={advancedConfig.error_handling}
          onChange={(e) => onConfigChange('error_handling', e.target.value)}
          label="Error Handling Strategy"
        >
          <MenuItem value="default">Default</MenuItem>
          <MenuItem value="retry">Retry</MenuItem>
          <MenuItem value="ignore">Ignore</MenuItem>
          <MenuItem value="fail">Fail Immediately</MenuItem>
        </Select>
      </FormControl>

      <Divider sx={{ my: 2 }} />
      <Typography variant="subtitle2" color="text.secondary">Advanced Functions</Typography>
      <Box sx={{ display: 'flex', gap: 2, alignItems: 'flex-start' }}>
        <FormControl fullWidth>
          <InputLabel>Callback</InputLabel>
          <Select
            value={advancedConfig.callback || ''}
            onChange={(e) => handleCallbackChange(e.target.value)}
            label="Callback Function"
          >
            <MenuItem value="">
              <em>None</em>
            </MenuItem>
            {TASK_CALLBACKS.map((callback) => (
              <MenuItem key={callback.value} value={callback.value}>
                <Box sx={{ display: 'flex', flexDirection: 'column' }}>
                  <Typography>{callback.label}</Typography>
                  <Typography variant="caption" color="text.secondary">
                    {callback.description}
                  </Typography>
                </Box>
              </MenuItem>
            ))}
          </Select>
        </FormControl>
        {selectedCallback && (
          <Tooltip title={selectedCallback.description}>
            <IconButton size="small" sx={{ mt: 1 }}>
              <HelpOutlineIcon />
            </IconButton>
          </Tooltip>
        )}
      </Box>

      {selectedCallback?.requiresPath && (
        <TextField
          label="Output File Path"
          value={advancedConfig.output_file || ''}
          onChange={(e) => onConfigChange('output_file', e.target.value || null)}
          fullWidth
          helperText="Path where the output will be saved"
        />
      )}
      
      {selectedCallback?.value === 'DatabricksVolumeCallback' && (
        <DatabricksVolumeConfigComponent
          config={databricksConfig}
          onChange={(config) => {
            setDatabricksConfig(config);
            onConfigChange('callback_config', {...config});
          }}
        />
      )}

      <TextField
        label="Output Parser"
        value={advancedConfig.output_parser || ''}
        onChange={(e) => onConfigChange('output_parser', e.target.value || null)}
        fullWidth
        helperText="Custom parser for task output"
      />
      <FormControl fullWidth>
        <InputLabel>Output Pydantic Model</InputLabel>
        <Select
          value={advancedConfig.output_pydantic ?? ''}
          onChange={(e) => {
            console.log('Pydantic model selection changed to:', e.target.value);
            const newValue = e.target.value === '' ? null : e.target.value;
            console.log('Sending value to parent component:', newValue);
            onConfigChange('output_pydantic', newValue);
          }}
          label="Output Pydantic Model"
        >
          <MenuItem value="">
            <em>None</em>
          </MenuItem>
          {pydanticModels.map((model) => (
            <MenuItem key={model.value} value={model.value}>
              {model.label}
            </MenuItem>
          ))}
        </Select>
      </FormControl>

      <Divider sx={{ my: 2 }} />
      <Typography variant="subtitle2" color="text.secondary">Condition Settings</Typography>
      
      <FormControl fullWidth>
        <InputLabel>Condition Type</InputLabel>
        <Select
          value={advancedConfig.condition || ''}
          onChange={(e) => handleConditionChange('type', e.target.value)}
          label="Condition Type"
        >
          <MenuItem value="">
            <em>None</em>
          </MenuItem>
          <MenuItem value="is_data_missing">Data Is Missing</MenuItem>
        </Select>
      </FormControl>

      <Divider sx={{ my: 2 }} />
      <Typography variant="subtitle2" color="text.secondary">Guardrail Settings</Typography>
      
      <FormControl fullWidth>
        <InputLabel>Guardrail Type</InputLabel>
        <Select
          value={guardrailConfig?.type || ''}
          onChange={(e) => handleGuardrailTypeChange(e.target.value)}
          label="Guardrail Type"
        >
          <MenuItem value="">None</MenuItem>
          <MenuItem value="company_count">Company Count</MenuItem>
          <MenuItem value="data_processing">Data Processing Status</MenuItem>
          <MenuItem value="empty_data_processing">Empty Data Processing Check</MenuItem>
          <MenuItem value="data_processing_count">Data Processing Count Check</MenuItem>
          <MenuItem value="company_name_not_null">Company Name Not Null Check</MenuItem>
          <MenuItem value="minimum_number">Minimum Number Check</MenuItem>
        </Select>
      </FormControl>

      {guardrailConfig?.type === 'company_count' && (
        <TextField
          type="number"
          label="Minimum Companies"
          value={guardrailConfig.min_companies || 50}
          onChange={(e) => handleGuardrailOptionChange('min_companies', parseInt(e.target.value))}
          fullWidth
          inputProps={{ min: 1 }}
        />
      )}

      {guardrailConfig?.type === 'data_processing_count' && (
        <Box sx={{ mt: 1 }}>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 1 }}>
            This guardrail checks if the total number of records in the data_processing table is at least the minimum count.
            The task will pass validation when the number of records equals or exceeds the minimum count.
          </Typography>
          <TextField
            type="number"
            label="Minimum Record Count"
            value={guardrailConfig.minimum_count || 0}
            onChange={(e) => handleGuardrailOptionChange('minimum_count', parseInt(e.target.value))}
            fullWidth
            inputProps={{ min: 0 }}
          />
        </Box>
      )}

      {guardrailConfig?.type === 'data_processing' && (
        <Box sx={{ mt: 1 }}>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 1 }}>
            This guardrail checks if all data records in the database have been processed. 
            The task will only pass validation when all records have processed=true.
            Enable &quot;Retry on Failure&quot; to automatically retry the task until all data has been processed.
          </Typography>
        </Box>
      )}

      {guardrailConfig?.type === 'empty_data_processing' && (
        <Box sx={{ mt: 1 }}>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 1 }}>
            This guardrail checks if the data_processing table is empty.
            The task will only pass validation when the table has NO records (is completely empty).
            Use this guardrail when you need to ensure that the table is clean before adding new data.
          </Typography>
        </Box>
      )}

      {guardrailConfig?.type === 'company_name_not_null' && (
        <Box sx={{ mt: 1 }}>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 1 }}>
            This guardrail checks if any company_name in the data_processing table is null.
            The task will only pass validation when all records have non-null company_name values.
            Enable &quot;Retry on Failure&quot; to automatically retry the task until all records have valid company names.
          </Typography>
        </Box>
      )}

      {guardrailConfig?.type === 'minimum_number' && (
        <Box sx={{ mt: 1 }}>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 1 }}>
            This guardrail validates that a number in the output exceeds a specified minimum value.
            Particularly useful for ensuring search results, data counts, or other numerical values meet minimum thresholds.
          </Typography>
          <TextField
            type="number"
            label="Minimum Value"
            value={guardrailConfig.min_value || 5}
            onChange={(e) => handleGuardrailOptionChange('min_value', parseInt(e.target.value))}
            fullWidth
            inputProps={{ min: 0 }}
            sx={{ mb: 2 }}
          />
          <TextField
            label="Field Name"
            value={guardrailConfig.field_name || 'total_count'}
            onChange={(e) => handleGuardrailOptionChange('field_name', e.target.value)}
            fullWidth
            helperText="Name of the field to check (e.g., 'total_count', 'results')"
          />
        </Box>
      )}

      <Divider sx={{ my: 2 }} />
      <Typography variant="subtitle2" color="text.secondary">
        LLM Guardrail Settings
        <Tooltip title="Uses an LLM agent to validate task output against criteria you define. This provides flexible, AI-powered validation.">
          <IconButton size="small" sx={{ ml: 0.5 }}>
            <HelpOutlineIcon fontSize="small" />
          </IconButton>
        </Tooltip>
      </Typography>

      <FormControlLabel
        control={
          <Switch
            checked={Boolean(advancedConfig.llm_guardrail)}
            onChange={(e) => handleLlmGuardrailToggle(e.target.checked)}
          />
        }
        label="Enable LLM Guardrail"
      />

      {advancedConfig.llm_guardrail && (
        <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2, mt: 1 }}>
          <TextField
            label="Validation Criteria"
            value={advancedConfig.llm_guardrail.description || ''}
            onChange={(e) => handleLlmGuardrailChange('description', e.target.value)}
            fullWidth
            multiline
            rows={3}
            placeholder="Describe how the LLM should validate the task output..."
            helperText="Describe the criteria the LLM will use to validate the task output"
          />
          <FormControl fullWidth>
            <InputLabel>Validation LLM Model</InputLabel>
            <Select
              value={advancedConfig.llm_guardrail.llm_model || 'databricks-claude-sonnet-4-5'}
              onChange={(e) => handleLlmGuardrailChange('llm_model', e.target.value)}
              label="Validation LLM Model"
              disabled={llmModelsLoading}
              startAdornment={llmModelsLoading ? <CircularProgress size={20} sx={{ mr: 1 }} /> : null}
            >
              {llmModels.map((model) => (
                <MenuItem key={model.name} value={model.name}>
                  {model.name}
                </MenuItem>
              ))}
            </Select>
          </FormControl>
          <Typography variant="body2" color="text.secondary">
            The LLM guardrail uses an AI model to validate task outputs against your criteria.
            If validation fails, the task can be retried (enable &quot;Retry on Failure&quot; above).
          </Typography>
        </Box>
      )}
    </Box>
  );
};

// Memoize the component
export const TaskAdvancedConfig = memo(TaskAdvancedConfigComponent);

// Add a display name to the memoized component
TaskAdvancedConfig.displayName = 'TaskAdvancedConfig'; 