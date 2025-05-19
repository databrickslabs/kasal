import React, { useState, useEffect } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  TextField,
  Box,
  CircularProgress,
  Alert,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  SelectChangeEvent,
} from '@mui/material';
import type { Task } from '../../api/TaskService';
import { GenerateService } from '../../api/GenerateService';
import { ModelService } from '../../api/ModelService';
import { Models } from '../../types/models';
import { TaskGenerationDialogProps } from '../../types/task';
import { useAPIKeysStore } from '../../store/apiKeys';
import * as ApiKeyUtils from '../../utils/apiKeyUtils';

const TaskGenerationDialog: React.FC<TaskGenerationDialogProps> = ({
  open,
  onClose,
  onTaskGenerated,
  selectedModel,
}) => {
  const [prompt, setPrompt] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [dialogModel, setDialogModel] = useState<string>('gpt-4o-mini');
  const [models, setModels] = useState<Models>({});
  const [modelsLoading, setModelsLoading] = useState(false);
  
  // API key related state
  const [isApiKeyError, setIsApiKeyError] = useState<boolean>(false);
  const [missingProvider, setMissingProvider] = useState<string>('');
  const { secrets: _apiKeys, fetchAPIKeys } = useAPIKeysStore();

  useEffect(() => {
    if (open) {
      setPrompt('');
      setError(null);
      setIsApiKeyError(false);
      setMissingProvider('');
      
      // Fetch models when dialog opens
      const fetchModels = async () => {
        setModelsLoading(true);
        try {
          const modelService = ModelService.getInstance();
          const fetchedModels = await modelService.getActiveModels();
          setModels(fetchedModels);
          
          // Auto-select first model if none is selected and models are available
          if (!selectedModel && Object.keys(fetchedModels).length > 0) {
            const firstModelKey = Object.keys(fetchedModels)[0];
            console.log(`Auto-selecting first available model: ${firstModelKey}`);
            setDialogModel(firstModelKey);
          } else {
            // Use selectedModel if provided, otherwise use gpt-4o-mini
            setDialogModel(selectedModel || 'gpt-4o-mini');
          }
        } catch (error) {
          console.error('Error fetching models:', error);
          // Fallback to synchronous method if async fails
          const modelService = ModelService.getInstance();
          const fallbackModels = modelService.getActiveModelsSync();
          setModels(fallbackModels);
          
          // Auto-select first model if none is selected and fallback models are available
          if (!selectedModel && Object.keys(fallbackModels).length > 0) {
            const firstModelKey = Object.keys(fallbackModels)[0];
            console.log(`Auto-selecting first available fallback model: ${firstModelKey}`);
            setDialogModel(firstModelKey);
          } else {
            // Use selectedModel if provided, otherwise use gpt-4o-mini
            setDialogModel(selectedModel || 'gpt-4o-mini');
          }
        } finally {
          setModelsLoading(false);
        }
      };
      
      fetchModels();
    }
  }, [open, selectedModel]);

  const handleModelChange = (event: SelectChangeEvent) => {
    setDialogModel(event.target.value);
  };

  // Handle navigation to API Keys page
  const handleNavigateToApiKeys = () => {
    // Use the Zustand store to open the API Keys editor with the missing provider
    useAPIKeysStore.getState().openApiKeyEditor(missingProvider);
    
    // Open the configuration dialog in the parent component via event
    const event = new CustomEvent('openConfigAPIKeys');
    window.dispatchEvent(event);
    
    onClose();
  };

  const handleGenerate = async () => {
    if (!prompt.trim()) {
      setError('Please enter a prompt');
      return;
    }

    setLoading(true);
    setError(null);
    setIsApiKeyError(false);
    setMissingProvider('');

    try {
      // First refresh API keys
      console.log('Refreshing API keys before generation...');
      await fetchAPIKeys();
      
      // Get the latest API keys from the store
      const latestApiKeys = useAPIKeysStore.getState().secrets;
      
      // Check if the API key is configured for the selected model
      if (dialogModel) {
        const modelInfo = models[dialogModel];
        const provider = modelInfo?.provider?.toLowerCase() || 'openai';
        
        console.log(`Using model: ${dialogModel}, provider: ${provider}`);
        
        // Check if API key is optional for this provider
        const isOptional = await ApiKeyUtils.isApiKeyOptional(provider);
        if (!isOptional) {
          // Check if API key is configured using the latest keys
          const isConfigured = await ApiKeyUtils.isApiKeyConfigured(provider, latestApiKeys);
          console.log('Key configuration check result:', isConfigured);
          
          if (!isConfigured) {
            setIsApiKeyError(true);
            setMissingProvider(provider);
            setError(`${provider.toUpperCase()} API key is required. Please configure it in the API Keys section.`);
            setLoading(false);
            return;
          }
        }
      }
      
      const response = await GenerateService.generateTask(prompt, dialogModel);
      if (response) {
        const completeTask: Task = {
          ...response,
          id: '',
          agent_id: null,
          async_execution: false,
          context: [],
          config: {
            cache_response: false,
            cache_ttl: 3600,
            retry_on_fail: true,
            max_retries: 3,
            timeout: null,
            priority: 1,
            error_handling: 'default',
            output_file: null,
            output_json: null,
            output_pydantic: null,
            callback: null,
            human_input: false,
          },
          tools: response.tools || [],
        };

        onTaskGenerated(completeTask);
        // After generating the task, it will be automatically added to canvas
        // via the onTaskGenerated handler in CrewCanvas
        onClose();
        setPrompt('');
      } else {
        setError('Failed to generate task');
      }
    } catch (err) {
      console.error('Error generating task:', err);
      
      // Check for API key error patterns in the response
      if (err && typeof err === 'object' && 'response' in err && 
          err.response && typeof err.response === 'object' && 'data' in err.response) {
        const responseData = err.response.data;
        if (responseData && typeof responseData === 'object' && 'detail' in responseData && 
            typeof responseData.detail === 'string') {
          // Check for missing API key error pattern
          const apiKeyErrorMatch = responseData.detail.match(/No API key found for provider: (\w+)/i);
          
          if (apiKeyErrorMatch && apiKeyErrorMatch[1]) {
            const provider = apiKeyErrorMatch[1].toLowerCase();
            setIsApiKeyError(true);
            setMissingProvider(provider);
            setError(`${provider.toUpperCase()} API key is required. Please configure it in the API Keys section.`);
          } else {
            setError(responseData.detail);
          }
        } else {
          setError(err instanceof Error ? err.message : 'Failed to generate task');
        }
      } else {
        setError(err instanceof Error ? err.message : 'Failed to generate task');
      }
    } finally {
      setLoading(false);
    }
  };

  const handleKeyPress = (event: React.KeyboardEvent) => {
    if (event.key === 'Enter' && !event.shiftKey) {
      event.preventDefault();
      handleGenerate();
    }
  };

  return (
    <Dialog open={open} onClose={onClose} maxWidth="md" fullWidth>
      <DialogTitle>Generate Task with AI</DialogTitle>
      <DialogContent>
        <Box sx={{ mt: 2, display: 'flex', flexDirection: 'column', gap: 2 }}>
          {error && (
            <Alert 
              severity="error" 
              sx={{ mb: 2 }}
              action={
                isApiKeyError && (
                  <Button color="inherit" size="small" onClick={handleNavigateToApiKeys}>
                    Configure API Keys
                  </Button>
                )
              }
            >
              {error}
            </Alert>
          )}
          
          <FormControl fullWidth>
            <InputLabel>Model</InputLabel>
            <Select
              value={dialogModel}
              onChange={handleModelChange}
              label="Model"
              disabled={loading || modelsLoading}
            >
              {modelsLoading ? (
                <MenuItem disabled>Loading models...</MenuItem>
              ) : (
                Object.entries(models)
                  .filter(([_, model]) => model.enabled !== false) // Only show enabled models
                  .map(([key, model]) => (
                    <MenuItem 
                      key={key} 
                      value={key}
                      sx={{
                        display: 'flex',
                        justifyContent: 'space-between',
                        gap: 1
                      }}
                    >
                      <span>{key}</span>
                      <span style={{ fontSize: '0.8em' }}>{model.provider ? `(${model.provider})` : ''}</span>
                    </MenuItem>
                  ))
              )}
            </Select>
          </FormControl>
          
          <TextField
            fullWidth
            label="Describe the task you want to create"
            multiline
            rows={4}
            value={prompt}
            onChange={(e) => setPrompt(e.target.value)}
            onKeyPress={handleKeyPress}
            placeholder="Example: Create a task that analyzes a dataset and generates a summary report"
            disabled={loading}
            autoFocus
          />
        </Box>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose} disabled={loading}>
          Cancel
        </Button>
        <Button
          onClick={handleGenerate}
          variant="contained"
          disabled={loading || !prompt.trim() || modelsLoading}
          startIcon={loading ? <CircularProgress size={20} /> : null}
        >
          {loading ? 'Generating...' : 'Generate'}
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default TaskGenerationDialog; 