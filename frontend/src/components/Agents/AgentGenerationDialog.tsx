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
  Typography,
  Chip,
} from '@mui/material';
import { AgentGenerationDialogProps } from '../../types/agent';
import { GenerateService } from '../../api/GenerateService';
import { ModelService } from '../../api/ModelService';
import { Models } from '../../types/models';
import { ToolService } from '../../api/ToolService';
import { Tool } from '../../types/tool';
import { useAPIKeysStore } from '../../store/apiKeys';
import * as ApiKeyUtils from '../../utils/apiKeyUtils';

const AgentGenerationDialog: React.FC<AgentGenerationDialogProps> = ({
  open,
  onClose,
  onAgentGenerated,
  selectedModel,
  tools = [],
  selectedTools = [],
  onToolsChange = () => {
    // Default no-op function
  },
}) => {
  const [prompt, setPrompt] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [dialogModel, setDialogModel] = useState<string>('gpt-4o-mini');
  const [models, setModels] = useState<Models>({});
  const [modelsLoading, setModelsLoading] = useState(false);
  const [localTools, setLocalTools] = useState<typeof tools>([]);
  
  // API key related state
  const [isApiKeyError, setIsApiKeyError] = useState<boolean>(false);
  const [missingProvider, setMissingProvider] = useState<string>('');
  const { secrets: _apiKeys, fetchAPIKeys } = useAPIKeysStore();

  // Initialize localTools from props once on mount or when tools prop changes
  useEffect(() => {
    if (tools.length > 0) {
      setLocalTools(tools);
    }
  }, [tools]);

  // Event listener for tool state changes
  useEffect(() => {
    const handleToolStateChanged = (event: CustomEvent<{tools: Tool[]}>) => {
      if (event.detail && Array.isArray(event.detail.tools)) {
        // Make sure we filter to only enabled tools
        const enabledTools = event.detail.tools.filter(tool => tool.enabled !== false);
        if (JSON.stringify(enabledTools) !== JSON.stringify(localTools)) {
          setLocalTools(enabledTools);
        }
      }
    };

    window.addEventListener('toolStateChanged', handleToolStateChanged as EventListener);

    return () => {
      window.removeEventListener('toolStateChanged', handleToolStateChanged as EventListener);
    };
  }, [localTools]);

  // Only log for debugging, remove in production
  useEffect(() => {
    if (process.env.NODE_ENV === 'development') {
      console.log("localTools updated in AgentGenerationDialog:", localTools);
    }
  }, [localTools]);

  useEffect(() => {
    if (open) {
      setPrompt('');
      setError(null);
      
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
      
      // Ensure we're using the most up-to-date tools data
      const updateLocalTools = async () => {
        try {
          const toolService = ToolService;
          const refreshedTools = await toolService.listTools();
          const formattedTools = refreshedTools.map(tool => ({
            ...tool,
            id: tool.id.toString(),
            enabled: tool.enabled !== undefined ? tool.enabled : true
          }));
          const enabledTools = formattedTools.filter(tool => tool.enabled !== false);
          setLocalTools(enabledTools);
        } catch (error) {
          console.error("Failed to refresh tools in AgentGenerationDialog:", error);
        }
      };
      
      if (localTools.length === 0) {
        updateLocalTools();
      }
    }
  }, [open, selectedModel, localTools.length]);

  const handleModelChange = (event: SelectChangeEvent) => {
    setDialogModel(event.target.value);
  };

  const handleToolClick = (toolId: string) => {
    if (selectedTools.includes(toolId)) {
      onToolsChange(selectedTools.filter(id => id !== toolId));
    } else {
      onToolsChange([...selectedTools, toolId]);
    }
  };

  // Safely get a tool ID, ensuring it's a string
  const getToolId = (tool: { id?: string | number }): string => {
    return typeof tool.id === 'string' ? tool.id : 
           typeof tool.id === 'number' ? tool.id.toString() : '';
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
      
      console.log(`Generating agent with model: ${dialogModel} and tools:`, selectedTools);
      // Ensure all tools are strings before sending to the backend
      const agent = await GenerateService.generateAgent(prompt, dialogModel, selectedTools);
      if (agent) {
        console.log('Generated agent structure:', JSON.stringify(agent, null, 2));
        onAgentGenerated(agent);
        // After generating the agent, it will be automatically added to canvas
        // via the onAgentGenerated handler in CrewCanvas
        onClose();
      } else {
        setError('Failed to generate agent');
      }
    } catch (err) {
      console.error('Error generating agent:', err);
      
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
          setError(err instanceof Error ? err.message : 'Failed to generate agent');
        }
      } else {
        setError(err instanceof Error ? err.message : 'Failed to generate agent');
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
      <DialogTitle>Generate Agent with AI</DialogTitle>
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
            label="Describe the agent you want to create"
            multiline
            rows={4}
            value={prompt}
            onChange={(e) => setPrompt(e.target.value)}
            onKeyPress={handleKeyPress}
            placeholder="Example: Create a research assistant agent specialized in analyzing scientific papers and summarizing key findings"
            disabled={loading}
            autoFocus
          />
          
          {/* Add Tools Selection */}
          {localTools.length > 0 && (
            <Box sx={{ mt: 2 }}>
              <Typography variant="subtitle1" gutterBottom>
                Available Tools
              </Typography>
              <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1 }}>
                {localTools
                  .filter(tool => tool.enabled !== false) // Only show enabled tools
                  .map((tool) => {
                    const toolId = getToolId(tool);
                    return (
                      <Chip
                        key={toolId}
                        label={tool.title || 'Unnamed Tool'}
                        onClick={() => handleToolClick(toolId)}
                        color={selectedTools.includes(toolId) ? 'primary' : 'default'}
                        sx={{ m: 0.5 }}
                      />
                    );
                  })}
              </Box>
              {localTools.filter(tool => tool.enabled !== false).length === 0 && (
                <Typography color="text.secondary" sx={{ mt: 1 }}>
                  No enabled tools available. Please enable tools in the Tools section.
                </Typography>
              )}
            </Box>
          )}
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

export default AgentGenerationDialog; 