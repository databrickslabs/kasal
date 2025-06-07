import React, { useState, useRef, useEffect, useMemo } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  TextField,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Box,
  Typography,
  Chip,
  SelectChangeEvent,
  CircularProgress,
  Alert,
  Checkbox,
  FormControlLabel,
} from '@mui/material';
import { CrewPlanningDialogProps } from '../../types/crewPlan';
import { ToolService } from '../../api/ToolService';
import { Tool as _Tool } from '../../types/tool';
import { useModelConfigStore } from '../../store/modelConfig';
import { useCrewExecutionStore } from '../../store/crewExecution';
import { ModelService } from '../../api/ModelService';
import { Models } from '../../types/models';
import { useAPIKeysStore } from '../../store/apiKeys';
import * as ApiKeyUtils from '../../utils/apiKeyUtils';
import { CrewPlanningService } from '../../api/CrewPlanningService';

// Define a proper type for the model objects
interface _ModelConfig {
  name: string;
  temperature?: number;
  provider?: string;
  context_window?: number;
  max_output_tokens?: number;
  enabled?: boolean;
}

const CrewPlanningDialog: React.FC<CrewPlanningDialogProps> = ({
  open,
  onClose,
  onGenerateCrew,
  selectedModel,
  tools,
  selectedTools,
  onToolsChange,
}): JSX.Element => {
  const [prompt, setPrompt] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [localTools, setLocalTools] = useState<typeof tools>(tools);
  const [selectedModelState, setSelectedModelState] = useState<string>('');
  const [directModels, setDirectModels] = useState<Models>({});
  const [isLoadingModels, setIsLoadingModels] = useState(false);
  const promptInputRef = useRef<HTMLInputElement>(null);
  const submitButtonRef = useRef<HTMLButtonElement>(null);
  const initRef = useRef<boolean>(false);
  const dialogModelRef = useRef<string>(selectedModel || '');
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [isApiKeyError, setIsApiKeyError] = useState<boolean>(false);
  const [missingProvider, setMissingProvider] = useState<string>('');
  const [executeAfterGeneration, setExecuteAfterGeneration] = useState(false);

  // Get models from Zustand store
  const modelConfigStore = useModelConfigStore();
  const { models: storeModels, loading: _modelsLoading } = modelConfigStore;
  const { setSelectedModel } = useCrewExecutionStore();
  
  // Get API keys from Zustand store
  const { secrets: _apiKeys, fetchAPIKeys } = useAPIKeysStore();

  // Combine models from store and direct fetch, with direct fetch taking precedence
  const models = useMemo(() => {
    // Prefer directly fetched models over store models
    return { ...storeModels, ...directModels };
  }, [directModels, storeModels]);
  
  // Load models directly from the service when dialog opens
  useEffect(() => {
    if (open) {
      setIsLoadingModels(true);
      const modelService = ModelService.getInstance();
      
      // First ensure models are initialized, then get active models
      modelService.getActiveModels().then((fetchedModels) => {
        setDirectModels(fetchedModels);
        setIsLoadingModels(false);
        
        // Set a default model if none is selected
        if (!selectedModelState && !dialogModelRef.current && Object.keys(fetchedModels).length > 0) {
          const firstModelKey = Object.keys(fetchedModels)[0];
          setSelectedModelState(firstModelKey);
          dialogModelRef.current = firstModelKey;
          setSelectedModel(firstModelKey);
        }
      }).catch(error => {
        console.error('Error fetching models:', error);
        setIsLoadingModels(false);
      });
    }
  }, [open, setSelectedModel, selectedModelState]);

  // Make sure dialogModelRef has an initial value
  useEffect(() => {
    if (models && Object.keys(models).length > 0 && (!dialogModelRef.current || dialogModelRef.current === '')) {
      const firstModelKey = Object.keys(models)[0];
      dialogModelRef.current = firstModelKey;
    }
  }, [models]);

  // Sync tools with props
  useEffect(() => {
    if (tools && tools.length > 0) {
      setLocalTools(tools);
    }
  }, [tools]);

  // Initialize dialog
  useEffect(() => {
    if (!open) {
      initRef.current = false;
      return;
    }

    if (initRef.current) {
      return;
    }

    const initializeDialog = async () => {
      initRef.current = true;
      setPrompt('');
      
      // If no model is selected, use the first available model
      if (!dialogModelRef.current) {
        if (Object.keys(models).length > 0) {
          dialogModelRef.current = Object.keys(models)[0];
          // Also update the model in the store
          setSelectedModel(dialogModelRef.current);
        }
      }

      // Only fetch tools if we don't already have them
      if (!localTools || localTools.length === 0) {
        try {
          const toolService = ToolService;
          const refreshedTools = await toolService.listTools();
          const formattedTools = refreshedTools
            .filter(tool => tool.category !== 'UnityCatalog') // Filter out UnityCatalog tools
            .map(tool => ({
              ...tool,
              id: tool.id.toString(),
              enabled: tool.enabled !== undefined ? tool.enabled : true,
              category: (tool.category === 'UnityCatalog' ? 'Custom' : tool.category) as 'PreBuilt' | 'Custom' | undefined
            }));
          setLocalTools(formattedTools);
        } catch (error) {
          console.error("Failed to refresh tools in CrewPlanningDialog:", error);
        }
      }
    };

    initializeDialog();
  }, [open, models, localTools, setSelectedModel]);

  // Handle model changes
  const handleModelChange = (event: SelectChangeEvent<string>) => {
    const newModel = event.target.value;
    setSelectedModelState(newModel);
    dialogModelRef.current = newModel;
    // Update the model in the crew execution store
    setSelectedModel(newModel);
  };

  // Handle tool click
  const handleToolClick = (toolId: string) => {
    if (selectedTools.includes(toolId)) {
      const newSelectedTools = selectedTools.filter(id => id !== toolId);
      onToolsChange(newSelectedTools);
    } else {
      const newSelectedTools = [...selectedTools, toolId];
      onToolsChange(newSelectedTools);
    }
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

  // Check if API key is configured using Zustand store
  const isApiKeyConfigured = async (modelKey: string): Promise<boolean> => {
    try {
      if (!models || !models[modelKey]) {
        console.error('Model not found:', modelKey);
        return false;
      }

      const modelInfo = models[modelKey];
      const provider = modelInfo.provider?.toLowerCase() || 'openai';
      
      // Get the latest API keys from the store
      const latestApiKeys = useAPIKeysStore.getState().secrets;
      
      // First check if this provider requires an API key
      if (await ApiKeyUtils.isApiKeyOptional(provider)) {
        return true;
      }
      
      // Check if there's a key with this name and it has a value
      const configured = await ApiKeyUtils.isApiKeyConfigured(provider, latestApiKeys);
      
      return configured;
    } catch (error) {
      console.error('Error checking API key:', error);
      return false;
    }
  };

  // Handle generate click
  const handleGenerateClick = async () => {
    if (!prompt.trim() || !dialogModelRef.current) {
      return;
    }

    setIsLoading(true);
    setErrorMessage(null);
    setIsApiKeyError(false);
    setMissingProvider('');
    
    // Update the model in the crewExecution store for consistency
    setSelectedModel(dialogModelRef.current);
    
    try {
      // First refresh API keys before generation
      await fetchAPIKeys();
      
      // Check if API key is configured using the Zustand store
      const modelInfo = models[dialogModelRef.current];
      const provider = modelInfo?.provider?.toLowerCase() || 'openai';
      
      const configured = await isApiKeyConfigured(dialogModelRef.current);
      
      if (!configured) {
        setIsApiKeyError(true);
        setMissingProvider(provider);
        setErrorMessage(`${provider.toUpperCase()} API key is required. Please configure it in the API Keys section.`);
        setIsLoading(false);
        return;
      }
      
      console.log("CrewPlanningDialog: Generating crew with model:", dialogModelRef.current);
      console.log("CrewPlanningDialog: Selected tools:", selectedTools);
      
      // Use the new CrewPlanningService to create the crew
      const result = await CrewPlanningService.createCrew({
        prompt: prompt,
        model: dialogModelRef.current,
        tools: selectedTools,
      });
      
      console.log("CrewPlanningDialog: Received crew data:", result);
      console.log("CrewPlanningDialog: Agents count:", result.agents?.length);
      console.log("CrewPlanningDialog: Tasks count:", result.tasks?.length);
      
      // Ensure agent_id is set on all tasks before returning the crew plan
      if (result.tasks && result.tasks.length > 0 && 
          result.agents && result.agents.length > 0) {
        
        // Default agent ID if needed
        const defaultAgentId = result.agents[0].id;
        
        // Force set agent_id on any tasks that are missing it
        result.tasks.forEach(task => {
          if (!task.agent_id && defaultAgentId) {
            task.agent_id = defaultAgentId;
            task.assigned_agent = defaultAgentId;
          }
          
          // Ensure context is always an array of strings (UUIDs)
          if (!task.context) {
            task.context = [];
          } else if (!Array.isArray(task.context)) {
            // Handle case where context might not be an array
            task.context = [];
            console.warn(`Task ${task.id} had invalid context format, resetting to empty array`);
          }
        });
      }
      
      // Check if we're missing task dependencies in the LLM response (Now using context)
      if (result.tasks && result.tasks.length > 1) {
        // Log which tasks have dependencies and which don't
        let tasksWithDependencies = 0;
        let tasksWithoutDependencies = 0;
        
        result.tasks.forEach(task => {
          // Check context field for dependencies
          const hasDependency = task.context && task.context.length > 0;
          
          if (hasDependency) {
            tasksWithDependencies++;
            console.log(`Task with dependencies (context): ${task.name} (id: ${task.id}), context:`, task.context);
          } else {
            tasksWithoutDependencies++;
            console.log(`Task without dependencies (context): ${task.name} (id: ${task.id})`);
          }
        });
        
        console.log(`CrewPlanningDialog: ${tasksWithDependencies} tasks have dependencies (from context), ${tasksWithoutDependencies} tasks don't`);
      }
      
      onGenerateCrew(result, executeAfterGeneration);
      
      // Don't execute here, as we now handle this in the onGenerateCrew callback
      onClose();
    } catch (error: Error | unknown) {
      console.error('Failed to generate crew:', error);
      
      // Check for API error responses
      if (error && typeof error === 'object' && 'response' in error && 
          error.response && typeof error.response === 'object' && 'data' in error.response) {
        const responseData = error.response.data;
        if (responseData && typeof responseData === 'object' && 'detail' in responseData && 
            typeof responseData.detail === 'string') {
          // Check for missing API key error pattern
          const apiKeyErrorMatch = responseData.detail.match(/No API key found for provider: (\w+)/i);
          
          if (apiKeyErrorMatch && apiKeyErrorMatch[1]) {
            const provider = apiKeyErrorMatch[1].toLowerCase();
            setIsApiKeyError(true);
            setMissingProvider(provider);
            setErrorMessage(`${provider.toUpperCase()} API key is required. Please configure it in the API Keys section.`);
          } else {
            setErrorMessage(responseData.detail);
          }
        } else {
          setErrorMessage('Failed to generate crew. Please try again later.');
        }
      } else {
        setErrorMessage('Failed to generate crew. Please try again later.');
      }
    } finally {
      setIsLoading(false);
    }
  };

  // Safely get a tool ID, ensuring it's a string
  const getToolId = (tool: { id?: string | number }): string => {
    return typeof tool.id === 'string' ? tool.id : 
           typeof tool.id === 'number' ? tool.id.toString() : '';
  };

  // Make sure we have an initial model selected
  useEffect(() => {
    const initializeModel = () => {
      // If we have models but no selection yet
      if (models && Object.keys(models).length > 0 && !selectedModelState) {
        // First try to find any enabled model
        const enabledModels = Object.entries(models)
          .filter(([_, model]) => model?.enabled === true);
        
        if (enabledModels.length > 0) {
          const [firstModelKey] = enabledModels[0];
          setSelectedModelState(firstModelKey);
          dialogModelRef.current = firstModelKey;
          setSelectedModel(firstModelKey);
        } else {
          // If no enabled models, just pick the first one
          const firstModelKey = Object.keys(models)[0];
          setSelectedModelState(firstModelKey);
          dialogModelRef.current = firstModelKey;
          setSelectedModel(firstModelKey);
        }
      }
    };
    
    initializeModel();
  }, [models, selectedModelState, setSelectedModel]);

  return (
    <Dialog 
      open={open} 
      onClose={onClose} 
      maxWidth="md" 
      fullWidth
      TransitionProps={{
        onEntered: () => {
          setTimeout(() => {
            if (promptInputRef.current) {
              promptInputRef.current.focus();
            }
          }, 150);
        }
      }}
    >
      <form onSubmit={(e) => {
        e.preventDefault();
        if (prompt && !isLoading && !isLoadingModels) {
          handleGenerateClick();
        }
      }}>
        <DialogTitle>Generate Crew from Description</DialogTitle>
        <DialogContent>
          {errorMessage && (
            <Alert
              severity="error"
              sx={{ mb: 2, mt: 1 }}
              action={
                isApiKeyError && (
                  <Button color="inherit" size="small" onClick={handleNavigateToApiKeys}>
                    Configure API Keys
                  </Button>
                )
              }
            >
              {errorMessage}
            </Alert>
          )}
          
          <FormControl fullWidth margin="normal">
            <InputLabel id="model-label">Model</InputLabel>
            <Select
              labelId="model-label"
              value={selectedModelState || dialogModelRef.current || ''}
              onChange={handleModelChange}
              label="Model"
              disabled={isLoading || isLoadingModels}
              MenuProps={{
                PaperProps: {
                  style: {
                    maxHeight: 300,
                    width: 'auto',
                  },
                },
              }}
              sx={{ 
                minHeight: 56,
                '& .MuiSelect-select': { 
                  display: 'flex', 
                  alignItems: 'center' 
                }
              }}
            >
              {isLoadingModels ? (
                <MenuItem disabled value="">
                  <CircularProgress size={20} /> Loading models...
                </MenuItem>
              ) : Object.keys(models).length === 0 ? (
                <MenuItem disabled value="">
                  No models available
                </MenuItem>
              ) : (
                Object.entries(models)
                  .filter(([_, model]) => model?.enabled === true)
                  .map(([key, model]) => (
                    <MenuItem key={key} value={key} sx={{ minHeight: '40px' }}>
                      {model?.name || key} {model?.provider ? `(${model.provider})` : ''}
                    </MenuItem>
                  ))
              )}
            </Select>
          </FormControl>

          <TextField
            inputRef={promptInputRef}
            label="Describe your crew"
            multiline
            rows={6}
            value={prompt}
            onChange={(e) => setPrompt(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'Enter' && !e.shiftKey) {
                e.preventDefault();
                if (prompt.trim() && !isLoading && !isLoadingModels) {
                  handleGenerateClick();
                }
              }
            }}
            fullWidth
            margin="normal"
            placeholder="Example: I need a crew to help me analyze market research data and create a report with visualizations"
            disabled={isLoading}
            autoFocus
          />

          <Box sx={{ mt: 2 }}>
            <Typography variant="subtitle1" gutterBottom>
              Available Tools
            </Typography>
            <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1 }}>
              {localTools && localTools
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
              {(!localTools || localTools.length === 0) && (
                <CircularProgress size={20} sx={{ m: 2 }} />
              )}
            </Box>
            {localTools && localTools.length > 0 && 
             localTools.filter(tool => tool.enabled !== false).length === 0 && (
              <Typography color="text.secondary" sx={{ mt: 1 }}>
                No enabled tools available. Please enable tools in the Tools section.
              </Typography>
            )}
          </Box>
          
          <Box sx={{ mt: 3 }}>
            <FormControlLabel
              control={
                <Checkbox 
                  checked={executeAfterGeneration}
                  onChange={(e) => setExecuteAfterGeneration(e.target.checked)}
                  disabled={isLoading}
                />
              }
              label="Execute crew after generation"
            />
          </Box>
        </DialogContent>
        <DialogActions>
          <Button onClick={onClose} disabled={isLoading}>
            Cancel
          </Button>
          <Button
            ref={submitButtonRef}
            onClick={handleGenerateClick}
            color="primary"
            disabled={!prompt.trim() || isLoading || isLoadingModels}
          >
            {isLoading ? <CircularProgress size={24} /> : 'Generate'}
          </Button>
        </DialogActions>
      </form>
    </Dialog>
  );
};

export default CrewPlanningDialog;