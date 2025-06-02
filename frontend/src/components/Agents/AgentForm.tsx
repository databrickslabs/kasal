import React, { useState, useEffect } from 'react';
import {
  TextField,
  Button,
  Box,
  FormControl,
  InputLabel,
  Select,
  Card,
  Typography,
  MenuItem,
  Switch,
  FormControlLabel,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  Grid,
  Chip,
  SelectChangeEvent,
  Divider,
  CircularProgress,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  IconButton,
  InputAdornment,
} from '@mui/material';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import OpenInFullIcon from '@mui/icons-material/OpenInFull';
import CloseIcon from '@mui/icons-material/Close';
import { AgentService } from '../../api/AgentService';
import { Agent, AgentFormProps, KnowledgeSource } from '../../types/agent';
import { ModelService } from '../../api/ModelService';
import { Models } from '../../types/models';

import { GenerateService } from '../../api/GenerateService';
import { KnowledgeSourcesSection } from './KnowledgeSourcesSection';

// Default fallback model when API is down
const DEFAULT_FALLBACK_MODEL = {
  'databricks-llama-4-maverick': {
    name: 'databricks-llama-4-maverick',
    temperature: 0.7,
    context_window: 128000,
    max_output_tokens: 4096,
    enabled: true
  }
};

type AgentFormData = Omit<Agent, 'id' | 'created_at'> & {
  id?: string;
};

const AgentForm: React.FC<AgentFormProps> = ({ initialData, onCancel, onAgentSaved, tools }) => {
  const [models, setModels] = useState<Models>(DEFAULT_FALLBACK_MODEL);
  const [loadingModels, setLoadingModels] = useState(true);
  const [expandedGoal, setExpandedGoal] = useState<boolean>(false);
  const [expandedBackstory, setExpandedBackstory] = useState<boolean>(false);
  
  // Function calling models are typically a subset - we'll filter for these
  const functionCallingModels = Object.entries(models).filter(([_, model]) => 
    model.provider === 'openai' || model.provider === 'anthropic'
  );
  
  const [formData, setFormData] = useState<AgentFormData>(() => {
    const data = {
      name: initialData?.name || '',
      role: initialData?.role || '',
      goal: initialData?.goal || '',
      backstory: initialData?.backstory || '',
      llm: initialData?.llm || 'databricks-llama-4-maverick',
      tools: initialData?.tools ? initialData.tools.map(id => String(id)) : [],
      function_calling_llm: initialData?.function_calling_llm || undefined,
      max_iter: initialData?.max_iter || 25,
      max_rpm: initialData?.max_rpm || 1,
      max_execution_time: initialData?.max_execution_time || 300,
      memory: initialData?.memory ?? true,
      verbose: initialData?.verbose ?? false,
      allow_delegation: initialData?.allow_delegation || false,
      cache: initialData?.cache || true,
      system_template: initialData?.system_template || undefined,
      prompt_template: initialData?.prompt_template || undefined,
      response_template: initialData?.response_template || undefined,
      allow_code_execution: initialData?.allow_code_execution || false,
      code_execution_mode: initialData?.code_execution_mode || 'safe',
      max_retry_limit: initialData?.max_retry_limit || 3,
      use_system_prompt: initialData?.use_system_prompt || true,
      respect_context_window: initialData?.respect_context_window || true,
      embedder_config: initialData?.embedder_config || undefined,
      knowledge_sources: initialData?.knowledge_sources || [],
    };
    
    if (initialData?.id) {
      return { ...data, id: initialData.id };
    }
    
    return data;
  });

  // Load models from ModelService - moved after formData is defined
  useEffect(() => {
    const fetchModels = async () => {
      try {
        setLoadingModels(true);
        const modelService = ModelService.getInstance();
        const fetchedModels = await modelService.getActiveModels();
        
        if (Object.keys(fetchedModels).length > 0) {
          setModels(fetchedModels);
          
          // Check if the current model is valid in the fetched models
          const currentModelKey = formData.llm;
          if (currentModelKey && !fetchedModels[currentModelKey]) {
            // If current model is invalid, select the first available one
            const firstModelKey = Object.keys(fetchedModels)[0];
            console.log(`Model ${currentModelKey} not available, using ${firstModelKey} instead`);
            
            // Update the form data with the new model
            setFormData(prev => ({
              ...prev,
              llm: firstModelKey
            }));
          }
        } else {
          // No models were fetched - keep the default model but log a warning
          console.warn('No models were fetched from the API, using default model as fallback');
        }
      } catch (error) {
        console.error('Error fetching models:', error);
        // In case of error, show a fallback message but don't change the form data
        console.warn('Using default model due to error fetching models');
      } finally {
        setLoadingModels(false);
      }
    };
    
    fetchModels();
  // We intentionally don't add formData.llm as a dependency to avoid infinite loops
  // since we're updating it inside the effect
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const [isGeneratingTemplates, setIsGeneratingTemplates] = useState(false);

  const handleSubmit = async () => {
    if (!formData) return;
    
    // Make a deep copy of the formData to avoid modifying the original
    const agentToSave = JSON.parse(JSON.stringify(formData));
    
    // Preserve file information in knowledge sources
    if (agentToSave.knowledge_sources && agentToSave.knowledge_sources.length > 0) {
      // Ensure each file source has its fileInfo preserved
      agentToSave.knowledge_sources = agentToSave.knowledge_sources.map((source: KnowledgeSource) => {
        // Skip non-file sources
        if (source.type === 'text' || source.type === 'url' || !source.source) {
          return source;
        }
        
        // Ensure fileInfo is preserved
        return {
          ...source,
          // If fileInfo doesn't exist but we have a source, add a placeholder
          fileInfo: source.fileInfo || {
            filename: source.source.split('/').pop() || '',
            path: source.source,
            exists: false,
            is_uploaded: true
          }
        };
      });
    }
    
    setIsGeneratingTemplates(true);
    try {
      let savedAgent: Agent | null;
      
      if (initialData?.id) {
        // Update existing agent
        savedAgent = await AgentService.updateAgentFull(initialData.id, agentToSave as Agent);
      } else {
        // Create new agent
        savedAgent = await AgentService.createAgent(agentToSave);
      }

      if (savedAgent && onAgentSaved) {
        onAgentSaved(savedAgent);
      }
    } catch (error) {
      console.error('Error saving agent:', error);
    } finally {
      setIsGeneratingTemplates(false);
    }
  };

  const handleInputChange = (
    field: keyof AgentFormData,
    value: string | number | boolean | string[] | undefined | Record<string, unknown> | KnowledgeSource[]
  ) => {
    setFormData((prev) => ({
      ...prev,
      [field]: value
    }));
  };

  const handleToolsChange = (event: SelectChangeEvent<string[]>) => {
    // Get the selected values from the event
    const selectedValues = Array.isArray(event.target.value) 
      ? event.target.value 
      : [event.target.value];
    
    // Log selections for debugging
    console.log('Current tools:', formData.tools);
    console.log('New selection from event:', selectedValues);
    
    // Create a new set of tools, ensuring all IDs are strings
    const newTools = selectedValues.map(id => String(id));
    
    // Check if any tools appear to be duplicated (different format but same ID)
    // This helps detect potential issues
    const toolCounts = new Map<string, number>();
    newTools.forEach(id => {
      const count = toolCounts.get(id) || 0;
      toolCounts.set(id, count + 1);
    });
    
    // Log any duplicates found
    const duplicates = Array.from(toolCounts.entries())
      .filter(([_, count]) => count > 1)
      .map(([id]) => id);
    
    if (duplicates.length > 0) {
      console.warn('Duplicate tool IDs detected:', duplicates);
    }
    
    // Ensure uniqueness
    const uniqueTools = Array.from(new Set(newTools));
    console.log('Final unique tools:', uniqueTools);
    
    // Update the form data
    handleInputChange('tools', uniqueTools);
  };

  const handleGenerateTemplates = async () => {
    if (!formData.role || !formData.goal || !formData.backstory) return;

    setIsGeneratingTemplates(true);
    try {
      const templates = await GenerateService.generateTemplates({
        role: formData.role,
        goal: formData.goal,
        backstory: formData.backstory,
        model: formData.llm
      });

      if (templates) {
        handleInputChange('system_template', templates.system_template);
        handleInputChange('prompt_template', templates.prompt_template);
        handleInputChange('response_template', templates.response_template);
      }
    } catch (error) {
      console.error('Error generating templates:', error);
    } finally {
      setIsGeneratingTemplates(false);
    }
  };

  const canGenerateTemplates = Boolean(formData.role && formData.goal && formData.backstory);



  const handleOpenGoalDialog = () => {
    setExpandedGoal(true);
  };

  const handleCloseGoalDialog = () => {
    setExpandedGoal(false);
  };

  const handleOpenBackstoryDialog = () => {
    setExpandedBackstory(true);
  };

  const handleCloseBackstoryDialog = () => {
    setExpandedBackstory(false);
  };

  return (
    <>
      <Card sx={{ 
        display: 'flex', 
        flexDirection: 'column', 
        height: '70vh',
        position: 'relative',
        overflow: 'hidden'
      }}>
        <Box sx={{ p: 3, pb: 2 }}>
          <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 3 }}>
            <Typography variant="h6">
              {initialData?.id ? 'Edit Agent' : 'Create New Agent'}
            </Typography>
          </Box>
          <Divider />
        </Box>

        <Box sx={{ 
          flex: '1 1 auto', 
          overflow: 'auto',
          px: 3, 
          pb: 2,
          height: 'calc(90vh - 170px)',
        }}>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
            {/* Basic Information */}
            <TextField
              fullWidth
              label="Name"
              value={formData.name}
              onChange={(e) => handleInputChange('name', e.target.value)}
              required
            />
            <TextField
              fullWidth
              label="Role"
              value={formData.role}
              onChange={(e) => handleInputChange('role', e.target.value)}
              required
            />
            <TextField
              fullWidth
              label="Goal"
              value={formData.goal}
              onChange={(e) => handleInputChange('goal', e.target.value)}
              multiline
              rows={2}
              required
              InputProps={{
                endAdornment: (
                  <InputAdornment position="end">
                    <IconButton
                      edge="end"
                      onClick={handleOpenGoalDialog}
                      size="small"
                      sx={{ opacity: 0.7 }}
                      title="Expand goal"
                    >
                      <OpenInFullIcon fontSize="small" />
                    </IconButton>
                  </InputAdornment>
                )
              }}
            />
            <TextField
              fullWidth
              label="Backstory"
              value={formData.backstory}
              onChange={(e) => handleInputChange('backstory', e.target.value)}
              multiline
              rows={3}
              required
              InputProps={{
                endAdornment: (
                  <InputAdornment position="end">
                    <IconButton
                      edge="end"
                      onClick={handleOpenBackstoryDialog}
                      size="small"
                      sx={{ opacity: 0.7 }}
                      title="Expand backstory"
                    >
                      <OpenInFullIcon fontSize="small" />
                    </IconButton>
                  </InputAdornment>
                )
              }}
            />

            {/* Templates Section */}
            <Accordion>
              <AccordionSummary expandIcon={<ExpandMoreIcon />}>
                <Typography>Templates</Typography>
              </AccordionSummary>
              <AccordionDetails>
                <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                  <Box sx={{ display: 'flex', justifyContent: 'flex-end', mb: 2 }}>
                    <Button
                      variant="contained"
                      onClick={handleGenerateTemplates}
                      disabled={!canGenerateTemplates || isGeneratingTemplates}
                    >
                      {isGeneratingTemplates ? 'Generating...' : 'Generate Templates'}
                    </Button>
                  </Box>
                  <TextField
                    fullWidth
                    label="System Template"
                    value={formData.system_template || ''}
                    onChange={(e) => handleInputChange('system_template', e.target.value)}
                    multiline
                    rows={4}
                  />
                  <TextField
                    fullWidth
                    label="Prompt Template"
                    value={formData.prompt_template || ''}
                    onChange={(e) => handleInputChange('prompt_template', e.target.value)}
                    multiline
                    rows={4}
                  />
                  <TextField
                    fullWidth
                    label="Response Template"
                    value={formData.response_template || ''}
                    onChange={(e) => handleInputChange('response_template', e.target.value)}
                    multiline
                    rows={4}
                  />
                </Box>
              </AccordionDetails>
            </Accordion>

            {/* Tools Selection */}
            <FormControl fullWidth>
              <InputLabel id="tools-label">Tools</InputLabel>
              <Select
                labelId="tools-label"
                multiple
                value={formData.tools}
                onChange={handleToolsChange}
                label="Tools"
                renderValue={(selected) => (
                  <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
                    {(selected as string[]).map((toolId, index) => {
                      // Ensure tool ID comparison works with both string and numeric IDs
                      const tool = tools.find(t => String(t.id) === String(toolId));
                      return (
                        <Chip 
                          key={`selected-tool-${toolId}-${index}`}
                          label={tool ? tool.title : `Tool ${index + 1}`}
                          size="small"
                        />
                      );
                    })}
                  </Box>
                )}
              >
                {tools
                  .filter(tool => tool.enabled !== false) // Only show enabled tools
                  .map((tool) => (
                    <MenuItem key={`tool-${tool.id}`} value={String(tool.id)}>
                      <Box sx={{ display: 'flex', flexDirection: 'column' }}>
                        <Typography>{tool.title}</Typography>
                        <Typography variant="caption" color="text.secondary">
                          {tool.description}
                        </Typography>
                      </Box>
                    </MenuItem>
                  ))}
              </Select>
            </FormControl>

            <Accordion>
              <AccordionSummary expandIcon={<ExpandMoreIcon />}>
                <Typography>Knowledge Sources</Typography>
              </AccordionSummary>
              <AccordionDetails>
                <KnowledgeSourcesSection
                  knowledgeSources={formData.knowledge_sources || []}
                  onChange={(sources) => handleInputChange('knowledge_sources', sources)}
                />
              </AccordionDetails>
            </Accordion>

            {/* LLM Configuration */}
            <Accordion>
              <AccordionSummary expandIcon={<ExpandMoreIcon />}>
                <Typography>LLM Configuration</Typography>
              </AccordionSummary>
              <AccordionDetails>
                <Grid container spacing={2}>
                  <Grid item xs={12}>
                    <FormControl fullWidth>
                      <InputLabel>LLM Model</InputLabel>
                      <Select
                        value={loadingModels ? '' : formData.llm}
                        onChange={(e) => handleInputChange('llm', e.target.value)}
                        label="LLM Model"
                        disabled={loadingModels}
                      >
                        {loadingModels ? (
                          <MenuItem key="loading-models" value="" disabled>
                            <Box sx={{ display: 'flex', alignItems: 'center' }}>
                              <CircularProgress size={20} sx={{ mr: 1 }} />
                              Loading models...
                            </Box>
                          </MenuItem>
                        ) : Object.keys(models).length > 0 ? (
                          Object.entries(models).map(([key, model]) => (
                            <MenuItem key={`llm-model-${key}`} value={key}>
                              {model.name}
                              {model.provider && (
                                <Typography variant="caption" sx={{ ml: 1, color: 'text.secondary' }}>
                                  ({model.provider})
                                </Typography>
                              )}
                            </MenuItem>
                          ))
                        ) : (
                          // Fallback option when no models are available
                          <MenuItem key="no-models" value={formData.llm}>
                            {formData.llm || 'Default Model'} (No models available)
                          </MenuItem>
                        )}
                      </Select>
                    </FormControl>
                  </Grid>
                  <Grid item xs={12}>
                    <FormControl fullWidth>
                      <InputLabel>Function Calling LLM</InputLabel>
                      <Select
                        value={loadingModels ? '' : (formData.function_calling_llm || '')}
                        onChange={(e) => handleInputChange('function_calling_llm', e.target.value)}
                        label="Function Calling LLM"
                        disabled={loadingModels}
                      >
                        <MenuItem key="default-function-model" value="">
                          <em>Default</em>
                        </MenuItem>
                        {loadingModels ? (
                          <MenuItem key="loading-function-models" value="" disabled>
                            <Box sx={{ display: 'flex', alignItems: 'center' }}>
                              <CircularProgress size={20} sx={{ mr: 1 }} />
                              Loading models...
                            </Box>
                          </MenuItem>
                        ) : functionCallingModels.length > 0 ? (
                          functionCallingModels.map(([key, model]) => (
                            <MenuItem key={`func-model-${key}`} value={key}>
                              {model.name}
                              {model.provider && (
                                <Typography variant="caption" sx={{ ml: 1, color: 'text.secondary' }}>
                                  ({model.provider})
                                </Typography>
                              )}
                            </MenuItem>
                          ))
                        ) : (
                          // Fallback option when no function calling models are available
                          <MenuItem key="no-function-models" value={formData.function_calling_llm || ''} disabled>
                            No function calling models available
                          </MenuItem>
                        )}
                      </Select>
                    </FormControl>
                  </Grid>
                </Grid>
              </AccordionDetails>
            </Accordion>

            {/* Execution Settings */}
            <Accordion>
              <AccordionSummary expandIcon={<ExpandMoreIcon />}>
                <Typography>Execution Settings</Typography>
              </AccordionSummary>
              <AccordionDetails>
                <Grid container spacing={2}>
                  <Grid item xs={12} md={4}>
                    <TextField
                      fullWidth
                      type="number"
                      label="Max Iterations"
                      value={formData.max_iter}
                      onChange={(e) => handleInputChange('max_iter', parseInt(e.target.value))}
                    />
                  </Grid>
                  <Grid item xs={12} md={4}>
                    <TextField
                      fullWidth
                      type="number"
                      label="Max RPM"
                      value={formData.max_rpm || ''}
                      onChange={(e) => handleInputChange('max_rpm', e.target.value ? parseInt(e.target.value) : undefined)}
                    />
                  </Grid>
                  <Grid item xs={12} md={4}>
                    <TextField
                      fullWidth
                      type="number"
                      label="Max Execution Time (s)"
                      value={formData.max_execution_time || ''}
                      onChange={(e) => handleInputChange('max_execution_time', e.target.value ? parseInt(e.target.value) : undefined)}
                    />
                  </Grid>
                  <Grid item xs={12} md={4}>
                    <TextField
                      fullWidth
                      type="number"
                      label="Max Retry Limit"
                      value={formData.max_retry_limit}
                      onChange={(e) => handleInputChange('max_retry_limit', parseInt(e.target.value))}
                    />
                  </Grid>
                </Grid>
              </AccordionDetails>
            </Accordion>

            {/* Behavior Settings */}
            <Accordion>
              <AccordionSummary expandIcon={<ExpandMoreIcon />}>
                <Typography>Behavior Settings</Typography>
              </AccordionSummary>
              <AccordionDetails>
                <Grid container spacing={2}>
                  <Grid item xs={12} md={6}>
                    <FormControlLabel
                      control={
                        <Switch
                          checked={formData.memory}
                          onClick={(e) => {
                            e.stopPropagation();
                            e.preventDefault();
                            handleInputChange('memory', !formData.memory);
                          }}
                          onChange={(e) => {
                            e.stopPropagation();
                          }}
                          onMouseDown={(e) => {
                            e.stopPropagation();
                          }}
                          onTouchStart={(e) => {
                            e.stopPropagation();
                          }}
                        />
                      }
                      label="Enable Memory"
                      onClick={(e) => {
                        e.stopPropagation();
                      }}
                    />
                  </Grid>
                  <Grid item xs={12} md={6}>
                    <FormControlLabel
                      control={
                        <Switch
                          checked={formData.verbose}
                          onClick={(e) => {
                            e.stopPropagation();
                            e.preventDefault();
                            handleInputChange('verbose', !formData.verbose);
                          }}
                          onChange={(e) => {
                            e.stopPropagation();
                          }}
                          onMouseDown={(e) => {
                            e.stopPropagation();
                          }}
                          onTouchStart={(e) => {
                            e.stopPropagation();
                          }}
                        />
                      }
                      label="Verbose Mode"
                      onClick={(e) => {
                        e.stopPropagation();
                      }}
                    />
                  </Grid>
                  <Grid item xs={12} md={6}>
                    <FormControlLabel
                      control={
                        <Switch
                          checked={formData.allow_delegation}
                          onClick={(e) => {
                            e.stopPropagation();
                            e.preventDefault();
                            handleInputChange('allow_delegation', !formData.allow_delegation);
                          }}
                          onChange={(e) => {
                            e.stopPropagation();
                          }}
                          onMouseDown={(e) => {
                            e.stopPropagation();
                          }}
                          onTouchStart={(e) => {
                            e.stopPropagation();
                          }}
                        />
                      }
                      label="Allow Delegation"
                      onClick={(e) => {
                        e.stopPropagation();
                      }}
                    />
                  </Grid>
                  <Grid item xs={12} md={6}>
                    <FormControlLabel
                      control={
                        <Switch
                          checked={formData.cache}
                          onClick={(e) => {
                            e.stopPropagation();
                            e.preventDefault();
                            handleInputChange('cache', !formData.cache);
                          }}
                          onChange={(e) => {
                            e.stopPropagation();
                          }}
                          onMouseDown={(e) => {
                            e.stopPropagation();
                          }}
                          onTouchStart={(e) => {
                            e.stopPropagation();
                          }}
                        />
                      }
                      label="Enable Cache"
                      onClick={(e) => {
                        e.stopPropagation();
                      }}
                    />
                  </Grid>
                  <Grid item xs={12} md={6}>
                    <FormControlLabel
                      control={
                        <Switch
                          checked={formData.allow_code_execution}
                          onClick={(e) => {
                            e.stopPropagation();
                            e.preventDefault();
                            handleInputChange('allow_code_execution', !formData.allow_code_execution);
                          }}
                          onChange={(e) => {
                            e.stopPropagation();
                          }}
                          onMouseDown={(e) => {
                            e.stopPropagation();
                          }}
                          onTouchStart={(e) => {
                            e.stopPropagation();
                          }}
                        />
                      }
                      label="Allow Code Execution"
                      onClick={(e) => {
                        e.stopPropagation();
                      }}
                    />
                  </Grid>
                  <Grid item xs={12} md={6}>
                    <FormControlLabel
                      control={
                        <Switch
                          checked={formData.use_system_prompt}
                          onClick={(e) => {
                            e.stopPropagation();
                            e.preventDefault();
                            handleInputChange('use_system_prompt', !formData.use_system_prompt);
                          }}
                          onChange={(e) => {
                            e.stopPropagation();
                          }}
                          onMouseDown={(e) => {
                            e.stopPropagation();
                          }}
                          onTouchStart={(e) => {
                            e.stopPropagation();
                          }}
                        />
                      }
                      label="Use System Prompt"
                      onClick={(e) => {
                        e.stopPropagation();
                      }}
                    />
                  </Grid>
                  <Grid item xs={12} md={6}>
                    <FormControlLabel
                      control={
                        <Switch
                          checked={formData.respect_context_window}
                          onClick={(e) => {
                            e.stopPropagation();
                            e.preventDefault();
                            handleInputChange('respect_context_window', !formData.respect_context_window);
                          }}
                          onChange={(e) => {
                            e.stopPropagation();
                          }}
                          onMouseDown={(e) => {
                            e.stopPropagation();
                          }}
                          onTouchStart={(e) => {
                            e.stopPropagation();
                          }}
                        />
                      }
                      label="Respect Context Window"
                      onClick={(e) => {
                        e.stopPropagation();
                      }}
                    />
                  </Grid>
                </Grid>
              </AccordionDetails>
            </Accordion>

            {/* Memory Configuration - NEW SECTION */}
            <Accordion>
              <AccordionSummary expandIcon={<ExpandMoreIcon />}>
                <Typography>Memory Configuration</Typography>
              </AccordionSummary>
              <AccordionDetails>
                <Grid container spacing={2}>
                  <Grid item xs={12}>
                    <Typography variant="subtitle2" sx={{ mb: 1 }}>
                      Memory provides agents with the ability to remember past interactions and context
                    </Typography>
                  </Grid>
                  
                  {/* Memory Embedding Provider */}
                  <Grid item xs={12} md={6}>
                    <FormControl fullWidth disabled={!formData.memory}>
                      <InputLabel>Embedding Provider</InputLabel>
                      <Select
                        value={formData.embedder_config?.provider || 'databricks'}
                        onChange={(e) => {
                          const currentConfig = formData.embedder_config || {};
                          const newProvider = e.target.value;
                          
                          // Set default model based on provider
                          let defaultModel = 'text-embedding-3-small';
                          if (newProvider === 'databricks') {
                            defaultModel = 'databricks-gte-large-en';
                          } else if (newProvider === 'ollama') {
                            defaultModel = 'nomic-embed-text';
                          } else if (newProvider === 'google') {
                            defaultModel = 'text-embedding-004';
                          }
                          
                          handleInputChange('embedder_config', {
                            ...currentConfig,
                            provider: newProvider,
                            config: {
                              ...((currentConfig as any).config || {}),
                              model: defaultModel
                            }
                          });
                        }}
                        label="Embedding Provider"
                      >
                        <MenuItem value="databricks">Databricks (Default)</MenuItem>
                        <MenuItem value="openai">OpenAI</MenuItem>
                        <MenuItem value="ollama">Ollama</MenuItem>
                        <MenuItem value="google">Google AI</MenuItem>
                        <MenuItem value="azure">Azure OpenAI</MenuItem>
                        <MenuItem value="vertex">Vertex AI</MenuItem>
                        <MenuItem value="cohere">Cohere</MenuItem>
                        <MenuItem value="voyage">VoyageAI</MenuItem>
                        <MenuItem value="huggingface">HuggingFace</MenuItem>
                        <MenuItem value="watson">Watson</MenuItem>
                        <MenuItem value="bedrock">Amazon Bedrock</MenuItem>
                      </Select>
                    </FormControl>
                  </Grid>

                  {/* Embedding Model */}
                  <Grid item xs={12} md={6}>
                    <TextField
                      fullWidth
                      label="Embedding Model"
                      value={(formData.embedder_config?.config as Record<string, unknown>)?.model || 'databricks-gte-large-en'}
                      onChange={(e) => {
                        const currentConfig = formData.embedder_config || { provider: 'databricks', config: { model: 'databricks-gte-large-en' } };
                        const currentInnerConfig = currentConfig.config || {};
                        handleInputChange('embedder_config', {
                          ...currentConfig,
                          config: {
                            ...currentInnerConfig,
                            model: e.target.value
                          }
                        });
                      }}
                      disabled={!formData.memory}
                      helperText="Model to use for text embeddings (e.g., databricks-gte-large-en for Databricks, text-embedding-3-small for OpenAI)"
                    />
                  </Grid>

                  {/* Customization Help */}
                  <Grid item xs={12}>
                    <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
                      The agent uses three memory types:
                    </Typography>
                    <ul style={{ color: 'rgba(0, 0, 0, 0.6)', paddingLeft: '20px', margin: '4px 0' }}>
                      <li>Short-Term Memory: Stores recent conversations and context</li>
                      <li>Long-Term Memory: Preserves insights and learnings between executions</li>
                      <li>Entity Memory: Tracks information about important entities</li>
                    </ul>
                    <Typography variant="body2" color="text.secondary">
                      Memory is stored in system-specific locations, which can be customized via the CREWAI_STORAGE_DIR environment variable.
                    </Typography>
                  </Grid>
                </Grid>
              </AccordionDetails>
            </Accordion>

            {/* Code Execution Mode */}
            <Accordion>
              <AccordionSummary expandIcon={<ExpandMoreIcon />}>
                <Typography>Code Execution Mode</Typography>
              </AccordionSummary>
              <AccordionDetails>
                <Grid container spacing={2}>
                  <Grid item xs={12} md={6}>
                    <FormControl fullWidth>
                      <InputLabel>Code Execution Mode</InputLabel>
                      <Select
                        value={formData.code_execution_mode}
                        onChange={(e) => handleInputChange('code_execution_mode', e.target.value)}
                        label="Code Execution Mode"
                        disabled={!formData.allow_code_execution}
                      >
                        <MenuItem key="safe-mode" value="safe">Safe (Docker)</MenuItem>
                        <MenuItem key="unsafe-mode" value="unsafe">Unsafe (Direct)</MenuItem>
                      </Select>
                    </FormControl>
                  </Grid>
                </Grid>
              </AccordionDetails>
            </Accordion>
          </Box>
        </Box>

        <Box 
          sx={{ 
            display: 'flex', 
            gap: 2, 
            justifyContent: 'flex-end', 
            p: 2,
            backgroundColor: 'white',
            borderTop: '1px solid rgba(0, 0, 0, 0.12)',
            position: 'static',
            width: '100%',
            zIndex: 1100
          }}
        >
          {onCancel && (
            <Button onClick={onCancel} variant="outlined">
              Cancel
            </Button>
          )}
          <Button 
            onClick={handleSubmit} 
            variant="contained" 
            color="primary"
          >
            Save
          </Button>
        </Box>
      </Card>

      {/* Goal Dialog */}
      <Dialog 
        open={expandedGoal} 
        onClose={handleCloseGoalDialog}
        fullWidth
        maxWidth="md"
      >
        <DialogTitle>
          <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            Agent Goal
            <IconButton onClick={handleCloseGoalDialog}>
              <CloseIcon />
            </IconButton>
          </Box>
        </DialogTitle>
        <DialogContent>
          <TextField
            autoFocus
            fullWidth
            multiline
            rows={15}
            value={formData.goal}
            onChange={(e) => handleInputChange('goal', e.target.value)}
            variant="outlined"
            sx={{ mt: 2 }}
          />
        </DialogContent>
        <DialogActions>
          <Button onClick={handleCloseGoalDialog} variant="contained">
            Done
          </Button>
        </DialogActions>
      </Dialog>

      {/* Backstory Dialog */}
      <Dialog 
        open={expandedBackstory} 
        onClose={handleCloseBackstoryDialog}
        fullWidth
        maxWidth="md"
      >
        <DialogTitle>
          <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            Agent Backstory
            <IconButton onClick={handleCloseBackstoryDialog}>
              <CloseIcon />
            </IconButton>
          </Box>
        </DialogTitle>
        <DialogContent>
          <TextField
            autoFocus
            fullWidth
            multiline
            rows={15}
            value={formData.backstory}
            onChange={(e) => handleInputChange('backstory', e.target.value)}
            variant="outlined"
            sx={{ mt: 2 }}
          />
        </DialogContent>
        <DialogActions>
          <Button onClick={handleCloseBackstoryDialog} variant="contained">
            Done
          </Button>
        </DialogActions>
      </Dialog>


    </>
  );
};

export default AgentForm; 