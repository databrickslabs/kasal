import React, { useState, useEffect, useCallback } from 'react';
import { 
  Box, 
  IconButton, 
  FormControl, 
  InputLabel, 
  Select, 
  MenuItem, 
  SelectChangeEvent, 
  Tooltip, 
  FormControlLabel, 
  Switch,
  CircularProgress,
  Menu,
  Collapse
} from '@mui/material';
import { useTranslation } from 'react-i18next';
import { Node, Edge } from 'reactflow';
import { useCrewExecutionStore } from '../../store/crewExecution';

// Icons
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import AutoFixHighIcon from '@mui/icons-material/AutoFixHigh';
import PersonAddIcon from '@mui/icons-material/PersonAdd';
import AddTaskIcon from '@mui/icons-material/AddTask';
import MenuBookIcon from '@mui/icons-material/MenuBook';
import _SaveIcon from '@mui/icons-material/Save';
import ScheduleIcon from '@mui/icons-material/Schedule';
import AssessmentIcon from '@mui/icons-material/Assessment';
import SettingsIcon from '@mui/icons-material/Settings';
import AccountTreeIcon from '@mui/icons-material/AccountTree';

// Components
import _SaveCrew from '../Crew/SaveCrew';

// Services and interfaces
import { Models } from '../../types/models';
import { ModelService } from '../../api/ModelService';

// Default fallback model when API is down
const DEFAULT_FALLBACK_MODEL = {
  'gpt-4o-mini': {
    name: 'gpt-4o-mini',
    temperature: 0.7,
    context_window: 128000,
    max_output_tokens: 4096,
    enabled: true
  }
};

interface WorkflowToolbarProps {
  selectedModel: string;
  setSelectedModel: (model: string) => void;
  planningEnabled: boolean;
  setPlanningEnabled: (enabled: boolean) => void;
  schemaDetectionEnabled: boolean;
  setSchemaDetectionEnabled: (enabled: boolean) => void;
  setIsScheduleDialogOpen: (open: boolean) => void;
  setIsAgentDialogOpen: (open: boolean) => void;
  setIsTaskDialogOpen: (open: boolean) => void;
  setIsFlowDialogOpen: (open: boolean) => void;
  setIsCrewPlanningOpen: (open: boolean) => void;
  setIsLogsDialogOpen: (open: boolean) => void;
  setIsConfigurationDialogOpen: (open: boolean) => void;
  setIsCrewDialogOpen: (open: boolean) => void;
  handleRunClick: (executionType?: 'crew' | 'flow') => Promise<void>;
  isRunning: boolean;
  nodes: Node[];
  edges: Edge[];
  saveCrewRef: React.RefObject<HTMLButtonElement>;
}

const WorkflowToolbar: React.FC<WorkflowToolbarProps> = ({
  selectedModel,
  setSelectedModel,
  planningEnabled,
  setPlanningEnabled,
  schemaDetectionEnabled,
  setSchemaDetectionEnabled,
  setIsScheduleDialogOpen,
  setIsAgentDialogOpen,
  setIsTaskDialogOpen,
  setIsFlowDialogOpen,
  setIsCrewPlanningOpen,
  setIsLogsDialogOpen,
  setIsConfigurationDialogOpen,
  setIsCrewDialogOpen,
  handleRunClick,
  isRunning,
  nodes,
  edges,
  saveCrewRef
}) => {
  const { t } = useTranslation();
  const [anchorEl, setAnchorEl] = useState<null | HTMLElement>(null);
  const open = Boolean(anchorEl);
  const [models, setModels] = useState<Models>(DEFAULT_FALLBACK_MODEL);
  const [isLoadingModels, setIsLoadingModels] = useState(true);
  const [planningModel, setPlanningModel] = useState<string>('');
  const { 
    executeCrew, 
    executeFlow,
    isExecuting,
    errorMessage,
    showError,
    successMessage,
    showSuccess,
    setShowError,
    setShowSuccess,
    setPlanningLLM,
    setErrorMessage,
  } = useCrewExecutionStore();
  
  const handleMenuClose = useCallback(() => {
    setAnchorEl(null);
  }, []);

  // Fetch models only once on component mount
  useEffect(() => {
    // Use a ref to track if we've already set a default model to prevent loops
    let hasSetDefaultModel = false;
    
    const fetchModels = async () => {
      setIsLoadingModels(true);
      try {
        const modelService = ModelService.getInstance();
        const response = await modelService.getEnabledModels();
        setModels(response);
        
        // Only set the default model if no model is selected and we haven't set one yet
        if (response && Object.keys(response).length > 0 && !selectedModel && !hasSetDefaultModel) {
          const firstModel = Object.keys(response)[0];
          setSelectedModel(firstModel);
          hasSetDefaultModel = true;
        }

        // Initialize planning model when models are loaded
        if (response && Object.keys(response).length > 0 && !planningModel) {
          const firstModel = Object.keys(response)[0];
          setPlanningModel(firstModel);
          setPlanningLLM(firstModel);
        }
      } catch (error) {
        console.error('Error fetching models:', error);
        // Set default fallback model if API fails
        if (!selectedModel && Object.keys(DEFAULT_FALLBACK_MODEL).length > 0 && !hasSetDefaultModel) {
          setSelectedModel(Object.keys(DEFAULT_FALLBACK_MODEL)[0]);
          hasSetDefaultModel = true;
        }
      } finally {
        setIsLoadingModels(false);
      }
    };
    
    fetchModels();
  }, [selectedModel, setSelectedModel, planningModel, setPlanningLLM]);

  // Memoize the valid select value to prevent unnecessary re-renders
  const validSelectValue = React.useMemo(() => {
    if (isLoadingModels || Object.keys(models).length === 0) {
      return '';
    }
    return selectedModel && models[selectedModel] ? selectedModel : '';
  }, [isLoadingModels, models, selectedModel]);

  const handleModelSelectChange = React.useCallback((event: SelectChangeEvent) => {
    setSelectedModel(event.target.value);
  }, [setSelectedModel]);

  const handlePlanningModelChange = React.useCallback((event: SelectChangeEvent) => {
    const value = event.target.value;
    setPlanningModel(value);
    setPlanningLLM(value);
  }, [setPlanningLLM]);

  const handleExecuteCrew = useCallback(() => {
    executeCrew(nodes, edges);
  }, [executeCrew, nodes, edges]);
  
  const handleExecuteFlow = useCallback(async () => {
    handleMenuClose();
    
    try {
      // Check if there are nodes on the canvas first
      if (nodes.length === 0) {
        console.error('[WorkflowToolbar] Cannot execute flow: No nodes on canvas');
        setErrorMessage('Cannot execute flow: No nodes on canvas');
        setShowError(true);
        return;
      }

      // Count node types on canvas for debugging
      const nodeTypes = nodes.reduce((acc: Record<string, number>, node) => {
        const type = node.type || 'unknown';
        acc[type] = (acc[type] || 0) + 1;
        return acc;
      }, {} as Record<string, number>);
      
      console.log('[WorkflowToolbar] Node types on canvas before execution:', nodeTypes);
      
      // Execute the flow
      console.log('[WorkflowToolbar] Executing flow with nodes:', nodes.length, 'edges:', edges.length);
      await executeFlow(nodes, edges);
      console.log('[WorkflowToolbar] Flow execution initiated successfully');
    } catch (error) {
      console.error('[WorkflowToolbar] Error executing flow:', error);
      // Display error to user
      if (error instanceof Error) {
        setErrorMessage(`Flow execution failed: ${error.message}`);
      } else {
        setErrorMessage('Flow execution failed with an unknown error');
      }
      setShowError(true);
    }
  }, [executeFlow, nodes, edges, handleMenuClose, setErrorMessage, setShowError]);

  // Handle click to open execution menu
  const handleExecuteClick = (event: React.MouseEvent<HTMLButtonElement>) => {
    setAnchorEl(event.currentTarget);
  };

  // Add error and success message handling
  useEffect(() => {
    if (showError) {
      // Show error message using your preferred notification system
      console.error(errorMessage);
      setShowError(false);
    }
  }, [showError, errorMessage, setShowError]);

  useEffect(() => {
    if (showSuccess) {
      // Show success message using your preferred notification system
      console.log(successMessage);
      setShowSuccess(false);
    }
  }, [showSuccess, successMessage, setShowSuccess]);

  return (
    <Box sx={{ 
      display: 'flex', 
      justifyContent: 'space-between', 
      p: 1.5,
      borderBottom: '1px solid',
      borderColor: 'divider',
      bgcolor: 'background.paper',
      position: 'fixed',
      top: 0,
      left: 0,
      right: 0,
      zIndex: 1000
    }}>
      <Box sx={{ display: 'flex', gap: 2, alignItems: 'center' }}>
        <Tooltip title={t('nemo.buttons.configuration')}>
          <IconButton
            onClick={() => setIsConfigurationDialogOpen(true)}
            size="small"
            sx={{
              border: '1px solid rgba(0, 0, 0, 0.12)',
              borderRadius: 1,
              p: 1,
              '&:hover': {
                bgcolor: 'rgba(0, 0, 0, 0.04)'
              }
            }}
          >
            <SettingsIcon sx={{ fontSize: 20 }} />
          </IconButton>
        </Tooltip>
        <FormControl size="small" sx={{ minWidth: 180 }}>
          <InputLabel>{t('nemo.model.label')}</InputLabel>
          <Select
            value={validSelectValue}
            onChange={handleModelSelectChange}
            label={t('nemo.model.label')}
            disabled={isLoadingModels}
            renderValue={(selected: string) => {
              const model = models[selected];
              return model ? model.name : selected;
            }}
          >
            {isLoadingModels ? (
              <MenuItem value="">
                <CircularProgress size={20} />
              </MenuItem>
            ) : Object.keys(models).length === 0 ? (
              <MenuItem value="">No models available</MenuItem>
            ) : (
              Object.entries(models).map(([key, model]) => (
                <MenuItem 
                  key={key} 
                  value={key}
                  sx={{
                    display: 'flex',
                    justifyContent: 'space-between',
                    gap: 1
                  }}
                >
                  <span>{model.name}</span>
                  {model.provider && (
                    <span style={{ fontSize: '0.8em' }}>{model.provider}</span>
                  )}
                </MenuItem>
              ))
            )}
          </Select>
        </FormControl>
        <Tooltip title={t('nemo.buttons.logs')}>
          <IconButton
            onClick={() => setIsLogsDialogOpen(true)}
            size="small"
            sx={{
              border: '1px solid rgba(0, 0, 0, 0.12)',
              borderRadius: 1,
              p: 1,
              '&:hover': {
                bgcolor: 'rgba(0, 0, 0, 0.04)'
              }
            }}
          >
            <AssessmentIcon sx={{ fontSize: 20 }} />
          </IconButton>
        </Tooltip>
      </Box>

      {/* Center section */}
      <Box sx={{ display: 'flex', gap: 1.5, alignItems: 'center' }}>
        <Tooltip title={t('nemo.planning.tooltip') || 'Enable planning mode'}>
          <FormControlLabel
            control={
              <Switch
                checked={planningEnabled}
                onChange={(e: React.ChangeEvent<HTMLInputElement>) => setPlanningEnabled(e.target.checked)}
                size="small"
              />
            }
            label={t('nemo.planning.label') || 'Planning'}
            sx={{ 
              mr: 1,
              '& .MuiFormControlLabel-label': {
                fontSize: '0.875rem'
              }
            }}
          />
        </Tooltip>

        <Collapse in={planningEnabled} orientation="horizontal">
          <FormControl size="small" sx={{ minWidth: 150, ml: 1 }}>
            <InputLabel>Planning LLM</InputLabel>
            <Select
              value={planningModel}
              onChange={handlePlanningModelChange}
              label="Planning LLM"
              disabled={isLoadingModels}
              renderValue={(selected: string) => {
                const model = models[selected];
                return model ? model.name : selected;
              }}
            >
              {isLoadingModels ? (
                <MenuItem value="">
                  <CircularProgress size={20} />
                </MenuItem>
              ) : Object.keys(models).length === 0 ? (
                <MenuItem value="">No models available</MenuItem>
              ) : (
                Object.entries(models).map(([key, model]) => (
                  <MenuItem 
                    key={key} 
                    value={key}
                    sx={{
                      display: 'flex',
                      justifyContent: 'space-between',
                      gap: 1
                    }}
                  >
                    <span>{model.name}</span>
                    {model.provider && (
                      <span style={{ fontSize: '0.8em' }}>{model.provider}</span>
                    )}
                  </MenuItem>
                ))
              )}
            </Select>
          </FormControl>
        </Collapse>

        <FormControlLabel
          control={
            <Switch
              checked={schemaDetectionEnabled}
              onChange={(e: React.ChangeEvent<HTMLInputElement>) => setSchemaDetectionEnabled(e.target.checked)}
              size="small"
            />
          }
          label="Schema Detection"
          sx={{ 
            ml: 1,
            '& .MuiFormControlLabel-label': {
              fontSize: '0.875rem'
            }
          }}
        />

        <Box sx={{ height: 24, mx: 1, borderLeft: '1px solid rgba(0, 0, 0, 0.12)' }} />

        <Tooltip title={t('nemo.buttons.schedule')}>
          <IconButton 
            onClick={() => setIsScheduleDialogOpen(true)}
            size="small"
            sx={{ 
              border: '1px solid rgba(0, 0, 0, 0.12)',
              borderRadius: 1,
              p: 1,
              '&:hover': {
                bgcolor: 'rgba(0, 0, 0, 0.04)'
              }
            }}
          >
            <ScheduleIcon sx={{ fontSize: 20 }} />
          </IconButton>
        </Tooltip>

        <Box sx={{ height: 24, mx: 1, borderLeft: '1px solid rgba(0, 0, 0, 0.12)' }} />

        <Tooltip title={t('nemo.buttons.addAgent')}>
          <span>
            <IconButton 
              onClick={() => setIsAgentDialogOpen(true)} 
              size="small"
              sx={{ 
                border: '1px solid rgba(0, 0, 0, 0.12)',
                borderRadius: 1,
                p: 1,
                '&:hover': {
                  bgcolor: 'rgba(0, 0, 0, 0.04)'
                }
              }}
              data-tour="add-agent"
            >
              <PersonAddIcon sx={{ fontSize: 20 }} />
            </IconButton>
          </span>
        </Tooltip>
        
        <Tooltip title={t('nemo.buttons.addTask')}>
          <span>
            <IconButton 
              onClick={() => setIsTaskDialogOpen(true)} 
              size="small"
              sx={{ 
                border: '1px solid rgba(0, 0, 0, 0.12)',
                borderRadius: 1,
                p: 1,
                '&:hover': {
                  bgcolor: 'rgba(0, 0, 0, 0.04)'
                }
              }}
              data-tour="add-task"
            >
              <AddTaskIcon sx={{ fontSize: 20 }} />
            </IconButton>
          </span>
        </Tooltip>

        <Tooltip title={"Add Flow"}>
          <IconButton 
            onClick={() => setIsFlowDialogOpen(true)} 
            size="small"
            sx={{ 
              border: '1px solid rgba(0, 0, 0, 0.12)',
              borderRadius: 1,
              p: 1,
              '&:hover': {
                bgcolor: 'rgba(0, 0, 0, 0.04)'
              }
            }}
          >
            <AccountTreeIcon sx={{ fontSize: 20 }} />
          </IconButton>
        </Tooltip>

        <Box sx={{ height: 24, mx: 1, borderLeft: '1px solid rgba(0, 0, 0, 0.12)' }} />

        <Tooltip title={t('nemo.buttons.generateCrew') || 'Generate Crew'}>
          <IconButton 
            onClick={() => setIsCrewPlanningOpen(true)}
            size="small"
            sx={{ 
              border: '1px solid #2E3B55',
              borderRadius: 1,
              p: 1,
              bgcolor: '#1976d2',
              color: 'white',
              '&:hover': {
                bgcolor: '#1a2337',
              },
            }}
            data-tour="generate-crew"
          >
            <AutoFixHighIcon sx={{ fontSize: 20 }} />
          </IconButton>
        </Tooltip>

        <Box sx={{ height: 24, mx: 1, borderLeft: '1px solid rgba(0, 0, 0, 0.12)' }} />

        <div>
          <Tooltip title={t('nemo.buttons.execute')} disableHoverListener={open}>
            <span>
              <IconButton
                onClick={handleExecuteClick}
                disabled={isExecuting}
                size="small"
                sx={{ 
                  border: '1px solid #2E3B55',
                  borderRadius: 1,
                  p: 1,
                  bgcolor: '#1976d2',
                  color: 'white',
                  '&:hover': {
                    bgcolor: '#1a2337',
                  },
                }}
              >
                {isExecuting ? (
                  <CircularProgress size={20} sx={{ color: 'white' }} />
                ) : (
                  <Box sx={{ display: 'flex', alignItems: 'center' }}>
                    <PlayArrowIcon sx={{ fontSize: 20 }} />
                  </Box>
                )}
              </IconButton>
            </span>
          </Tooltip>
          
          <Menu
            id="execution-menu"
            anchorEl={anchorEl}
            open={open}
            onClose={handleMenuClose}
            MenuListProps={{
              'aria-labelledby': 'execute-button',
            }}
          >
            <MenuItem onClick={handleExecuteCrew}>Execute Crew</MenuItem>
            <MenuItem onClick={handleExecuteFlow}>Execute Flow</MenuItem>
          </Menu>
        </div>

        <Tooltip title={t('nemo.buttons.openCrew') || 'Open Crew or Flow'}>
          <IconButton
            onClick={() => setIsCrewDialogOpen(true)}
            size="small"
            sx={{
              border: '1px solid rgba(0, 0, 0, 0.12)',
              borderRadius: 1,
              p: 1,
              '&:hover': { backgroundColor: 'action.hover' }
            }}
            data-tour="load-crew"
          >
            <MenuBookIcon sx={{ fontSize: 20 }} />
          </IconButton>
        </Tooltip>

        <Tooltip title={t('nemo.buttons.saveCrew') || 'Save Crew'}>
          <span>
            <IconButton
              ref={saveCrewRef}
              size="small"
              sx={{
                border: '1px solid rgba(0, 0, 0, 0.12)',
                borderRadius: 1,
                p: 1,
                '&:hover': { backgroundColor: 'action.hover' }
              }}
              data-tour="save-crew"
              onClick={() => {
                // Trigger the save crew dialog via an event
                const event = new CustomEvent('openSaveCrewDialog');
                window.dispatchEvent(event);
              }}
            >
              <_SaveIcon sx={{ fontSize: 20 }} />
            </IconButton>
          </span>
        </Tooltip>
      </Box>
    </Box>
  );
};

export default WorkflowToolbar; 