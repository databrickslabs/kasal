import React, { useState, useMemo, useEffect, useCallback } from 'react';
import {
  Box,
  IconButton,
  Tooltip,
  Divider,
  Paper,
  useTheme,
  Typography,
  alpha,
  Switch,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  CircularProgress,
  SelectChangeEvent
} from '@mui/material';
import {
  CleaningServices as ClearIcon,
  ZoomIn as ZoomInIcon,
  ZoomOut as ZoomOutIcon,
  CenterFocusStrong as FitViewIcon,
  AccountTree as GenerateConnectionsIcon,
  Keyboard as KeyboardIcon,
  Tune as TuneIcon,
  Memory as ModelIcon,
  Settings as SettingsIcon,
} from '@mui/icons-material';

import { DEFAULT_SHORTCUTS } from '../../hooks/global/useShortcuts';
import { ShortcutConfig } from '../../types/shortcuts';
import { Models } from '../../types/models';
import { ModelService } from '../../api/ModelService';
import { useCrewExecutionStore } from '../../store/crewExecution';

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

interface LeftSidebarProps {
  onClearCanvas: () => void;
  onGenerateConnections: () => Promise<void>;
  onZoomIn: () => void;
  onZoomOut: () => void;
  onFitView: () => void;
  onToggleInteractivity: () => void;
  isGeneratingConnections: boolean;
  // Runtime features props
  planningEnabled: boolean;
  setPlanningEnabled: (enabled: boolean) => void;
  reasoningEnabled: boolean;
  setReasoningEnabled: (enabled: boolean) => void;
  schemaDetectionEnabled: boolean;
  setSchemaDetectionEnabled: (enabled: boolean) => void;
  // Model selection props
  selectedModel: string;
  setSelectedModel: (model: string) => void;
  // New prop for configuration
  setIsConfigurationDialogOpen?: (open: boolean) => void;
  // Execution history visibility
  showRunHistory?: boolean;
}

const LeftSidebar: React.FC<LeftSidebarProps> = ({
  onClearCanvas,
  onGenerateConnections,
  onZoomIn,
  onZoomOut,
  onFitView,
  onToggleInteractivity,
  isGeneratingConnections,
  planningEnabled,
  setPlanningEnabled,
  reasoningEnabled,
  setReasoningEnabled,
  schemaDetectionEnabled,
  setSchemaDetectionEnabled,
  selectedModel,
  setSelectedModel,
  setIsConfigurationDialogOpen,
  showRunHistory
}) => {
  const theme = useTheme();
  const [activeSection, setActiveSection] = useState<string | null>(null);
  const [models, setModels] = useState<Models>(DEFAULT_FALLBACK_MODEL);
  const [isLoadingModels, setIsLoadingModels] = useState(true);
  const [planningModel, setPlanningModel] = useState<string>('');
  const [reasoningModel, setReasoningModel] = useState<string>('');
  
  const { 
    setPlanningLLM,
    setReasoningLLM,
  } = useCrewExecutionStore();

  // Fetch models on component mount
  useEffect(() => {
    const fetchModels = async () => {
      setIsLoadingModels(true);
      try {
        const modelService = ModelService.getInstance();
        const response = await modelService.getEnabledModels();
        setModels(response);
        
        // Initialize planning model when models are loaded
        if (response && Object.keys(response).length > 0 && !planningModel) {
          const firstModel = Object.keys(response)[0];
          setPlanningModel(firstModel);
          setPlanningLLM(firstModel);
        }

        // Initialize reasoning model when models are loaded
        if (response && Object.keys(response).length > 0 && !reasoningModel) {
          const firstModel = Object.keys(response)[0];
          setReasoningModel(firstModel);
          setReasoningLLM(firstModel);
        }
      } catch (error) {
        console.error('Error fetching models:', error);
      } finally {
        setIsLoadingModels(false);
      }
    };
    
    fetchModels();
  }, [planningModel, setPlanningLLM, reasoningModel, setReasoningLLM]);

  const handlePlanningModelChange = useCallback((event: SelectChangeEvent) => {
    const value = event.target.value;
    setPlanningModel(value);
    setPlanningLLM(value);
  }, [setPlanningLLM]);

  const handleReasoningModelChange = useCallback((event: SelectChangeEvent) => {
    const value = event.target.value;
    setReasoningModel(value);
    setReasoningLLM(value);
  }, [setReasoningLLM]);

  const handleMainModelChange = useCallback((event: SelectChangeEvent) => {
    setSelectedModel(event.target.value);
  }, [setSelectedModel]);

  // Group shortcuts by category
  const groupedShortcuts = useMemo(() => {
    const result: Record<string, ShortcutConfig[]> = {
      'Canvas': [],
      'Creation': [],
      'Execution': [],
      'Management': []
    };

    DEFAULT_SHORTCUTS.forEach(shortcut => {
      const action = shortcut.action;
      
      if (action.includes('zoom') || action.includes('fit') || action.includes('clear') || 
          action.includes('delete') || action.includes('select') || action === 'undo' || 
          action === 'redo' || action === 'copy' || action === 'paste') {
        result['Canvas'].push(shortcut);
      } else if (action.includes('open') || action.includes('generate')) {
        result['Creation'].push(shortcut);
      } else if (action.includes('execute')) {
        result['Execution'].push(shortcut);
      } else {
        result['Management'].push(shortcut);
      }
    });

    return result;
  }, []);

  const sidebarItems = [
    {
      id: 'configuration',
      icon: <SettingsIcon />,
      tooltip: 'Configuration',
      content: null // No expandable content, handled by direct click
    },
    {
      id: 'model-selection',
      icon: <ModelIcon />,
      tooltip: 'Model Selection',
      content: (
        <Box
          sx={{
            maxHeight: showRunHistory ? 'calc(100vh - 48px - 200px - 20px)' : 'calc(100vh - 48px - 20px)',
            overflowY: 'auto',
            p: 1,
          }}
        >
          <Box sx={{ mb: 2 }}>
            <Typography 
              variant="subtitle2" 
              sx={{ 
                color: theme.palette.primary.main, 
                mb: 1,
                fontWeight: 600,
                textTransform: 'uppercase',
                letterSpacing: '0.5px',
                fontSize: '0.7rem'
              }}
            >
              Main Model
            </Typography>
            <Divider sx={{ mb: 1 }} />
            
            <FormControl size="small" fullWidth>
              <InputLabel sx={{ fontSize: '0.75rem' }}>Selected Model</InputLabel>
              <Select
                value={selectedModel}
                onChange={handleMainModelChange}
                label="Selected Model"
                disabled={isLoadingModels}
                sx={{ fontSize: '0.75rem' }}
                renderValue={(selected: string) => {
                  const model = models[selected];
                  return model ? model.name : selected;
                }}
              >
                {isLoadingModels ? (
                  <MenuItem value="">
                    <CircularProgress size={16} />
                  </MenuItem>
                ) : Object.keys(models).length === 0 ? (
                  <MenuItem value="">No models available</MenuItem>
                ) : (
                  Object.entries(models).map(([key, model]) => (
                    <MenuItem key={key} value={key} sx={{ fontSize: '0.75rem' }}>
                      <Box sx={{ display: 'flex', flexDirection: 'column', width: '100%' }}>
                        <span>{model.name}</span>
                        {model.provider && (
                          <span style={{ fontSize: '0.65rem', opacity: 0.7 }}>{model.provider}</span>
                        )}
                      </Box>
                    </MenuItem>
                  ))
                )}
              </Select>
            </FormControl>
          </Box>
        </Box>
      )
    },
    {
      id: 'runtime-features',
      icon: <TuneIcon />,
      tooltip: 'Runtime Features',
      content: (
        <Box
          sx={{
            maxHeight: showRunHistory ? 'calc(100vh - 48px - 200px - 20px)' : 'calc(100vh - 48px - 20px)',
            overflowY: 'auto',
            p: 1,
          }}
        >
          {/* Planning Section */}
          <Box sx={{ mb: 2 }}>
            <Typography 
              variant="subtitle2" 
              sx={{ 
                color: theme.palette.primary.main, 
                mb: 1,
                fontWeight: 600,
                textTransform: 'uppercase',
                letterSpacing: '0.5px',
                fontSize: '0.7rem'
              }}
            >
              Planning
            </Typography>
            <Divider sx={{ mb: 1 }} />
            
            <Box
              sx={{
                display: 'flex',
                flexDirection: 'column',
                gap: 1,
                py: 0.5,
                px: 0.5,
                borderRadius: 1,
              }}
            >
              <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                <Typography variant="caption" sx={{ color: 'text.primary', fontSize: '0.75rem' }}>
                  Crew Level Planning
                </Typography>
                <Switch
                  checked={planningEnabled}
                  onChange={(e) => setPlanningEnabled(e.target.checked)}
                  size="small"
                />
              </Box>
              
              {planningEnabled && (
                <FormControl size="small" fullWidth sx={{ mt: 1 }}>
                  <InputLabel sx={{ fontSize: '0.75rem' }}>Planning LLM</InputLabel>
                  <Select
                    value={planningModel}
                    onChange={handlePlanningModelChange}
                    label="Planning LLM"
                    disabled={isLoadingModels}
                    sx={{ fontSize: '0.75rem' }}
                    renderValue={(selected: string) => {
                      const model = models[selected];
                      return model ? model.name : selected;
                    }}
                  >
                    {isLoadingModels ? (
                      <MenuItem value="">
                        <CircularProgress size={16} />
                      </MenuItem>
                    ) : Object.keys(models).length === 0 ? (
                      <MenuItem value="">No models available</MenuItem>
                    ) : (
                      Object.entries(models).map(([key, model]) => (
                        <MenuItem key={key} value={key} sx={{ fontSize: '0.75rem' }}>
                          <span>{model.name}</span>
                        </MenuItem>
                      ))
                    )}
                  </Select>
                </FormControl>
              )}
            </Box>
          </Box>

          {/* Reasoning Section */}
          <Box sx={{ mb: 2 }}>
            <Typography 
              variant="subtitle2" 
              sx={{ 
                color: theme.palette.primary.main, 
                mb: 1,
                fontWeight: 600,
                textTransform: 'uppercase',
                letterSpacing: '0.5px',
                fontSize: '0.7rem'
              }}
            >
              Reasoning
            </Typography>
            <Divider sx={{ mb: 1 }} />
            
            <Box
              sx={{
                display: 'flex',
                flexDirection: 'column',
                gap: 1,
                py: 0.5,
                px: 0.5,
                borderRadius: 1,
              }}
            >
              <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                <Typography variant="caption" sx={{ color: 'text.primary', fontSize: '0.75rem' }}>
                  Agent Level Reasoning
                </Typography>
                <Switch
                  checked={reasoningEnabled}
                  onChange={(e) => setReasoningEnabled(e.target.checked)}
                  size="small"
                />
              </Box>
              
              {reasoningEnabled && (
                <FormControl size="small" fullWidth sx={{ mt: 1 }}>
                  <InputLabel sx={{ fontSize: '0.75rem' }}>Reasoning LLM</InputLabel>
                  <Select
                    value={reasoningModel}
                    onChange={handleReasoningModelChange}
                    label="Reasoning LLM"
                    disabled={isLoadingModels}
                    sx={{ fontSize: '0.75rem' }}
                    renderValue={(selected: string) => {
                      const model = models[selected];
                      return model ? model.name : selected;
                    }}
                  >
                    {isLoadingModels ? (
                      <MenuItem value="">
                        <CircularProgress size={16} />
                      </MenuItem>
                    ) : Object.keys(models).length === 0 ? (
                      <MenuItem value="">No models available</MenuItem>
                    ) : (
                      Object.entries(models).map(([key, model]) => (
                        <MenuItem key={key} value={key} sx={{ fontSize: '0.75rem' }}>
                          <span>{model.name}</span>
                        </MenuItem>
                      ))
                    )}
                  </Select>
                </FormControl>
              )}
            </Box>
          </Box>

          {/* Schema Detection Section */}
          <Box sx={{ mb: 2 }}>
            <Typography 
              variant="subtitle2" 
              sx={{ 
                color: theme.palette.primary.main, 
                mb: 1,
                fontWeight: 600,
                textTransform: 'uppercase',
                letterSpacing: '0.5px',
                fontSize: '0.7rem'
              }}
            >
              Schema Detection
            </Typography>
            <Divider sx={{ mb: 1 }} />
            
            <Box
              sx={{
                display: 'flex',
                flexDirection: 'column',
                gap: 1,
                py: 0.5,
                px: 0.5,
                borderRadius: 1,
              }}
            >
              <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                <Typography variant="caption" sx={{ color: 'text.primary', fontSize: '0.75rem' }}>
                  Auto Schema Detection
                </Typography>
                <Switch
                  checked={schemaDetectionEnabled}
                  onChange={(e) => setSchemaDetectionEnabled(e.target.checked)}
                  size="small"
                />
              </Box>
            </Box>
          </Box>
        </Box>
      )
    },
    {
      id: 'shortcuts',
      icon: <KeyboardIcon />,
      tooltip: 'Keyboard Shortcuts',
      content: (
        <Box
          sx={{
            maxHeight: showRunHistory ? 'calc(100vh - 48px - 200px - 20px)' : 'calc(100vh - 48px - 20px)',
            overflowY: 'auto',
            p: 1,
          }}
        >
          {Object.entries(groupedShortcuts).map(([category, shortcuts]) => {
            if (shortcuts.length === 0) return null;
            
            return (
              <Box key={category} sx={{ mb: 2 }}>
                <Typography 
                  variant="subtitle2" 
                  sx={{ 
                    color: theme.palette.primary.main, 
                    mb: 1,
                    fontWeight: 600,
                    textTransform: 'uppercase',
                    letterSpacing: '0.5px',
                    fontSize: '0.7rem'
                  }}
                >
                  {category}
                </Typography>
                <Divider sx={{ mb: 1 }} />
                
                {shortcuts.map((shortcut, index) => (
                  <Box
                    key={`${shortcut.action}-${index}`}
                    sx={{
                      display: 'flex',
                      flexDirection: 'column',
                      gap: 0.5,
                      mb: 1,
                      py: 0.5,
                      px: 0.5,
                      borderRadius: 1,
                      '&:hover': {
                        backgroundColor: 'action.hover',
                      },
                    }}
                  >
                    <Typography 
                      variant="caption" 
                      sx={{ 
                        color: 'text.primary',
                        fontSize: '0.65rem',
                        lineHeight: 1.2
                      }}
                    >
                      {shortcut.description}
                    </Typography>
                    <Box 
                      sx={{ 
                        display: 'flex', 
                        gap: 0.25,
                        flexWrap: 'wrap'
                      }}
                    >
                      {shortcut.keys.map((key: string, keyIndex: number) => (
                        <Typography
                          key={`${key}-${keyIndex}`}
                          variant="caption"
                          component="span"
                          sx={{
                            fontFamily: 'monospace',
                            backgroundColor: alpha(theme.palette.primary.main, 0.1),
                            color: theme.palette.primary.main,
                            px: 0.4,
                            py: 0.2,
                            borderRadius: 0.5,
                            fontWeight: 500,
                            border: `1px solid ${alpha(theme.palette.primary.main, 0.2)}`,
                            fontSize: '0.6rem',
                            minWidth: '1rem',
                            textAlign: 'center',
                            display: 'inline-block'
                          }}
                        >
                          {key === 'Control' ? 'Ctrl' : key === ' ' ? 'Space' : key}
                        </Typography>
                      ))}
                    </Box>
                  </Box>
                ))}
              </Box>
            );
          })}
        </Box>
      )
    }
  ];

  const handleSectionClick = (sectionId: string) => {
    if (sectionId === 'configuration') {
      // Directly open configuration dialog instead of expanding section
      setIsConfigurationDialogOpen && setIsConfigurationDialogOpen(true);
      return;
    }
    setActiveSection(activeSection === sectionId ? null : sectionId);
  };

  return (
    <Box
      sx={{
        position: 'absolute',
        top: '48px', // Account for TabBar height
        left: 0,
        height: showRunHistory ? 'calc(100% - 48px - 200px)' : 'calc(100% - 48px)',
        zIndex: 10,
        display: 'flex',
        flexDirection: 'row'
      }}
    >
          {/* Activity Bar (like VS Code) */}
          <Paper
            elevation={0}
            sx={{
              width: 48,
              height: '100%',
              bgcolor: 'background.paper',
              borderRadius: 0,
              borderRight: '1px solid',
              borderColor: 'divider',
              display: 'flex',
              flexDirection: 'column',
              alignItems: 'center',
              py: 1
            }}
          >
            {sidebarItems.map((item) => (
              <React.Fragment key={item.id}>
                <Tooltip title={item.tooltip} placement="right">
                  <IconButton
                    onMouseEnter={() => {
                      // Don't set active section for configuration since it opens a dialog
                      if (item.id !== 'configuration') {
                        setActiveSection(item.id);
                      }
                    }}
                    onClick={() => handleSectionClick(item.id)}
                    sx={{
                      width: 40,
                      height: 40,
                      mb: 1,
                      color: 'text.secondary',
                      backgroundColor: activeSection === item.id 
                        ? 'action.selected'
                        : 'transparent',
                      borderLeft: activeSection === item.id 
                        ? `2px solid ${theme.palette.primary.main}`
                        : '2px solid transparent',
                      borderRadius: 0,
                      transition: 'all 0.2s ease-in-out',
                      '&:hover': {
                        backgroundColor: 'action.hover',
                        color: 'text.primary'
                      }
                    }}
                  >
                    {item.icon}
                  </IconButton>
                </Tooltip>
                {/* Add separator after Configuration */}
                {item.id === 'configuration' && (
                  <Box
                    sx={{
                      width: '80%',
                      height: '1px',
                      backgroundColor: 'divider',
                      mb: 1,
                      alignSelf: 'center'
                    }}
                  />
                )}
                {/* Insert action icons right after the Runtime Features */}
                {item.id === 'runtime-features' && (
                  <>
                    {/* Separator */}
                    <Box
                      sx={{
                        width: '80%',
                        height: '1px',
                        backgroundColor: 'divider',
                        mb: 1,
                        alignSelf: 'center'
                      }}
                    />
                    
                    <Tooltip title="Clear Canvas" placement="right">
                      <IconButton
                        onClick={onClearCanvas}
                        sx={{
                          width: 40,
                          height: 40,
                          mb: 1,
                          color: 'text.secondary',
                          borderRadius: 0,
                          transition: 'all 0.2s ease-in-out',
                          '&:hover': {
                            backgroundColor: 'action.hover',
                            color: 'text.primary'
                          }
                        }}
                      >
                        <ClearIcon fontSize="small" />
                      </IconButton>
                    </Tooltip>
                    <Tooltip title="Fit View" placement="right">
                      <IconButton
                        onClick={onFitView}
                        sx={{
                          width: 40,
                          height: 40,
                          mb: 1,
                          color: 'text.secondary',
                          borderRadius: 0,
                          transition: 'all 0.2s ease-in-out',
                          '&:hover': {
                            backgroundColor: 'action.hover',
                            color: 'text.primary'
                          }
                        }}
                      >
                        <FitViewIcon fontSize="small" />
                      </IconButton>
                    </Tooltip>
                    <Tooltip title="Zoom In" placement="right">
                      <IconButton
                        onClick={onZoomIn}
                        sx={{
                          width: 40,
                          height: 40,
                          mb: 1,
                          color: 'text.secondary',
                          borderRadius: 0,
                          transition: 'all 0.2s ease-in-out',
                          '&:hover': {
                            backgroundColor: 'action.hover',
                            color: 'text.primary'
                          }
                        }}
                      >
                        <ZoomInIcon fontSize="small" />
                      </IconButton>
                    </Tooltip>
                    <Tooltip title="Zoom Out" placement="right">
                      <IconButton
                        onClick={onZoomOut}
                        sx={{
                          width: 40,
                          height: 40,
                          mb: 1,
                          color: 'text.secondary',
                          borderRadius: 0,
                          transition: 'all 0.2s ease-in-out',
                          '&:hover': {
                            backgroundColor: 'action.hover',
                            color: 'text.primary'
                          }
                        }}
                      >
                        <ZoomOutIcon fontSize="small" />
                      </IconButton>
                    </Tooltip>
                    <Tooltip title="Generate Connections" placement="right">
                      <IconButton
                        onClick={onGenerateConnections}
                        disabled={isGeneratingConnections}
                        sx={{
                          width: 40,
                          height: 40,
                          mb: 1,
                          color: 'text.secondary',
                          borderRadius: 0,
                          transition: 'all 0.2s ease-in-out',
                          '&:hover': {
                            backgroundColor: 'action.hover',
                            color: 'text.primary'
                          }
                        }}
                      >
                        <GenerateConnectionsIcon fontSize="small" />
                      </IconButton>
                    </Tooltip>

                    {/* Separator before Keyboard Shortcuts */}
                    <Box
                      sx={{
                        width: '80%',
                        height: '1px',
                        backgroundColor: 'divider',
                        mb: 1,
                        alignSelf: 'center'
                      }}
                    />
                  </>
                )}
              </React.Fragment>
            ))}
          </Paper>

          {/* Side Panel Content */}
          {activeSection && (
            <Paper
              elevation={0}
              sx={{
                width: 280,
                height: '100%',
                bgcolor: 'background.paper',
                borderRadius: 0,
                borderRight: '1px solid',
                borderColor: 'divider',
                overflow: 'hidden',
                transition: 'all 0.2s ease-in-out'
              }}
            >
              {sidebarItems.find(item => item.id === activeSection)?.content}
            </Paper>
          )}
    </Box>
  );
};

export default LeftSidebar; 