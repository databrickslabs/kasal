import React, { useState, useEffect, useCallback, useMemo } from 'react';
import {
  Box,
  IconButton,
  Tooltip,
  Divider,
  Paper,
  useTheme,
  Typography,
  Switch,
  FormControl,
  FormHelperText,
  InputLabel,
  Select,
  MenuItem,
  CircularProgress,
  SelectChangeEvent,
  Badge
} from '@mui/material';
import {
  CleaningServices as ClearIcon,
  ZoomIn as ZoomInIcon,
  ZoomOut as ZoomOutIcon,
  CenterFocusStrong as FitViewIcon,
  Tune as TuneIcon,
  SwapHoriz as SwapHorizIcon,

  Settings as SettingsIcon,
  InfoOutlined as InfoOutlinedIcon,
  HelpOutline as HelpOutlineIcon,
} from '@mui/icons-material';
import { Models } from '../../types/models';
import { ModelService } from '../../api/ModelService';
import { useCrewExecutionStore, ReasoningConfig } from '../../store/crewExecution';
import { usePermissionStore } from '../../store/permissions';
import { useWorkflowStore } from '../../store/workflow';
import { useTabManagerStore } from '../../store/tabManager';
import { useUILayoutStore } from '../../store/uiLayout';


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
  onZoomIn: () => void;
  onZoomOut: () => void;
  onFitView: () => void;
  onToggleInteractivity: () => void;
  // Runtime features props
  reasoningEnabled: boolean;
  setReasoningEnabled: (enabled: boolean) => void;
  schemaDetectionEnabled: boolean;
  setSchemaDetectionEnabled: (enabled: boolean) => void;
  processType?: 'sequential' | 'hierarchical';
  setProcessType?: (type: 'sequential' | 'hierarchical') => void;

  // New prop for configuration
  setIsConfigurationDialogOpen?: (open: boolean) => void;
  // Logs dialog prop
  onOpenLogsDialog?: () => void;
  // Execution history visibility
  showRunHistory?: boolean;
  executionHistoryHeight?: number;
  // Tutorial dialog prop
  onOpenTutorial?: () => void;
  // Hide runtime filters when on flow canvas
  hideRuntimeFilters?: boolean;
}

const LeftSidebar: React.FC<LeftSidebarProps> = ({
  onClearCanvas,
  onZoomIn,
  onZoomOut,
  onFitView,
  onToggleInteractivity,
  reasoningEnabled,
  setReasoningEnabled,
  schemaDetectionEnabled,
  setSchemaDetectionEnabled,
  processType = 'sequential',
  setProcessType,
  setIsConfigurationDialogOpen,
  onOpenLogsDialog,
  showRunHistory,
  executionHistoryHeight = 200,
  onOpenTutorial,
  hideRuntimeFilters = false
}) => {
  const theme = useTheme();
  const [activeSection, setActiveSection] = useState<string | null>(null);
  const [models, setModels] = useState<Models>(DEFAULT_FALLBACK_MODEL);
  const [isLoadingModels, setIsLoadingModels] = useState(true);
  const { layoutOrientation, setLayoutOrientation } = useUILayoutStore();

  const toggleLayoutOrientation = useCallback(() => {
    const next = (layoutOrientation === 'horizontal') ? 'vertical' : 'horizontal';
    setLayoutOrientation(next);
    // Trigger node repositioning and fit view (recalculateNodePositions already calls fitView)
    setTimeout(() => {
      window.dispatchEvent(new CustomEvent('recalculateNodePositions', { detail: { reason: 'layout-orientation-toggle' } }));
    }, 50);
  }, [layoutOrientation, setLayoutOrientation]);

  const [managerModel, setManagerModel] = useState<string>('');
  const { setLeftSidebarExpanded } = useUILayoutStore();

  // Reflect expanded state of the left sidebar (when a section is active) into the UI layout store
  useEffect(() => {
    setLeftSidebarExpanded(!!activeSection);
  }, [activeSection, setLeftSidebarExpanded]);


  const {
    setProcessType: setStoreProcessType,
    setManagerLLM,
    processType: storeProcessType,
    managerLLM: storeManagerLLM,
    reasoningConfig,
    setReasoningConfig,
  } = useCrewExecutionStore();

  // Get user permissions
  const { userRole } = usePermissionStore();
  const isOperator = userRole === 'operator';

  // Get tutorial status
  const { hasSeenTutorial } = useWorkflowStore();

  // Canvas nodes come from the ACTIVE TAB, not useWorkflowStore: that store has
  // a single shared nodes array that goes stale when switching between the crew
  // and flow canvases (the same reason handleRunClick resolves from the tab).
  const activeTabNodes = useTabManagerStore(
    (s) => s.tabs.find((t) => t.id === s.activeTabId)?.nodes,
  );

  // Reasoning applies to each AGENT's own model, so whether the control can do
  // anything depends on the agents currently on the canvas — not on a crew-level
  // model setting. Distinct models first, so the helper text names them.
  const agentModelNames = useMemo(() => {
    const seen = new Set<string>();
    for (const node of activeTabNodes || []) {
      if (node.type !== 'agentNode') continue;
      const llm = (node.data as { llm?: string } | undefined)?.llm;
      if (llm) seen.add(llm);
    }
    return Array.from(seen);
  }, [activeTabNodes]);

  // Enabled when ANY agent's model has a reasoning budget: a mixed crew still
  // benefits, and the engine applies the effort per agent.
  const reasoningSupported = useMemo(
    () => agentModelNames.some((key: string) => models[key]?.supports_reasoning_effort),
    [agentModelNames, models],
  );

  // Fetch models on component mount
  useEffect(() => {
    const fetchModels = async () => {
      setIsLoadingModels(true);
      try {
        const modelService = ModelService.getInstance();
        const response = await modelService.getEnabledModels();
        setModels(response);

        // Initialize manager model when models are loaded
        if (response && Object.keys(response).length > 0) {
          // Use store value if available, otherwise set first model
          if (storeManagerLLM && response[storeManagerLLM]) {
            setManagerModel(storeManagerLLM);
          } else if (!managerModel) {
            const firstModel = Object.keys(response)[0];
            setManagerModel(firstModel);
            setManagerLLM(firstModel);
          }
        }
      } catch (error) { /* ignore error to keep UI responsive */ } finally {
        setIsLoadingModels(false);
      }
    };

    fetchModels();
  }, [managerModel, setManagerLLM, storeManagerLLM]);

  // Sync local state with store values when they change (e.g., when loading a crew)
  useEffect(() => {
    if (storeManagerLLM && storeManagerLLM !== managerModel) {
      console.log('LeftSidebar: Syncing manager model from store:', storeManagerLLM);
      setManagerModel(storeManagerLLM);
    }
  }, [storeManagerLLM, managerModel]);

  const handleManagerModelChange = useCallback((event: SelectChangeEvent) => {
    const value = event.target.value;
    setManagerModel(value);
    setManagerLLM(value);
  }, [setManagerLLM]);

  const handleProcessTypeChange = useCallback((event: SelectChangeEvent) => {
    const value = event.target.value as 'sequential' | 'hierarchical';

    setStoreProcessType(value);
    // Also call the prop setter if it exists for backward compatibility
    if (setProcessType) {
      setProcessType(value);
    }
  }, [setProcessType, setStoreProcessType]);

  const sidebarItems = [
    {
      id: 'configuration',
      icon: <SettingsIcon />,
      tooltip: 'Configuration',
      content: null, // No expandable content, handled by direct click
      dataTour: 'configuration-button'
    },
    {
      id: 'help',
      icon: <HelpOutlineIcon />,
      tooltip: 'Start Tutorial / Help',
      content: null, // No expandable content, handled by direct click
      dataTour: 'help-button'
    },

    // Only show runtime-features for non-operators AND when not on flow canvas
    ...(!isOperator && !hideRuntimeFilters ? [{
      id: 'runtime-features',
      icon: <TuneIcon />,
      tooltip: 'Runtime Features',
      dataTour: 'runtime-features',
      content: (
        <Box
          sx={{
            maxHeight: showRunHistory ? `calc(100vh - 48px - ${executionHistoryHeight}px - 20px)` : 'calc(100vh - 48px - 20px)',
            overflowY: 'auto',
            p: 1,
          }}
        >
          {/* Process Type Section */}
          <Box sx={{ mb: 2 }}>
            <Box sx={{ display: 'flex', alignItems: 'center', mb: 1 }}>
              <Typography
                variant="subtitle2"
                sx={{
                  color: theme.palette.primary.main,
                  fontWeight: 600,
                  textTransform: 'uppercase',
                  letterSpacing: '0.5px',
                  fontSize: '0.7rem'
                }}
              >
                Process Type
              </Typography>
              <Tooltip title="Determines how agents collaborate. Sequential: agents work one after another in a fixed order. Hierarchical: a manager agent dynamically delegates tasks to specialized agents. Use Hierarchical for complex workflows requiring adaptive task distribution and parallel execution." placement="right">
                <InfoOutlinedIcon sx={{ ml: 0.5, fontSize: 14, color: theme.palette.primary.main, cursor: 'help' }} />
              </Tooltip>
            </Box>
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
              <FormControl size="small" fullWidth>
                <InputLabel sx={{ fontSize: '0.75rem' }}>Execution Process</InputLabel>
                <Select
                  value={storeProcessType}
                  onChange={handleProcessTypeChange}
                  label="Execution Process"
                  sx={{ fontSize: '0.75rem' }}
                >
                  <MenuItem value="sequential" sx={{ fontSize: '0.75rem' }}>
                    Sequential - Linear task execution
                  </MenuItem>
                  <MenuItem value="hierarchical" sx={{ fontSize: '0.75rem' }}>
                    Hierarchical - Manager-based delegation
                  </MenuItem>
                </Select>
              </FormControl>

              {(storeProcessType || processType) === 'hierarchical' && (
                <FormControl size="small" fullWidth sx={{ mt: 1 }}>
                  <InputLabel sx={{ fontSize: '0.75rem' }}>Manager LLM</InputLabel>
                  <Select
                    value={managerModel}
                    onChange={handleManagerModelChange}
                    label="Manager LLM"
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

              {(storeProcessType || processType) === 'hierarchical' && (
                <Typography variant="caption" sx={{ color: 'text.secondary', fontSize: '0.65rem', mt: 0.5 }}>
                  Manager coordinates task delegation to agents
                </Typography>
              )}
            </Box>
          </Box>

          {/* Runtime Filters - Hidden when on flow canvas */}
          {!hideRuntimeFilters && (
            <>
              {/* Reasoning Section */}
          <Box sx={{ mb: 2 }}>
            <Box sx={{ display: 'flex', alignItems: 'center', mb: 1 }}>
              <Typography
                variant="subtitle2"
                sx={{
                  color: theme.palette.primary.main,
                  fontWeight: 600,
                  textTransform: 'uppercase',
                  letterSpacing: '0.5px',
                  fontSize: '0.7rem'
                }}
              >
                Reasoning
              </Typography>
              <Tooltip title="Sets the model's native reasoning (thinking) budget for this crew's agents. Higher effort lets the model think longer before answering — better on hard, multi-step problems, slower and more expensive on simple ones. Applied only to models that support a reasoning budget; models without it ignore the setting silently." placement="right">
                <InfoOutlinedIcon sx={{ ml: 0.5, fontSize: 14, color: theme.palette.primary.main, cursor: 'help' }} />
              </Tooltip>
            </Box>
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

              {/* No "Reasoning LLM" picker: reasoning is the model's OWN native
                  thinking budget applied to each agent's configured LLM, not a
                  separate model. The backend has ignored reasoning_llm since the
                  planner was removed (crew_config_builder logs a warning and
                  drops it), so the control only ever implied a capability that
                  does not exist. */}
              {reasoningEnabled && (
                <>
                  {/* Thinking budget — the model's native reasoning effort.
                      Disabled when no agent on the canvas runs a model that has
                      one: the engine drops the setting in that case, so offering
                      it would let the user pick High and see nothing change. */}
                  <FormControl
                    size="small"
                    fullWidth
                    sx={{ mt: 1 }}
                    disabled={!reasoningSupported}
                  >
                    <InputLabel sx={{ fontSize: '0.75rem' }}>Reasoning Effort</InputLabel>
                    <Select
                      value={reasoningConfig.reasoning_effort ?? 'low'}
                      label="Reasoning Effort"
                      onChange={(e) => setReasoningConfig({ reasoning_effort: e.target.value as ReasoningConfig['reasoning_effort'] })}
                      sx={{ fontSize: '0.75rem' }}
                    >
                      <MenuItem value="low" sx={{ fontSize: '0.75rem' }}>Low — minimal thinking (fastest)</MenuItem>
                      <MenuItem value="medium" sx={{ fontSize: '0.75rem' }}>Medium — balanced thinking</MenuItem>
                      <MenuItem value="high" sx={{ fontSize: '0.75rem' }}>High — maximum thinking (slowest)</MenuItem>
                    </Select>
                    {!reasoningSupported && (
                      <FormHelperText sx={{ fontSize: '0.65rem' }}>
                        {agentModelNames.length
                          ? `${agentModelNames.join(', ')} has no reasoning budget — this setting would be ignored.`
                          : 'Add an agent whose model has a reasoning budget (e.g. a GPT-5 / o3 / o4 model).'}
                      </FormHelperText>
                    )}
                  </FormControl>
                </>
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
            </>
          )}
        </Box>
      )
    }] : [])
  ];

  // Separate help item to render it at the very bottom of the activity bar
  const topSidebarItems = sidebarItems.filter(item => item.id !== 'help');
  const helpItem = sidebarItems.find(item => item.id === 'help');


  const handleSectionClick = (sectionId: string) => {
    if (sectionId === 'configuration') {
      // Directly open configuration dialog instead of expanding section
      setIsConfigurationDialogOpen && setIsConfigurationDialogOpen(true);
      return;
    }
    if (sectionId === 'help') {
      // Open tutorial dialog

      if (onOpenTutorial) {
        onOpenTutorial();
      }
      return;
    }
    setActiveSection(activeSection === sectionId ? null : sectionId);
  };

  return (
    <Box
      data-tour="left-sidebar"
      sx={{
        position: 'absolute',
        top: '48px', // Account for TabBar height
        left: 0,
        height: showRunHistory ? `calc(100% - 48px - ${executionHistoryHeight}px)` : 'calc(100% - 48px)',
        zIndex: 5, // Lower than execution history to prevent overlap at high zoom
        display: 'flex',
        flexDirection: 'row'
      }}
      onMouseLeave={() => setActiveSection(null)}
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
              boxShadow: '2px 0 4px rgba(0,0,0,0.1)', // Temporary shadow for visibility
              display: 'flex',
              flexDirection: 'column',
              alignItems: 'center',
              py: 1
            }}
          >
            {topSidebarItems.map((item, index) => (
              <React.Fragment key={item.id}>
                <Tooltip
                  title={
                    item.id === 'help' && !hasSeenTutorial
                      ? '🎯 Click to start your personalized tutorial!'
                      : item.tooltip
                  }
                  placement="right"
                  arrow={item.id === 'help' && !hasSeenTutorial}
                >
                  <Badge
                    badgeContent={item.id === 'help' && !hasSeenTutorial ? '!' : null}
                    color="primary"
                    variant="dot"
                    invisible={item.id !== 'help' || hasSeenTutorial}
                    sx={{
                      '& .MuiBadge-dot': {
                        animation: 'pulse 2s infinite',
                        '@keyframes pulse': {
                          '0%': { transform: 'scale(1)' },
                          '50%': { transform: 'scale(1.2)' },
                          '100%': { transform: 'scale(1)' }
                        }
                      }
                    }}
                  >
                    <IconButton
                      data-tour={item.dataTour}
                      onMouseEnter={() => {
                        // Don't set active section for configuration or help since they open dialogs
                        if (item.id !== 'configuration' && item.id !== 'help') {
                          setActiveSection(item.id);
                        }
                      }}
                      onClick={() => handleSectionClick(item.id)}
                      sx={{
                        width: 40,
                        height: 40,
                        mb: 1,
                        color: item.id === 'help'
                          ? (!hasSeenTutorial ? theme.palette.primary.main : theme.palette.info.main)
                          : 'text.secondary',
                        animation: item.id === 'help' && !hasSeenTutorial
                          ? 'pulse 2s infinite'
                          : 'none',
                        '@keyframes pulse': {
                          '0%': { boxShadow: '0 0 0 0 rgba(25, 118, 210, 0.4)' },
                          '70%': { boxShadow: '0 0 0 8px rgba(25, 118, 210, 0)' },
                          '100%': { boxShadow: '0 0 0 0 rgba(25, 118, 210, 0)' }
                        },
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
                          color: item.id === 'help' ? theme.palette.info.dark : 'text.primary',
                          transform: item.id === 'help' ? 'scale(1.1)' : 'none'
                        }
                      }}
                    >
                      {item.icon}
                    </IconButton>
                  </Badge>
                </Tooltip>
                {/* Insert action icons after the last sidebar item */}
                {index === topSidebarItems.length - 1 && (
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
                    <Tooltip
                      title={`Current: ${layoutOrientation === 'horizontal' ? 'Horizontal' : 'Vertical'} Layout (Click to toggle)`}
                      placement="right"
                    >
                      <IconButton
                        onClick={toggleLayoutOrientation}
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
                        <SwapHorizIcon fontSize="small" />
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
                  </>
                )}
              </React.Fragment>
            ))}

            {/* Spacer to push the bottom group to the end */}
            <Box sx={{ flexGrow: 1 }} />

            {/* Help button pinned to bottom */}
            {helpItem && (
              <>
                <Tooltip
                  title={!hasSeenTutorial ? '🎯 Click to start your personalized tutorial!' : helpItem.tooltip}
                  placement="right"
                  arrow={!hasSeenTutorial}
                >
                  <Badge
                    badgeContent={!hasSeenTutorial ? '!' : null}
                    color="primary"
                    variant="dot"
                    invisible={hasSeenTutorial}
                    sx={{
                      '& .MuiBadge-dot': {
                        animation: 'pulse 2s infinite',
                        '@keyframes pulse': {
                          '0%': { transform: 'scale(1)' },
                          '50%': { transform: 'scale(1.2)' },
                          '100%': { transform: 'scale(1)' }
                        }
                      }
                    }}
                  >
                    <IconButton
                      data-tour={helpItem.dataTour}
                      onMouseEnter={() => { /* no-op for help */ }}
                      onClick={() => handleSectionClick(helpItem.id)}
                      sx={{
                        width: 40,
                        height: 40,
                        mb: 1,
                        color: !hasSeenTutorial ? theme.palette.primary.main : theme.palette.info.main,
                        animation: !hasSeenTutorial ? 'pulse 2s infinite' : 'none',
                        '@keyframes pulse': {
                          '0%': { boxShadow: '0 0 0 0 rgba(25, 118, 210, 0.4)' },
                          '70%': { boxShadow: '0 0 0 8px rgba(25, 118, 210, 0)' },
                          '100%': { boxShadow: '0 0 0 0 rgba(25, 118, 210, 0)' }
                        },
                        backgroundColor: activeSection === helpItem.id
                          ? 'action.selected'
                          : 'transparent',
                        borderLeft: activeSection === helpItem.id
                          ? `2px solid ${theme.palette.primary.main}`
                          : '2px solid transparent',
                        borderRadius: 0,
                        transition: 'all 0.2s ease-in-out',
                        '&:hover': {
                          backgroundColor: 'action.hover',
                          color: theme.palette.info.dark,
                          transform: 'scale(1.1)'
                        }
                      }}
                    >
                      {helpItem.icon}
                    </IconButton>
                  </Badge>
                </Tooltip>
              </>
            )}
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