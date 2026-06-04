import React, { useState, useEffect, useCallback } from 'react';
import {
  Box,
  IconButton,
  Tooltip,
  CircularProgress,
  Menu,
  MenuItem
} from '@mui/material';
import { useTranslation } from 'react-i18next';
import { Node, Edge } from 'reactflow';
import { useCrewExecutionStore } from '../../store/crewExecution';
import { useTabManagerStore } from '../../store/tabManager';

// Icons
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import AutoFixHighIcon from '@mui/icons-material/AutoFixHigh';
import MenuBookIcon from '@mui/icons-material/MenuBook';
import _SaveIcon from '@mui/icons-material/Save';
import SettingsIcon from '@mui/icons-material/Settings';
import AccountTreeIcon from '@mui/icons-material/AccountTree';

// Components
import _SaveCrew from '../Crew/SaveCrew';
import { hasCrewContent } from '../Chat/utils/chatHelpers';

interface WorkflowToolbarProps {
  selectedModel: string;
  setSelectedModel: (model: string) => void;
  planningEnabled: boolean;
  setPlanningEnabled: (enabled: boolean) => void;
  schemaDetectionEnabled: boolean;
  setSchemaDetectionEnabled: (enabled: boolean) => void;
  reasoningEnabled: boolean;
  setReasoningEnabled: (enabled: boolean) => void;
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
  saveFlowRef: React.RefObject<HTMLButtonElement>;
}

const WorkflowToolbar: React.FC<WorkflowToolbarProps> = ({
  setIsCrewPlanningOpen,
  setIsConfigurationDialogOpen,
  setIsCrewDialogOpen,
  nodes,
  edges,
  saveCrewRef,
  saveFlowRef
}) => {
  const { t } = useTranslation();
  const [anchorEl, setAnchorEl] = useState<null | HTMLElement>(null);
  const open = Boolean(anchorEl);

  const {
    executeCrew,
    isExecuting,
    errorMessage,
    showError,
    successMessage,
    showSuccess,
    setShowError,
    setShowSuccess,
    setErrorMessage,
    handleRunClick: storeHandleRunClick,
  } = useCrewExecutionStore();

  const canRunCrew = React.useMemo(() => hasCrewContent(nodes), [nodes]);

  // Get the active tab's info so we can overwrite instead of creating new
  const { activeTabSavedCrewId, activeTabId } = useTabManagerStore(state => {
    const activeTab = state.tabs.find(t => t.id === state.activeTabId);
    return {
      activeTabSavedCrewId: activeTab?.savedCrewId,
      activeTabId: state.activeTabId,
    };
  });

  // Check if all edges are configured for flow execution
  const canRunFlow = React.useMemo(() => {
    if (edges.length === 0) return true; // No edges means simple flow
    return edges.every(edge => {
      const hasSourceTasks = edge.data?.listenToTaskIds && edge.data.listenToTaskIds.length > 0;
      const hasTargetTasks = edge.data?.targetTaskIds && edge.data.targetTaskIds.length > 0;
      return hasSourceTasks && hasTargetTasks;
    });
  }, [edges]);

  const handleMenuClose = useCallback(() => {
    setAnchorEl(null);
  }, []);

  const handleExecuteCrew = useCallback(async () => {
    handleMenuClose();
    try {
      await executeCrew(nodes, edges);
    } catch (error) {
      console.error('[WorkflowToolbar] Error executing crew:', error);
    }
  }, [executeCrew, nodes, edges, handleMenuClose]);

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

      // CRITICAL: Validate all edges are properly configured before execution
      const unconfiguredEdges = edges.filter(edge => {
        const hasSourceTasks = edge.data?.listenToTaskIds && edge.data.listenToTaskIds.length > 0;
        const hasTargetTasks = edge.data?.targetTaskIds && edge.data.targetTaskIds.length > 0;
        return !hasSourceTasks || !hasTargetTasks;
      });

      if (unconfiguredEdges.length > 0) {
        const edgeDescriptions = unconfiguredEdges.map((edge, index) => {
          const sourceNode = nodes.find(n => n.id === edge.source);
          const targetNode = nodes.find(n => n.id === edge.target);
          return `${index + 1}. ${sourceNode?.data?.crewName || 'Unknown'} → ${targetNode?.data?.crewName || 'Unknown'}`;
        }).join('\n');

        const errorMsg = `Cannot execute flow: ${unconfiguredEdges.length} connection(s) not configured.\n\nPlease configure these connections by clicking on them and selecting tasks:\n${edgeDescriptions}`;
        console.error('[WorkflowToolbar]', errorMsg);
        setErrorMessage(errorMsg);
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
      console.log('[WorkflowToolbar] All edges validated successfully');

      // Use the store's handleRunClick which includes checkpoint checking
      console.log('[WorkflowToolbar] Calling store handleRunClick for flow execution');
      await storeHandleRunClick('flow');
    } catch (error) {
      console.error('[WorkflowToolbar] Error executing flow:', error);
      if (error instanceof Error) {
        setErrorMessage(`Flow execution failed: ${error.message}`);
      } else {
        setErrorMessage('Flow execution failed with an unknown error');
      }
      setShowError(true);
    }
  }, [nodes, edges, handleMenuClose, setErrorMessage, setShowError, storeHandleRunClick]);

  // Handle click to open execution menu
  const handleExecuteClick = (event: React.MouseEvent<HTMLButtonElement>) => {
    setAnchorEl(event.currentTarget);
  };

  // Add error and success message handling
  useEffect(() => {
    console.log('[WorkflowToolbar] useEffect triggered - showError:', showError, 'errorMessage:', errorMessage);
    if (showError && errorMessage) {
      console.log('[WorkflowToolbar] Conditions met, showing toast...');
      import('react-hot-toast').then(({ toast }) => {
        console.log('[WorkflowToolbar] Toast loaded, showing error toast:', errorMessage);
        toast.error(errorMessage, {
          duration: 6000,
          position: 'top-center',
          style: {
            maxWidth: '500px',
            fontSize: '14px',
            padding: '12px',
          },
        });
      }).catch((error) => {
        console.error('[WorkflowToolbar] Failed to load toast:', error);
        alert(`Execution Error: ${errorMessage}`);
      });
      setShowError(false);
    }
  }, [showError, errorMessage, setShowError]);

  useEffect(() => {
    if (showSuccess) {
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
      top: '48px',
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
      </Box>

      {/* Center section */}
      <Box sx={{ display: 'flex', gap: 1.5, alignItems: 'center' }}>
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
              <IconButton
                onClick={handleExecuteClick}
                disabled={isExecuting || !canRunCrew}
                size="small"
                data-tour="execute-button"
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
            <Tooltip
              title={!canRunFlow ? "Some connections are not configured. Click on orange connections to configure them." : ""}
              placement="right"
            >
              <span>
                <MenuItem
                  onClick={handleExecuteFlow}
                  disabled={!canRunFlow}
                  sx={{
                    '&.Mui-disabled': {
                      opacity: 0.5,
                      pointerEvents: 'auto',
                    }
                  }}
                >
                  Execute Flow
                </MenuItem>
              </span>
            </Tooltip>
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
            data-tour="open-workflow"
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
              data-tour="save-button"
              onClick={() => {
                if (activeTabSavedCrewId) {
                  // Overwrite the existing crew
                  window.dispatchEvent(new CustomEvent('updateExistingCrew', {
                    detail: { crewId: activeTabSavedCrewId, tabId: activeTabId }
                  }));
                } else {
                  // No existing crew — open save-as dialog
                  window.dispatchEvent(new CustomEvent('openSaveCrewDialog'));
                }
              }}
            >
              <_SaveIcon sx={{ fontSize: 20 }} />
            </IconButton>
          </span>
        </Tooltip>

        <Tooltip title={t('nemo.buttons.saveFlow') || 'Save Flow'}>
          <span>
            <IconButton
              ref={saveFlowRef}
              size="small"
              sx={{
                border: '1px solid rgba(0, 0, 0, 0.12)',
                borderRadius: 1,
                p: 1,
                '&:hover': { backgroundColor: 'action.hover' }
              }}
              data-tour="save-flow-button"
              onClick={() => {
                const event = new CustomEvent('openSaveFlowDialog');
                window.dispatchEvent(event);
              }}
            >
              <AccountTreeIcon sx={{ fontSize: 20 }} />
            </IconButton>
          </span>
        </Tooltip>
      </Box>
    </Box>
  );
};

export default WorkflowToolbar;
