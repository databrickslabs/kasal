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

// Icons
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import AutoFixHighIcon from '@mui/icons-material/AutoFixHigh';
import MenuBookIcon from '@mui/icons-material/MenuBook';
import _SaveIcon from '@mui/icons-material/Save';
import SettingsIcon from '@mui/icons-material/Settings';

// Components
import _SaveCrew from '../Crew/SaveCrew';

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
}

const WorkflowToolbar: React.FC<WorkflowToolbarProps> = ({
  selectedModel,
  setSelectedModel,
  planningEnabled,
  setPlanningEnabled,
  schemaDetectionEnabled,
  setSchemaDetectionEnabled,
  reasoningEnabled,
  setReasoningEnabled,
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
    setErrorMessage,
  } = useCrewExecutionStore();
  
  const handleMenuClose = useCallback(() => {
    setAnchorEl(null);
  }, []);

  const handleExecuteCrew = useCallback(async () => {
    handleMenuClose();
    try {
      await executeCrew(nodes, edges);
    } catch (error) {
      console.error('[WorkflowToolbar] Error executing crew:', error);
      // The error will also be handled by the store and useEffect, but this provides backup
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
    console.log('[WorkflowToolbar] useEffect triggered - showError:', showError, 'errorMessage:', errorMessage);
    if (showError && errorMessage) {
      console.log('[WorkflowToolbar] Conditions met, showing toast...');
      // Import toast from react-hot-toast and show error
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
        // Fallback: show alert if toast fails to load
        alert(`Execution Error: ${errorMessage}`);
      });
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
      top: '48px', // Position below TabBar
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