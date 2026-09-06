import React, { useState, useEffect } from 'react';
import {
  Box,
  IconButton,
  Tooltip,
  Paper,
  GlobalStyles,
} from '@mui/material';
import {
  PersonAdd as PersonAddIcon,
  AddTask as AddTaskIcon,
  Save as SaveIcon,
  MenuBook as MenuBookIcon,
  Webhook as WebhookIcon,
  Schedule as ScheduleIcon,
  Assessment as LogsIcon,
  PlayArrow as PlayArrowIcon,
  FileDownload as FileDownloadIcon,
} from '@mui/icons-material';
import { Edge } from 'reactflow';
import { usePermissionStore } from '../../store/permissions';
import { useTabManagerStore } from '../../store/tabManager';
import { useEventTriggersStore } from '../../store/eventTriggers';
import { History, MessageSquare } from 'lucide-react';
import { useUILayoutStore } from '../../store/uiLayout';
import ExportCrewDialog from '../../features/workflow/export/components/ExportCrewDialog';

interface SidebarItem {
  id: string;
  icon?: React.ReactNode;
  tooltip?: string;
  onClick?: (event?: React.MouseEvent<HTMLElement>) => void;
  disabled?: boolean;
  isActive?: boolean;
  isSeparator?: boolean;
}

interface RightSidebarProps {
  showWorkspaceActions?: boolean;
  onOpenLogsDialog: () => void;
  onToggleChat: () => void;
  isChatOpen: boolean;
  setIsAgentDialogOpen: (open: boolean) => void;
  setIsTaskDialogOpen: (open: boolean) => void;
  setIsCrewDialogOpen?: (open: boolean) => void;
  onSaveCrewClick?: () => void;
  onSaveFlowClick?: () => void;
  showRunHistory?: boolean;
  executionHistoryHeight?: number;
  onOpenSchedulesDialog?: () => void;
  onOpenTriggersDialog?: () => void;
  onToggleExecutionHistory?: () => void;
  areFlowsVisible?: boolean;
  toggleFlowsVisibility?: () => void;
  // Play button props
  hasCrewNodes?: boolean;
  hasFlowNodes?: boolean;
  onPlayPlan?: () => void;
  onPlayFlow?: () => void;
  edges?: Edge[]; // Add edges for validation
}

const RightSidebar: React.FC<RightSidebarProps> = ({
  showWorkspaceActions = false,
  onOpenLogsDialog,
  onToggleChat,
  isChatOpen,
  setIsAgentDialogOpen,
  setIsTaskDialogOpen,
  setIsCrewDialogOpen,
  onSaveCrewClick,
  onSaveFlowClick,
  showRunHistory = false,
  executionHistoryHeight = 200,
  onOpenSchedulesDialog,
  onOpenTriggersDialog,
  onToggleExecutionHistory,
  areFlowsVisible = false,
  toggleFlowsVisibility: _toggleFlowsVisibility, // moved to TabBar mode switcher
  hasCrewNodes = false,
  hasFlowNodes = false,
  onPlayPlan,
  onPlayFlow,
  edges = [],
}) => {
  const { assistantPanelVisible, executionHistoryVisible, setAssistantPanelVisible, setExecutionHistoryVisible } = useUILayoutStore();
  const flowPanelTab = useUILayoutStore(state => state.flowPanelTab);
  const historySelected = executionHistoryVisible && !assistantPanelVisible && (!areFlowsVisible || flowPanelTab === 'runs');
  const [animateAIAssistant, setAnimateAIAssistant] = useState(true);
  const [chatOpenedByClick, setChatOpenedByClick] = useState(false);
  const [isExportDialogOpen, setIsExportDialogOpen] = useState(false);
  const [pulsePlay, setPulsePlay] = useState(false);
  // Event Triggers is a Preview feature toggled in Configuration → Engines.
  // Read from the shared Zustand store so flipping it in Configuration hides/
  // shows this sidebar action live, without a refresh. Default OFF.
  const eventTriggersEnabled = useEventTriggersStore((s) => s.enabled);
  const loadEventTriggers = useEventTriggersStore((s) => s.load);

  useEffect(() => {
    loadEventTriggers();
  }, [loadEventTriggers]);

  // Get user permissions
  const { userRole } = usePermissionStore();
  const isOperator = userRole === 'operator';
  const _isAdmin = userRole === 'admin';
  const _isEditor = userRole === 'editor';

  // Get active tab's crew info from tabManager
  const { getActiveTab } = useTabManagerStore();
  const activeTab = getActiveTab();
  const savedCrewId = activeTab?.savedCrewId;
  const savedCrewName = activeTab?.savedCrewName || 'Unnamed Crew';

  // Validate all edges are configured before allowing flow execution
  const canRunFlow = React.useMemo(() => {
    if (!hasFlowNodes) return false;
    if (edges.length === 0) return true; // No edges means simple flow
    return edges.every(edge => {
      const hasSourceTasks = edge.data?.listenToTaskIds && edge.data.listenToTaskIds.length > 0;
      const hasTargetTasks = edge.data?.targetTaskIds && edge.data.targetTaskIds.length > 0;
      return hasSourceTasks && hasTargetTasks;
    });
  }, [hasFlowNodes, edges]);

  // Context-aware play handler - no menu needed
  const handlePlayClick = () => {
    // If on flow canvas, run flow; otherwise run crew
    if (areFlowsVisible) {
      if (onPlayFlow) {
        console.log('[RightSidebar] Running flow (context: flow canvas visible)');
        onPlayFlow();
      }
    } else {
      if (onPlayPlan) {
        console.log('[RightSidebar] Running crew (context: crew canvas visible)');
        onPlayPlan();
      }
    }
  };

  // Context-aware save handler - no menu needed
  const handleSaveClick = () => {
    // If on flow canvas, save flow; otherwise save crew
    if (areFlowsVisible) {
      if (onSaveFlowClick) {
        console.log('[RightSidebar] Saving flow (context: flow canvas visible)');
        onSaveFlowClick();
      }
    } else {
      if (onSaveCrewClick) {
        console.log('[RightSidebar] Saving crew (context: crew canvas visible)');
        onSaveCrewClick();
      }
    }
  };


  useEffect(() => {
    // Trigger animation on mount, then stop after 1.5s
    if (animateAIAssistant) {
      const timeout = setTimeout(() => setAnimateAIAssistant(false), 1500);
      return () => clearTimeout(timeout);
    }
  }, [animateAIAssistant]);


  // Open chat by default on mount
  useEffect(() => {
    if (!isChatOpen && !chatOpenedByClick) {
      onToggleChat();
    }
  }, [isChatOpen, chatOpenedByClick, onToggleChat]);

  // Reset chatOpenedByClick when chat is closed
  useEffect(() => {
    if (!isChatOpen) {
      setChatOpenedByClick(false);
    }
  }, [isChatOpen]);

  // Listen for crew-ready event to pulse the Play button
  useEffect(() => {
    const handleCrewReady = () => {
      setPulsePlay(true);
      setTimeout(() => setPulsePlay(false), 3000);
    };
    window.addEventListener('crew-ready', handleCrewReady);
    return () => window.removeEventListener('crew-ready', handleCrewReady);
  }, []);

  // Compute play button pulse state
  const isPlayPulsing = (itemId: string) => itemId === 'play-execution' && pulsePlay;

  // Determine if play button should be enabled based on context
  const canExecute = areFlowsVisible ? canRunFlow : hasCrewNodes;
  const playButtonTooltip = areFlowsVisible
    ? (canRunFlow ? 'Run Flow' : 'Configure flow connections to run')
    : (hasCrewNodes ? 'Run Crew' : 'No Crew to Execute');

  const sidebarItems: SidebarItem[] = [
    // Play button at the top - context-aware
    {
      id: 'play-execution',
      icon: <PlayArrowIcon />,
      tooltip: playButtonTooltip,
      onClick: handlePlayClick,
      disabled: !canExecute
    },
    // Only show Add Agent, Add Task, and Save for non-operators AND when not on flow canvas
    ...(!isOperator && !areFlowsVisible ? [
      {
        id: 'separator1',
        isSeparator: true
      },
      {
        id: 'add-agent',
        icon: <PersonAddIcon />,
        tooltip: 'Add Agent',
        onClick: () => setIsAgentDialogOpen(true),
        disabled: false
      },
      {
        id: 'add-task',
        icon: <AddTaskIcon />,
        tooltip: 'Add Task',
        onClick: () => setIsTaskDialogOpen(true),
        disabled: false
      },
      {
        id: 'separator2',
        isSeparator: true
      },
      {
        id: 'save-context',
        icon: <SaveIcon />,
        tooltip: 'Save Crew',
        onClick: handleSaveClick,
        disabled: false
      }
    ] : []),
    // Show Save Flow button when on flow canvas and user is not an operator
    ...(!isOperator && areFlowsVisible ? [
      {
        id: 'separator2',
        isSeparator: true
      },
      {
        id: 'save-context',
        icon: <SaveIcon />,
        tooltip: 'Save Flow',
        onClick: handleSaveClick,
        disabled: false
      }
    ] : []),
    // Deploy button - only on the crew canvas (hidden on the flow canvas) for non-operators
    ...(!isOperator && !areFlowsVisible ? [
      {
        id: 'separator-export',
        isSeparator: true
      },
      {
        id: 'export-notebook',
        icon: <FileDownloadIcon />,
        tooltip: savedCrewId ? 'Deploy to Databricks Apps' : 'Save crew first to deploy',
        onClick: () => setIsExportDialogOpen(true),
        disabled: !savedCrewId
      }
    ] : []),
    {
      id: 'separator-catalog',
      isSeparator: true
    },
    {
      id: 'open-catalog',
      icon: <MenuBookIcon />,
      tooltip: areFlowsVisible ? 'Open Workflow Catalog' : 'Open Catalog',
      onClick: () => setIsCrewDialogOpen?.(true),
      disabled: false
    },
    // The "Show Workflow Panel" toggle was moved out of this sidebar — Flow is
    // now a top-level mode reachable via the grid mode switcher in the TabBar.
    // Only show View Assistant Logs when NOT on flow canvas
    ...(!areFlowsVisible ? [
      {
        id: 'separator4',
        isSeparator: true
      },
      {
        id: 'view-logs',
        icon: <LogsIcon />,
        tooltip: 'View Assistant Logs',
        onClick: onOpenLogsDialog,
        disabled: false
      }
    ] : []),
    {
      id: 'schedules',
      icon: <ScheduleIcon />,
      tooltip: 'Schedules',
      onClick: onOpenSchedulesDialog,
      disabled: !onOpenSchedulesDialog
    },
    ...(eventTriggersEnabled ? [{
      id: 'triggers',
      icon: <WebhookIcon />,
      tooltip: 'Event Triggers',
      onClick: onOpenTriggersDialog,
      disabled: !onOpenTriggersDialog
    }] : [])
  ];

  return (
    <>
      <GlobalStyles styles={`
        @keyframes ai-bounce {
          0% { transform: scale(1) translateY(0); }
          20% { transform: scale(1.2) translateY(-8px); }
          40% { transform: scale(0.95) translateY(0); }
          60% { transform: scale(1.1) translateY(-4px); }
          80% { transform: scale(0.98) translateY(0); }
          100% { transform: scale(1) translateY(0); }
        }
        @keyframes play-pulse {
          0% { box-shadow: 0 0 0 0 rgba(70, 83, 98, 0.7); transform: scale(1); }
          50% { box-shadow: 0 0 12px 6px rgba(70, 83, 98, 0.3); transform: scale(1.15); }
          100% { box-shadow: 0 0 0 0 rgba(70, 83, 98, 0.7); transform: scale(1); }
        }
      `} />
      <Box
        sx={{
          position: 'absolute',
          top: '48px', // Account for TabBar height
          right: 0,
          height: showRunHistory ? `calc(100% - 48px - ${executionHistoryHeight}px)` : 'calc(100% - 48px)', // Account for TabBar and execution history
          zIndex: 5,
          display: 'flex',
          flexDirection: 'row'
        }}
      >

        {/* Activity Bar (like VS Code) */}
        <Paper
          data-tour="right-sidebar"
          elevation={0}
          sx={{
            position: 'fixed',
            top: 48,
            right: 0,
            width: 48,
            height: showRunHistory ? `calc(100% - 48px - ${executionHistoryHeight}px)` : 'calc(100% - 48px)',
            display: 'flex',
            flexDirection: 'column',
            alignItems: 'center',
            justifyContent: 'flex-start',
            py: 1,
            borderLeft: 0,
            borderColor: 'divider',
            background: 'transparent',
            zIndex: 5,
            borderRadius: 0
          }}
        >
          {sidebarItems.map((item) => (
            <React.Fragment key={item.id}>
              {item.isSeparator ? (
                <Box sx={{ height: 8 }} />
              ) : (
                <Tooltip title={item.tooltip} placement="left">
                  {/* span wrapper: a disabled button doesn't fire the events the
                      Tooltip needs, so the tooltip listens on the span instead. */}
                  <span style={{ display: 'inline-flex' }}>
                  <IconButton
                    aria-label={item.tooltip}
                    onClick={(e) => {
                      if (item.onClick && !item.disabled) {
                        item.onClick(e);
                      }
                    }}
                    disabled={item.disabled}
                    sx={{
                      width: 40,
                      height: 40,
                      mb: 1,
                      color: item.isActive ? 'text.primary' : 'text.secondary',
                      backgroundColor: isPlayPulsing(item.id) || item.isActive ? 'action.selected' : 'transparent',
                      borderRight: '2px solid transparent',
                      borderRadius: '50%',
                      display: 'flex',
                      alignItems: 'center',
                      justifyContent: 'center',
                      transition: 'all 0.2s cubic-bezier(.4,2,.6,1)',
                      opacity: item.disabled ? 0.6 : 1,
                      cursor: item.disabled ? 'not-allowed' : 'pointer',
                      '&:hover': !item.disabled ? {
                        backgroundColor: 'action.hover',
                        color: 'text.primary',
                      } : {},
                      animation: isPlayPulsing(item.id)
                        ? 'play-pulse 0.8s ease-in-out infinite'
                        : 'none',
                    }}
                  >
                    {item.icon}
                  </IconButton>
                  </span>
                </Tooltip>
              )}
            </React.Fragment>
          ))}
          {showWorkspaceActions && <Box sx={{ mt: 'auto', pt: 1, pb: { xs: 9, sm: 0 }, flexShrink: 0, display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
            <Tooltip title="Execution history" placement="left"><IconButton aria-label="Execution history" aria-pressed={historySelected} onClick={() => setExecutionHistoryVisible(!historySelected)} sx={{ width: 40, height: 40, mb: 1, borderRadius: 2.5, color: 'text.secondary', bgcolor: historySelected ? 'action.selected' : 'transparent' }}><History size={20} strokeWidth={1.7} /></IconButton></Tooltip>
            <Tooltip title="Assistant responses" placement="left"><IconButton aria-label="Assistant responses" aria-pressed={assistantPanelVisible} onClick={() => setAssistantPanelVisible(!assistantPanelVisible)} sx={{ width: 40, height: 40, mb: 1, borderRadius: 2.5, color: 'text.secondary', bgcolor: assistantPanelVisible ? 'action.selected' : 'transparent' }}><MessageSquare size={19} strokeWidth={1.7} /></IconButton></Tooltip>
          </Box>}
        </Paper>
      </Box>

      {/* Export Dialog */}
      {savedCrewId && (
        <ExportCrewDialog
          open={isExportDialogOpen}
          onClose={() => setIsExportDialogOpen(false)}
          crewId={savedCrewId}
          crewName={savedCrewName}
        />
      )}
    </>
  );
};

export default RightSidebar;
