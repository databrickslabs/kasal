import React, { useState, useEffect } from 'react';
import {
  Box,
  IconButton,
  Tooltip,
  Paper,
  Divider,
  GlobalStyles,
} from '@mui/material';
import {
  PersonAdd as PersonAddIcon,
  AddTask as AddTaskIcon,
  AccountTree as AccountTreeIcon,
  Save as SaveIcon,
  MenuBook as MenuBookIcon,
  Schedule as ScheduleIcon,
  Assessment as LogsIcon,
} from '@mui/icons-material';
import { useFlowConfigStore } from '../../store/flowConfig';

interface RightSidebarProps {
  onOpenLogsDialog: () => void;
  onToggleChat: () => void;
  isChatOpen: boolean;
  setIsAgentDialogOpen: (open: boolean) => void;
  setIsTaskDialogOpen: (open: boolean) => void;
  setIsFlowDialogOpen: (open: boolean) => void;
  setIsCrewDialogOpen?: (open: boolean) => void;
  onSaveCrewClick?: () => void;
  showRunHistory?: boolean;
  executionHistoryHeight?: number;
  onOpenSchedulesDialog?: () => void;
}

const RightSidebar: React.FC<RightSidebarProps> = ({
  onOpenLogsDialog,
  onToggleChat,
  isChatOpen,
  setIsAgentDialogOpen,
  setIsTaskDialogOpen,
  setIsFlowDialogOpen,
  setIsCrewDialogOpen,
  onSaveCrewClick,
  showRunHistory = false,
  executionHistoryHeight = 200,
  onOpenSchedulesDialog,
}) => {
  const [animateAIAssistant, setAnimateAIAssistant] = useState(true);
  const [chatOpenedByClick, setChatOpenedByClick] = useState(false);
  const { crewAIFlowEnabled } = useFlowConfigStore();


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


  const sidebarItems = [
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
      id: 'save-crew',
      icon: <SaveIcon />,
      tooltip: 'Save Crew',
      onClick: onSaveCrewClick,
      disabled: false
    },
    {
      id: 'open-catalog',
      icon: <MenuBookIcon />,
      tooltip: 'Open Catalog',
      onClick: () => setIsCrewDialogOpen?.(true),
      disabled: false
    },
    ...(crewAIFlowEnabled ? [
      {
        id: 'separator3',
        isSeparator: true
      },
      {
        id: 'add-flow',
        icon: <AccountTreeIcon />,
        tooltip: 'Add Flow',
        onClick: () => setIsFlowDialogOpen(true),
        disabled: false
      }
    ] : []),
    {
      id: 'separator4',
      isSeparator: true
    },
    {
      id: 'view-logs',
      icon: <LogsIcon />,
      tooltip: 'View System Logs',
      onClick: onOpenLogsDialog,
      disabled: false
    },
    {
      id: 'schedules',
      icon: <ScheduleIcon />,
      tooltip: 'Schedules',
      onClick: onOpenSchedulesDialog,
      disabled: !onOpenSchedulesDialog
    }
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
            borderLeft: 1,
            borderColor: 'divider',
            backgroundColor: 'background.paper',
            zIndex: 5,
            borderRadius: 0
          }}
        >
          {sidebarItems.map((item) => (
            <React.Fragment key={item.id}>
              {item.isSeparator ? (
                <Divider sx={{ width: '80%', my: 0.5 }} />
              ) : (
                <Tooltip title={item.tooltip} placement="left">
                  <IconButton
                    onClick={() => {
                      if (item.onClick && !item.disabled) {
                        item.onClick();
                      }
                    }}
                    disabled={item.disabled}
                    sx={{
                      width: 40,
                      height: 40,
                      mb: 1,
                      color: 'text.secondary',
                      backgroundColor: 'transparent',
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
                      animation: 'none',
                    }}
                  >
                    {item.icon}
                  </IconButton>
                </Tooltip>
              )}
            </React.Fragment>
          ))}
        </Paper>
      </Box>
    </>
  );
};

export default RightSidebar;