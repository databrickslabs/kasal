import React, { useState, useEffect } from 'react';
import {
  Box,
  IconButton,
  Tooltip,
  Paper,
  useTheme,
  CircularProgress,
  Divider,
  GlobalStyles,
} from '@mui/material';
import {
  SmartToy as SmartToyIcon,
  PersonAdd as PersonAddIcon,
  AddTask as AddTaskIcon,
  AccountTree as AccountTreeIcon,
  PlayArrow as PlayArrowIcon,
  Save as SaveIcon,
  MenuBook as MenuBookIcon,
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
  handleExecuteCrew?: () => void;
  handleExecuteFlow?: () => void;
  isExecuting?: boolean;
  onSaveCrewClick?: () => void;
  showRunHistory?: boolean;
}

const RightSidebar: React.FC<RightSidebarProps> = ({
  onOpenLogsDialog,
  onToggleChat,
  isChatOpen,
  setIsAgentDialogOpen,
  setIsTaskDialogOpen,
  setIsFlowDialogOpen,
  setIsCrewDialogOpen,
  handleExecuteCrew,
  handleExecuteFlow,
  isExecuting = false,
  onSaveCrewClick,
  showRunHistory = false,
}) => {
  const theme = useTheme();
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
      id: 'chat',
      icon: <SmartToyIcon />,
      tooltip: 'Kasal',
      onClick: onToggleChat
    },
    {
      id: 'execute-crew',
      icon: isExecuting ? <CircularProgress size={20} /> : <PlayArrowIcon />,
      tooltip: 'Execute Crew',
      onClick: handleExecuteCrew,
      disabled: isExecuting
    },
    {
      id: 'separator1',
      isSeparator: true
    },
    {
      id: 'add-agent',
      icon: <PersonAddIcon />,
      tooltip: 'Add Agent',
      onClick: () => setIsAgentDialogOpen(true)
    },
    {
      id: 'add-task',
      icon: <AddTaskIcon />,
      tooltip: 'Add Task',
      onClick: () => setIsTaskDialogOpen(true)
    },
    {
      id: 'separator2',
      isSeparator: true
    },
    {
      id: 'save-crew',
      icon: <SaveIcon />,
      tooltip: 'Save Crew',
      onClick: onSaveCrewClick
    },
    {
      id: 'open-catalog',
      icon: <MenuBookIcon />,
      tooltip: 'Open Catalog',
      onClick: () => setIsCrewDialogOpen?.(true)
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
        onClick: () => setIsFlowDialogOpen(true)
      },
      {
        id: 'execute-flow',
        icon: isExecuting ? <CircularProgress size={20} /> : <PlayArrowIcon />,
        tooltip: 'Execute Flow',
        onClick: handleExecuteFlow,
        disabled: isExecuting
      }
    ] : [])
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
          height: showRunHistory ? 'calc(100% - 48px - 200px)' : 'calc(100% - 48px)', // Account for TabBar and execution history
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
            height: showRunHistory ? 'calc(100% - 48px - 200px)' : 'calc(100% - 48px)',
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
                      if (item.onClick) {
                        // Chat icon toggles chat visibility
                        if (item.id === 'chat') {
                          setChatOpenedByClick(!chatOpenedByClick);
                          item.onClick();
                        }
                        // Other items execute their actions if not disabled
                        else if (!item.disabled) {
                          item.onClick();
                        }
                      }
                    }}
                    disabled={item.disabled}
                    sx={{
                      width: 40,
                      height: 40,
                      mb: 1,
                      color: item.id === 'chat' ? theme.palette.primary.main : 
                            item.id === 'execute-crew' ? theme.palette.primary.main : 'text.secondary',
                      backgroundColor: (item.id === 'chat' && isChatOpen)
                        ? 'action.selected'
                        : 'transparent',
                      borderRight: (item.id === 'chat' && isChatOpen)
                        ? `2px solid ${theme.palette.primary.main}`
                        : '2px solid transparent',
                      borderRadius: '50%',
                      display: 'flex',
                      alignItems: 'center',
                      justifyContent: 'center',
                      transition: 'all 0.2s cubic-bezier(.4,2,.6,1)',
                      opacity: item.disabled ? 0.6 : 1,
                      cursor: item.disabled ? 'not-allowed' : 'pointer',
                      '&:hover': !item.disabled ? {
                        backgroundColor: 'action.hover',
                        color: item.id === 'chat' ? theme.palette.primary.main : 
                              item.id === 'execute-crew' ? theme.palette.primary.main : 'text.primary',
                      } : {},
                      animation: item.id === 'chat' && animateAIAssistant ? 'ai-bounce 1.2s' : 'none',
                    }}
                  >
                    {item.id === 'chat' ? (
                      <SmartToyIcon sx={{ fontSize: '2rem', color: theme.palette.primary.main }} />
                    ) : (
                      item.icon
                    )}
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