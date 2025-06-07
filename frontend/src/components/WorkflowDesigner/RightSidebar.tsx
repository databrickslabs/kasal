import React, { useState, useEffect } from 'react';
import {
  Box,
  IconButton,
  Tooltip,
  Paper,
  useTheme,
  Typography,
  CircularProgress,
  Divider,
  GlobalStyles,
} from '@mui/material';
import {
  SmartToy as SmartToyIcon,
  Schedule as ScheduleIcon,
  PersonAdd as PersonAddIcon,
  AddTask as AddTaskIcon,
  AccountTree as AccountTreeIcon,
  PlayArrow as PlayArrowIcon,
  Save as SaveIcon,
  MenuBook as MenuBookIcon,
  AutoFixHigh as AutoFixHighIcon,
  FolderOpen as WorkflowIcon,
} from '@mui/icons-material';
import ScheduleList from '../Schedule/ScheduleList';
import { useFlowConfigStore } from '../../store/flowConfig';

interface RightSidebarProps {
  onOpenLogsDialog: () => void;
  onToggleChat: () => void;
  isChatOpen: boolean;
  setIsAgentDialogOpen: (open: boolean) => void;
  setIsTaskDialogOpen: (open: boolean) => void;
  setIsFlowDialogOpen: (open: boolean) => void;
  setIsCrewPlanningOpen?: (open: boolean) => void;
  setIsCrewDialogOpen?: (open: boolean) => void;
  setIsAgentGenerationDialogOpen?: (open: boolean) => void;
  setIsTaskGenerationDialogOpen?: (open: boolean) => void;
  handleExecuteCrew?: () => void;
  handleExecuteFlow?: () => void;
  isExecuting?: boolean;
  onSaveCrewClick?: () => void;
  showRunHistory?: boolean;
  onEditSchedule?: (schedule: unknown) => void;
}

const RightSidebar: React.FC<RightSidebarProps> = ({
  onOpenLogsDialog,
  onToggleChat,
  isChatOpen,
  setIsAgentDialogOpen,
  setIsTaskDialogOpen,
  setIsFlowDialogOpen,
  setIsCrewPlanningOpen,
  setIsCrewDialogOpen,
  setIsAgentGenerationDialogOpen,
  setIsTaskGenerationDialogOpen,
  handleExecuteCrew,
  handleExecuteFlow,
  isExecuting = false,
  onSaveCrewClick,
  showRunHistory = false,
  onEditSchedule,
}) => {
  const theme = useTheme();
  const [activeSection, setActiveSection] = useState<string | null>(null);
  const [animateAIAssistant, setAnimateAIAssistant] = useState(true);
  const [shouldKeepChatOpen, setShouldKeepChatOpen] = useState(false);
  const [chatOpenedByClick, setChatOpenedByClick] = useState(false);
  const { crewAIFlowEnabled } = useFlowConfigStore();

  useEffect(() => {
    // Trigger animation on mount, then stop after 1.5s
    if (animateAIAssistant) {
      const timeout = setTimeout(() => setAnimateAIAssistant(false), 1500);
      return () => clearTimeout(timeout);
    }
  }, [animateAIAssistant]);

  // Listen for messages from the chat panel about mouse hover state
  useEffect(() => {
    const handleMessage = (event: MessageEvent) => {
      if (event.data?.type === 'chat-hover-state') {
        setShouldKeepChatOpen(event.data.isHovering);
      }
    };

    window.addEventListener('message', handleMessage);
    return () => window.removeEventListener('message', handleMessage);
  }, []);

  // Reset chatOpenedByClick when chat is closed
  useEffect(() => {
    if (!isChatOpen) {
      setChatOpenedByClick(false);
    }
  }, [isChatOpen]);

  const handleSectionClick = (sectionId: string) => {
    setActiveSection(activeSection === sectionId ? null : sectionId);
  };

  // Calculate the proper height accounting for TabBar and optional bottom panel
  const contentHeight = showRunHistory 
    ? 'calc(100vh - 48px - 200px - 20px)' // TabBar (48px) + Bottom panel (200px) + padding
    : 'calc(100vh - 48px - 20px)'; // Just TabBar (48px) + padding

  const sidebarItems = [
    {
      id: 'execute',
      icon: <PlayArrowIcon />,
      tooltip: 'Execute Workflow',
      content: (
        <Box
          sx={{
            maxHeight: contentHeight,
            overflowY: 'auto',
            p: 1,
            width: '100%',
            boxSizing: 'border-box'
          }}
        >
          <Box sx={{ 
            mb: 2,
            width: '100%',
            boxSizing: 'border-box',
            px: 1
          }}>
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
              Execute Workflow
            </Typography>
            
            <Box
              onClick={handleExecuteCrew}
              sx={{
                display: 'flex',
                alignItems: 'center',
                gap: 1,
                py: 1,
                px: 1,
                borderRadius: 1,
                cursor: isExecuting ? 'not-allowed' : 'pointer',
                border: `1px solid ${theme.palette.divider}`,
                backgroundColor: 'background.paper',
                transition: 'all 0.2s ease-in-out',
                opacity: isExecuting ? 0.6 : 1,
                width: '220px',
                boxSizing: 'border-box',
                ml: 2,
                '&:hover': !isExecuting ? {
                  backgroundColor: 'action.hover',
                  borderColor: theme.palette.primary.main,
                  transform: 'translateY(-1px)',
                  boxShadow: theme.shadows[2],
                } : {},
                mb: 1
              }}
            >
              {isExecuting ? (
                <CircularProgress size={20} sx={{ color: theme.palette.primary.main }} />
              ) : (
                <PlayArrowIcon 
                  sx={{ 
                    fontSize: '1.2rem', 
                    color: theme.palette.primary.main 
                  }} 
                />
              )}
              <Box>
                <Typography 
                  variant="body2" 
                  sx={{ 
                    fontWeight: 500,
                    color: 'text.primary'
                  }}
                >
                  Execute Crew
                </Typography>
                <Typography 
                  variant="caption" 
                  sx={{ 
                    color: 'text.secondary',
                    fontSize: '0.7rem'
                  }}
                >
                  Run the current crew
                </Typography>
              </Box>
            </Box>

            {crewAIFlowEnabled && (
              <Box
                onClick={handleExecuteFlow}
                sx={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: 1,
                  py: 1,
                  px: 1,
                  borderRadius: 1,
                  cursor: isExecuting ? 'not-allowed' : 'pointer',
                  border: `1px solid ${theme.palette.divider}`,
                  backgroundColor: 'background.paper',
                  transition: 'all 0.2s ease-in-out',
                  opacity: isExecuting ? 0.6 : 1,
                  width: '220px',
                  boxSizing: 'border-box',
                  ml: 2,
                  '&:hover': !isExecuting ? {
                    backgroundColor: 'action.hover',
                    borderColor: theme.palette.primary.main,
                    transform: 'translateY(-1px)',
                    boxShadow: theme.shadows[2],
                  } : {},
                  mb: 1
                }}
              >
                {isExecuting ? (
                  <CircularProgress size={20} sx={{ color: theme.palette.primary.main }} />
                ) : (
                  <PlayArrowIcon 
                    sx={{ 
                      fontSize: '1.2rem', 
                      color: theme.palette.primary.main 
                    }} 
                  />
                )}
                <Box>
                  <Typography 
                    variant="body2" 
                    sx={{ 
                      fontWeight: 500,
                      color: 'text.primary'
                    }}
                  >
                    Execute Flow
                  </Typography>
                  <Typography 
                    variant="caption" 
                    sx={{ 
                      color: 'text.secondary',
                      fontSize: '0.7rem'
                    }}
                  >
                    Run the current flow
                  </Typography>
                </Box>
              </Box>
            )}
          </Box>
        </Box>
      )
    },
    {
      id: 'separator1',
      isSeparator: true
    },
    {
      id: 'workflow',
      icon: <WorkflowIcon />,
      tooltip: 'Workflow Actions',
      content: (
        <Box
          sx={{
            maxHeight: contentHeight,
            overflowY: 'auto',
            p: 1,
            width: '100%',
            boxSizing: 'border-box'
          }}
        >
          <Box sx={{ 
            mb: 2,
            width: '100%',
            boxSizing: 'border-box',
            px: 1
          }}>
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
              Save & Load
            </Typography>
            
            <Box
              onClick={onSaveCrewClick}
              sx={{
                display: 'flex',
                alignItems: 'center',
                gap: 1,
                py: 1,
                px: 1,
                borderRadius: 1,
                cursor: 'pointer',
                border: `1px solid ${theme.palette.divider}`,
                backgroundColor: 'background.paper',
                transition: 'all 0.2s ease-in-out',
                width: '220px',
                boxSizing: 'border-box',
                ml: 2,
                '&:hover': {
                  backgroundColor: 'action.hover',
                  borderColor: theme.palette.primary.main,
                  transform: 'translateY(-1px)',
                  boxShadow: theme.shadows[2],
                },
                mb: 1
              }}
            >
              <SaveIcon 
                sx={{ 
                  fontSize: '1.2rem', 
                  color: theme.palette.primary.main 
                }} 
              />
              <Box>
                <Typography 
                  variant="body2" 
                  sx={{ 
                    fontWeight: 500,
                    color: 'text.primary'
                  }}
                >
                  Save Crew
                </Typography>
                <Typography 
                  variant="caption" 
                  sx={{ 
                    color: 'text.secondary',
                    fontSize: '0.7rem'
                  }}
                >
                  Save current workflow
                </Typography>
              </Box>
            </Box>

            <Box
              onClick={() => setIsCrewDialogOpen?.(true)}
              sx={{
                display: 'flex',
                alignItems: 'center',
                gap: 1,
                py: 1,
                px: 1,
                borderRadius: 1,
                cursor: 'pointer',
                border: `1px solid ${theme.palette.divider}`,
                backgroundColor: 'background.paper',
                transition: 'all 0.2s ease-in-out',
                width: '220px',
                boxSizing: 'border-box',
                ml: 2,
                '&:hover': {
                  backgroundColor: 'action.hover',
                  borderColor: theme.palette.primary.main,
                  transform: 'translateY(-1px)',
                  boxShadow: theme.shadows[2],
                }
              }}
            >
              <MenuBookIcon 
                sx={{ 
                  fontSize: '1.2rem', 
                  color: theme.palette.primary.main 
                }} 
              />
              <Box>
                <Typography 
                  variant="body2" 
                  sx={{ 
                    fontWeight: 500,
                    color: 'text.primary'
                  }}
                >
                  Open Workflow
                </Typography>
                <Typography 
                  variant="caption" 
                  sx={{ 
                    color: 'text.secondary',
                    fontSize: '0.7rem'
                  }}
                >
                  Load saved crew or flow
                </Typography>
              </Box>
            </Box>
          </Box>
        </Box>
      )
    },
    {
      id: 'creation',
      icon: <PersonAddIcon />,
      tooltip: 'Create & Generate',
      content: (
        <Box
          sx={{
            maxHeight: contentHeight,
            overflowY: 'auto',
            p: 1,
            width: '100%',
            boxSizing: 'border-box'
          }}
        >
          <Box sx={{ 
            mb: 2,
            width: '100%',
            boxSizing: 'border-box',
            px: 1
          }}>
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
              Create Elements
            </Typography>
            
            <Box
              onClick={() => setIsAgentDialogOpen(true)}
              sx={{
                display: 'flex',
                alignItems: 'center',
                gap: 1,
                py: 1,
                px: 1,
                borderRadius: 1,
                cursor: 'pointer',
                border: `1px solid ${theme.palette.divider}`,
                backgroundColor: 'background.paper',
                transition: 'all 0.2s ease-in-out',
                width: '220px',
                boxSizing: 'border-box',
                ml: 2,
                '&:hover': {
                  backgroundColor: 'action.hover',
                  borderColor: theme.palette.primary.main,
                  transform: 'translateY(-1px)',
                  boxShadow: theme.shadows[2],
                },
                mb: 1
              }}
            >
              <PersonAddIcon 
                sx={{ 
                  fontSize: '1.2rem', 
                  color: theme.palette.primary.main 
                }} 
              />
              <Box>
                <Typography 
                  variant="body2" 
                  sx={{ 
                    fontWeight: 500,
                    color: 'text.primary'
                  }}
                >
                  Add Agent
                </Typography>
                <Typography 
                  variant="caption" 
                  sx={{ 
                    color: 'text.secondary',
                    fontSize: '0.7rem'
                  }}
                >
                  Create a new agent
                </Typography>
              </Box>
            </Box>

            <Box
              onClick={() => setIsTaskDialogOpen(true)}
              sx={{
                display: 'flex',
                alignItems: 'center',
                gap: 1,
                py: 1,
                px: 1,
                borderRadius: 1,
                cursor: 'pointer',
                border: `1px solid ${theme.palette.divider}`,
                backgroundColor: 'background.paper',
                transition: 'all 0.2s ease-in-out',
                width: '220px',
                boxSizing: 'border-box',
                ml: 2,
                '&:hover': {
                  backgroundColor: 'action.hover',
                  borderColor: theme.palette.primary.main,
                  transform: 'translateY(-1px)',
                  boxShadow: theme.shadows[2],
                },
                mb: 1
              }}
            >
              <AddTaskIcon 
                sx={{ 
                  fontSize: '1.2rem', 
                  color: theme.palette.primary.main 
                }} 
              />
              <Box>
                <Typography 
                  variant="body2" 
                  sx={{ 
                    fontWeight: 500,
                    color: 'text.primary'
                  }}
                >
                  Add Task
                </Typography>
                <Typography 
                  variant="caption" 
                  sx={{ 
                    color: 'text.secondary',
                    fontSize: '0.7rem'
                  }}
                >
                  Create a new task
                </Typography>
              </Box>
            </Box>

            {crewAIFlowEnabled && (
              <Box
                onClick={() => setIsFlowDialogOpen(true)}
                sx={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: 1,
                  py: 1,
                  px: 1,
                  borderRadius: 1,
                  cursor: 'pointer',
                  border: `1px solid ${theme.palette.divider}`,
                  backgroundColor: 'background.paper',
                  transition: 'all 0.2s ease-in-out',
                  width: '220px',
                  boxSizing: 'border-box',
                  ml: 2,
                  '&:hover': {
                    backgroundColor: 'action.hover',
                    borderColor: theme.palette.primary.main,
                    transform: 'translateY(-1px)',
                    boxShadow: theme.shadows[2],
                  },
                  mb: 1
                }}
              >
                <AccountTreeIcon 
                  sx={{ 
                    fontSize: '1.2rem', 
                    color: theme.palette.primary.main 
                  }} 
                />
                <Box>
                  <Typography 
                    variant="body2" 
                    sx={{ 
                      fontWeight: 500,
                      color: 'text.primary'
                    }}
                  >
                    Add Flow
                  </Typography>
                  <Typography 
                    variant="caption" 
                    sx={{ 
                      color: 'text.secondary',
                      fontSize: '0.7rem'
                    }}
                  >
                    Create a new flow
                  </Typography>
                </Box>
              </Box>
            )}

            <Divider sx={{ my: 2 }} />
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
              AI Generation
            </Typography>

            <Box
              onClick={() => setIsAgentGenerationDialogOpen?.(true)}
              sx={{
                display: 'flex',
                alignItems: 'center',
                gap: 1,
                py: 1,
                px: 1,
                borderRadius: 1,
                cursor: 'pointer',
                border: `1px solid ${theme.palette.divider}`,
                backgroundColor: 'background.paper',
                transition: 'all 0.2s ease-in-out',
                width: '220px',
                boxSizing: 'border-box',
                ml: 2,
                '&:hover': {
                  backgroundColor: 'action.hover',
                  borderColor: theme.palette.primary.main,
                  transform: 'translateY(-1px)',
                  boxShadow: theme.shadows[2],
                },
                mb: 1
              }}
            >
              <AutoFixHighIcon 
                sx={{ 
                  fontSize: '1.2rem', 
                  color: theme.palette.primary.main 
                }} 
              />
              <Box>
                <Typography 
                  variant="body2" 
                  sx={{ 
                    fontWeight: 500,
                    color: 'text.primary'
                  }}
                >
                  Generate Agent with AI
                </Typography>
                <Typography 
                  variant="caption" 
                  sx={{ 
                    color: 'text.secondary',
                    fontSize: '0.7rem'
                  }}
                >
                  AI-powered agent creation
                </Typography>
              </Box>
            </Box>

            <Box
              onClick={() => setIsTaskGenerationDialogOpen?.(true)}
              sx={{
                display: 'flex',
                alignItems: 'center',
                gap: 1,
                py: 1,
                px: 1,
                borderRadius: 1,
                cursor: 'pointer',
                border: `1px solid ${theme.palette.divider}`,
                backgroundColor: 'background.paper',
                transition: 'all 0.2s ease-in-out',
                width: '220px',
                boxSizing: 'border-box',
                ml: 2,
                '&:hover': {
                  backgroundColor: 'action.hover',
                  borderColor: theme.palette.primary.main,
                  transform: 'translateY(-1px)',
                  boxShadow: theme.shadows[2],
                },
                mb: 1
              }}
            >
              <AutoFixHighIcon 
                sx={{ 
                  fontSize: '1.2rem', 
                  color: theme.palette.primary.main 
                }} 
              />
              <Box>
                <Typography 
                  variant="body2" 
                  sx={{ 
                    fontWeight: 500,
                    color: 'text.primary'
                  }}
                >
                  Generate Task with AI
                </Typography>
                <Typography 
                  variant="caption" 
                  sx={{ 
                    color: 'text.secondary',
                    fontSize: '0.7rem'
                  }}
                >
                  AI-powered task creation
                </Typography>
              </Box>
            </Box>

            <Box
              onClick={() => setIsCrewPlanningOpen?.(true)}
              sx={{
                display: 'flex',
                alignItems: 'center',
                gap: 1,
                py: 1,
                px: 1,
                borderRadius: 1,
                cursor: 'pointer',
                border: `1px solid ${theme.palette.divider}`,
                backgroundColor: 'background.paper',
                transition: 'all 0.2s ease-in-out',
                width: '220px',
                boxSizing: 'border-box',
                ml: 2,
                '&:hover': {
                  backgroundColor: 'action.hover',
                  borderColor: theme.palette.primary.main,
                  transform: 'translateY(-1px)',
                  boxShadow: theme.shadows[2],
                }
              }}
            >
              <AutoFixHighIcon 
                sx={{ 
                  fontSize: '1.2rem', 
                  color: theme.palette.primary.main 
                }} 
              />
              <Box>
                <Typography 
                  variant="body2" 
                  sx={{ 
                    fontWeight: 500,
                    color: 'text.primary'
                  }}
                >
                  Generate Crew
                </Typography>
                <Typography 
                  variant="caption" 
                  sx={{ 
                    color: 'text.secondary',
                    fontSize: '0.7rem'
                  }}
                >
                  AI-powered crew generation
                </Typography>
              </Box>
            </Box>
          </Box>
        </Box>
      )
    },
    {
      id: 'separator2',
      isSeparator: true
    },
    {
      id: 'chat',
      icon: <SmartToyIcon />,
      tooltip: 'Kasal',
      content: (
        <Box
          sx={{
            maxHeight: contentHeight,
            overflowY: 'auto',
            p: 1,
            width: '100%',
            boxSizing: 'border-box'
          }}
        >
          <Box sx={{ 
            mb: 2,
            width: '100%',
            boxSizing: 'border-box',
            px: 1
          }}>
            <Box
              onClick={onToggleChat}
              sx={{
                display: 'flex',
                alignItems: 'center',
                gap: 1,
                py: 1,
                px: 1,
                borderRadius: 1,
                cursor: 'pointer',
                border: `1px solid ${isChatOpen ? theme.palette.primary.main : theme.palette.divider}`,
                backgroundColor: isChatOpen ? 'action.selected' : 'background.paper',
                transition: 'all 0.2s ease-in-out',
                width: '220px',
                boxSizing: 'border-box',
                ml: 2,
                '&:hover': {
                  backgroundColor: 'action.hover',
                  borderColor: theme.palette.primary.main,
                  transform: 'translateY(-1px)',
                  boxShadow: theme.shadows[2],
                },
              }}
            >
              <SmartToyIcon 
                sx={{ 
                  fontSize: '1.2rem', 
                  color: theme.palette.primary.main 
                }} 
              />
              <Box>
                <Typography 
                  variant="body2" 
                  sx={{ 
                    fontWeight: 500,
                    color: 'text.primary'
                  }}
                >
                  {isChatOpen ? 'Hide Kasal' : 'Show Kasal'}
                </Typography>
                <Typography 
                  variant="caption" 
                  sx={{ 
                    color: 'text.secondary',
                    fontSize: '0.7rem'
                  }}
                >
                  Get help with workflow design and automation
                </Typography>
              </Box>
            </Box>
          </Box>
        </Box>
      )
    },
    {
      id: 'separator3',
      isSeparator: true
    },
    {
      id: 'schedule',
      icon: <ScheduleIcon />,
      tooltip: 'Schedules',
      content: (
        <Box
          sx={{
            maxHeight: contentHeight,
            overflowY: 'auto',
            p: 1,
            width: '100%',
            boxSizing: 'border-box'
          }}
        >
          <ScheduleList onEditSchedule={onEditSchedule} />
        </Box>
      )
    },
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
        onMouseLeave={() => {
          setActiveSection(null);
          // Only close chat if it was opened by hover (not click) and mouse is not over the chat panel
          if (isChatOpen && !shouldKeepChatOpen && !chatOpenedByClick) {
            onToggleChat();
          }
        }}
      >
        {/* Side Panel Content */}
        {activeSection && (
          <Paper
            elevation={2}
            sx={{
              width: 320,
              height: '100%',
              bgcolor: 'background.paper',
              borderRadius: 0,
              borderLeft: '1px solid',
              borderColor: 'divider',
              overflow: 'hidden',
              transition: 'all 0.2s ease-in-out'
            }}
          >
            {sidebarItems.find(item => item.id === activeSection)?.content}
          </Paper>
        )}

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
                    onMouseEnter={() => {
                      // Special handling for chat - open it directly only if not opened by click
                      if (item.id === 'chat' && !isChatOpen) {
                        // Don't set chatOpenedByClick when opening via hover
                        onToggleChat();
                      }
                      // For other icons, close chat if it's open (unless opened by click) and set active section
                      else if (item.id !== 'chat') {
                        if (isChatOpen && !shouldKeepChatOpen && !chatOpenedByClick) {
                          onToggleChat();
                        }
                        setActiveSection(item.id);
                      }
                    }}
                    onClick={() => {
                      if (item.id === 'chat') {
                        // Toggle the click state when clicking chat
                        setChatOpenedByClick(!isChatOpen);
                        onToggleChat();
                      } else if (item.id === 'schedule') {
                        // Close chat if it's open when switching to schedule section
                        if (isChatOpen) {
                          setChatOpenedByClick(false);
                          onToggleChat();
                        }
                        handleSectionClick(item.id);
                      } else {
                        // Close chat if it's open when switching to other sections
                        if (isChatOpen) {
                          setChatOpenedByClick(false);
                          onToggleChat();
                        }
                        handleSectionClick(item.id);
                      }
                    }}
                    sx={{
                      width: 40,
                      height: 40,
                      mb: 1,
                      color: item.id === 'chat' ? theme.palette.primary.main : 'text.secondary',
                      backgroundColor: (item.id === 'chat' && isChatOpen) || activeSection === item.id 
                        ? 'action.selected'
                        : 'transparent',
                      borderRight: (item.id === 'chat' && isChatOpen) || activeSection === item.id 
                        ? `2px solid ${theme.palette.primary.main}`
                        : '2px solid transparent',
                      borderRadius: '50%',
                      display: 'flex',
                      alignItems: 'center',
                      justifyContent: 'center',
                      transition: 'all 0.2s cubic-bezier(.4,2,.6,1)',
                      '&:hover': {
                        backgroundColor: 'action.hover',
                        color: item.id === 'chat' ? theme.palette.primary.main : 'text.primary',
                      },
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