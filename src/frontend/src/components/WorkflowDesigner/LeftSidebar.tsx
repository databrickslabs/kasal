import React, { useState, useEffect, useCallback } from 'react';
import {
  Box,
  IconButton,
  Tooltip,
  Paper,
  useTheme,
  Badge
} from '@mui/material';
import {
  CleaningServices as ClearIcon,
  ZoomIn as ZoomInIcon,
  ZoomOut as ZoomOutIcon,
  CenterFocusStrong as FitViewIcon,
  SwapHoriz as SwapHorizIcon,
  Settings as SettingsIcon,
  HelpOutline as HelpOutlineIcon,
} from '@mui/icons-material';
import { useWorkflowStore } from '../../store/workflow';
import { useUILayoutStore } from '../../store/uiLayout';



interface LeftSidebarProps {
  onClearCanvas: () => void;
  onZoomIn: () => void;
  onZoomOut: () => void;
  onFitView: () => void;
  onToggleInteractivity: () => void;
  // Runtime features props

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
}

const LeftSidebar: React.FC<LeftSidebarProps> = ({
  onClearCanvas,
  onZoomIn,
  onZoomOut,
  onFitView,
  onToggleInteractivity,
  setIsConfigurationDialogOpen,
  onOpenLogsDialog,
  showRunHistory,
  executionHistoryHeight = 200,
  onOpenTutorial
}) => {
  const theme = useTheme();
  const [activeSection, setActiveSection] = useState<string | null>(null);
  const { layoutOrientation, setLayoutOrientation } = useUILayoutStore();

  const toggleLayoutOrientation = useCallback(() => {
    const next = (layoutOrientation === 'horizontal') ? 'vertical' : 'horizontal';
    setLayoutOrientation(next);
    // Trigger node repositioning and fit view (recalculateNodePositions already calls fitView)
    setTimeout(() => {
      window.dispatchEvent(new CustomEvent('recalculateNodePositions', { detail: { reason: 'layout-orientation-toggle' } }));
    }, 50);
  }, [layoutOrientation, setLayoutOrientation]);

  const { setLeftSidebarExpanded } = useUILayoutStore();

  // Reflect expanded state of the left sidebar (when a section is active) into the UI layout store
  useEffect(() => {
    setLeftSidebarExpanded(!!activeSection);
  }, [activeSection, setLeftSidebarExpanded]);


  // Get tutorial status
  const { hasSeenTutorial } = useWorkflowStore();

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