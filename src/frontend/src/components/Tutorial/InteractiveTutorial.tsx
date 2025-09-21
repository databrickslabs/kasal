import React, { useState, useEffect, useMemo } from 'react';
import Joyride, { CallBackProps, Step } from 'react-joyride';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  Box,
  Card,
  CardContent,
  Typography,
  Button,
  Chip,
  Fade,
  IconButton,
  Stack,
  Avatar,
} from '@mui/material';
import {
  AdminPanelSettings as AdminIcon,
  Edit as EditorIcon,
  PlayCircle as OperatorIcon,
  Close as CloseIcon,
  School as SchoolIcon,
  Settings as SettingsIcon,
  Build as BuildIcon,
  PlayArrow as PlayIcon,
} from '@mui/icons-material';

// Define STATUS constants since they're not exported properly
const STATUS = {
  FINISHED: 'finished',
  SKIPPED: 'skipped',
  ERROR: 'error',
  READY: 'ready',
  RUNNING: 'running',
  PAUSED: 'paused',
  IDLE: 'idle',
  WAITING: 'waiting',
};
import { usePermissionStore } from '../../store/permissions';
import { useWorkflowStore } from '../../store/workflow';
import { useTheme } from '@mui/material/styles';
import { TutorialProps } from '../../types/tutorial';

// Custom styles for the tour
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const getJoyrideStyles = (isDarkMode: boolean): any => ({
  options: {
    primaryColor: '#1976d2',
    textColor: isDarkMode ? '#fff' : '#333',
    backgroundColor: isDarkMode ? '#1e1e1e' : '#fff',
    overlayColor: 'rgba(0, 0, 0, 0.5)',
    spotlightShadow: '0 0 15px rgba(25, 118, 210, 0.5)',
    beaconSize: 36,
    arrowColor: isDarkMode ? '#1e1e1e' : '#fff',
    zIndex: 10000,
  },
  tooltip: {
    borderRadius: 8,
    fontSize: 14,
  },
  tooltipContainer: {
    textAlign: 'left',
  },
  tooltipTitle: {
    fontSize: 18,
    marginBottom: 8,
    fontWeight: 'bold',
  },
  tooltipContent: {
    fontSize: 14,
    lineHeight: 1.6,
  },
  buttonNext: {
    backgroundColor: '#1976d2',
    borderRadius: 4,
    color: '#fff',
    fontSize: 14,
    padding: '8px 16px',
  },
  buttonBack: {
    color: isDarkMode ? '#90caf9' : '#1976d2',
    fontSize: 14,
    marginRight: 8,
  },
  buttonSkip: {
    color: isDarkMode ? '#999' : '#666',
    fontSize: 13,
  },
  beacon: {
    animation: 'pulse 2s infinite',
  },
  spotlight: {
    borderRadius: 8,
  },
});

const InteractiveTutorial: React.FC<TutorialProps> = ({ isOpen, onClose }) => {
  const theme = useTheme();
  const isDarkMode = theme.palette.mode === 'dark';
  const { userRole } = usePermissionStore();
  const { setHasSeenTutorial } = useWorkflowStore();
  const [run, setRun] = useState(false);
  const [selectedRole, setSelectedRole] = useState<string | null>(null);
  const [showRoleSelection, setShowRoleSelection] = useState(true);
  const [showSelectionDialog, setShowSelectionDialog] = useState(false);

  // Tutorial role options
  const tutorialRoles = [
    {
      id: 'admin',
      title: 'Admin Tutorial',
      icon: <AdminIcon />,
      color: '#f44336',
      description: 'System configuration and platform management',
      features: [
        'Database configuration',
        'Memory backend setup',
        'User & group management',
        'API key management',
      ],
    },
    {
      id: 'editor',
      title: 'Editor Tutorial',
      icon: <EditorIcon />,
      color: '#2196f3',
      description: 'Create and build AI workflows',
      features: [
        'Create agents & tasks',
        'Build workflows',
        'Execute crews',
        'Monitor execution',
      ],
    },
    {
      id: 'operator',
      title: 'Operator Tutorial',
      icon: <OperatorIcon />,
      color: '#4caf50',
      description: 'Run and monitor workflows',
      features: [
        'Browse catalog',
        'Execute workflows',
        'View trace & logs',
        'Manage results',
      ],
    },
  ];

  const handleRoleSelect = (role: string) => {
    setSelectedRole(role);
    setShowRoleSelection(false);
    setShowSelectionDialog(false);
    setTimeout(() => {
      setRun(true);
    }, 300);
  };

  // Admin-specific steps - Focus on Configuration
  const adminSteps: Step[] = [
    {
      target: '[data-tour="workflow-designer"]',
      content: '👋 Welcome Admin! This tutorial will guide you through system configuration and management. As an admin, you have full control over the platform settings, user management, and system resources.',
      placement: 'center',
      disableBeacon: true,
    },
    {
      target: '[data-tour="configuration-button"]',
      content: '⚙️ Configuration Center: This is your main control panel. Here you can:\n• Configure database connections\n• Set up memory backends\n• Manage API keys and model providers\n• Control user access and groups\n• Set system-wide preferences',
      placement: 'right',
    },
    {
      target: '[data-tour="left-sidebar"]',
      content: '🎛️ System Controls: The left sidebar gives you quick access to runtime features, process types, and model configurations. You can switch between Sequential and Hierarchical processing modes here.',
      placement: 'right',
    },
    {
      target: '[data-tour="runtime-features"]',
      content: '🚀 Advanced Features: Configure how AI agents work:\n• Planning: Enable agents to plan before executing\n• Reasoning: Allow agents to explain their decisions\n• Process Type: Choose Sequential (one-by-one) or Hierarchical (manager delegates)',
      placement: 'right',
    },
    {
      target: '[data-tour="chat-toggle"]',
      content: '💬 Admin Chat Commands: You can use special admin commands:\n• "configure database" - Database settings\n• "setup memory" - Memory backend configuration\n• "manage users" - User administration\n• "system status" - Check system health',
      placement: 'left',
    },
    {
      target: '[data-tour="save-button"]',
      content: '📁 Template Management: As an admin, you can save workflows as global templates that all users can access. This helps standardize processes across your organization.',
      placement: 'bottom',
    },
    {
      target: '[data-tour="open-workflow"]',
      content: '📚 Catalog Access: View and manage all workflows in the system catalog. You can edit permissions, archive old workflows, and monitor usage.',
      placement: 'bottom',
    },
  ];

  // Editor-specific steps - Focus on Creating and Running Plans
  const editorSteps: Step[] = [
    {
      target: '[data-tour="workflow-designer"]',
      content: '👋 Welcome Editor! This tutorial will teach you how to create AI agents and tasks, then execute them. You have two ways to build workflows: using the chat panel (AI-assisted) or the right sidebar (manual control).',
      placement: 'center',
      disableBeacon: true,
    },
    {
      target: '[data-tour="chat-toggle"]',
      content: '💬 Chat Panel - The Easy Way: This is your AI assistant for creating workflows. Use these commands:\n• "create agent: [name]" - Creates an AI agent\n• "create task: [description]" - Creates a task\n• "create plan" - Generates a complete workflow\n• "execute crew" or "ec" - Runs your workflow',
      placement: 'left',
    },
    {
      target: '[data-tour="chat-panel"]',
      content: '📝 Important Rule: Every agent MUST have at least one task assigned to it, otherwise the execution will fail. Think of it as: agents are workers, tasks are their assignments. No assignments = no work!',
      placement: 'left',
    },
    {
      target: '[data-tour="right-sidebar"]',
      content: '🎨 Right Sidebar - Manual Control: If you prefer full control, use the right sidebar to:\n• Add agents manually\n• Add tasks manually\n• Configure each component in detail\n• Set up connections precisely',
      placement: 'left',
    },
    {
      target: '[data-tour="canvas-area"]',
      content: '🖼️ Canvas Area: Your workflow visualizes here. You can:\n• Right-click to add components\n• Drag to connect agents to tasks\n• Double-click to edit\n• See the flow of work visually',
      placement: 'center',
    },
    {
      target: '[data-tour="agent-node"]',
      content: '🤖 Agents: These are your AI workers. Each agent needs:\n• A role (what they are)\n• A goal (what they achieve)\n• Tools (what they can use)\n• At least ONE task (critical!)',
      placement: 'auto',
    },
    {
      target: '[data-tour="task-node"]',
      content: '📋 Tasks: These define the work. Each task needs:\n• A clear description\n• Expected output\n• An assigned agent\nRemember: Unassigned tasks will cause execution to fail!',
      placement: 'auto',
    },
    {
      target: '[data-tour="execute-button"]',
      content: '▶️ Execute Button: Once all agents have tasks, click here OR type "execute crew" in chat. The execution will fail if any agent lacks a task!',
      placement: 'bottom',
    },
    {
      target: '[data-tour="trace-button"]',
      content: '📊 View Trace: After execution starts, click here to monitor progress. You\'ll see:\n• Which agent is working\n• Task completion status\n• Real-time logs\n• Any errors or issues',
      placement: 'bottom',
    },
    {
      target: '[data-tour="save-button"]',
      content: '💾 Save Workflow: Once your workflow runs successfully, save it as a template for reuse. Give it a clear name and description.',
      placement: 'bottom',
    },
  ];

  // Operator-specific steps - Focus on Running Plans from Catalog
  const operatorSteps: Step[] = [
    {
      target: '[data-tour="workflow-designer"]',
      content: '👋 Welcome Operator! This tutorial will show you how to run pre-built workflows from the catalog. As an operator, your role is to execute and monitor AI workflows created by editors.',
      placement: 'center',
      disableBeacon: true,
    },
    {
      target: '[data-tour="open-workflow"]',
      content: '📚 Open from Catalog: Click here to browse available workflows. The catalog contains pre-built, tested workflows that are ready to run. Look for workflows marked as "Production Ready".',
      placement: 'bottom',
    },
    {
      target: '[data-tour="catalog-dialog"]',
      content: '🔍 Catalog Browser: In the catalog, you can:\n• Search workflows by name or description\n• Filter by category or tags\n• View workflow details and requirements\n• Check execution history and success rates',
      placement: 'center',
    },
    {
      target: '[data-tour="canvas-area"]',
      content: '👁️ Workflow Visualization: Once loaded, you\'ll see:\n• All agents (AI workers) in the workflow\n• All tasks and their connections\n• Required inputs highlighted in yellow\n• Execution flow with arrows',
      placement: 'center',
    },
    {
      target: '[data-tour="chat-toggle"]',
      content: '💬 Quick Execution: You can also use chat commands:\n• "open [workflow name]" - Load a workflow\n• "execute crew" or "ec" - Run the loaded workflow\n• "status" - Check execution status\n• "stop" - Halt execution if needed',
      placement: 'left',
    },
    {
      target: '[data-tour="execute-button"]',
      content: '▶️ Execute Workflow: Before clicking:\n1. Check all agents have tasks (green checkmarks)\n2. Fill any required input fields\n3. Verify the selected model is available\n4. Click to start execution',
      placement: 'bottom',
    },
    {
      target: '[data-tour="trace-button"]',
      content: '📊 Monitor Execution: Click "View Trace" to see:\n• Live progress of each agent\n• Task completion percentages\n• Real-time logs and outputs\n• Any errors or warnings\n• Estimated time remaining',
      placement: 'bottom',
    },
    {
      target: '[data-tour="execution-panel"]',
      content: '📈 Execution Panel: This shows:\n• Current agent activity\n• Completed vs pending tasks\n• Resource usage\n• Performance metrics\nGreen = Success, Yellow = In Progress, Red = Error',
      placement: 'left',
    },
    {
      target: '[data-tour="logs-button"]',
      content: '📜 View Logs: Access detailed logs to:\n• See exactly what each agent did\n• Debug any issues\n• Export logs for reporting\n• Share results with team',
      placement: 'left',
    },
    {
      target: '[data-tour="history-tab"]',
      content: '📅 Execution History: Review past runs to:\n• Download outputs and reports\n• Compare execution times\n• Identify patterns in failures\n• Generate performance reports',
      placement: 'top',
    },
    {
      target: '[data-tour="stop-button"]',
      content: '🛑 Emergency Stop: If something goes wrong, you can stop execution immediately. The system will save partial results and log the reason for stopping.',
      placement: 'bottom',
    },
  ];

  // Get steps based on selected role
  const steps = useMemo(() => {
    // Use selected role if chosen, otherwise use user's actual role
    const roleToUse = selectedRole || userRole;

    switch (roleToUse) {
      case 'admin':
        return adminSteps;
      case 'editor':
        return editorSteps;
      case 'operator':
        return operatorSteps;
      default:
        return editorSteps; // Default to editor steps
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedRole, userRole]);

  // Start the tour when component mounts and isOpen is true
  useEffect(() => {
    if (isOpen) {
      console.log('[InteractiveTutorial] Opening with role:', userRole);

      if (showRoleSelection) {
        // Show the selection dialog
        setShowSelectionDialog(true);
      } else {
        // Start the tour directly if role is already selected
        console.log('[InteractiveTutorial] Using steps:', steps.length, 'steps');
        setTimeout(() => {
          setRun(true);
        }, 500);
      }
    } else {
      // Reset when closing
      setShowSelectionDialog(false);
      setRun(false);
    }
  }, [isOpen, userRole, steps.length, showRoleSelection]);

  const handleJoyrideCallback = (data: CallBackProps) => {
    const { status, action } = data;

    // Handle tour completion or skip
    if (status === STATUS.FINISHED || status === STATUS.SKIPPED) {
      setRun(false);
      setHasSeenTutorial(true);
      // Reset for next time
      setSelectedRole(null);
      setShowRoleSelection(true);
      onClose();
    }

    // Handle going back to role selection
    if (action === 'prev' && data.index === 0 && selectedRole) {
      setShowRoleSelection(true);
      setSelectedRole(null);
    }
  };

  if (!isOpen) return null;

  return (
    <>
      {/* Role Selection Dialog */}
      <Dialog
        open={showSelectionDialog}
        onClose={() => {
          setShowSelectionDialog(false);
          onClose();
        }}
        maxWidth="md"
        fullWidth
        PaperProps={{
          sx: {
            borderRadius: 3,
            background: isDarkMode
              ? 'linear-gradient(135deg, #1e1e1e 0%, #2d2d2d 100%)'
              : 'linear-gradient(135deg, #ffffff 0%, #f5f5f5 100%)',
          },
        }}
      >
        <DialogTitle sx={{
          display: 'flex',
          alignItems: 'center',
          gap: 2,
          borderBottom: 1,
          borderColor: 'divider',
          pb: 2,
        }}>
          <Avatar sx={{ bgcolor: 'primary.main', width: 48, height: 48 }}>
            <SchoolIcon />
          </Avatar>
          <Box flex={1}>
            <Typography variant="h5" fontWeight="bold">
              Choose Your Learning Path
            </Typography>
            <Typography variant="body2" color="text.secondary">
              Select the tutorial that matches your role and learning goals
            </Typography>
          </Box>
          <IconButton
            onClick={() => {
              setShowSelectionDialog(false);
              onClose();
            }}
            sx={{ alignSelf: 'flex-start' }}
          >
            <CloseIcon />
          </IconButton>
        </DialogTitle>

        <DialogContent sx={{ p: 3 }}>
          <Stack spacing={3}>
            {tutorialRoles.map((role) => (
              <Fade in={true} timeout={500} key={role.id}>
                <Card
                  sx={{
                    cursor: 'pointer',
                    transition: 'all 0.3s ease',
                    border: 2,
                    borderColor: selectedRole === role.id ? role.color : 'transparent',
                    '&:hover': {
                      transform: 'translateY(-4px)',
                      boxShadow: 6,
                      borderColor: role.color,
                    },
                  }}
                  onClick={() => handleRoleSelect(role.id)}
                >
                  <CardContent sx={{ p: 3 }}>
                    <Stack direction="row" spacing={3} alignItems="center">
                      <Avatar
                        sx={{
                          bgcolor: `${role.color}20`,
                          color: role.color,
                          width: 64,
                          height: 64,
                        }}
                      >
                        {role.icon}
                      </Avatar>

                      <Box flex={1}>
                        <Stack direction="row" alignItems="center" gap={2} mb={1}>
                          <Typography variant="h6" fontWeight="bold">
                            {role.title}
                          </Typography>
                          {userRole === role.id && (
                            <Chip
                              label="Your Role"
                              size="small"
                              color="primary"
                              variant="filled"
                            />
                          )}
                        </Stack>

                        <Typography variant="body2" color="text.secondary" mb={2}>
                          {role.description}
                        </Typography>

                        <Stack direction="row" flexWrap="wrap" gap={1}>
                          {role.features.map((feature, index) => (
                            <Chip
                              key={index}
                              label={feature}
                              size="small"
                              variant="outlined"
                              sx={{
                                borderColor: `${role.color}40`,
                                color: role.color,
                                fontSize: '0.75rem',
                              }}
                            />
                          ))}
                        </Stack>
                      </Box>

                      <Button
                        variant="contained"
                        size="large"
                        sx={{
                          bgcolor: role.color,
                          minWidth: 120,
                          '&:hover': {
                            bgcolor: role.color,
                            filter: 'brightness(0.9)',
                          }
                        }}
                        onClick={(e) => {
                          e.stopPropagation();
                          handleRoleSelect(role.id);
                        }}
                      >
                        Start
                      </Button>
                    </Stack>
                  </CardContent>
                </Card>
              </Fade>
            ))}
          </Stack>

          <Box mt={3} p={2} bgcolor="action.hover" borderRadius={2}>
            <Typography variant="body2" color="text.secondary" align="center">
              💡 <strong>Tip:</strong> You can restart the tutorial anytime by clicking the help button in the sidebar
            </Typography>
          </Box>
        </DialogContent>
      </Dialog>

      {/* Joyride Tutorial */}
      {!showSelectionDialog && (
        <Joyride
          steps={steps}
          run={run}
          continuous
          showProgress
          showSkipButton
          styles={getJoyrideStyles(isDarkMode)}
          callback={handleJoyrideCallback}
          disableCloseOnEsc={false}
          disableOverlayClose={false}
          hideCloseButton={false}
        />
      )}
    </>
  );
};

export default InteractiveTutorial;