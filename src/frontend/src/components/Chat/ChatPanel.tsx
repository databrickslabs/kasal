import React from 'react';
import { 
  Paper, 
  Box, 
  IconButton,
  Tooltip,
} from '@mui/material';
import {
  ChevronRight as ChevronRightIcon,
  ChevronLeft as ChevronLeftIcon,
} from '@mui/icons-material';
import WorkflowChat from './WorkflowChat';
import { Node, Edge } from 'reactflow';
import { useCrewExecutionStore } from '../../store/crewExecution';
import { useJobManagementStore } from '../../store/jobManagement';

interface ChatPanelProps {
  onNodesGenerated?: (nodes: Node[], edges: Edge[]) => void;
  onLoadingStateChange?: (isLoading: boolean) => void;
  isVisible?: boolean;
  nodes?: Node[];
  edges?: Edge[];
  onExecuteCrew?: () => void;
  isCollapsed?: boolean;
  onToggleCollapse?: () => void;
  chatSessionId?: string;
  onOpenLogs?: (jobId: string) => void;
  chatSide?: 'left' | 'right';
}

const ChatPanel: React.FC<ChatPanelProps> = ({
  onNodesGenerated,
  onLoadingStateChange,
  isVisible = true,
  nodes = [],
  edges = [],
  onExecuteCrew,
  isCollapsed = false,
  onToggleCollapse,
  chatSessionId,
  onOpenLogs,
  chatSide = 'right'
}) => {
  const { selectedModel, setSelectedModel } = useCrewExecutionStore();
  const { selectedTools } = useJobManagementStore();

  if (isCollapsed) {
    // Collapsed state - show only icon and expand button
    return (
      <Paper
        data-tour="chat-toggle"
        sx={{
          width: 60,
          height: '100%',
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center',
          ...(chatSide === 'right' ? { borderLeft: 1 } : { borderRight: 1 }),
          borderColor: 'divider',
          borderRadius: 0,
          boxShadow: 'none',
          backgroundColor: 'background.paper',
          overflow: 'hidden',
          contain: 'layout size',
        }}
      >
        <Box sx={{
          p: 1.5,
          borderBottom: 1,
          borderColor: 'divider',
          backgroundColor: theme => theme.palette.mode === 'dark' ? 'grey.900' : 'grey.50',
          width: '100%',
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center',
          gap: 1
        }}>
          <Box component="img" src="/kasal-icon-24.png" alt="Kasal"
            sx={{ width: 24, height: 24, borderRadius: 0.5 }}
          />
          <Tooltip title="Expand Kasal Chat" placement={chatSide === 'right' ? 'left' : 'right'}>
            <IconButton
              size="small"
              onClick={onToggleCollapse}
              sx={{
                backgroundColor: 'primary.main',
                color: 'primary.contrastText',
                '&:hover': {
                  backgroundColor: 'primary.dark',
                }
              }}
            >
              {chatSide === 'right' ? (
                <ChevronLeftIcon fontSize="small" />
              ) : (
                <ChevronRightIcon fontSize="small" />
              )}
            </IconButton>
          </Tooltip>
        </Box>
      </Paper>
    );
  }

  // Expanded state - full chat panel
  return (
    <Paper
      data-tour="chat-panel"
      sx={{
        height: '100%',
        width: '100%',
        display: 'flex',
        flexDirection: 'column',
        ...(chatSide === 'right' ? { borderLeft: 1 } : { borderRight: 1 }),
        borderColor: 'divider',
        borderRadius: 0,
        boxShadow: 'none',
        transition: 'all 0.3s ease-in-out', // Smooth animation
        overflow: 'hidden', // Ensure nothing escapes this container
        position: 'relative',
        minWidth: 0, // Critical for preventing flex expansion
        isolation: 'isolate', // Create a new stacking context
        contain: 'layout size', // Strict CSS containment
      }}
    >
      <Box sx={{
        flex: 1,
        overflow: 'hidden',
        display: 'flex',
        flexDirection: 'column',
        position: 'relative',
        width: '100%',
        minWidth: 0, // Prevent flex item from growing beyond parent
        '& > *': {
          minWidth: 0, // Apply to all children to prevent overflow
          maxWidth: '100%',
        }
      }}>
        <WorkflowChat
          onNodesGenerated={onNodesGenerated}
          onLoadingStateChange={onLoadingStateChange}
          selectedModel={selectedModel}
          selectedTools={selectedTools}
          isVisible={isVisible}
          setSelectedModel={setSelectedModel}
          nodes={nodes}
          edges={edges}
          onExecuteCrew={onExecuteCrew}
          onToggleCollapse={onToggleCollapse}
          chatSessionId={chatSessionId}
          onOpenLogs={onOpenLogs}
        />
      </Box>
    </Paper>
  );
};

export default ChatPanel; 