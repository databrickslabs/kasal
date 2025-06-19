import React from 'react';
import { 
  Paper, 
  Box, 
  IconButton,
  Tooltip,
} from '@mui/material';
import { 
  SmartToy as SmartToyIcon,
  ChevronRight as ChevronRightIcon,
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
  onOpenLogs
}) => {
  const { selectedModel, setSelectedModel } = useCrewExecutionStore();
  const { selectedTools } = useJobManagementStore();

  if (isCollapsed) {
    // Collapsed state - show only icon and expand button
    return (
      <Paper 
        sx={{ 
          width: 60,
          height: '100%', 
          display: 'flex', 
          flexDirection: 'column',
          alignItems: 'center',
          borderLeft: 1,
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
          <SmartToyIcon 
            sx={{ 
              fontSize: '1.5rem', 
              color: 'primary.main',
            }} 
          />
          <Tooltip title="Expand Kasal Chat" placement="left">
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
              <ChevronRightIcon fontSize="small" />
            </IconButton>
          </Tooltip>
        </Box>
      </Paper>
    );
  }

  // Expanded state - full chat panel
  return (
    <Paper 
      sx={{ 
        height: '100%', 
        width: '100%',
        display: 'flex', 
        flexDirection: 'column',
        borderLeft: 1,
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