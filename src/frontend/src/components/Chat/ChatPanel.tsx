import React from 'react';
import { 
  Paper, 
  Box, 
  Typography, 
} from '@mui/material';
import SmartToyIcon from '@mui/icons-material/SmartToy';
import WorkflowChat from './WorkflowChat';
import { Node, Edge } from 'reactflow';
import { useCrewExecutionStore } from '../../store/crewExecution';
import { useJobManagementStore } from '../../store/jobManagement';

interface ChatPanelProps {
  onNodesGenerated?: (nodes: Node[], edges: Edge[]) => void;
  onLoadingStateChange?: (isLoading: boolean) => void;
  isVisible?: boolean;
}

const ChatPanel: React.FC<ChatPanelProps> = ({ onNodesGenerated, onLoadingStateChange, isVisible = true }) => {
  const { selectedModel, setSelectedModel } = useCrewExecutionStore();
  const { selectedTools } = useJobManagementStore();

  return (
    <Paper 
      sx={{ 
        height: '100%', 
        display: 'flex', 
        flexDirection: 'column',
        borderLeft: 1,
        borderColor: 'divider',
      }}
    >
      <Box 
        sx={{ 
          p: 1.5, 
          borderBottom: 1, 
          borderColor: 'divider',
          backgroundColor: theme => theme.palette.mode === 'dark' ? 'grey.900' : 'grey.50'
        }}
      >
        <Typography 
          variant="subtitle2" 
          sx={{ 
            display: 'flex', 
            alignItems: 'center', 
            gap: 1,
            fontWeight: 600
          }}
        >
          <SmartToyIcon fontSize="small" />
          Kasal
        </Typography>
      </Box>
      <Box sx={{ flex: 1, overflow: 'hidden' }}>
        <WorkflowChat
          onNodesGenerated={onNodesGenerated}
          onLoadingStateChange={onLoadingStateChange}
          selectedModel={selectedModel}
          selectedTools={selectedTools}
          isVisible={isVisible}
          setSelectedModel={setSelectedModel}
        />
      </Box>
    </Paper>
  );
};

export default ChatPanel; 