import React from 'react';
import { Paper, Box, Typography } from '@mui/material';
import SmartToyIcon from '@mui/icons-material/SmartToy';
import WorkflowChat from './WorkflowChat';
import { Node, Edge } from 'reactflow';
import { useCrewExecutionStore } from '../../store/crewExecution';
import { useJobManagementStore } from '../../store/jobManagement';

interface ChatPanelProps {
  onNodesGenerated?: (nodes: Node[], edges: Edge[]) => void;
}

const ChatPanel: React.FC<ChatPanelProps> = ({ onNodesGenerated }) => {
  const { selectedModel } = useCrewExecutionStore();
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
          selectedModel={selectedModel}
          selectedTools={selectedTools}
        />
      </Box>
    </Paper>
  );
};

export default ChatPanel; 