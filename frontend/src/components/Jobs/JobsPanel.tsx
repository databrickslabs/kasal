import React, { useRef } from 'react';
import { Box, Paper } from '@mui/material';
import ExecutionHistory, { RunHistoryRef } from './ExecutionHistory';
import { Node, Edge } from 'reactflow';

interface JobsPanelProps {
  onCrewLoad?: (nodes: Node[], edges: Edge[]) => void;
}

const JobsPanel: React.FC<JobsPanelProps> = ({ onCrewLoad }) => {
  const runHistoryRef = useRef<RunHistoryRef>(null);

  return (
    <Paper sx={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
      <Box sx={{ flex: 1, overflow: 'hidden' }}>
        <ExecutionHistory 
          ref={runHistoryRef}
          onCrewLoad={onCrewLoad} 
        />
      </Box>
    </Paper>
  );
};

export default JobsPanel; 