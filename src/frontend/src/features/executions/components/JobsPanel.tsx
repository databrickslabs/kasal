import React, { useRef } from 'react';
import { Box, Paper } from '@mui/material';
import ExecutionHistory, { RunHistoryRef } from './ExecutionHistory';

interface JobsPanelProps {
  onClose?: () => void;
  executionHistoryHeight?: number;
  onExecutionCountChange?: (count: number) => void;
}

const JobsPanel: React.FC<JobsPanelProps> = ({ executionHistoryHeight = 200, onExecutionCountChange, onClose }) => {
  const runHistoryRef = useRef<RunHistoryRef>(null);

  return (
    <Paper elevation={0} sx={{ height: '100%', display: 'flex', flexDirection: 'column', background: 'transparent' }}>
      <Box sx={{ flex: 1, overflow: 'hidden' }}>
        <ExecutionHistory 
          ref={runHistoryRef}
          onClose={onClose}
          executionHistoryHeight={executionHistoryHeight}
          onExecutionCountChange={onExecutionCountChange}
        />
      </Box>
    </Paper>
  );
};

export default JobsPanel; 