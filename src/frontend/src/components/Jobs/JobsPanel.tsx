import React, { useRef } from 'react';
import { Box, Paper } from '@mui/material';
import ExecutionHistory, { RunHistoryRef } from './ExecutionHistory';

interface JobsPanelProps {
  executionHistoryHeight?: number;
  onExecutionCountChange?: (count: number) => void;
}

const JobsPanel: React.FC<JobsPanelProps> = ({ executionHistoryHeight = 200, onExecutionCountChange }) => {
  const runHistoryRef = useRef<RunHistoryRef>(null);

  return (
    <Paper sx={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
      <Box sx={{ flex: 1, overflow: 'hidden' }}>
        <ExecutionHistory 
          ref={runHistoryRef}
          executionHistoryHeight={executionHistoryHeight}
          onExecutionCountChange={onExecutionCountChange}
        />
      </Box>
    </Paper>
  );
};

export default JobsPanel; 