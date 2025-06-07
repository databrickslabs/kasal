import React, { useRef } from 'react';
import { Box, Paper } from '@mui/material';
import ExecutionHistory, { RunHistoryRef } from './ExecutionHistory';

const JobsPanel: React.FC = () => {
  const runHistoryRef = useRef<RunHistoryRef>(null);

  return (
    <Paper sx={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
      <Box sx={{ flex: 1, overflow: 'hidden' }}>
        <ExecutionHistory 
          ref={runHistoryRef}
        />
      </Box>
    </Paper>
  );
};

export default JobsPanel; 