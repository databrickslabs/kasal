import React from 'react';
import { useWorkflowRedux } from '../hooks/workflow/useWorkflowRedux';
import { Box, Button, Typography } from '@mui/material';

export const WorkflowTest: React.FC = () => {
  const {
    nodes,
    edges,
    handleClearCanvas,
    handleDeleteEdge,
  } = useWorkflowRedux({
    showErrorMessage: (message) => console.error(message)
  });

  return (
    <Box sx={{ p: 2 }}>
      <Typography variant="h6">Workflow Redux Test</Typography>
      <Box sx={{ mt: 2 }}>
        <Typography>Nodes: {nodes.length}</Typography>
        <Typography>Edges: {edges.length}</Typography>
      </Box>
      <Box sx={{ mt: 2 }}>
        <Button 
          variant="contained" 
          onClick={handleClearCanvas}
          sx={{ mr: 1 }}
        >
          Clear Canvas
        </Button>
        {edges.length > 0 && (
          <Button 
            variant="contained" 
            color="error"
            onClick={() => handleDeleteEdge(edges[0].id)}
          >
            Delete First Edge
          </Button>
        )}
      </Box>
    </Box>
  );
}; 