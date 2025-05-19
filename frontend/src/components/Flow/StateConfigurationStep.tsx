import { useState } from 'react';
import { Box, Typography, Paper, Grid } from '@mui/material';
import { Listener } from '../../types/flow';
import { EdgeStateForm } from './EdgeStateForm';

interface StateConfigurationStepProps {
  listeners: Listener[];
  onStateUpdate: (state: { stateType: 'structured' | 'unstructured'; stateDefinition: string; stateData: Record<string, unknown> }) => void;
}

const StateConfigurationStep = ({ listeners, onStateUpdate }: StateConfigurationStepProps) => {
  const [currentListenerIndex, setCurrentListenerIndex] = useState<number>(-1);

  if (listeners.length === 0) {
    return (
      <Typography>No listeners configured. Go back to add listeners.</Typography>
    );
  }

  return (
    <Box sx={{ pt: 2, pb: 2 }}>
      <Typography variant="body1" gutterBottom>
        Configure state handling for listeners:
      </Typography>
      <Typography variant="caption" color="text.secondary" sx={{ mb: 2, display: 'block' }}>
        Define how each listener will process the output from the task it&apos;s listening to.
      </Typography>
      
      <Grid container spacing={2}>
        <Grid item xs={12} sm={4}>
          <Typography variant="subtitle2" gutterBottom>
            Listeners
          </Typography>
          <Paper variant="outlined" sx={{ p: 2, height: '400px', overflow: 'auto' }}>
            {listeners.map((listener, index) => (
              <Box 
                key={listener.id}
                onClick={() => setCurrentListenerIndex(index)}
                sx={{
                  p: 1.5,
                  mb: 1,
                  borderRadius: 1,
                  cursor: 'pointer',
                  bgcolor: currentListenerIndex === index ? 'primary.light' : 'grey.100',
                  '&:hover': {
                    bgcolor: currentListenerIndex === index ? 'primary.light' : 'grey.200'
                  }
                }}
              >
                <Typography variant="body2" fontWeight={currentListenerIndex === index ? 'bold' : 'normal'}>
                  {listener.name}
                </Typography>
                <Typography variant="caption" color="text.secondary" display="block">
                  Listening to: {listener.listenToTaskNames.join(', ') || 'Not set'}
                </Typography>
                <Typography variant="caption" color="text.secondary">
                  State: {listener.state.stateType}
                </Typography>
              </Box>
            ))}
          </Paper>
        </Grid>
        
        {currentListenerIndex >= 0 && (
          <Grid item xs={12} sm={8}>
            <Typography variant="subtitle2" gutterBottom>
              Configure State for Listener: {listeners[currentListenerIndex]?.name}
            </Typography>
            <Typography variant="caption" color="text.secondary" sx={{ mb: 2, display: 'block' }}>
              Listening to: {listeners[currentListenerIndex]?.listenToTaskNames.join(', ') || 'Not set'}
            </Typography>
            <Paper variant="outlined" sx={{ p: 2, height: '400px', overflow: 'auto' }}>
              <EdgeStateForm
                initialData={listeners[currentListenerIndex]?.state}
                onSubmit={onStateUpdate}
              />
            </Paper>
          </Grid>
        )}
      </Grid>
    </Box>
  );
};

export default StateConfigurationStep; 