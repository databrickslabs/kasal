import { Box, Typography, TextField, Paper, Chip, Divider } from '@mui/material';
import { Listener, Action, StartingPoint } from '../../types/flow';

interface ReviewStepProps {
  flowName: string;
  setFlowName: (name: string) => void;
  listeners: Listener[];
  actions: Action[];
  startingPoints: StartingPoint[];
}

const ReviewStep = ({ flowName, setFlowName, listeners, actions, startingPoints }: ReviewStepProps) => {
  return (
    <Box sx={{ pt: 2, pb: 2 }}>
      <Typography variant="h6" gutterBottom>
        Review Your Flow Configuration
      </Typography>
      
      <TextField
        fullWidth
        label="Flow Name"
        value={flowName}
        onChange={(e) => setFlowName(e.target.value)}
        required
        error={!flowName.trim()}
        helperText={!flowName.trim() ? 'Flow name is required' : ''}
        sx={{ mb: 3 }}
      />
      
      <Paper variant="outlined" sx={{ p: 2, mb: 3 }}>
        <Typography variant="subtitle1" gutterBottom>
          Starting Point Tasks
        </Typography>
        {startingPoints.filter(point => point.isStartPoint).length === 0 ? (
          <Typography variant="body2" color="error">
            No starting point tasks defined. Please go back and select at least one task.
          </Typography>
        ) : (
          <Box>
            <Typography variant="caption" color="text.secondary" sx={{ mb: 1, display: 'block' }}>
              The following tasks will trigger the flow execution:
            </Typography>
            {startingPoints
              .filter(point => point.isStartPoint)
              .map(point => (
                <Box key={point.taskId} sx={{ mb: 1 }}>
                  <Typography variant="body2">
                    {point.crewName}:{point.taskName}
                  </Typography>
                </Box>
              ))}
          </Box>
        )}
      </Paper>
      
      <Paper variant="outlined" sx={{ p: 2, mb: 3 }}>
        <Typography variant="subtitle1" gutterBottom>
          Listeners
        </Typography>
        {listeners.map((listener) => (
          <Box key={listener.id} sx={{ mb: 2 }}>
            <Typography variant="body2" fontWeight="bold">
              {listener.name}
            </Typography>
            <Typography variant="body2">
              Listening to: {listener.listenToTaskNames.length > 0 
                ? listener.listenToTaskNames.join(', ')
                : 'Not set'}
            </Typography>
            <Typography variant="body2">
              State Type: {listener.state.stateType}
            </Typography>
            <Typography variant="body2">
              Tasks to Execute: {listener.tasks.map(t => t.name).join(', ')}
            </Typography>
            <Divider sx={{ my: 1 }} />
          </Box>
        ))}
      </Paper>
      
      {actions.length > 0 && (
        <Paper variant="outlined" sx={{ p: 2, mb: 3 }}>
          <Typography variant="subtitle1" gutterBottom>
            Actions
          </Typography>
          <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1 }}>
            {actions.map(action => (
              <Chip 
                key={action.id} 
                label={`${action.crewName}:${action.taskName}`} 
                variant="outlined" 
              />
            ))}
          </Box>
        </Paper>
      )}
    </Box>
  );
};

export default ReviewStep; 