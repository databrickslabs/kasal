import { Box, Typography, Paper, List, ListItem, ListItemText, Checkbox } from '@mui/material';
import { CrewTask } from '../../types/crewPlan';
import { StartingPoint } from '../../types/flow';

interface StartingPointsStepProps {
  tasks: CrewTask[];
  startingPoints: StartingPoint[];
  onToggleStartingPoint: (taskId: string) => void;
}

const StartingPointsStep = ({ tasks, startingPoints, onToggleStartingPoint }: StartingPointsStepProps) => {
  const validStartingPoints = startingPoints.filter(point => {
    const task = tasks.find(t => t.id === point.taskId);
    return task && !task.id.startsWith('agent-');
  });

  if (validStartingPoints.length === 0) {
    return (
      <Typography variant="body2" color="text.secondary" sx={{ p: 2 }}>
        No valid tasks found for the selected crews. Please go back and select different crews.
      </Typography>
    );
  }

  return (
    <Box sx={{ pt: 2, pb: 2 }}>
      <Typography variant="body1" gutterBottom>
        Select tasks for your flow:
      </Typography>
      
      <Box sx={{ mt: 2 }}>
        <Typography variant="caption" color="text.secondary" sx={{ mb: 1, display: 'block' }}>
          At least one task must be selected as a starting point. Starting points are specific tasks that will trigger the workflow execution.
        </Typography>
        
        <Paper variant="outlined" sx={{ mb: 2, p: 2 }}>
          <Typography variant="subtitle2" color="primary" gutterBottom>
            Only tasks (with numeric IDs) can be starting points, not agents
          </Typography>
          
          <List dense>
            {validStartingPoints.map(point => {
              const task = tasks.find(t => t.id === point.taskId);
              
              if (!task) {
                return null;
              }
              
              return (
                <ListItem 
                  key={point.taskId}
                  secondaryAction={
                    <Checkbox
                      edge="end"
                      checked={point.isStartPoint}
                      onChange={() => onToggleStartingPoint(point.taskId)}
                      inputProps={{ 'aria-labelledby': `task-${point.taskId}` }}
                    />
                  }
                >
                  <ListItemText
                    id={`task-${point.taskId}`}
                    primary={`${point.crewName}:${task.name}`}
                    secondary={`ID: ${task.id} (Task)`}
                  />
                </ListItem>
              );
            })}
          </List>
        </Paper>
      </Box>
    </Box>
  );
};

export default StartingPointsStep; 