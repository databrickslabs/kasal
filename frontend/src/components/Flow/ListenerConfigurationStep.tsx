import { useState } from 'react';
import {
  Box,
  Typography,
  Button,
  Grid,
  Paper,
  IconButton,
  TextField,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Checkbox,
  ListItemText,
  Chip,
  FormLabel,
  RadioGroup,
  FormControlLabel,
  Radio,
  Divider,
  Card,
  CardContent,
  CardHeader,
  CardActions
} from '@mui/material';
import DeleteIcon from '@mui/icons-material/Delete';
import AddIcon from '@mui/icons-material/Add';
import { CrewTask } from '../../types/crewPlan';
import { Listener } from '../../types/flow';
import { CrewResponse } from '../../types/crews';

interface Route {
  name: string;
  condition: string;
  taskIds: string[];
}

interface ListenerConfigurationStepProps {
  tasks: CrewTask[];
  listeners: Listener[];
  selectedCrewIds: string[];
  crews: CrewResponse[];
  onListenerTaskChange: (tasks: string[]) => void;
  onListenToTaskChange: (tasks: string[]) => void;
  onAddListener: () => void;
  onDeleteListener: (id: string) => void;
  onUpdateListenerName: (index: number, name: string) => void;
  onUpdateConditionType?: (index: number, conditionType: 'NONE' | 'AND' | 'OR' | 'ROUTER') => void;
  onUpdateRouterConfig?: (index: number, defaultRoute: string, routes: Route[]) => void;
}

const ListenerConfigurationStep = ({
  tasks,
  listeners,
  selectedCrewIds,
  crews,
  onListenerTaskChange,
  onListenToTaskChange,
  onAddListener,
  onDeleteListener,
  onUpdateListenerName,
  onUpdateConditionType,
  onUpdateRouterConfig
}: ListenerConfigurationStepProps) => {
  const [currentListenerIndex, setCurrentListenerIndex] = useState<number>(-1);
  const [routes, setRoutes] = useState<Route[]>([]);
  const [defaultRoute, setDefaultRoute] = useState<string>('');

  const filteredTasks = tasks.filter(task => {
    return task.agent_id && selectedCrewIds.includes(task.agent_id);
  });

  const handleConditionTypeChange = (value: 'NONE' | 'AND' | 'OR' | 'ROUTER') => {
    if (currentListenerIndex >= 0 && onUpdateConditionType) {
      onUpdateConditionType(currentListenerIndex, value);
      
      // Initialize router config if switching to ROUTER
      if (value === 'ROUTER') {
        const currentListener = listeners[currentListenerIndex];
        if (!currentListener.routerConfig) {
          // Initialize with default values
          setRoutes([{ name: 'Default', condition: '', taskIds: [] }]);
          setDefaultRoute('Default');
          
          if (onUpdateRouterConfig) {
            onUpdateRouterConfig(currentListenerIndex, 'Default', [{ name: 'Default', condition: '', taskIds: [] }]);
          }
        } else {
          // Load existing router config
          setRoutes(currentListener.routerConfig.routes);
          setDefaultRoute(currentListener.routerConfig.defaultRoute);
        }
      }
    }
  };

  const addRoute = () => {
    const newRoutes = [...routes, {
      name: `Route ${routes.length + 1}`,
      condition: '',
      taskIds: []
    }];
    setRoutes(newRoutes);
    
    if (currentListenerIndex >= 0 && onUpdateRouterConfig) {
      onUpdateRouterConfig(currentListenerIndex, defaultRoute, newRoutes);
    }
  };

  const removeRoute = (index: number) => {
    const newRoutes = routes.filter((_, i) => i !== index);
    setRoutes(newRoutes);
    
    // If we're removing the default route, set a new default
    if (routes[index].name === defaultRoute && newRoutes.length > 0) {
      setDefaultRoute(newRoutes[0].name);
      if (currentListenerIndex >= 0 && onUpdateRouterConfig) {
        onUpdateRouterConfig(currentListenerIndex, newRoutes[0].name, newRoutes);
      }
    } else if (currentListenerIndex >= 0 && onUpdateRouterConfig) {
      onUpdateRouterConfig(currentListenerIndex, defaultRoute, newRoutes);
    }
  };

  const updateRouteName = (index: number, name: string) => {
    const newRoutes = [...routes];
    newRoutes[index].name = name;
    setRoutes(newRoutes);
    
    // Update default route name if it was renamed
    if (routes[index].name === defaultRoute) {
      setDefaultRoute(name);
    }
    
    if (currentListenerIndex >= 0 && onUpdateRouterConfig) {
      onUpdateRouterConfig(currentListenerIndex, defaultRoute === routes[index].name ? name : defaultRoute, newRoutes);
    }
  };

  const updateRouteCondition = (index: number, condition: string) => {
    const newRoutes = [...routes];
    newRoutes[index].condition = condition;
    setRoutes(newRoutes);
    
    if (currentListenerIndex >= 0 && onUpdateRouterConfig) {
      onUpdateRouterConfig(currentListenerIndex, defaultRoute, newRoutes);
    }
  };

  const updateRouteTaskIds = (index: number, taskIds: string[]) => {
    const newRoutes = [...routes];
    newRoutes[index].taskIds = taskIds;
    setRoutes(newRoutes);
    
    if (currentListenerIndex >= 0 && onUpdateRouterConfig) {
      onUpdateRouterConfig(currentListenerIndex, defaultRoute, newRoutes);
    }
  };

  const handleDefaultRouteChange = (routeName: string) => {
    setDefaultRoute(routeName);
    
    if (currentListenerIndex >= 0 && onUpdateRouterConfig) {
      onUpdateRouterConfig(currentListenerIndex, routeName, routes);
    }
  };

  return (
    <Box sx={{ pt: 2, pb: 2 }}>
      <Typography variant="body1" gutterBottom>
        Configure listeners for your flow:
      </Typography>
      <Typography variant="caption" color="text.secondary" sx={{ mb: 2, display: 'block' }}>
        Listeners respond to task outputs in the flow. Each listener can execute one or more tasks when triggered.
      </Typography>
      
      <Button 
        variant="outlined" 
        color="primary" 
        onClick={onAddListener}
        sx={{ mb: 3 }}
      >
        Add New Listener
      </Button>
      
      {listeners.length === 0 ? (
        <Typography variant="body2" sx={{ my: 2 }}>
          No listeners configured. Click the button above to add a listener.
        </Typography>
      ) : (
        <Grid container spacing={2} sx={{ mt: 1, mb: 2 }}>
          <Grid item xs={12} sm={4}>
            <Typography variant="subtitle2" gutterBottom>
              Listeners
            </Typography>
            <Paper variant="outlined" sx={{ p: 2, height: '300px', overflow: 'auto' }}>
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
                    },
                    position: 'relative'
                  }}
                >
                  <Typography variant="body2" fontWeight={currentListenerIndex === index ? 'bold' : 'normal'}>
                    {listener.name} {listener.conditionType !== 'NONE' ? `(${listener.conditionType})` : ''}
                  </Typography>
                  <Typography variant="caption" color="text.secondary" display="block">
                    Listening to: {listener.listenToTaskNames.join(', ') || 'Not set'}
                  </Typography>
                  <Typography variant="caption" color="text.secondary">
                    {listener.tasks.length} task(s) to execute
                  </Typography>
                  <IconButton
                    size="small"
                    onClick={(e) => {
                      e.stopPropagation();
                      onDeleteListener(listener.id);
                    }}
                    sx={{
                      position: 'absolute',
                      right: 8,
                      top: 8,
                      opacity: currentListenerIndex === index ? 1 : 0,
                      '&:hover': {
                        opacity: 1
                      }
                    }}
                  >
                    <DeleteIcon fontSize="small" />
                  </IconButton>
                </Box>
              ))}
            </Paper>
          </Grid>
          
          {currentListenerIndex >= 0 && (
            <Grid item xs={12} sm={8}>
              <Typography variant="subtitle2" gutterBottom>
                Configure Listener: {listeners[currentListenerIndex]?.name}
              </Typography>
              <Paper variant="outlined" sx={{ p: 2 }}>
                <TextField
                  fullWidth
                  label="Listener Name"
                  value={listeners[currentListenerIndex]?.name || ''}
                  onChange={(e) => onUpdateListenerName(currentListenerIndex, e.target.value)}
                  sx={{ mb: 2 }}
                />
                
                <FormControl component="fieldset" sx={{ mb: 2 }}>
                  <FormLabel component="legend">Connection Type</FormLabel>
                  <RadioGroup
                    row
                    value={listeners[currentListenerIndex]?.conditionType || 'NONE'}
                    onChange={(e) => handleConditionTypeChange(e.target.value as 'NONE' | 'AND' | 'OR' | 'ROUTER')}
                  >
                    <FormControlLabel value="NONE" control={<Radio />} label="Default" />
                    <FormControlLabel value="AND" control={<Radio />} label="AND" />
                    <FormControlLabel value="OR" control={<Radio />} label="OR" />
                    <FormControlLabel value="ROUTER" control={<Radio />} label="ROUTER" />
                  </RadioGroup>
                  <Typography variant="caption" color="text.secondary">
                    <strong>Default:</strong> Executes after all specified tasks complete. 
                    <strong> AND:</strong> Requires ALL tasks to complete before triggering. 
                    <strong> OR:</strong> Triggers when ANY of the tasks completes. 
                    <strong> ROUTER:</strong> Routes execution based on conditions.
                  </Typography>
                </FormControl>
                
                {listeners[currentListenerIndex]?.conditionType !== 'ROUTER' ? (
                  <>
                    <FormControl fullWidth sx={{ mt: 2 }}>
                      <InputLabel id="listen-to-task-label">Listen To Tasks</InputLabel>
                      <Select
                        labelId="listen-to-task-label"
                        multiple
                        value={listeners[currentListenerIndex].listenToTaskIds}
                        onChange={(e) => onListenToTaskChange(e.target.value as string[])}
                        label="Listen To Tasks"
                        renderValue={(selected) => (
                          <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
                            {(selected as string[]).map((taskId) => {
                              const task = filteredTasks.find(t => String(t.id) === taskId);
                              const crew = task?.agent_id ? crews.find(c => c.id === task.agent_id) : undefined;
                              return (
                                <Chip key={taskId} label={`${crew?.name || 'Unknown'}:${task?.name || taskId}`} />
                              );
                            })}
                          </Box>
                        )}
                      >
                        {filteredTasks.map((task) => {
                          const taskId = String(task.id);
                          const crew = task.agent_id ? crews.find(c => c.id === task.agent_id) : undefined;
                          return (
                            <MenuItem key={taskId} value={taskId}>
                              <Checkbox checked={listeners[currentListenerIndex].listenToTaskIds.includes(taskId)} />
                              <ListItemText 
                                primary={task.name}
                                secondary={`${crew?.name || 'Unknown Crew'} (Task ID: ${task.id})`}
                              />
                            </MenuItem>
                          );
                        })}
                      </Select>
                    </FormControl>
                    
                    <FormControl fullWidth sx={{ mt: 3 }}>
                      <InputLabel id="listener-task-label">Tasks to Execute</InputLabel>
                      <Select
                        labelId="listener-task-label"
                        multiple
                        value={listeners[currentListenerIndex].tasks.map(t => String(t.id))}
                        onChange={(e) => onListenerTaskChange(e.target.value as string[])}
                        label="Tasks to Execute"
                        renderValue={(selected) => (
                          <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
                            {(selected as string[]).map((taskId) => {
                              const task = filteredTasks.find(t => String(t.id) === taskId);
                              const crew = task?.agent_id ? crews.find(c => c.id === task.agent_id) : undefined;
                              return (
                                <Chip key={taskId} label={`${crew?.name || 'Unknown'}:${task?.name || taskId}`} />
                              );
                            })}
                          </Box>
                        )}
                      >
                        {filteredTasks.map((task) => {
                          const taskId = String(task.id);
                          const crew = task.agent_id ? crews.find(c => c.id === task.agent_id) : undefined;
                          return (
                            <MenuItem key={taskId} value={taskId}>
                              <Checkbox checked={listeners[currentListenerIndex].tasks.map(t => String(t.id)).includes(taskId)} />
                              <ListItemText 
                                primary={task.name}
                                secondary={`${crew?.name || 'Unknown Crew'} (Task ID: ${task.id})`}
                              />
                            </MenuItem>
                          );
                        })}
                      </Select>
                    </FormControl>
                  </>
                ) : (
                  // Router Configuration Section
                  <Box sx={{ mt: 3 }}>
                    <Divider sx={{ mb: 2 }} />
                    <Typography variant="subtitle2" gutterBottom>
                      Router Configuration
                    </Typography>
                    
                    <FormControl fullWidth sx={{ mb: 2 }}>
                      <InputLabel id="listen-to-router-task-label">Listen To Task</InputLabel>
                      <Select
                        labelId="listen-to-router-task-label"
                        value={listeners[currentListenerIndex].listenToTaskIds.length > 0 ? listeners[currentListenerIndex].listenToTaskIds[0] : ''}
                        onChange={(e) => onListenToTaskChange([e.target.value as string])}
                        label="Listen To Task"
                      >
                        {filteredTasks.map((task) => {
                          const taskId = String(task.id);
                          const crew = task.agent_id ? crews.find(c => c.id === task.agent_id) : undefined;
                          return (
                            <MenuItem key={taskId} value={taskId}>
                              <ListItemText 
                                primary={task.name}
                                secondary={`${crew?.name || 'Unknown Crew'} (Task ID: ${task.id})`}
                              />
                            </MenuItem>
                          );
                        })}
                      </Select>
                      <Typography variant="caption" color="text.secondary" sx={{ mt: 1 }}>
                        For a router, you should select exactly one task to listen to.
                      </Typography>
                    </FormControl>
                    
                    <FormControl fullWidth sx={{ mb: 2 }}>
                      <InputLabel id="default-route-label">Default Route</InputLabel>
                      <Select
                        labelId="default-route-label"
                        value={defaultRoute}
                        onChange={(e) => handleDefaultRouteChange(e.target.value)}
                        label="Default Route"
                      >
                        {routes.map((route, idx) => (
                          <MenuItem key={idx} value={route.name}>{route.name}</MenuItem>
                        ))}
                      </Select>
                      <Typography variant="caption" color="text.secondary" sx={{ mt: 1 }}>
                        The default route will be taken if no condition matches.
                      </Typography>
                    </FormControl>
                    
                    <Box sx={{ display: 'flex', justifyContent: 'flex-end', mb: 2 }}>
                      <Button 
                        startIcon={<AddIcon />} 
                        onClick={addRoute}
                        variant="outlined"
                      >
                        Add Route
                      </Button>
                    </Box>
                    
                    {routes.map((route, idx) => (
                      <Card key={idx} variant="outlined" sx={{ mb: 2 }}>
                        <CardHeader
                          title={
                            <TextField
                              fullWidth
                              label="Route Name"
                              value={route.name}
                              onChange={(e) => updateRouteName(idx, e.target.value)}
                              size="small"
                            />
                          }
                          action={
                            <IconButton onClick={() => removeRoute(idx)} disabled={routes.length <= 1}>
                              <DeleteIcon />
                            </IconButton>
                          }
                        />
                        <CardContent>
                          <TextField
                            fullWidth
                            label="Condition"
                            value={route.condition}
                            onChange={(e) => updateRouteCondition(idx, e.target.value)}
                            placeholder="e.g., state.success_flag === true"
                            helperText="JavaScript condition that evaluates to true/false"
                            sx={{ mb: 2 }}
                          />
                          
                          <FormControl fullWidth>
                            <InputLabel id={`route-tasks-${idx}-label`}>Tasks to Execute</InputLabel>
                            <Select
                              labelId={`route-tasks-${idx}-label`}
                              multiple
                              value={route.taskIds}
                              onChange={(e) => updateRouteTaskIds(idx, e.target.value as string[])}
                              label="Tasks to Execute"
                              renderValue={(selected) => (
                                <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
                                  {(selected as string[]).map((taskId) => {
                                    const task = filteredTasks.find(t => String(t.id) === taskId);
                                    const crew = task?.agent_id ? crews.find(c => c.id === task.agent_id) : undefined;
                                    return (
                                      <Chip key={taskId} label={`${crew?.name || 'Unknown'}:${task?.name || taskId}`} />
                                    );
                                  })}
                                </Box>
                              )}
                            >
                              {filteredTasks.map((task) => {
                                const taskId = String(task.id);
                                const crew = task.agent_id ? crews.find(c => c.id === task.agent_id) : undefined;
                                return (
                                  <MenuItem key={taskId} value={taskId}>
                                    <Checkbox checked={route.taskIds.includes(taskId)} />
                                    <ListItemText 
                                      primary={task.name}
                                      secondary={`${crew?.name || 'Unknown Crew'} (Task ID: ${task.id})`}
                                    />
                                  </MenuItem>
                                );
                              })}
                            </Select>
                          </FormControl>
                        </CardContent>
                        <CardActions>
                          {route.name === defaultRoute && (
                            <Typography variant="caption" color="primary" sx={{ ml: 1 }}>
                              Default Route
                            </Typography>
                          )}
                        </CardActions>
                      </Card>
                    ))}
                  </Box>
                )}
              </Paper>
            </Grid>
          )}
        </Grid>
      )}
    </Box>
  );
};

export default ListenerConfigurationStep; 