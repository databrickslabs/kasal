import React, { useState, useEffect } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  FormControl,
  FormLabel,
  RadioGroup,
  Radio,
  FormControlLabel,
  Typography,
  Box,
  Alert,
  TextField,
  Divider,
  Checkbox,
  FormGroup
} from '@mui/material';
import { Edge, Node } from 'reactflow';

export type FlowLogicType = 'AND' | 'OR' | 'ROUTER' | 'NONE';

interface Task {
  id: string;
  name: string;
}

interface AggregatedSourceTasks {
  crewName: string;
  tasks: Task[];
}

interface EdgeConfigDialogProps {
  open: boolean;
  onClose: () => void;
  edge: Edge | null;
  nodes: Node[];  // Pass nodes as prop instead of using useReactFlow
  onSave: (edgeId: string, config: EdgeConfig) => void;
  aggregatedSourceTasks?: AggregatedSourceTasks[];  // Tasks from all source crews
}

export interface StateWrite {
  variable: string;
  value?: unknown;
  expression?: string;  // Python expression to compute value
}

export interface StateOperations {
  reads?: string[];  // State variables to read
  writes?: StateWrite[];  // State variables to write
  condition?: string;  // State-based condition for routing
}

export interface EdgeConfig {
  logicType: FlowLogicType;
  routerCondition?: string;
  description?: string;
  listenToTaskIds?: string[];  // Tasks from source crew
  targetTaskIds?: string[];     // Tasks from target crew
  stateOperations?: StateOperations;  // State operations during transition
  persistAfterExecution?: boolean;  // Method-level persistence
}

const EdgeConfigDialog: React.FC<EdgeConfigDialogProps> = ({
  open,
  onClose,
  edge,
  nodes,
  onSave,
  aggregatedSourceTasks = []
}) => {
  const [logicType, setLogicType] = useState<FlowLogicType>('NONE');
  const [routerCondition, setRouterCondition] = useState('');
  const [description, setDescription] = useState('');
  const [listenToTaskIds, setListenToTaskIds] = useState<string[]>([]);
  const [targetTaskIds, setTargetTaskIds] = useState<string[]>([]);

  // State management fields
  const [stateReads, setStateReads] = useState<string[]>([]);
  const [stateWrites, setStateWrites] = useState<StateWrite[]>([]);
  const [stateCondition, setStateCondition] = useState('');
  const [persistAfterExecution, setPersistAfterExecution] = useState(false);

  // Get target node from passed nodes prop
  const targetNode = edge ? nodes.find(n => n.id === edge.target) : null;

  // Get tasks from target node
  const targetTasks: Task[] = targetNode?.data?.allTasks || [];

  // Use aggregated source tasks if provided, otherwise fall back to single source
  const sourceNode = edge ? nodes.find(n => n.id === edge.source) : null;
  const fallbackSourceTasks: Task[] = sourceNode?.data?.allTasks || [];

  // Load existing configuration when edge changes
  useEffect(() => {
    if (edge && edge.data) {
      setLogicType(edge.data.logicType || 'NONE');
      setRouterCondition(edge.data.routerCondition || '');
      setDescription(edge.data.description || '');
      setListenToTaskIds(edge.data.listenToTaskIds || []);
      setTargetTaskIds(edge.data.targetTaskIds || []);

      // Load state operations
      setStateReads(edge.data.stateOperations?.reads || []);
      setStateWrites(edge.data.stateOperations?.writes || []);
      setStateCondition(edge.data.stateOperations?.condition || '');
      setPersistAfterExecution(edge.data.persistAfterExecution || false);
    } else {
      // Reset to defaults
      setLogicType('NONE');
      setRouterCondition('');
      setDescription('');
      setListenToTaskIds([]);
      setTargetTaskIds([]);
      setStateReads([]);
      setStateWrites([]);
      setStateCondition('');
      setPersistAfterExecution(false);
    }
  }, [edge]);

  // Auto-adjust logic type when multiple tasks are selected/deselected
  useEffect(() => {
    if (listenToTaskIds.length > 1 && logicType === 'NONE') {
      // Switch to AND when multiple tasks selected and currently NONE
      setLogicType('AND');
    }
  }, [listenToTaskIds.length, logicType]);

  const handleSave = () => {
    if (!edge) return;

    const config: EdgeConfig = {
      logicType,
      description,
      listenToTaskIds,
      targetTaskIds,
      stateOperations: {
        reads: stateReads.length > 0 ? stateReads : undefined,
        writes: stateWrites.length > 0 ? stateWrites : undefined,
        condition: stateCondition || undefined,
      },
      persistAfterExecution,
    };

    // Only include router condition if logic type is ROUTER
    if (logicType === 'ROUTER') {
      config.routerCondition = routerCondition;
    }

    onSave(edge.id, config);
    onClose();
  };

  const handleSourceTaskToggle = (taskId: string) => {
    setListenToTaskIds(prev =>
      prev.includes(taskId)
        ? prev.filter(id => id !== taskId)
        : [...prev, taskId]
    );
  };

  const handleTargetTaskToggle = (taskId: string) => {
    setTargetTaskIds(prev =>
      prev.includes(taskId)
        ? prev.filter(id => id !== taskId)
        : [...prev, taskId]
    );
  };

  const handleCancel = () => {
    onClose();
  };

  return (
    <Dialog
      open={open}
      onClose={handleCancel}
      maxWidth="md"
      fullWidth
      PaperProps={{
        sx: { height: '80vh', maxHeight: '700px' }
      }}
    >
      <DialogTitle sx={{ pb: 1 }}>
        Configure Connection Logic
      </DialogTitle>

      <DialogContent sx={{ pt: 1 }}>
        <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
          {/* Task Selection Section - Two Column Layout */}
          <Box sx={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 2 }}>
            {/* Source Tasks Selection */}
            <Box>
              <FormLabel component="legend" sx={{ fontSize: '0.875rem', fontWeight: 600, mb: 0.5 }}>
                Source Tasks (Listen To)
              </FormLabel>
              <Typography variant="caption" color="text.secondary" sx={{ mb: 1, display: 'block', fontSize: '0.75rem' }}>
                {aggregatedSourceTasks.length > 1
                  ? `From ${aggregatedSourceTasks.map(g => g.crewName).join(', ')}`
                  : `From ${sourceNode?.data?.crewName || aggregatedSourceTasks[0]?.crewName || 'source crew'}`
                }
              </Typography>
            {(aggregatedSourceTasks.length === 0 && fallbackSourceTasks.length === 0) ? (
              <Alert severity="warning" sx={{ fontSize: '0.75rem', py: 0.5 }}>
                No tasks available
              </Alert>
            ) : (
              <Box sx={{
                maxHeight: '180px',
                overflowY: 'auto',
                border: '1px solid',
                borderColor: 'divider',
                borderRadius: 1,
                p: 1.5
              }}>
                <FormGroup>
                  {aggregatedSourceTasks.length > 0 ? (
                    // Show crew names only when there are MULTIPLE source crews
                    aggregatedSourceTasks.length > 1 ? (
                      aggregatedSourceTasks.map((group) => (
                        <Box key={group.crewName} sx={{ mb: 1.5 }}>
                          <Typography
                            variant="caption"
                            sx={{
                              color: 'primary.main',
                              fontWeight: 600,
                              fontSize: '0.7rem',
                              display: 'block',
                              mb: 0.5
                            }}
                          >
                            {group.crewName}
                          </Typography>
                          {group.tasks.map((task) => (
                            <FormControlLabel
                              key={task.id}
                              control={
                                <Checkbox
                                  size="small"
                                  checked={listenToTaskIds.includes(task.id)}
                                  onChange={() => handleSourceTaskToggle(task.id)}
                                  sx={{ py: 0.25, pl: 0 }}
                                />
                              }
                              label={
                                <Typography variant="body2" sx={{ fontSize: '0.8rem', lineHeight: 1.4 }}>
                                  {task.name}
                                </Typography>
                              }
                              sx={{
                                ml: 0,
                                mb: 0.25,
                                display: 'flex',
                                alignItems: 'flex-start',
                                width: '100%'
                              }}
                            />
                          ))}
                        </Box>
                      ))
                    ) : (
                      // Single source crew - no need to show crew name again
                      aggregatedSourceTasks[0].tasks.map((task) => (
                        <FormControlLabel
                          key={task.id}
                          control={
                            <Checkbox
                              size="small"
                              checked={listenToTaskIds.includes(task.id)}
                              onChange={() => handleSourceTaskToggle(task.id)}
                              sx={{ py: 0.25, pl: 0 }}
                            />
                          }
                          label={
                            <Typography variant="body2" sx={{ fontSize: '0.8rem', lineHeight: 1.4 }}>
                              {task.name}
                            </Typography>
                          }
                          sx={{
                            ml: 0,
                            mb: 0.25,
                            display: 'flex',
                            alignItems: 'flex-start',
                            width: '100%'
                          }}
                        />
                      ))
                    )
                  ) : (
                    fallbackSourceTasks.map((task) => (
                      <FormControlLabel
                        key={task.id}
                        control={
                          <Checkbox
                            size="small"
                            checked={listenToTaskIds.includes(task.id)}
                            onChange={() => handleSourceTaskToggle(task.id)}
                            sx={{ py: 0.25, pl: 0 }}
                          />
                        }
                        label={
                          <Typography variant="body2" sx={{ fontSize: '0.8rem', lineHeight: 1.4 }}>
                            {task.name}
                          </Typography>
                        }
                        sx={{
                          ml: 0,
                          mb: 0.25,
                          display: 'flex',
                          alignItems: 'flex-start',
                          width: '100%'
                        }}
                      />
                    ))
                  )}
                </FormGroup>
              </Box>
            )}
              {listenToTaskIds.length > 0 && (
                <Box sx={{ mt: 0.5 }}>
                  <Typography variant="caption" color="text.secondary" sx={{ fontSize: '0.7rem' }}>
                    Selected: {listenToTaskIds.length}
                  </Typography>
                </Box>
              )}

            </Box>

            {/* Target Tasks Selection */}
            <Box>
              <FormLabel component="legend" sx={{ fontSize: '0.875rem', fontWeight: 600, mb: 0.5 }}>
                Target Tasks (Execute)
              </FormLabel>
              <Typography variant="caption" color="text.secondary" sx={{ mb: 1, display: 'block', fontSize: '0.75rem' }}>
                From {targetNode?.data?.crewName || 'target crew'}
              </Typography>
            {targetTasks.length === 0 ? (
              <Alert severity="warning" sx={{ fontSize: '0.75rem', py: 0.5 }}>
                No tasks available
              </Alert>
            ) : (
              <Box sx={{
                maxHeight: '180px',
                overflowY: 'auto',
                border: '1px solid',
                borderColor: 'divider',
                borderRadius: 1,
                p: 1.5
              }}>
                <FormGroup>
                  {targetTasks.map((task) => (
                    <FormControlLabel
                      key={task.id}
                      control={
                        <Checkbox
                          size="small"
                          checked={targetTaskIds.includes(task.id)}
                          onChange={() => handleTargetTaskToggle(task.id)}
                          sx={{ py: 0.25, pl: 0 }}
                        />
                      }
                      label={
                        <Typography variant="body2" sx={{ fontSize: '0.8rem', lineHeight: 1.4 }}>
                          {task.name}
                        </Typography>
                      }
                      sx={{
                        ml: 0,
                        mb: 0.25,
                        display: 'flex',
                        alignItems: 'flex-start',
                        width: '100%'
                      }}
                    />
                  ))}
                </FormGroup>
              </Box>
            )}
              {targetTaskIds.length > 0 && (
                <Box sx={{ mt: 0.5 }}>
                  <Typography variant="caption" color="text.secondary" sx={{ fontSize: '0.7rem' }}>
                    Selected: {targetTaskIds.length}
                  </Typography>
                </Box>
              )}
            </Box>
          </Box>

          <Divider sx={{ my: 1 }} />

          {/* Logic Type Selection */}
          <FormControl component="fieldset" size="small">
            <FormLabel component="legend" sx={{ fontSize: '0.875rem', fontWeight: 600, mb: 0.5 }}>
              Flow Logic Type
            </FormLabel>
            <RadioGroup
              value={logicType}
              onChange={(e) => setLogicType(e.target.value as FlowLogicType)}
              sx={{ gap: 0.5 }}
            >
              <FormControlLabel
                value="NONE"
                control={<Radio size="small" />}
                disabled={listenToTaskIds.length > 1}
                label={
                  <Box sx={{ ml: 0.5 }}>
                    <Typography variant="body2" sx={{ fontSize: '0.85rem', fontWeight: 500 }}>
                      None (Default) <Typography component="span" variant="caption" color="text.secondary">— Sequential</Typography>
                    </Typography>
                    {listenToTaskIds.length > 1 && (
                      <Typography variant="caption" color="error" sx={{ fontSize: '0.7rem', display: 'block' }}>
                        Not available with multiple source tasks
                      </Typography>
                    )}
                  </Box>
                }
                sx={{ mb: 0 }}
              />
              <FormControlLabel
                value="AND"
                control={<Radio size="small" />}
                label={
                  <Box sx={{ ml: 0.5 }}>
                    <Typography variant="body2" sx={{ fontSize: '0.85rem', fontWeight: 500 }}>
                      AND Logic <Typography component="span" variant="caption" color="text.secondary">— Wait for all</Typography>
                    </Typography>
                  </Box>
                }
                sx={{ mb: 0 }}
              />
              <FormControlLabel
                value="OR"
                control={<Radio size="small" />}
                label={
                  <Box sx={{ ml: 0.5 }}>
                    <Typography variant="body2" sx={{ fontSize: '0.85rem', fontWeight: 500 }}>
                      OR Logic <Typography component="span" variant="caption" color="text.secondary">— Execute when any completes</Typography>
                    </Typography>
                  </Box>
                }
                sx={{ mb: 0 }}
              />
              <FormControlLabel
                value="ROUTER"
                control={<Radio size="small" />}
                label={
                  <Box sx={{ ml: 0.5 }}>
                    <Typography variant="body2" sx={{ fontSize: '0.85rem', fontWeight: 500 }}>
                      Router <Typography component="span" variant="caption" color="text.secondary">— Conditional routing</Typography>
                    </Typography>
                  </Box>
                }
                sx={{ mb: 0 }}
              />
            </RadioGroup>
          </FormControl>

          {/* Router Condition (only shown when ROUTER is selected) */}
          {logicType === 'ROUTER' && (
            <TextField
              fullWidth
              multiline
              rows={2}
              size="small"
              label="Router Condition"
              placeholder="e.g., state.get('score', 0) > 0.8"
              value={routerCondition}
              onChange={(e) => setRouterCondition(e.target.value)}
              helperText="Python expression evaluated during execution"
            />
          )}

          {/* State Operations Section */}
          <Box sx={{ mt: 2 }}>
            <Typography variant="subtitle2" sx={{ mb: 1, fontWeight: 600 }}>
              State Management (Optional)
            </Typography>

            {/* State Reads */}
            <TextField
              fullWidth
              size="small"
              label="State Variables to Read"
              placeholder="e.g., score, count, status (comma-separated)"
              value={stateReads.join(', ')}
              onChange={(e) => setStateReads(
                e.target.value.split(',').map(v => v.trim()).filter(v => v)
              )}
              helperText="Variables to read from flow state"
              sx={{ mb: 1.5 }}
            />

            {/* State Writes */}
            <TextField
              fullWidth
              multiline
              rows={2}
              size="small"
              label="State Variables to Write"
              placeholder='e.g., result=computed_value, count=state.get("count", 0) + 1'
              value={stateWrites.map(w =>
                w.expression ? `${w.variable}=${w.expression}` : `${w.variable}=${w.value}`
              ).join(', ')}
              onChange={(e) => {
                const writes = e.target.value.split(',').map(entry => {
                  const [variable, ...valueParts] = entry.split('=');
                  const value = valueParts.join('=').trim();
                  return {
                    variable: variable.trim(),
                    expression: value || undefined
                  };
                }).filter(w => w.variable);
                setStateWrites(writes);
              }}
              helperText="Variables to write to flow state (variable=expression format)"
              sx={{ mb: 1.5 }}
            />

            {/* State Condition */}
            <TextField
              fullWidth
              size="small"
              label="State-Based Condition"
              placeholder='e.g., state.get("quality", 0) > 0.8'
              value={stateCondition}
              onChange={(e) => setStateCondition(e.target.value)}
              helperText="Python expression for state-based routing"
              sx={{ mb: 1.5 }}
            />

            {/* Persist After Execution */}
            <FormControlLabel
              control={
                <Checkbox
                  size="small"
                  checked={persistAfterExecution}
                  onChange={(e) => setPersistAfterExecution(e.target.checked)}
                />
              }
              label={
                <Typography variant="body2" sx={{ fontSize: '0.85rem' }}>
                  Persist state after execution (method-level persistence)
                </Typography>
              }
            />
          </Box>

          {/* Description */}
          <TextField
            fullWidth
            multiline
            rows={2}
            size="small"
            label="Description (Optional)"
            placeholder="Describe this connection..."
            value={description}
            onChange={(e) => setDescription(e.target.value)}
            sx={{ mt: 2 }}
          />
        </Box>
      </DialogContent>

      <DialogActions sx={{ px: 3, py: 1.5 }}>
        <Button onClick={handleCancel} size="small">
          Cancel
        </Button>
        <Button
          onClick={handleSave}
          variant="contained"
          size="small"
          disabled={
            listenToTaskIds.length === 0 ||
            targetTaskIds.length === 0 ||
            (logicType === 'ROUTER' && !routerCondition.trim())
          }
        >
          Save
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default EdgeConfigDialog;
