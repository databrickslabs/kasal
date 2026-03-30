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
  FormGroup,
  Select,
  MenuItem,
  IconButton,
  InputLabel,
} from '@mui/material';
import {
  Delete as DeleteIcon,
  Add as AddIcon,
  PanTool as PanToolIcon,
} from '@mui/icons-material';
import { Edge, Node } from 'reactflow';
import ConditionBuilder, { Condition, conditionsToPython, pythonToConditions } from './ConditionBuilder';

export type FlowLogicType = 'AND' | 'OR' | 'ROUTER' | 'NONE';

// State mapping: extract task output field and save to state variable
export interface StateMapping {
  sourceTaskId: string;   // Which task's output to read from
  outputField: string;    // Field to extract from task output (e.g., "confidence", "result.status")
  stateVariable: string;  // State variable name to save to (e.g., "confidence", "last_status")
}

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
  flowStateVariables?: string[];  // State variables from other edges in the flow
}

// HITL (Human in the Loop) configuration
export interface HITLConfig {
  enabled: boolean;
  message: string;                          // Message shown to approvers
  timeout_seconds: number;                  // Timeout before automatic action
  timeout_action: 'auto_reject' | 'fail';   // Action on timeout
  require_comment: boolean;                 // Require comment for approval/rejection
}

export interface EdgeConfig {
  logicType: FlowLogicType;
  routerCondition?: string;       // Evaluated against state variables (e.g., "state.confidence > 0.8")
  description?: string;
  listenToTaskIds?: string[];     // Tasks from source crew to wait for
  targetTaskIds?: string[];       // Tasks from target crew to execute
  // State management (aligned with CrewAI Flow state)
  stateMappings?: StateMapping[]; // Extract task outputs → state variables (with sourceTaskId)
  checkpoint?: boolean;           // Enable @persist - checkpoint after this step for resume capability
  // HITL (Human in the Loop) - requires checkpoint to be enabled
  hitl?: HITLConfig;
}

const EdgeConfigDialog: React.FC<EdgeConfigDialogProps> = ({
  open,
  onClose,
  edge,
  nodes,
  onSave,
  aggregatedSourceTasks = [],
  flowStateVariables = []
}) => {
  const [logicType, setLogicType] = useState<FlowLogicType>('NONE');
  const [routerConditions, setRouterConditions] = useState<Condition[]>([]);
  const [description, setDescription] = useState('');
  const [listenToTaskIds, setListenToTaskIds] = useState<string[]>([]);
  const [targetTaskIds, setTargetTaskIds] = useState<string[]>([]);

  // State management
  const [stateMappings, setStateMappings] = useState<StateMapping[]>([]);
  const [checkpoint, setCheckpoint] = useState(false);

  // HITL configuration
  const [hitlEnabled, setHitlEnabled] = useState(false);
  const [hitlMessage, setHitlMessage] = useState('Please review and approve to continue');
  const [hitlTimeoutSeconds, setHitlTimeoutSeconds] = useState(86400); // 24 hours default
  const [hitlTimeoutAction, setHitlTimeoutAction] = useState<'auto_reject' | 'fail'>('auto_reject');
  const [hitlRequireComment, setHitlRequireComment] = useState(false);

  // Get target node from passed nodes prop
  const targetNode = edge ? nodes.find(n => n.id === edge.target) : null;

  // Get tasks from target node
  const targetTasks: Task[] = targetNode?.data?.allTasks || [];

  // Use aggregated source tasks if provided, otherwise fall back to single source
  const sourceNode = edge ? nodes.find(n => n.id === edge.source) : null;
  const fallbackSourceTasks: Task[] = sourceNode?.data?.allTasks || [];

  // Debug logging for source tasks
  console.log('EdgeConfigDialog: source tasks check', {
    aggregatedSourceTasksLength: aggregatedSourceTasks.length,
    aggregatedSourceTasks: aggregatedSourceTasks,
    fallbackSourceTasksLength: fallbackSourceTasks.length,
    fallbackSourceTasks: fallbackSourceTasks,
    sourceNodeId: sourceNode?.id,
    sourceNodeDataKeys: sourceNode?.data ? Object.keys(sourceNode.data) : []
  });

  // Load existing configuration when edge changes
  useEffect(() => {
    if (edge && edge.data) {
      console.log('EdgeConfigDialog: Loading edge data', {
        edgeId: edge.id,
        logicType: edge.data.logicType,
        listenToTaskIds: edge.data.listenToTaskIds,
        stateMappings: edge.data.stateMappings,
        checkpoint: edge.data.checkpoint,
        'checkpoint type': typeof edge.data.checkpoint,
        hitl: edge.data.hitl,
        'hitl.enabled': edge.data.hitl?.enabled,
        'hitl.enabled type': typeof edge.data.hitl?.enabled,
        allDataKeys: Object.keys(edge.data)
      });

      setLogicType(edge.data.logicType || 'NONE');

      // Parse router condition from string to conditions array
      const routerCondStr = edge.data.routerCondition || '';
      setRouterConditions(routerCondStr ? pythonToConditions(routerCondStr) : []);

      setDescription(edge.data.description || '');
      setListenToTaskIds(edge.data.listenToTaskIds || []);
      setTargetTaskIds(edge.data.targetTaskIds || []);

      // Load state management settings
      setStateMappings(edge.data.stateMappings || []);
      // Use explicit boolean check to handle false values correctly
      setCheckpoint(edge.data.checkpoint === true);

      // Load HITL configuration
      if (edge.data.hitl) {
        // Use explicit boolean check to handle false values correctly
        setHitlEnabled(edge.data.hitl.enabled === true);
        setHitlMessage(edge.data.hitl.message || 'Please review and approve to continue');
        setHitlTimeoutSeconds(edge.data.hitl.timeout_seconds || 86400);
        setHitlTimeoutAction(edge.data.hitl.timeout_action || 'auto_reject');
        setHitlRequireComment(edge.data.hitl.require_comment === true);
      } else {
        setHitlEnabled(false);
        setHitlMessage('Please review and approve to continue');
        setHitlTimeoutSeconds(86400);
        setHitlTimeoutAction('auto_reject');
        setHitlRequireComment(false);
      }
    } else {
      // Reset to defaults
      setLogicType('NONE');
      setRouterConditions([]);
      setDescription('');
      setListenToTaskIds([]);
      setTargetTaskIds([]);
      setStateMappings([]);
      setCheckpoint(false);
      // Reset HITL
      setHitlEnabled(false);
      setHitlMessage('Please review and approve to continue');
      setHitlTimeoutSeconds(86400);
      setHitlTimeoutAction('auto_reject');
      setHitlRequireComment(false);
    }
  }, [edge]);

  // Auto-adjust logic type when multiple tasks are selected/deselected
  useEffect(() => {
    if (listenToTaskIds.length > 1 && logicType === 'NONE') {
      // Switch to AND when multiple tasks selected and currently NONE
      setLogicType('AND');
    } else if (listenToTaskIds.length <= 1 && (logicType === 'AND' || logicType === 'OR')) {
      // Switch to NONE when tasks reduced to 1 or 0 and currently AND/OR
      setLogicType('NONE');
    }
  }, [listenToTaskIds.length, logicType]);

  const handleSave = () => {
    if (!edge) return;

    // Convert conditions to Python expressions (conditions now reference state variables)
    const routerConditionStr = conditionsToPython(routerConditions);

    // Filter out incomplete state mappings (need sourceTaskId, outputField, and stateVariable)
    const validStateMappings = stateMappings.filter(
      m => m.sourceTaskId && m.outputField.trim() && m.stateVariable.trim()
    );

    const config: EdgeConfig = {
      logicType,
      description,
      listenToTaskIds,
      targetTaskIds,
      // Include state management if configured
      ...(validStateMappings.length > 0 && { stateMappings: validStateMappings }),
      // Always include checkpoint (explicit true/false)
      checkpoint: checkpoint,
      // Always include HITL config (when checkpoint enabled, use settings; when disabled, explicitly disable)
      hitl: checkpoint ? {
        enabled: hitlEnabled,
        message: hitlMessage,
        timeout_seconds: hitlTimeoutSeconds,
        timeout_action: hitlTimeoutAction,
        require_comment: hitlRequireComment,
      } : {
        enabled: false,
        message: 'Please review and approve to continue',
        timeout_seconds: 86400,
        timeout_action: 'auto_reject' as const,
        require_comment: false,
      },
    };

    // Only include router condition if logic type is ROUTER
    // Router conditions are evaluated against state variables (populated by stateMappings)
    if (logicType === 'ROUTER' && routerConditionStr) {
      config.routerCondition = routerConditionStr;
    }

    console.log('EdgeConfigDialog: Saving config', {
      edgeId: edge.id,
      logicType: config.logicType,
      routerCondition: config.routerCondition,
      stateMappings: config.stateMappings,
      listenToTaskIds: config.listenToTaskIds,
      allConfigKeys: Object.keys(config)
    });

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

  // State mapping helpers
  const handleAddStateMapping = () => {
    // Default to first selected task if available
    const defaultTaskId = listenToTaskIds.length > 0 ? listenToTaskIds[0] : '';
    setStateMappings([...stateMappings, { sourceTaskId: defaultTaskId, outputField: '', stateVariable: '' }]);
  };

  const handleRemoveStateMapping = (index: number) => {
    setStateMappings(stateMappings.filter((_, i) => i !== index));
  };

  const handleStateMappingChange = (index: number, field: 'sourceTaskId' | 'outputField' | 'stateVariable', value: string) => {
    const updated = [...stateMappings];
    updated[index] = { ...updated[index], [field]: value };
    setStateMappings(updated);
  };

  // Get all source tasks as flat list for state mapping dropdown
  const allSourceTasks: Task[] = aggregatedSourceTasks.length > 0
    ? aggregatedSourceTasks.flatMap(g => g.tasks)
    : fallbackSourceTasks;

  // Get state variable names for displaying in condition builder hint
  // Combine current edge's state mappings with flow-wide state variables
  const currentEdgeStateVars = stateMappings
    .filter(m => m.stateVariable.trim())
    .map(m => m.stateVariable.trim());

  // Combine and deduplicate: current edge vars + flow-wide vars
  const allStateVariableNames = [...new Set([...currentEdgeStateVars, ...flowStateVariables])];

  // Separate into "this edge" vs "from flow" for better display
  const flowOnlyStateVars = flowStateVariables.filter(v => !currentEdgeStateVars.includes(v));

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
                disabled={listenToTaskIds.length <= 1}
                label={
                  <Box sx={{ ml: 0.5 }}>
                    <Typography variant="body2" sx={{ fontSize: '0.85rem', fontWeight: 500 }}>
                      AND Logic <Typography component="span" variant="caption" color="text.secondary">— Wait for all</Typography>
                    </Typography>
                    {listenToTaskIds.length <= 1 && (
                      <Typography variant="caption" color="text.secondary" sx={{ fontSize: '0.7rem', display: 'block' }}>
                        Requires multiple source tasks
                      </Typography>
                    )}
                  </Box>
                }
                sx={{ mb: 0 }}
              />
              <FormControlLabel
                value="OR"
                control={<Radio size="small" />}
                disabled={listenToTaskIds.length <= 1}
                label={
                  <Box sx={{ ml: 0.5 }}>
                    <Typography variant="body2" sx={{ fontSize: '0.85rem', fontWeight: 500 }}>
                      OR Logic <Typography component="span" variant="caption" color="text.secondary">— Execute when any completes</Typography>
                    </Typography>
                    {listenToTaskIds.length <= 1 && (
                      <Typography variant="caption" color="text.secondary" sx={{ fontSize: '0.7rem', display: 'block' }}>
                        Requires multiple source tasks
                      </Typography>
                    )}
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

          {/* Router Configuration (only shown when ROUTER is selected) */}
          {logicType === 'ROUTER' && (
            <Box sx={{ mt: 1, p: 2, bgcolor: 'action.hover', borderRadius: 1 }}>
              <Typography variant="subtitle2" sx={{ mb: 1.5, fontWeight: 600, color: 'primary.main' }}>
                Router Configuration
              </Typography>

              {/* Step 1: State Mappings - extract task outputs to state variables */}
              <Box sx={{ mb: 2 }}>
                <Typography variant="subtitle2" sx={{ fontWeight: 600, fontSize: '0.8rem', mb: 0.5, display: 'flex', alignItems: 'center', gap: 1 }}>
                  <Box component="span" sx={{ bgcolor: 'primary.main', color: 'white', borderRadius: '50%', width: 20, height: 20, display: 'inline-flex', alignItems: 'center', justifyContent: 'center', fontSize: '0.7rem' }}>1</Box>
                  Save Task Output to State
                </Typography>
                <Typography variant="caption" color="text.secondary" sx={{ fontSize: '0.7rem', display: 'block', mb: 1.5, ml: 3.5 }}>
                  Extract values from task outputs to use in the route condition
                </Typography>

                {listenToTaskIds.length === 0 ? (
                  <Alert severity="warning" sx={{ fontSize: '0.75rem', py: 0.5, ml: 3.5 }}>
                    <Typography variant="caption" sx={{ fontSize: '0.7rem' }}>
                      Select source tasks above first to enable state mappings
                    </Typography>
                  </Alert>
                ) : stateMappings.length === 0 ? (
                  <Box
                    sx={{
                      border: '1px dashed',
                      borderColor: 'divider',
                      borderRadius: 1,
                      p: 1.5,
                      textAlign: 'center',
                      cursor: 'pointer',
                      ml: 3.5,
                      '&:hover': { borderColor: 'primary.main', bgcolor: 'background.paper' }
                    }}
                    onClick={handleAddStateMapping}
                  >
                    <AddIcon fontSize="small" sx={{ color: 'text.secondary', mb: 0.5 }} />
                    <Typography variant="body2" color="text.secondary" sx={{ fontSize: '0.8rem' }}>
                      Add state mapping
                    </Typography>
                  </Box>
                ) : (
                  <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1, ml: 3.5 }}>
                    {stateMappings.map((mapping, index) => (
                      <Box key={index} sx={{ display: 'flex', gap: 1, alignItems: 'center', flexWrap: 'wrap' }}>
                        <FormControl size="small" sx={{ minWidth: 120 }}>
                          <Select
                            value={mapping.sourceTaskId || ''}
                            onChange={(e) => handleStateMappingChange(index, 'sourceTaskId', e.target.value)}
                            displayEmpty
                            sx={{ fontSize: '0.75rem' }}
                          >
                            <MenuItem value="" disabled><em>Task</em></MenuItem>
                            {allSourceTasks.map((task) => (
                              <MenuItem key={task.id} value={task.id} sx={{ fontSize: '0.75rem' }}>{task.name}</MenuItem>
                            ))}
                          </Select>
                        </FormControl>
                        <Typography variant="body2" sx={{ color: 'text.secondary', fontSize: '0.7rem' }}>.</Typography>
                        <TextField
                          size="small"
                          placeholder="field"
                          value={mapping.outputField}
                          onChange={(e) => handleStateMappingChange(index, 'outputField', e.target.value)}
                          sx={{ width: 80, '& input': { fontSize: '0.75rem' } }}
                        />
                        <Typography variant="body2" sx={{ color: 'primary.main', fontWeight: 600 }}>→</Typography>
                        <TextField
                          size="small"
                          placeholder="state var"
                          value={mapping.stateVariable}
                          onChange={(e) => handleStateMappingChange(index, 'stateVariable', e.target.value)}
                          sx={{ width: 90, '& input': { fontSize: '0.75rem' } }}
                        />
                        <IconButton size="small" onClick={() => handleRemoveStateMapping(index)} sx={{ color: 'error.main', p: 0.5 }}>
                          <DeleteIcon fontSize="small" />
                        </IconButton>
                      </Box>
                    ))}
                    <Button size="small" startIcon={<AddIcon />} onClick={handleAddStateMapping} sx={{ alignSelf: 'flex-start', fontSize: '0.7rem' }}>
                      Add mapping
                    </Button>
                  </Box>
                )}
              </Box>

              <Divider sx={{ my: 2 }} />

              {/* Step 2: Route Condition - evaluates state variables */}
              <Box sx={{ mb: 1 }}>
                <Typography variant="subtitle2" sx={{ fontWeight: 600, fontSize: '0.8rem', mb: 0.5, display: 'flex', alignItems: 'center', gap: 1 }}>
                  <Box component="span" sx={{ bgcolor: 'primary.main', color: 'white', borderRadius: '50%', width: 20, height: 20, display: 'inline-flex', alignItems: 'center', justifyContent: 'center', fontSize: '0.7rem' }}>2</Box>
                  Route Condition
                </Typography>

                {/* Show available state variables */}
                <Box sx={{ ml: 3.5, mb: 1.5 }}>
                  {allStateVariableNames.length > 0 ? (
                    <Box sx={{ display: 'flex', flexDirection: 'column', gap: 0.5 }}>
                      {currentEdgeStateVars.length > 0 && (
                        <Typography variant="caption" color="text.secondary" sx={{ fontSize: '0.7rem' }}>
                          <strong>This edge:</strong> {currentEdgeStateVars.join(', ')}
                        </Typography>
                      )}
                      {flowOnlyStateVars.length > 0 && (
                        <Typography variant="caption" color="text.secondary" sx={{ fontSize: '0.7rem' }}>
                          <strong>From flow:</strong> {flowOnlyStateVars.join(', ')}
                        </Typography>
                      )}
                      {currentEdgeStateVars.length === 0 && flowOnlyStateVars.length > 0 && (
                        <Typography variant="caption" color="warning.main" sx={{ fontSize: '0.65rem', fontStyle: 'italic' }}>
                          Tip: Add state mappings above to extract values from this edge&apos;s tasks
                        </Typography>
                      )}
                    </Box>
                  ) : (
                    <Typography variant="caption" color="text.secondary" sx={{ fontSize: '0.7rem' }}>
                      Add state mappings above first, then use state variables here
                    </Typography>
                  )}
                </Box>

                <Box sx={{ ml: 3.5 }}>
                  <ConditionBuilder
                    conditions={routerConditions}
                    onChange={setRouterConditions}
                    label=""
                    helperText="If TRUE → this route activates. If FALSE → route is skipped."
                  />
                </Box>
              </Box>
            </Box>
          )}

          {/* Checkpoint - Simple inline checkbox */}
          <FormControlLabel
            control={
              <Checkbox
                size="small"
                checked={checkpoint}
                onChange={(e) => {
                  setCheckpoint(e.target.checked);
                  // Disable HITL if checkpoint is disabled
                  if (!e.target.checked) {
                    setHitlEnabled(false);
                  }
                }}
              />
            }
            label={
              <Typography variant="body2" sx={{ fontSize: '0.85rem' }}>
                Enable Checkpoint <Typography component="span" variant="caption" color="text.secondary">— Resume flow from here if interrupted</Typography>
              </Typography>
            }
            sx={{ mt: 1, ml: 0 }}
          />

          {/* HITL (Human in the Loop) Configuration - visible always, enabled only when checkpoint is enabled */}
          <Box sx={{
            mt: 2,
            p: 2,
            bgcolor: checkpoint ? 'warning.light' : 'action.disabledBackground',
            borderRadius: 1,
            border: '1px solid',
            borderColor: checkpoint ? 'warning.main' : 'divider',
            opacity: checkpoint ? 1 : 0.7,
          }}>
            <FormControlLabel
              control={
                <Checkbox
                  size="small"
                  checked={hitlEnabled}
                  onChange={(e) => setHitlEnabled(e.target.checked)}
                  disabled={!checkpoint}
                />
              }
              label={
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                  <PanToolIcon sx={{ fontSize: 18, color: checkpoint ? 'warning.dark' : 'text.disabled' }} />
                  <Typography variant="body2" sx={{ fontSize: '0.85rem', fontWeight: 600, color: checkpoint ? 'text.primary' : 'text.disabled' }}>
                    Require Human Approval (HITL)
                  </Typography>
                  {!checkpoint && (
                    <Typography variant="caption" sx={{ color: 'text.secondary', fontStyle: 'italic' }}>
                      — Enable checkpoint first
                    </Typography>
                  )}
                </Box>
              }
              sx={{ ml: 0, mb: (hitlEnabled && checkpoint) ? 2 : 0 }}
            />

            {hitlEnabled && checkpoint && (
              <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2, pl: 4 }}>
                <TextField
                  fullWidth
                  size="small"
                  label="Approval Message"
                  placeholder="Message shown to approvers..."
                  value={hitlMessage}
                  onChange={(e) => setHitlMessage(e.target.value)}
                  multiline
                  rows={2}
                  helperText="This message will be displayed to reviewers when requesting approval"
                />

                <Box sx={{ display: 'flex', gap: 2 }}>
                  <TextField
                    size="small"
                    type="number"
                    label="Timeout (seconds)"
                    value={hitlTimeoutSeconds}
                    onChange={(e) => setHitlTimeoutSeconds(parseInt(e.target.value) || 86400)}
                    sx={{ width: 150 }}
                    helperText={
                      hitlTimeoutSeconds < 3600
                        ? `${Math.floor(hitlTimeoutSeconds / 60)} minutes`
                        : hitlTimeoutSeconds < 86400
                        ? `${Math.floor(hitlTimeoutSeconds / 3600)} hours`
                        : `${Math.floor(hitlTimeoutSeconds / 86400)} days`
                    }
                  />

                  <FormControl size="small" sx={{ minWidth: 200 }}>
                    <InputLabel>On Timeout</InputLabel>
                    <Select
                      value={hitlTimeoutAction}
                      label="On Timeout"
                      onChange={(e) => setHitlTimeoutAction(e.target.value as 'auto_reject' | 'fail')}
                    >
                      <MenuItem value="auto_reject">Auto-reject (allow retry)</MenuItem>
                      <MenuItem value="fail">Fail flow execution</MenuItem>
                    </Select>
                  </FormControl>
                </Box>

                <FormControlLabel
                  control={
                    <Checkbox
                      size="small"
                      checked={hitlRequireComment}
                      onChange={(e) => setHitlRequireComment(e.target.checked)}
                    />
                  }
                  label={
                    <Typography variant="body2" sx={{ fontSize: '0.8rem' }}>
                      Require comment for approval/rejection
                    </Typography>
                  }
                  sx={{ ml: 0 }}
                />
              </Box>
            )}
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
            (logicType === 'ROUTER' && routerConditions.length === 0)
          }
        >
          Save
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default EdgeConfigDialog;
