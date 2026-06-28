import React, { useState, useEffect } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  FormControl,
  FormLabel,
  FormControlLabel,
  Typography,
  Box,
  Alert,
  TextField,
  Divider,
  Checkbox,
  Select,
  MenuItem,
  InputLabel,
  Link,
} from '@mui/material';
import {
  Add as AddIcon,
  PanTool as PanToolIcon,
  HelpOutline as HelpOutlineIcon,
} from '@mui/icons-material';
import { Edge, Node } from 'reactflow';
import ConditionBuilder, { Condition, conditionsToPython, pythonToConditions } from './ConditionBuilder';
import SchemaQuickCreateDialog from './SchemaQuickCreateDialog';
import { SchemaService } from '../../api/SchemaService';
import { TaskService } from '../../api/TaskService';
import { Schema } from '../../types/schema';

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
  routerSchema?: string;          // Name of the output schema the router routes on (set on source crew's final task)
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
  aggregatedSourceTasks = []
}) => {
  const [logicType, setLogicType] = useState<FlowLogicType>('NONE');
  const [routerConditions, setRouterConditions] = useState<Condition[]>([]);
  const [description, setDescription] = useState('');
  const [listenToTaskIds, setListenToTaskIds] = useState<string[]>([]);
  const [targetTaskIds, setTargetTaskIds] = useState<string[]>([]);

  // Router schema-driven routing
  const [schemas, setSchemas] = useState<Schema[]>([]);
  const [routerSchema, setRouterSchema] = useState<string>('');
  const [schemaCreateOpen, setSchemaCreateOpen] = useState(false);
  const [saveError, setSaveError] = useState<string | null>(null);

  // State management
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

  // Stable id keys for the auto-include effect (avoids array-identity churn in deps)
  const sourceTaskIdsKey = (aggregatedSourceTasks.length > 0
    ? aggregatedSourceTasks.flatMap(g => g.tasks)
    : fallbackSourceTasks).map(t => t.id).join(',');
  const targetTaskIdsKey = targetTasks.map(t => t.id).join(',');

  // Load existing configuration when edge changes
  useEffect(() => {
    if (edge && edge.data) {
      setLogicType(edge.data.logicType || 'NONE');

      // Parse router condition from string to conditions array
      const routerCondStr = edge.data.routerCondition || '';
      setRouterConditions(routerCondStr ? pythonToConditions(routerCondStr) : []);

      setDescription(edge.data.description || '');
      setListenToTaskIds(edge.data.listenToTaskIds || []);
      setTargetTaskIds(edge.data.targetTaskIds || []);

      // Load router schema selection
      setRouterSchema(edge.data.routerSchema || '');
      setSaveError(null);
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
      setRouterSchema('');
      setSaveError(null);
      setCheckpoint(false);
      // Reset HITL
      setHitlEnabled(false);
      setHitlMessage('Please review and approve to continue');
      setHitlTimeoutSeconds(86400);
      setHitlTimeoutAction('auto_reject');
      setHitlRequireComment(false);
    }
  }, [edge]);

  // Auto-include all source/target tasks — task selection was removed from the UI.
  // The edge endpoints already define source → target, so we listen to every source
  // task (wait-for-all of the source crew) and run every target task by default.
  useEffect(() => {
    if (!open) return;
    const srcTasks = aggregatedSourceTasks.length > 0
      ? aggregatedSourceTasks.flatMap(g => g.tasks)
      : fallbackSourceTasks;
    setListenToTaskIds(srcTasks.map(t => t.id));
    setTargetTaskIds(targetTasks.map(t => t.id));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open, edge?.id, sourceTaskIdsKey, targetTaskIdsKey]);

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

  // Get all source tasks as a flat list. The crew's final task is the one whose
  // output becomes the crew result (what the router routes on), so the schema is
  // applied there.
  const allSourceTasks: Task[] = aggregatedSourceTasks.length > 0
    ? aggregatedSourceTasks.flatMap(g => g.tasks)
    : fallbackSourceTasks;
  const finalSourceTask: Task | undefined = allSourceTasks[allSourceTasks.length - 1];
  const sourceCrewName = aggregatedSourceTasks[0]?.crewName
    || sourceNode?.data?.crewName
    || sourceNode?.data?.label
    || 'the source crew';

  // Resolve the selected schema's fields → router condition variables.
  // Only scalar fields (string / number / integer / boolean) are routable, since
  // the condition operators compare single values; arrays/objects are excluded.
  const selectedSchema = schemas.find(s => s.name === routerSchema);
  const schemaProperties = (selectedSchema?.schema_definition as { properties?: Record<string, { type?: string }> } | undefined)?.properties;
  const ROUTABLE_TYPES = ['string', 'number', 'integer', 'boolean'];
  const routableSchemaFields = schemaProperties
    ? Object.entries(schemaProperties).filter(([, def]) => ROUTABLE_TYPES.includes(def?.type ?? ''))
    : [];
  const schemaFieldOptions = routableSchemaFields.map(([key]) => key);
  const schemaFieldTypes: Record<string, string> = Object.fromEntries(
    routableSchemaFields.map(([key, def]) => [key, def?.type ?? 'string'])
  );

  // Load schemas for the router picker whenever the dialog opens.
  useEffect(() => {
    if (!open) return;
    let cancelled = false;
    void SchemaService.getInstance().getSchemas().then((list) => {
      if (!cancelled) setSchemas(list);
    });
    return () => { cancelled = true; };
  }, [open]);

  // For an existing ROUTER edge without a stored schema, prefill from the source
  // crew's final task if it already declares an output schema.
  useEffect(() => {
    if (!open || logicType !== 'ROUTER' || routerSchema || !finalSourceTask) return;
    let cancelled = false;
    void TaskService.getTask(finalSourceTask.id).then((task) => {
      const existing = task?.config?.output_pydantic;
      if (!cancelled && existing) setRouterSchema(existing);
    });
    return () => { cancelled = true; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open, logicType, routerSchema, finalSourceTask?.id]);

  const handleSave = async () => {
    if (!edge) return;
    setSaveError(null);

    const routerConditionStr = conditionsToPython(routerConditions);

    const config: EdgeConfig = {
      logicType,
      description,
      listenToTaskIds,
      targetTaskIds,
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

    if (logicType === 'ROUTER') {
      if (routerConditionStr) config.routerCondition = routerConditionStr;
      if (routerSchema) config.routerSchema = routerSchema;

      // Apply the chosen schema to the source crew's final task so it produces the
      // structured output the router evaluates. Fetch-then-update preserves the
      // task's other config fields.
      if (routerSchema && finalSourceTask) {
        try {
          const fullTask = await TaskService.getTask(finalSourceTask.id);
          if (fullTask && fullTask.config?.output_pydantic !== routerSchema) {
            await TaskService.updateTask(finalSourceTask.id, {
              ...fullTask,
              config: { ...fullTask.config, output_pydantic: routerSchema },
            });
          }
        } catch (e) {
          setSaveError(e instanceof Error ? e.message : 'Failed to apply the schema to the source task.');
          return; // keep the dialog open so the user can retry
        }
      }
    }

    onSave(edge.id, config);
    onClose();
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
            {allSourceTasks.length === 0 ? (
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
                {aggregatedSourceTasks.length > 1 ? (
                  // Multiple source crews — group the read-only list by crew name
                  aggregatedSourceTasks.map((group) => (
                    <Box key={group.crewName} sx={{ mb: 1.5 }}>
                      <Typography
                        variant="caption"
                        sx={{ color: 'primary.main', fontWeight: 600, fontSize: '0.7rem', display: 'block', mb: 0.5 }}
                      >
                        {group.crewName}
                      </Typography>
                      {group.tasks.map((task) => (
                        <Typography key={task.id} variant="body2" sx={{ fontSize: '0.8rem', lineHeight: 1.6 }}>
                          • {task.name}
                        </Typography>
                      ))}
                    </Box>
                  ))
                ) : (
                  allSourceTasks.map((task) => (
                    <Typography key={task.id} variant="body2" sx={{ fontSize: '0.8rem', lineHeight: 1.6 }}>
                      • {task.name}
                    </Typography>
                  ))
                )}
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
                {targetTasks.map((task) => (
                  <Typography key={task.id} variant="body2" sx={{ fontSize: '0.8rem', lineHeight: 1.6 }}>
                    • {task.name}
                  </Typography>
                ))}
              </Box>
            )}
            </Box>
          </Box>

          <Divider sx={{ my: 1 }} />

          {/* Logic Type Selection */}
          <FormControl fullWidth size="small">
            <FormLabel component="legend" sx={{ fontSize: '0.875rem', fontWeight: 600, mb: 0.5 }}>
              Flow Logic Type
            </FormLabel>
            <Select
              value={logicType}
              onChange={(e) => setLogicType(e.target.value as FlowLogicType)}
              sx={{ fontSize: '0.85rem' }}
              renderValue={(value) => {
                const labels: Record<FlowLogicType, string> = {
                  NONE: 'None (Default) — Sequential',
                  AND: 'AND Logic — Wait for all',
                  OR: 'OR Logic — Execute when any completes',
                  ROUTER: 'Router — Conditional routing',
                };
                return (
                  <Typography variant="body2" sx={{ fontSize: '0.85rem', fontWeight: 500 }}>
                    {labels[value as FlowLogicType]}
                  </Typography>
                );
              }}
            >
              <MenuItem value="NONE" disabled={listenToTaskIds.length > 1}>
                <Box>
                  <Typography variant="body2" sx={{ fontSize: '0.85rem', fontWeight: 500 }}>
                    None (Default) <Typography component="span" variant="caption" color="text.secondary">— Sequential</Typography>
                  </Typography>
                  {listenToTaskIds.length > 1 && (
                    <Typography variant="caption" color="error" sx={{ fontSize: '0.7rem', display: 'block' }}>
                      Not available with multiple source tasks
                    </Typography>
                  )}
                </Box>
              </MenuItem>
              <MenuItem value="AND" disabled={listenToTaskIds.length <= 1}>
                <Box>
                  <Typography variant="body2" sx={{ fontSize: '0.85rem', fontWeight: 500 }}>
                    AND Logic <Typography component="span" variant="caption" color="text.secondary">— Wait for all</Typography>
                  </Typography>
                  {listenToTaskIds.length <= 1 && (
                    <Typography variant="caption" color="text.secondary" sx={{ fontSize: '0.7rem', display: 'block' }}>
                      Requires multiple source tasks
                    </Typography>
                  )}
                </Box>
              </MenuItem>
              <MenuItem value="OR" disabled={listenToTaskIds.length <= 1}>
                <Box>
                  <Typography variant="body2" sx={{ fontSize: '0.85rem', fontWeight: 500 }}>
                    OR Logic <Typography component="span" variant="caption" color="text.secondary">— Execute when any completes</Typography>
                  </Typography>
                  {listenToTaskIds.length <= 1 && (
                    <Typography variant="caption" color="text.secondary" sx={{ fontSize: '0.7rem', display: 'block' }}>
                      Requires multiple source tasks
                    </Typography>
                  )}
                </Box>
              </MenuItem>
              <MenuItem value="ROUTER">
                <Box>
                  <Typography variant="body2" sx={{ fontSize: '0.85rem', fontWeight: 500 }}>
                    Router <Typography component="span" variant="caption" color="text.secondary">— Conditional routing</Typography>
                  </Typography>
                </Box>
              </MenuItem>
            </Select>
          </FormControl>

          {/* Router Configuration (only shown when ROUTER is selected) */}
          {logicType === 'ROUTER' && (
            <Box sx={{ mt: 1, p: 2, bgcolor: 'action.hover', borderRadius: 1 }}>
              <Typography variant="subtitle2" sx={{ mb: 0.5, fontWeight: 600, color: 'primary.main' }}>
                Router Configuration
              </Typography>
              <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mb: 0.5 }}>
                A crew returns <strong>one structured result</strong> for the whole run. Pick or create the
                output schema for <strong>{sourceCrewName}</strong>, then choose a field to branch on
                (e.g. <code>status == &quot;failed&quot;</code>).
              </Typography>
              <Link
                href="/docs/flow-routing"
                target="_blank"
                rel="noopener noreferrer"
                sx={{ display: 'inline-flex', alignItems: 'center', gap: 0.5, fontSize: '0.75rem', mb: 1.5 }}
              >
                <HelpOutlineIcon sx={{ fontSize: 14 }} /> Learn how routing works
              </Link>

              {/* Step 1: Output schema */}
              <FormControl fullWidth size="small" sx={{ mb: 1.5 }}>
                <InputLabel>Output schema</InputLabel>
                <Select
                  value={routerSchema}
                  label="Output schema"
                  onChange={(e) => {
                    const value = e.target.value;
                    if (value === '__create__') {
                      setSchemaCreateOpen(true);
                    } else if (value !== routerSchema) {
                      setRouterSchema(value);
                      // Existing conditions reference the previous schema's fields — start fresh.
                      setRouterConditions([]);
                    }
                  }}
                >
                  <MenuItem value="" disabled sx={{ fontSize: '0.85rem' }}>
                    <em>Select a schema…</em>
                  </MenuItem>
                  {schemas.map((s) => (
                    <MenuItem key={s.id} value={s.name} sx={{ fontSize: '0.85rem' }}>{s.name}</MenuItem>
                  ))}
                  <MenuItem value="__create__" sx={{ color: 'primary.main', fontSize: '0.85rem' }}>
                    <AddIcon fontSize="small" sx={{ mr: 1 }} /> Add new schema…
                  </MenuItem>
                </Select>
              </FormControl>

              {/* Step 2: Condition on a schema variable */}
              {!routerSchema ? (
                <Alert severity="info" sx={{ fontSize: '0.75rem', py: 0.5 }}>
                  Select or create an output schema to route on.
                </Alert>
              ) : (schemas.length > 0 && !selectedSchema) ? (
                <Alert severity="warning" sx={{ fontSize: '0.75rem', py: 0.5 }}>
                  Schema &ldquo;{routerSchema}&rdquo; isn&apos;t available anymore — pick another above.
                </Alert>
              ) : schemaFieldOptions.length === 0 ? (
                <Alert severity="warning" sx={{ fontSize: '0.75rem', py: 0.5 }}>
                  This schema has no scalar fields to route on.
                </Alert>
              ) : (
                <Box>
                  <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mb: 1 }}>
                    <strong>Variables:</strong> {schemaFieldOptions.join(', ')}
                  </Typography>
                  <ConditionBuilder
                    conditions={routerConditions}
                    onChange={setRouterConditions}
                    label=""
                    fieldOptions={schemaFieldOptions}
                    fieldTypes={schemaFieldTypes}
                    helperText="If TRUE → this route activates. If FALSE → route is skipped."
                  />
                </Box>
              )}

              {saveError && (
                <Alert severity="error" sx={{ fontSize: '0.75rem', py: 0.5, mt: 1 }}>{saveError}</Alert>
              )}
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
          disabled={logicType === 'ROUTER' && (!routerSchema || routerConditions.length === 0)}
        >
          Save
        </Button>
      </DialogActions>

      <SchemaQuickCreateDialog
        open={schemaCreateOpen}
        onClose={() => setSchemaCreateOpen(false)}
        onCreated={(schema) => {
          setSchemas((prev) => [...prev.filter(s => s.name !== schema.name), schema]);
          setRouterSchema(schema.name);
          setRouterConditions([]);
          setSchemaCreateOpen(false);
        }}
      />
    </Dialog>
  );
};

export default EdgeConfigDialog;
