import React, { useCallback, useState, useEffect } from 'react';
import { Handle, Position, useReactFlow } from 'reactflow';
import { Box, Typography, Dialog, DialogTitle, DialogContent, Tooltip, CircularProgress } from '@mui/material';
import AddTaskIcon from '@mui/icons-material/AddTask';
import DeleteIcon from '@mui/icons-material/Delete';
import IconButton from '@mui/material/IconButton';
import EditIcon from '@mui/icons-material/Edit';
import CloseIcon from '@mui/icons-material/Close';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import ErrorIcon from '@mui/icons-material/Error';
import AttachFileIcon from '@mui/icons-material/AttachFile';
import { Task } from '../../api/TaskService';
import { ToolService, Tool } from '../../api/ToolService';
import TaskForm from './TaskForm';
import { Theme } from '@mui/material/styles';
import { useTabDirtyState } from '../../hooks/workflow/useTabDirtyState';
import { useTaskExecutionStore } from '../../store/taskExecutionStore';
import { useUILayoutStore } from '../../store/uiLayout';

import { type LLMGuardrailConfig } from '../../types/task';

export interface TaskNodeData {
  label?: string;
  name?: string;
  taskId?: string;
  tools?: string[];
  tool_configs?: Record<string, unknown>;  // User-specific tool configuration overrides
  context?: string[];
  async_execution?: boolean;
  config?: {
    cache_response?: boolean;
    cache_ttl?: number;
    retry_on_fail?: boolean;
    max_retries?: number;
    timeout?: number | null;
    priority?: number;
    error_handling?: string;
    output_file?: string | null;
    output_json?: string | null;
    output_pydantic?: string | null;
    callback?: string | null;
    human_input?: boolean;
    condition?: string;
    guardrail?: string;
    llm_guardrail?: LLMGuardrailConfig | null;
    markdown?: boolean;
  };
  description?: string;
  expected_output?: string;
}

interface TaskNodeProps {
  data: {
    label: string;
    description?: string;
    expected_output?: string;
    tools?: string[];
    tool_configs?: Record<string, unknown>;  // User-specific tool configuration overrides
    icon?: string;
    taskId: string;
    onEdit?: (task: Task) => void;
    async_execution?: boolean;
    context?: string[];
    callback?: string | null;
    config?: {
      cache_response?: boolean;
      cache_ttl?: number;
      retry_on_fail?: boolean;
      max_retries?: number;
      timeout?: number | null;
      priority?: number;
      error_handling?: string;
      output_file?: string | null;
      output_json?: string | null;
      output_pydantic?: string | null;
      callback?: string | null;
      human_input?: boolean;
      condition?: string;
      guardrail?: string | null;
      llm_guardrail?: LLMGuardrailConfig | null;
      markdown?: boolean;
    };
  };
  id: string;
}

const TaskNode: React.FC<TaskNodeProps> = ({ data, id }) => {
  const { setNodes, setEdges, getNodes, getEdges } = useReactFlow();
  const [isEditing, setIsEditing] = useState(false);
  const [availableTools, setAvailableTools] = useState<Tool[]>([]);
  const [editTooltipOpen, setEditTooltipOpen] = useState(false);
  const [deleteTooltipOpen, setDeleteTooltipOpen] = useState(false);

  // Local selection state
  const [isSelected, setIsSelected] = useState(false);

  // Tab dirty state management
  const { markCurrentTabDirty } = useTabDirtyState();

  // Get current layout orientation
  const layoutOrientation = useUILayoutStore(state => state.layoutOrientation);

  // Task execution state - try multiple ID formats for compatibility
  const taskStatus = useTaskExecutionStore(state => {
    // DEBUG: Log what we're looking for

    let status = null;

    // Try with the label first (most reliable match with backend task names)
    if (data.label) {
      status = state.getTaskStatus(data.label);

      // Try lowercase version of label
      if (!status) {
        status = state.getTaskStatus(data.label.toLowerCase());
      }

      // Try with underscores replaced by spaces
      if (!status) {
        const labelWithSpaces = data.label.replace(/_/g, ' ');
        status = state.getTaskStatus(labelWithSpaces);
      }

      // Try with task_ prefix and label
      if (!status) {
        const labelBasedId = `task_${data.label.replace(/\s+/g, '_').toLowerCase()}`;
        status = state.getTaskStatus(labelBasedId);
      }

      // Check if any task state key contains keywords from the label
      // This handles cases where backend sends full description but label is short
      if (!status) {
        const labelLower = data.label.toLowerCase();
        const labelWords = labelLower.split(/\s+/).filter(word => word.length > 2); // Get significant words
        const allKeys = Array.from(state.taskStates.keys());

        for (const key of allKeys) {
          const keyLower = key.toLowerCase();

          // Check if key starts with the label
          if (keyLower.startsWith(labelLower) ||
              keyLower.startsWith(labelLower.replace(/\s+/g, '_')) ||
              keyLower.startsWith(labelLower.replace(/\s+/g, '-'))) {
            status = state.getTaskStatus(key);
            if (status) {
              break;
            }
          }

          // Check if all significant words from label are in the key
          if (!status && labelWords.length > 0) {
            const allWordsFound = labelWords.every(word => keyLower.includes(word));
            if (allWordsFound) {
              status = state.getTaskStatus(key);
              if (status) {
                break;
              }
            }
          }
        }
      }
    }

    // Try exact taskId if provided and no match found yet
    if (!status && data.taskId) {
      status = state.getTaskStatus(data.taskId);

      // If taskId starts with "task-", also try just the UUID part
      if (!status && data.taskId.startsWith('task-')) {
        const uuidOnly = data.taskId.substring(5); // Remove "task-" prefix
        status = state.getTaskStatus(uuidOnly);
      }
    }

    return status;
  });

  // Add debugging logs on component mount
  useEffect(() => {
    // Monitor edge connections
    getEdges();
  }, [id, data, getEdges]);

  useEffect(() => {
    if (isEditing) {
      const fetchTools = async () => {
        try {
          const tools = await ToolService.listEnabledTools();
          setAvailableTools(tools);
        } catch (error) {
          console.error('Error fetching tools:', error);
        }
      };
      void fetchTools();
    }
  }, [isEditing]);

  // Add a new useEffect that loads tools on component mount
  useEffect(() => {
    const fetchTools = async () => {
      try {
        const tools = await ToolService.listEnabledTools();
        setAvailableTools(tools);
      } catch (error) {
        console.error('Error fetching tools:', error);
      }
    };
    void fetchTools();
  }, []);

  // Simple toggle function for selection
  const toggleSelection = useCallback(() => {
    setIsSelected(prev => !prev);
  }, []);

  const handleDelete = useCallback(() => {
    setEditTooltipOpen(false);
    setDeleteTooltipOpen(false);
    setNodes(nodes => nodes.filter(node => node.id !== id));
    setEdges(edges => edges.filter(edge =>
      edge.source !== id && edge.target !== id
    ));
  }, [id, setNodes, setEdges]);

  const handleEditClick = () => {
    setEditTooltipOpen(false);
    setDeleteTooltipOpen(false);
    document.activeElement && (document.activeElement as HTMLElement).blur();
    setIsEditing(true);
  };

  const handleRightHandleDoubleClick = useCallback(() => {
    const nodes = getNodes();
    const edges = getEdges();
    const currentNode = nodes.find(node => node.id === id);


    if (!currentNode) {
      return;
    }

    // Get all task nodes
    const taskNodes = nodes.filter(node => node.type === 'taskNode');

    // Find the task node that's directly below this one
    const taskNodeBelow = taskNodes.find(taskNode => {
      // Check if the node is below (higher y value)
      const isBelow = taskNode.position.y > currentNode.position.y;
      // Check if the node is roughly in the same vertical line (within 100 pixels horizontally)
      const isAligned = Math.abs(taskNode.position.x - currentNode.position.x) < 100;
      // Check if this is the closest node that meets our criteria
      const isClosest = !taskNodes.some(otherNode => {
        const isOtherBelow = otherNode.position.y > currentNode.position.y;
        const isOtherAligned = Math.abs(otherNode.position.x - currentNode.position.x) < 100;
        const isOtherCloser = otherNode.position.y < taskNode.position.y;
        return otherNode.id !== taskNode.id && isOtherBelow && isOtherAligned && isOtherCloser;
      });

      return isBelow && isAligned && isClosest;
    });

    // If we found a task node below, create a connection
    if (taskNodeBelow) {

      // Check if this connection already exists
      const connectionExists = edges.some(
        edge => edge.source === id && edge.target === taskNodeBelow.id
      );

      if (!connectionExists) {

        const newEdge = {
          id: `${id}-${taskNodeBelow.id}`,
          source: id,
          target: taskNodeBelow.id,
          sourceHandle: 'right',
          targetHandle: 'left',
          type: 'default',
          animated: true, // This will make it look different from agent-task connections
        };

        setEdges(edges => [...edges, newEdge]);
      }
    }
  }, [id, getNodes, getEdges, setEdges]);

  // Add effect to close tooltips when dialog opens/closes
  useEffect(() => {
    if (isEditing) {
      setEditTooltipOpen(false);
      setDeleteTooltipOpen(false);
    }
  }, [isEditing]);

  // Enhanced click handler: left-click opens form, right-click enables dragging
  const handleNodeClick = useCallback((event: React.MouseEvent) => {
    // Completely stop event propagation
    event.preventDefault();
    event.stopPropagation();

    // Check if the click was on an interactive element
    const target = event.target as HTMLElement;
    const isButton = !!target.closest('button');
    const isActionButton = !!target.closest('.action-buttons');

    if (!isButton && !isActionButton) {
      if (event.button === 0) {
        // Left-click: Open task form for editing
        console.log(`TaskNode left-click on ${id} - opening edit form`);
        handleEditClick();
      } else if (event.button === 2) {
        // Right-click: Enable dragging by selecting the node
        console.log(`TaskNode right-click on ${id} - enabling drag mode`);
        toggleSelection();
      }
    }
  }, [id, toggleSelection, handleEditClick]);

  // Handle context menu (right-click) to prevent browser default menu
  const handleContextMenu = useCallback((event: React.MouseEvent) => {
    event.preventDefault();
    event.stopPropagation();
  }, []);

  const iconStyles = {
    mr: 1.5,
    color: (theme: Theme) => theme.palette.primary.main,
    fontSize: '2rem',
    padding: '4px',
    borderRadius: '50%',
    backgroundColor: 'rgba(25, 118, 210, 0.05)',
  };

  const getTaskIcon = () => {
    if (data.icon) {
      return <Box component="span" sx={iconStyles}>{data.icon}</Box>;
    }

    return <AddTaskIcon sx={iconStyles} />;
  };

  const getStatusIcon = () => {
    if (!taskStatus) return null;

    switch (taskStatus.status) {
      case 'running':
        return <CircularProgress size={14} sx={{ color: 'info.main' }} />;
      case 'completed':
        return <CheckCircleIcon sx={{ fontSize: 16, color: 'success.main' }} />;
      case 'failed':
        return <ErrorIcon sx={{ fontSize: 16, color: 'error.main' }} />;
      default:
        return null;
    }
  };

  const getTaskStyles = () => {
    const isRunning = taskStatus?.status === 'running';
    const isCompleted = taskStatus?.status === 'completed';
    const isFailed = taskStatus?.status === 'failed';

    const baseStyles = {
      minWidth: 160,
      minHeight: 120,
      display: 'flex',
      flexDirection: 'column',
      position: 'relative',
      padding: 2,
      cursor: 'pointer',
      background: (theme: Theme) => {
        if (isRunning) {
          return `linear-gradient(135deg, ${theme.palette.info.light}15, ${theme.palette.info.main}10)`;
        }
        if (isCompleted) {
          return `linear-gradient(135deg, ${theme.palette.success.light}15, ${theme.palette.success.main}10)`;
        }
        if (isFailed) {
          return `linear-gradient(135deg, ${theme.palette.error.light}15, ${theme.palette.error.main}10)`;
        }
        return isSelected
          ? `${theme.palette.primary.light}20`
          : theme.palette.background.paper;
      },
      borderRadius: '8px',
      border: '1px solid',
      borderColor: (theme: Theme) => {
        if (isRunning) return theme.palette.info.main;
        if (isCompleted) return theme.palette.success.main;
        if (isFailed) return theme.palette.error.main;
        return isSelected
          ? theme.palette.primary.main
          : theme.palette.grey[300];
      },
      boxShadow: (theme: Theme) => isSelected
        ? `0 0 0 2px ${theme.palette.primary.main}`
        : `0 2px 4px ${theme.palette.mode === 'light' ? 'rgba(0, 0, 0, 0.05)' : 'rgba(0, 0, 0, 0.2)'}`,
      animation: isRunning ? 'pulse 2s infinite' : 'none',
      '@keyframes pulse': {
        '0%': { boxShadow: '0 0 0 0 rgba(33, 150, 243, 0.4)' },
        '70%': { boxShadow: '0 0 0 10px rgba(33, 150, 243, 0)' },
        '100%': { boxShadow: '0 0 0 0 rgba(33, 150, 243, 0)' }
      },
      '&:hover': {
        boxShadow: '0 4px 8px rgba(0, 0, 0, 0.15)',
        '& .action-buttons': {
          display: 'flex'
        }
      },
      '& .action-buttons': {
        display: 'none',
        position: 'absolute',
        top: 4,
        right: 4,
        zIndex: 10,
        pointerEvents: 'all'
      }
    };

    return baseStyles;
  };

  const handlePrepareTaskData = () => {
    // Convert the node data to the format expected by TaskForm
    const taskData = {
      id: data.taskId,
      name: data.label,
      description: data.description || '',
      expected_output: data.expected_output || '',
      tools: data.tools || [],
      tool_configs: data.tool_configs || {},  // Include tool_configs
      agent_id: '',  // This will be set by TaskForm
      async_execution: data.async_execution || false,
      context: data.context || [],
      markdown: data.config?.markdown || false,
      config: {
        cache_response: data.config?.cache_response || false,
        cache_ttl: data.config?.cache_ttl || 3600,
        retry_on_fail: data.config?.retry_on_fail || true,
        max_retries: data.config?.max_retries || 3,
        timeout: data.config?.timeout || null,
        priority: data.config?.priority || 1,
        error_handling: (data.config?.error_handling as 'default' | 'retry' | 'ignore' | 'fail') || 'default',
        output_file: data.config?.output_file || null,
        // output_json should be a string or null, not a boolean
        output_json: data.config?.output_json || null,
        // Ensure output_pydantic is properly retrieved from the config
        output_pydantic: data.config?.output_pydantic || null,
        callback: data.config?.callback || null,
        human_input: data.config?.human_input || false,
        condition: data.config?.condition,
        // Use undefined instead of null for guardrail if it's not present
        guardrail: data.config?.guardrail || undefined,
        // Include llm_guardrail for LLM-based validation
        llm_guardrail: data.config?.llm_guardrail || null,
        markdown: data.config?.markdown || false
      }
    };

    return taskData;
  };

  // Check if task has DatabricksKnowledgeSearchTool
  const hasKnowledgeSearchTool = data.tools?.includes('DatabricksKnowledgeSearchTool') ||
                                  data.tools?.includes('36'); // Also check for tool ID

  return (
    <>
      {/* Top handle - visible only in vertical layout */}
      <Handle
        type="target"
        position={Position.Top}
        id="top"
        style={{
          background: '#2196f3',
          width: '7px',
          height: '7px',
          opacity: layoutOrientation === 'vertical' ? 1 : 0,
          pointerEvents: layoutOrientation === 'vertical' ? 'all' : 'none'
        }}
      />

      {/* Left handle - visible only in horizontal layout */}
      <Handle
        type="target"
        position={Position.Left}
        id="left"
        style={{
          background: '#2196f3',
          width: '7px',
          height: '7px',
          opacity: layoutOrientation === 'horizontal' ? 1 : 0,
          pointerEvents: layoutOrientation === 'horizontal' ? 'all' : 'none'
        }}
      />
      <Box
        sx={getTaskStyles()}
        onClick={handleNodeClick}
        onContextMenu={handleContextMenu}
        data-taskid={data.taskId}
        data-label={data.label}
        data-nodeid={id}
        data-nodetype="task"
        data-selected={isSelected ? 'true' : 'false'}
      >
        {/* Knowledge source indicator - shows when task has knowledge search tool */}
        {hasKnowledgeSearchTool && (
          <Box
            sx={{
              position: 'absolute',
              top: 4,
              left: 4,
              color: 'primary.main',
              display: 'flex',
              zIndex: 5
            }}
          >
            <Tooltip
              title="Task has knowledge search capability"
              disableInteractive
              placement="top"
            >
              <AttachFileIcon fontSize="small" />
            </Tooltip>
          </Box>
        )}

        {taskStatus && (
          <Box
            sx={{
              position: 'absolute',
              top: 8,
              right: 8,
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center'
            }}
          >
            {getStatusIcon()}
          </Box>
        )}
        <div className="action-buttons">
          <Tooltip title="Edit Task" open={editTooltipOpen} onOpen={() => setEditTooltipOpen(true)} onClose={() => setEditTooltipOpen(false)}>
            <IconButton
              size="small"
              onClick={handleEditClick}
              onMouseEnter={() => setEditTooltipOpen(true)}
              onMouseLeave={() => setEditTooltipOpen(false)}
              sx={{
                mr: 0.5,
                backgroundColor: 'rgba(255, 255, 255, 0.3)',
                '&:hover': { backgroundColor: 'rgba(255, 255, 255, 0.5)' },
                zIndex: 20
              }}
            >
              <EditIcon fontSize="small" />
            </IconButton>
          </Tooltip>
          <Tooltip title="Delete Task" open={deleteTooltipOpen} onOpen={() => setDeleteTooltipOpen(true)} onClose={() => setDeleteTooltipOpen(false)}>
            <IconButton
              size="small"
              onClick={handleDelete}
              onMouseEnter={() => setDeleteTooltipOpen(true)}
              onMouseLeave={() => setDeleteTooltipOpen(false)}
              sx={{
                backgroundColor: 'rgba(255, 255, 255, 0.3)',
                '&:hover': { backgroundColor: 'rgba(255, 255, 255, 0.5)' },
                zIndex: 20
              }}
            >
              <DeleteIcon fontSize="small" />
            </IconButton>
          </Tooltip>
        </div>
        <Box sx={{ display: 'flex', alignItems: 'center', mb: 1 }}>
          {getTaskIcon()}
          <Typography variant="body2" sx={{
            fontWeight: 500,
            color: (theme: Theme) => theme.palette.primary.main,
            fontSize: '0.9rem'
          }}>
            {data.label}
          </Typography>
        </Box>

        <Typography
          variant="body2"
          color="textSecondary"
          sx={{
            fontSize: '0.8rem',
            overflow: 'hidden',
            textOverflow: 'ellipsis',
            display: '-webkit-box',
            WebkitLineClamp: 2,
            WebkitBoxOrient: 'vertical'
          }}
        >
          {data.description}
        </Typography>

        <Typography
          variant="caption"
          color="textSecondary"
          sx={{
            mt: 'auto',
            pt: 1,
            fontSize: '0.7rem',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            width: '100%'
          }}
        >
          <span>Tools: {Array.isArray(data.tools) ? data.tools.length : 0}</span>
          {data.config?.human_input && (
            <span style={{ color: 'orange' }}>Human Input</span>
          )}
        </Typography>

        {Boolean((data as Record<string, unknown>)?.loading) && (
          <Box
            sx={{
              position: 'absolute',
              inset: 0,
              borderRadius: '8px',
              background: (theme: { palette: { mode: string } }) => theme.palette.mode === 'light'
                ? 'linear-gradient(90deg, rgba(255,255,255,0.35) 25%, rgba(255,255,255,0.6) 37%, rgba(255,255,255,0.35) 63%)'
                : 'linear-gradient(90deg, rgba(255,255,255,0.08) 25%, rgba(255,255,255,0.16) 37%, rgba(255,255,255,0.08) 63%)',
              backgroundSize: '400% 100%',
              animation: 'shimmer 1.6s linear infinite',
              '@keyframes shimmer': {
                '0%': { backgroundPosition: '-200% 0' },
                '100%': { backgroundPosition: '200% 0' },
              },
              backdropFilter: 'blur(1px)',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              zIndex: 5,
            }}
          >
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
              <CircularProgress size={16} sx={{ color: 'primary.main' }} />
              <Typography variant="caption" color="textSecondary">Creating…</Typography>
            </Box>
          </Box>
        )}
      </Box>
      <Handle
        type="source"
        position={Position.Right}
        style={{ background: '#2196f3', width: '7px', height: '7px' }}
        onDoubleClick={handleRightHandleDoubleClick}
      />

      {/* Edit Task Form Dialog */}
      <Dialog
        open={isEditing}
        onClose={() => setIsEditing(false)}
        maxWidth="md"
        fullWidth
        PaperProps={{
          sx: {
            maxHeight: '80vh',
            position: 'relative'
          }
        }}
      >
        <DialogTitle>
          Edit Task
          <IconButton
            aria-label="close"
            onClick={() => setIsEditing(false)}
            sx={{
              position: 'absolute',
              right: 8,
              top: 8
            }}
          >
            <CloseIcon />
          </IconButton>
        </DialogTitle>
        <DialogContent>
          <Box sx={{ mt: 2 }}>
            <TaskForm
              initialData={handlePrepareTaskData()}
              onCancel={() => setIsEditing(false)}
              onTaskSaved={(savedTask) => {
                // Mark tab as dirty since task was modified
                markCurrentTabDirty();

                // Update the node with the saved task data
                setNodes(nodes =>
                  nodes.map(node => {
                    if (node.id === id) {
                      const updatedData = {
                        ...node.data,
                        label: savedTask.name,
                        description: savedTask.description,
                        expected_output: savedTask.expected_output,
                        tools: savedTask.tools,
                        tool_configs: savedTask.tool_configs || {},  // Include tool_configs from saved task
                        async_execution: savedTask.async_execution,
                        context: savedTask.context,
                        // Synchronize both markdown fields with the saved task - prioritize the saved task's top-level markdown
                        markdown: savedTask.markdown !== undefined ? savedTask.markdown : (savedTask.config?.markdown || false),
                        // Ensure all config values are preserved
                        config: {
                          ...node.data.config, // Preserve existing config structure
                          ...savedTask.config, // Override with saved task config
                          // Explicitly preserve these important fields
                          output_pydantic: savedTask.config?.output_pydantic || null,
                          output_json: savedTask.config?.output_json || null,
                          output_file: savedTask.config?.output_file || null,
                          callback: savedTask.config?.callback || null,
                          guardrail: savedTask.config?.guardrail || undefined,
                          // Include llm_guardrail for LLM-based validation
                          llm_guardrail: savedTask.config?.llm_guardrail || null,
                          // Force markdown to be included in config - use the same value as top-level
                          markdown: savedTask.markdown !== undefined ? savedTask.markdown : (savedTask.config?.markdown || false)
                        }
                      };

                      return {
                        ...node,
                        data: updatedData
                      };
                    }
                    return node;
                  })
                );
                setIsEditing(false);
              }}
              tools={availableTools}
              hideTitle
            />
          </Box>
        </DialogContent>
      </Dialog>
    </>
  );
};

export default React.memo(TaskNode);