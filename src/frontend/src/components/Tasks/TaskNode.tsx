import React, { useCallback, useState, useEffect } from 'react';
import { Handle, Position, useReactFlow } from 'reactflow';
import { Box, Typography, Dialog, DialogTitle, DialogContent, Tooltip } from '@mui/material';
import AddTaskIcon from '@mui/icons-material/AddTask';
import DeleteIcon from '@mui/icons-material/Delete';
import IconButton from '@mui/material/IconButton';
import EditIcon from '@mui/icons-material/Edit';
import CloseIcon from '@mui/icons-material/Close';
import { Task } from '../../api/TaskService';
import { ToolService, Tool } from '../../api/ToolService';
import TaskForm from './TaskForm';
import { Theme } from '@mui/material/styles';
import { useTabDirtyState } from '../../hooks/workflow/useTabDirtyState';

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
  
  // Add debugging logs on component mount
  useEffect(() => {
    console.log(`TaskNode: Initialized node ${id} with label "${data.label}"`);
    console.log(`TaskNode: Node ${id} data:`, data);
    
    // Log incoming and outgoing connections
    const edges = getEdges();
    const incomingEdges = edges.filter(edge => edge.target === id);
    const outgoingEdges = edges.filter(edge => edge.source === id);
    
    console.log(`TaskNode: Node ${id} has ${incomingEdges.length} incoming edges and ${outgoingEdges.length} outgoing edges`);
    
    if (incomingEdges.length > 0) {
      console.log(`TaskNode: Incoming edges to ${id}:`, incomingEdges);
    }
    
    if (outgoingEdges.length > 0) {
      console.log(`TaskNode: Outgoing edges from ${id}:`, outgoingEdges);
    }
  }, [id, data, getEdges]);

  useEffect(() => {
    if (isEditing) {
      const fetchTools = async () => {
        try {
          const tools = await ToolService.listTools();
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
        const tools = await ToolService.listTools();
        setAvailableTools(tools);
      } catch (error) {
        console.error('Error fetching tools:', error);
      }
    };
    void fetchTools();
  }, []);

  // Simple toggle function for selection
  const toggleSelection = useCallback(() => {
    console.log(`TaskNode ${id}: Toggling selection from ${isSelected} to ${!isSelected}`);
    setIsSelected(prev => !prev);
  }, [id, isSelected]);

  const handleDelete = useCallback(() => {
    console.log(`TaskNode: Deleting node ${id}`);
    
    // Log edges to be removed
    const edges = getEdges();
    const connectedEdges = edges.filter(edge => edge.source === id || edge.target === id);
    console.log(`TaskNode: Removing ${connectedEdges.length} connected edges during node deletion`);
    
    setEditTooltipOpen(false);
    setDeleteTooltipOpen(false);
    setNodes(nodes => nodes.filter(node => node.id !== id));
    setEdges(edges => edges.filter(edge => 
      edge.source !== id && edge.target !== id
    ));
  }, [id, getEdges, setNodes, setEdges]);

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
    
    console.log(`TaskNode: Double-click on right handle of node ${id}`);
    
    if (!currentNode) {
      console.warn(`TaskNode: Could not find current node with id ${id}`);
      return;
    }

    // Get all task nodes
    const taskNodes = nodes.filter(node => node.type === 'taskNode');
    console.log(`TaskNode: Found ${taskNodes.length} task nodes for potential connection`);
    
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
      console.log(`TaskNode: Found task node below: ${taskNodeBelow.id} (${taskNodeBelow.data.label})`);
      
      // Check if this connection already exists
      const connectionExists = edges.some(
        edge => edge.source === id && edge.target === taskNodeBelow.id
      );

      if (!connectionExists) {
        console.log(`TaskNode: Creating new edge from ${id} to ${taskNodeBelow.id}`);
        
        const newEdge = {
          id: `${id}-${taskNodeBelow.id}`,
          source: id,
          target: taskNodeBelow.id,
          type: 'default',
          animated: true, // This will make it look different from agent-task connections
        };

        setEdges(edges => [...edges, newEdge]);
      } else {
        console.log(`TaskNode: Connection already exists from ${id} to ${taskNodeBelow.id}`);
      }
    } else {
      console.log(`TaskNode: No suitable task node found below ${id}`);
    }
  }, [id, getNodes, getEdges, setEdges]);

  // Add effect to close tooltips when dialog opens/closes
  useEffect(() => {
    if (isEditing) {
      setEditTooltipOpen(false);
      setDeleteTooltipOpen(false);
    }
  }, [isEditing]);

  // Improved click handler with local selection
  const handleNodeClick = useCallback((event: React.MouseEvent) => {
    // Completely stop event propagation
    event.preventDefault();
    event.stopPropagation();
    
    // Check if the click was on an interactive element
    const target = event.target as HTMLElement;
    const isButton = !!target.closest('button');
    const isActionButton = !!target.closest('.action-buttons');
    
    if (!isButton && !isActionButton) {
      console.log(`TaskNode click on ${id} - toggling selection`);
      toggleSelection();
    } else {
      console.log(`TaskNode click on ${id} ignored - clicked on button or action button`);
    }
  }, [id, toggleSelection]);

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

  const getTaskStyles = () => {
    const baseStyles = {
      minWidth: 160,
      minHeight: 120,
      display: 'flex',
      flexDirection: 'column',
      position: 'relative',
      padding: 2,
      background: (theme: Theme) => isSelected 
        ? `${theme.palette.primary.light}20` // Light background when selected
        : theme.palette.background.paper,
      borderRadius: '8px',
      border: '1px solid',
      borderColor: (theme: Theme) => isSelected 
        ? theme.palette.primary.main 
        : theme.palette.grey[300],
      boxShadow: (theme: Theme) => isSelected 
        ? `0 0 0 2px ${theme.palette.primary.main}` 
        : `0 2px 4px ${theme.palette.mode === 'light' ? 'rgba(0, 0, 0, 0.05)' : 'rgba(0, 0, 0, 0.2)'}`,
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
        markdown: data.config?.markdown || false
      }
    };
    
    return taskData;
  };

  return (
    <>
      <Handle
        type="target"
        position={Position.Left}
        style={{ background: '#2196f3', width: '7px', height: '7px' }}
      />
      <Box 
        sx={getTaskStyles()}
        onClick={handleNodeClick}
        data-taskid={data.taskId}
        data-label={data.label}
        data-nodeid={id}
        data-nodetype="task"
        data-selected={isSelected ? 'true' : 'false'}
      >
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
                          // Force markdown to be included in config - use the same value as top-level
                          markdown: savedTask.markdown !== undefined ? savedTask.markdown : (savedTask.config?.markdown || false)
                        }
                      };
                      
                      console.log(`TaskNode: Updated task ${id} after save`, {
                        savedTaskMarkdown: savedTask.markdown,
                        savedTaskConfigMarkdown: savedTask.config?.markdown,
                        resultTopLevelMarkdown: updatedData.markdown,
                        resultConfigMarkdown: updatedData.config.markdown
                      });
                      
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

export default TaskNode; 