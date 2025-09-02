import React, { useCallback, useState, useEffect } from 'react';
import { Handle, Position, useReactFlow } from 'reactflow';
import { Box, Typography, IconButton, Dialog, DialogContent, DialogTitle, Paper } from '@mui/material';
import DeleteIcon from '@mui/icons-material/Delete';
import EditIcon from '@mui/icons-material/Edit';
import CompareArrowsIcon from '@mui/icons-material/CompareArrows';
import CallSplitIcon from '@mui/icons-material/CallSplit';
import RouterIcon from '@mui/icons-material/Router';
import { Theme } from '@mui/material/styles';
import ConditionEditForm from './ConditionEditForm';
import type { FlowFormData } from '../../types/flow';
import { apiClient } from '../../config/api/ApiConfig';
import { CrewService } from '../../api/CrewService';

interface ConditionFlowNodeData {
  conditionId: string;
  label: string;
  conditionType: 'and' | 'or' | 'router';
  routerCondition?: string;
  inputs?: string[];
  outputs?: string[];
  isActive?: boolean;
  isCompleted?: boolean;
  crewRef?: string;
  taskRef?: string;
}

interface UpdatedConditionData {
  name: string;
  conditionType: 'and' | 'or' | 'router';
  routerCondition?: string;
  inputs?: string[];
  outputs?: string[];
  crewRef?: string;
  taskRef?: string;
}

const ConditionFlowNode: React.FC<{ data: ConditionFlowNodeData; id: string }> = ({ data, id }) => {
  const { setNodes, setEdges, getNodes } = useReactFlow();
  const [isEditing, setIsEditing] = useState(false);
  const [_conditionType, _setConditionType] = useState<'and' | 'or' | 'router'>(data.conditionType || 'and');
  const [availableNodes, setAvailableNodes] = useState<{ id: string; label: string }[]>([]);
  const [_availableCrews, setAvailableCrews] = useState<{ id: string; name: string }[]>([]);
  const [_availableTasks, setAvailableTasks] = useState<{ id: string; name: string }[]>([]);
  
  // Fetch available crews and tasks when edit dialog opens
  useEffect(() => {
    if (isEditing) {
      // Get all nodes from reactflow
      const nodes = getNodes();
      setAvailableNodes(nodes.map(node => ({
        id: node.id,
        label: node.data?.label || 'Unnamed Node'
      })));

      // Fetch crews
      const fetchCrews = async () => {
        try {
          const crews = await CrewService.getCrews();
          setAvailableCrews(crews.map(crew => ({
            id: crew.id.toString(),
            name: crew.name
          })));
        } catch (error) {
          console.error('Error fetching crews:', error);
        }
      };

      // Fetch tasks
      const fetchTasks = async () => {
        try {
          const response = await apiClient.get('/tasks');
          const tasks = response.data;
          setAvailableTasks(tasks.map((task: { id: string; name: string }) => ({
            id: task.id.toString(),
            name: task.name
          })));
        } catch (error) {
          console.error('Error fetching tasks:', error);
        }
      };

      fetchCrews();
      fetchTasks();
    }
  }, [isEditing, getNodes]);
  
  const handleDelete = useCallback(() => {
    setNodes(nodes => nodes.filter(node => node.id !== id));
    setEdges(edges => edges.filter(edge => 
      edge.source !== id && edge.target !== id
    ));
  }, [id, setNodes, setEdges]);

  const handleEditClick = () => {
    setIsEditing(true);
  };

  const handleUpdateNode = (flowData: FlowFormData) => {
    // The ConditionEditForm already provides the correct conditionType
    const updatedCondition: UpdatedConditionData = {
      name: flowData.name,
      conditionType: flowData.conditionType as 'and' | 'or' | 'router',
      routerCondition: flowData.routerCondition,
      crewRef: flowData.crewRef,
      taskRef: flowData.taskRef,
      inputs: flowData.listenTo,
    };
    
    setNodes(nodes => nodes.map(node => {
      if (node.id === id) {
        return {
          ...node,
          data: {
            ...node.data,
            label: updatedCondition.name,
            conditionType: updatedCondition.conditionType,
            routerCondition: updatedCondition.routerCondition,
            inputs: updatedCondition.inputs,
            crewRef: updatedCondition.crewRef,
            taskRef: updatedCondition.taskRef
          }
        };
      }
      return node;
    }));
    setIsEditing(false);
  };

  const getNodeStyles = () => {
    // Base styles for condition nodes - diamond shape
    const baseStyles = {
      width: 120,
      height: 120,
      display: 'flex',
      flexDirection: 'column',
      alignItems: 'center',
      justifyContent: 'center',
      position: 'relative',
      background: (theme: Theme) => theme.palette.background.paper,
      borderRadius: '4px',
      transform: 'rotate(45deg)',
      boxShadow: (theme: Theme) => `0 2px 4px ${theme.palette.mode === 'light' 
        ? 'rgba(0, 0, 0, 0.1)' 
        : 'rgba(0, 0, 0, 0.4)'}`,
      transition: 'all 0.3s ease',
      padding: '16px',
      '&:hover': {
        boxShadow: (theme: Theme) => `0 4px 12px ${theme.palette.mode === 'light'
          ? 'rgba(0, 0, 0, 0.2)'
          : 'rgba(0, 0, 0, 0.6)'}`,
        transform: 'rotate(45deg) translateY(-2px)',
      },
      '& .action-buttons': {
        display: 'none',
        position: 'absolute',
        top: 5,
        right: 5,
        transform: 'rotate(-45deg)',
      },
      '&:hover .action-buttons': {
        display: 'flex'
      },
      '& .content': {
        transform: 'rotate(-45deg)',
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        justifyContent: 'center',
        width: '100%',
        height: '100%'
      }
    };

    // Special styling for different condition types
    if (data.conditionType === 'and') {
      return {
        ...baseStyles,
        border: (theme: Theme) => `2px solid ${theme.palette.primary.main}`,
        background: (theme: Theme) => `${theme.palette.primary.light}30`,
      };
    }
    
    if (data.conditionType === 'or') {
      return {
        ...baseStyles,
        border: (theme: Theme) => `2px solid ${theme.palette.secondary.main}`,
        background: (theme: Theme) => `${theme.palette.secondary.light}30`,
      };
    }
    
    if (data.conditionType === 'router') {
      return {
        ...baseStyles,
        border: (theme: Theme) => `2px solid ${theme.palette.warning.main}`,
        background: (theme: Theme) => `${theme.palette.warning.light}30`,
      };
    }

    return baseStyles;
  };

  const getConditionIcon = () => {
    if (data.conditionType === 'router') {
      return <RouterIcon sx={{ 
        fontSize: '2rem',
        color: (theme: Theme) => theme.palette.warning.main
      }} />;
    }
    
    if (data.conditionType === 'or') {
      return <CallSplitIcon sx={{ 
        fontSize: '2rem',
        color: (theme: Theme) => theme.palette.secondary.main 
      }} />;
    }
    
    return <CompareArrowsIcon sx={{ 
      fontSize: '2rem',
      color: (theme: Theme) => theme.palette.primary.main
    }} />;
  };

  const getConditionDescription = () => {
    switch (data.conditionType) {
      case 'and':
        return 'Executes when ALL input conditions are met';
      case 'or':
        return 'Executes when ANY input condition is met';
      case 'router':
        return 'Routes flow based on condition evaluation';
      default:
        return '';
    }
  };

  const getReferencedItem = () => {
    if (data.crewRef) {
      return `Crew: ${data.crewRef}`;
    }
    if (data.taskRef) {
      return `Task: ${data.taskRef}`;
    }
    return null;
  };

  return (
    <Box sx={getNodeStyles()}>
      {/* Following BPMN and flowchart standards for diamond decision nodes */}
      
      {/* Top handle - used for incoming flow */}
      <Handle
        type="target"
        position={Position.Top}
        style={{
          background: '#fff',
          border: '2px solid #555',
          width: '10px',
          height: '10px',
          borderRadius: '50%',
          position: 'absolute',
          left: '50%',
          top: '0',
          transform: 'translate(-50%, -50%)'
        }}
        id="top"
      />
      
      {/* Right handle - used for outgoing flow (typically "yes" path) */}
      <Handle
        type="source"
        position={Position.Right}
        style={{
          background: '#fff',
          border: '2px solid #555',
          width: '10px',
          height: '10px',
          borderRadius: '50%',
          position: 'absolute',
          right: '0',
          top: '50%',
          transform: 'translate(50%, -50%)'
        }}
        id="right"
      />
      
      {/* Bottom handle - used for outgoing flow (typically "default" path) */}
      <Handle
        type="source"
        position={Position.Bottom}
        style={{
          background: '#fff',
          border: '2px solid #555',
          width: '10px',
          height: '10px',
          borderRadius: '50%',
          position: 'absolute',
          left: '50%',
          bottom: '0',
          transform: 'translate(-50%, 50%)'
        }}
        id="bottom"
      />
      
      {/* Left handle - used for outgoing flow (typically "no" path) */}
      <Handle
        type="source"
        position={Position.Left}
        style={{
          background: '#fff',
          border: '2px solid #555',
          width: '10px',
          height: '10px',
          borderRadius: '50%',
          position: 'absolute',
          left: '0',
          top: '50%',
          transform: 'translate(-50%, -50%)'
        }}
        id="left"
      />

      <Box className="action-buttons">
        <IconButton size="small" onClick={handleEditClick}>
          <EditIcon fontSize="small" />
        </IconButton>
        <IconButton size="small" onClick={handleDelete}>
          <DeleteIcon fontSize="small" />
        </IconButton>
      </Box>

      <Box className="content">
        {getConditionIcon()}

        <Typography variant="subtitle2" sx={{ 
          textAlign: 'center', 
          fontWeight: 'bold',
          mb: 0.5,
          fontSize: '0.9rem'
        }}>
          {data.conditionType.toUpperCase()}
        </Typography>

        <Typography variant="caption" sx={{ 
          textAlign: 'center',
          fontSize: '0.7rem'
        }}>
          {data.label}
        </Typography>
        
        {getReferencedItem() && (
          <Typography variant="caption" sx={{ 
            textAlign: 'center',
            fontSize: '0.7rem',
            mt: 0.5,
            fontStyle: 'italic'
          }}>
            {getReferencedItem()}
          </Typography>
        )}
        
        <Typography variant="caption" sx={{ 
          textAlign: 'center',
          fontSize: '0.6rem',
          mt: 0.5,
          opacity: 0.7,
          maxWidth: '100px'
        }}>
          {getConditionDescription()}
        </Typography>
      </Box>

      <Dialog 
        open={isEditing} 
        onClose={() => setIsEditing(false)} 
        maxWidth="md" 
        fullWidth
        sx={{
          zIndex: 9999, // Higher z-index to ensure visibility
          '& .MuiDialog-paper': {
            boxShadow: '0 8px 24px rgba(0,0,0,0.2)'
          }
        }}
        disableScrollLock={false}
        keepMounted={false}
      >
        <DialogTitle sx={{ 
          borderBottom: '1px solid #e0e0e0',
          bgcolor: (theme) => theme.palette.background.default
        }}>
          Edit {data.conditionType.toUpperCase()} Condition
        </DialogTitle>
        <DialogContent sx={{ pt: 2 }}>
          <Paper elevation={0} sx={{ p: 2 }}>
            <ConditionEditForm 
              initialData={{
                conditionId: data.conditionId,
                label: data.label,
                conditionType: data.conditionType,
                routerCondition: data.routerCondition,
                inputs: data.inputs,
                crewRef: data.crewRef,
                taskRef: data.taskRef
              }}
              onCancel={() => setIsEditing(false)}
              onSubmit={handleUpdateNode}
              availableNodes={availableNodes}
            />
          </Paper>
        </DialogContent>
      </Dialog>
    </Box>
  );
};

export default ConditionFlowNode; 