import React, { useCallback, useState, useEffect } from 'react';
import { Handle, Position, useReactFlow } from 'reactflow';
import { Box, Typography, Dialog, DialogContent, IconButton, Tooltip } from '@mui/material';
import PersonIcon from '@mui/icons-material/Person';
import DeleteIcon from '@mui/icons-material/Delete';
import EditIcon from '@mui/icons-material/Edit';
import CodeIcon from '@mui/icons-material/Code';
import MemoryIcon from '@mui/icons-material/Memory';
import FileIcon from '@mui/icons-material/FileUpload';
import { AgentService, Agent } from '../../api/AgentService';
import AgentForm from './AgentForm';
import { ToolService } from '../../api/ToolService';
import { Tool, KnowledgeSource } from '../../types/agent';
import { Theme } from '@mui/material/styles';

interface AgentNodeData {
  agentId: string;
  label: string;
  role?: string;
  goal?: string;
  backstory?: string;
  icon?: string;
  isActive?: boolean;
  isCompleted?: boolean;
  llm?: string;
  function_calling_llm?: string;
  max_iter?: number;
  max_rpm?: number;
  max_execution_time?: number;
  memory?: boolean;
  verbose?: boolean;
  allow_delegation?: boolean;
  cache?: boolean;
  system_template?: string;
  prompt_template?: string;
  response_template?: string;
  allow_code_execution?: boolean;
  code_execution_mode?: string;
  max_retry_limit?: number;
  use_system_prompt?: boolean;
  respect_context_window?: boolean;
  embedder_config?: Record<string, unknown>;
  knowledge_sources?: KnowledgeSource[];
  [key: string]: unknown; // For flexibility with other properties
}

const AgentNode: React.FC<{ data: AgentNodeData; id: string }> = ({ data, id }) => {
  const { setNodes, setEdges, getNodes, getEdges } = useReactFlow();
  const [isEditing, setIsEditing] = useState(false);
  const [agentData, setAgentData] = useState<Agent | null>(null);
  const [tools, setTools] = useState<Tool[]>([]);
  const [editTooltipOpen, setEditTooltipOpen] = useState(false);
  const [deleteTooltipOpen, setDeleteTooltipOpen] = useState(false);
  const [fileTooltipOpen, setFileTooltipOpen] = useState(false);
  const [codeTooltipOpen, setCodeTooltipOpen] = useState(false);
  const [memoryTooltipOpen, setMemoryTooltipOpen] = useState(false);

  // Local selection state
  const [isSelected, setIsSelected] = useState(false);

  useEffect(() => {
    loadTools();
  }, []);

  const loadTools = async () => {
    try {
      const toolsList = await ToolService.listTools();
      setTools(toolsList.map(tool => ({
        ...tool,
        id: String(tool.id)
      })));
    } catch (error) {
      console.error('Error loading tools:', error);
    }
  };

  // Simple toggle function for selection
  const toggleSelection = useCallback(() => {
    console.log(`AgentNode ${id}: Toggling selection from ${isSelected} to ${!isSelected}`);
    setIsSelected(prev => !prev);
  }, [id, isSelected]);

  const handleDelete = useCallback(() => {
    setEditTooltipOpen(false);
    setDeleteTooltipOpen(false);
    setFileTooltipOpen(false);
    setCodeTooltipOpen(false);
    setMemoryTooltipOpen(false);
    
    setNodes(nodes => nodes.filter(node => node.id !== id));
    setEdges(edges => edges.filter(edge => 
      edge.source !== id && edge.target !== id
    ));
  }, [id, setNodes, setEdges]);

  const handleEditClick = async () => {
    try {
      setEditTooltipOpen(false);
      setDeleteTooltipOpen(false);
      setFileTooltipOpen(false);
      setCodeTooltipOpen(false);
      setMemoryTooltipOpen(false);
      
      document.activeElement && (document.activeElement as HTMLElement).blur();
      
      const agentIdToUse = data.agentId;
      
      if (!agentIdToUse) {
        console.error('Agent ID is missing in node data:', data);
        return;
      }

      const response = await AgentService.getAgent(agentIdToUse);
      if (response) {
        setAgentData(response);
        setIsEditing(true);
      }
    } catch (error) {
      console.error('Failed to fetch agent data:', error);
    }
  };

  useEffect(() => {
    if (isEditing) {
      setEditTooltipOpen(false);
      setDeleteTooltipOpen(false);
      setFileTooltipOpen(false);
      setCodeTooltipOpen(false);
      setMemoryTooltipOpen(false);
    }
  }, [isEditing]);

  const handleDoubleClick = useCallback(() => {
    const nodes = getNodes();
    const edges = getEdges();
    
    const taskNodes = nodes.filter(node => node.type === 'taskNode');
    
    const availableTaskNodes = taskNodes.filter(taskNode => {
      const hasIncomingEdge = edges.some(edge => edge.target === taskNode.id);
      return !hasIncomingEdge;
    });

    const sortedTaskNodes = [...availableTaskNodes].sort((a, b) => a.position.y - b.position.y);

    if (sortedTaskNodes.length > 0) {
      const targetNode = sortedTaskNodes[0];

      const newEdge = {
        id: `${id}-${targetNode.id}`,
        source: id,
        target: targetNode.id,
        type: 'default',
      };

      setEdges(edges => [...edges, newEdge]);
    }
  }, [id, getNodes, getEdges, setEdges]);

  const handleUpdateNode = useCallback(async (updatedAgent: Agent) => {
    try {
      setNodes(nodes => nodes.map(node => {
        if (node.id === id) {
          return {
            ...node,
            data: {
              ...node.data,
              label: updatedAgent.name,
              role: updatedAgent.role,
              goal: updatedAgent.goal,
              backstory: updatedAgent.backstory,
              tools: updatedAgent.tools,
              llm: updatedAgent.llm,
              function_calling_llm: updatedAgent.function_calling_llm,
              max_iter: updatedAgent.max_iter,
              max_rpm: updatedAgent.max_rpm,
              max_execution_time: updatedAgent.max_execution_time,
              memory: updatedAgent.memory,
              verbose: updatedAgent.verbose,
              allow_delegation: updatedAgent.allow_delegation,
              cache: updatedAgent.cache,
              system_template: updatedAgent.system_template,
              prompt_template: updatedAgent.prompt_template,
              response_template: updatedAgent.response_template,
              allow_code_execution: updatedAgent.allow_code_execution,
              code_execution_mode: updatedAgent.code_execution_mode,
              max_retry_limit: updatedAgent.max_retry_limit,
              use_system_prompt: updatedAgent.use_system_prompt,
              respect_context_window: updatedAgent.respect_context_window,
              embedder_config: updatedAgent.embedder_config,
              knowledge_sources: updatedAgent.knowledge_sources,
            }
          };
        }
        return node;
      }));
    } catch (error) {
      console.error('Failed to update node:', error);
    }
  }, [id, setNodes]);

  useEffect(() => {
    if (!isEditing && data.agentId) {
      const refreshAgentData = async () => {
        try {
          const refreshedAgent = await AgentService.getAgent(data.agentId);
          if (refreshedAgent) {
            handleUpdateNode(refreshedAgent);
          }
        } catch (error) {
          console.error('Failed to refresh agent data:', error);
        }
      };
      
      refreshAgentData();
    }
  }, [isEditing, data.agentId, handleUpdateNode]);

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
      console.log(`AgentNode click on ${id} - toggling selection`);
      toggleSelection();
    } else {
      console.log(`AgentNode click on ${id} ignored - clicked on button or action button`);
    }
  }, [id, toggleSelection]);

  const getAgentNodeStyles = () => {
    const baseStyles = {
      width: 160,
      minHeight: 140,
      display: 'flex',
      flexDirection: 'column',
      alignItems: 'center',
      justifyContent: 'space-between',
      gap: 0.1,
      position: 'relative',
      background: (theme: Theme) => isSelected 
        ? `${theme.palette.primary.light}20` // Light background when selected
        : theme.palette.background.paper,
      borderRadius: '12px',
      boxShadow: (theme: Theme) => isSelected 
        ? `0 0 0 2px ${theme.palette.primary.main}` 
        : `0 2px 4px ${theme.palette.mode === 'light' 
          ? 'rgba(0, 0, 0, 0.1)' 
          : 'rgba(0, 0, 0, 0.4)'}`,
      border: (theme: Theme) => `1px solid ${isSelected 
        ? theme.palette.primary.main 
        : theme.palette.primary.light}`,
      transition: 'all 0.3s ease',
      padding: '16px 8px',
      '&:hover': {
        boxShadow: (theme: Theme) => `0 4px 12px ${theme.palette.mode === 'light'
          ? 'rgba(0, 0, 0, 0.2)'
          : 'rgba(0, 0, 0, 0.6)'}`,
        transform: 'translateY(-1px)',
      },
      '& .action-buttons': {
        display: 'none',
        position: 'absolute',
        top: 2,
        right: 4,
        gap: '2px'
      },
      '&:hover .action-buttons': {
        display: 'flex'
      }
    };

    if (data.isActive) {
      return {
        ...baseStyles,
        background: (theme: Theme) => theme.palette.mode === 'light'
          ? `rgba(${theme.palette.primary.main}, 0.15)`
          : theme.palette.background.paper,
        border: (theme: Theme) => `3px solid ${theme.palette.primary.main}`,
        transform: 'scale(1.05)',
        boxShadow: (theme: Theme) => `0 0 12px ${theme.palette.primary.main}70`,
        '&::before': {
          content: '"ACTIVE"',
          position: 'absolute',
          top: '-10px',
          left: '50%',
          transform: 'translateX(-50%)',
          backgroundColor: (theme: Theme) => theme.palette.primary.main,
          color: (theme: Theme) => theme.palette.primary.contrastText,
          padding: '2px 6px',
          borderRadius: '4px',
          fontSize: '11px',
          fontWeight: 'bold',
          zIndex: 10
        }
      };
    }
    
    if (data.isCompleted) {
      return {
        ...baseStyles,
        background: (theme: Theme) => theme.palette.mode === 'light'
          ? `rgba(${theme.palette.success.main}, 0.1)`
          : theme.palette.background.paper,
        border: (theme: Theme) => `2px solid ${theme.palette.success.main}`,
        boxShadow: (theme: Theme) => `0 0 8px ${theme.palette.success.main}70`,
        '&::before': {
          content: '"COMPLETED"',
          position: 'absolute',
          top: '-10px',
          left: '50%',
          transform: 'translateX(-50%)',
          backgroundColor: (theme: Theme) => theme.palette.success.main,
          color: '#ffffff',
          padding: '2px 6px',
          borderRadius: '4px',
          fontSize: '10px',
          fontWeight: 'bold',
          zIndex: 10
        }
      };
    }

    return baseStyles;
  };

  const hasFiles = data.knowledge_sources?.some(source => 
    source.type !== 'text' && source.type !== 'url' && source.fileInfo?.exists
  );

  return (
    <Box
      sx={{
        ...getAgentNodeStyles(),
        cursor: 'move'
      }}
      onClick={handleNodeClick}
      data-agentid={data.agentId}
      data-nodeid={id}
      data-nodetype="agent"
      data-selected={isSelected ? 'true' : 'false'}
    >
      {hasFiles && (
        <Box 
          sx={{ 
            position: 'absolute', 
            top: 4, 
            left: 4, 
            color: 'success.main',
            display: 'flex'
          }}
        >
          <Tooltip 
            title="Has uploaded files" 
            disableInteractive 
            placement="top"
            open={fileTooltipOpen}
            onOpen={() => setFileTooltipOpen(true)}
            onClose={() => setFileTooltipOpen(false)}
          >
            <FileIcon fontSize="small" />
          </Tooltip>
        </Box>
      )}
      
      <Handle
        type="source"
        position={Position.Right}
        style={{ background: '#2196f3', width: '7px', height: '7px' }}
        onDoubleClick={handleDoubleClick}
      />
      
      <Box sx={{
        backgroundColor: (theme: Theme) => `${theme.palette.primary.main}20`,
        borderRadius: '50%',
        padding: '8px',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        border: (theme: Theme) => `2px solid ${theme.palette.primary.main}`,
      }}>
        <PersonIcon sx={{ color: (theme: Theme) => theme.palette.primary.main, fontSize: '1.5rem' }} />
      </Box>
      
      <Typography variant="body2" sx={{
        fontWeight: 500,
        textAlign: 'center',
        color: (theme: Theme) => theme.palette.primary.main,
        maxWidth: '140px',
        overflow: 'hidden',
        textOverflow: 'ellipsis',
        whiteSpace: 'nowrap',
      }}>
        {data.role || 'Agent'}
      </Typography>

      <Box sx={{
        background: (theme: Theme) => `linear-gradient(135deg, ${theme.palette.primary.main}15, ${theme.palette.primary.main}30)`,
        borderRadius: '4px',
        padding: '2px 6px',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        mt: 0.25,
        mb: 0.25,
        border: (theme: Theme) => `1px solid ${theme.palette.primary.main}20`,
        boxShadow: (theme: Theme) => `0 1px 2px ${theme.palette.primary.main}10`,
        transition: 'all 0.2s ease',
        maxWidth: '120px',
        '&:hover': {
          background: (theme: Theme) => `linear-gradient(135deg, ${theme.palette.primary.main}25, ${theme.palette.primary.main}40)`,
          boxShadow: (theme: Theme) => `0 2px 4px ${theme.palette.primary.main}15`,
        }
      }}>
        <MemoryIcon sx={{ 
          fontSize: '0.65rem',
          mr: 0.25,
          color: (theme: Theme) => theme.palette.primary.main,
          opacity: 0.8
        }} />
        <Typography variant="caption" sx={{
          color: (theme: Theme) => theme.palette.primary.main,
          fontSize: '0.65rem',
          fontWeight: 500,
          textAlign: 'center',
          maxWidth: '100px',
          overflow: 'hidden',
          textOverflow: 'ellipsis',
          whiteSpace: 'nowrap',
        }}>
          {data.llm || 'databricks-llama-4-maverick'}
        </Typography>
      </Box>

      <Box sx={{ display: 'flex', gap: 1, mt: 1 }}>
        {data.allow_code_execution && (
          <Tooltip 
            title="Code Execution Enabled" 
            disableInteractive 
            placement="top"
            open={codeTooltipOpen}
            onOpen={() => setCodeTooltipOpen(true)}
            onClose={() => setCodeTooltipOpen(false)}
          >
            <div>
              <CodeIcon sx={{ fontSize: '1rem', color: '#2196f3' }} />
            </div>
          </Tooltip>
        )}
        {data.memory && (
          <Tooltip 
            title={`Memory: ${
              data.embedder_config?.provider 
                ? `${data.embedder_config.provider} embeddings` 
                : 'OpenAI embeddings (default)'
            }`}
            disableInteractive
            placement="top"
            open={memoryTooltipOpen}
            onOpen={() => setMemoryTooltipOpen(true)}
            onClose={() => setMemoryTooltipOpen(false)}
          >
            <div>
              <MemoryIcon sx={{ fontSize: '1rem', color: '#2196f3' }} />
            </div>
          </Tooltip>
        )}
      </Box>

      <Box className="action-buttons">
        <Tooltip 
          title="Edit Agent" 
          disableInteractive 
          placement="top"
          open={editTooltipOpen}
          onOpen={() => setEditTooltipOpen(true)}
          onClose={() => setEditTooltipOpen(false)}
        >
          <IconButton
            size="small"
            onClick={handleEditClick}
            sx={{
              opacity: 0.4,
              padding: '4px',
              '&:hover': {
                opacity: 1,
                backgroundColor: 'transparent',
              },
            }}
          >
            <EditIcon sx={{ fontSize: '1rem', color: '#2196f3' }} />
          </IconButton>
        </Tooltip>
        <Tooltip 
          title="Delete Agent" 
          disableInteractive 
          placement="top"
          open={deleteTooltipOpen}
          onOpen={() => setDeleteTooltipOpen(true)}
          onClose={() => setDeleteTooltipOpen(false)}
        >
          <IconButton
            size="small"
            onClick={handleDelete}
            sx={{
              opacity: 0.4,
              padding: '4px',
              '&:hover': {
                opacity: 1,
                backgroundColor: 'transparent',
              },
            }}
          >
            <DeleteIcon sx={{ fontSize: '1rem', color: '#2196f3' }} />
          </IconButton>
        </Tooltip>
      </Box>

      {isEditing && agentData && (
        <Dialog
          open={isEditing}
          onClose={() => setIsEditing(false)}
          maxWidth="md"
          fullWidth
          PaperProps={{
            sx: { 
              display: 'flex', 
              flexDirection: 'column',
              height: '85vh', 
              maxHeight: '85vh' 
            }
          }}
        >
          <DialogContent sx={{ p: 2, overflow: 'hidden', display: 'flex', flexDirection: 'column', flex: 1 }}>
            <AgentForm
              initialData={agentData}
              tools={tools}
              onCancel={() => setIsEditing(false)}
              onAgentSaved={(updatedAgent) => {
                setIsEditing(false);
                if (updatedAgent) {
                  // Direct update with the received agent data
                  handleUpdateNode(updatedAgent);
                }
              }}
            />
          </DialogContent>
        </Dialog>
      )}
    </Box>
  );
};

export default AgentNode; 