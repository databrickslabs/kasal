import React, { useCallback, useState, useEffect } from 'react';
import { Handle, Position, useReactFlow } from 'reactflow';
import { Box, Typography, Dialog, DialogContent, IconButton, Tooltip, CircularProgress } from '@mui/material';
import PersonIcon from '@mui/icons-material/Person';
import DeleteIcon from '@mui/icons-material/Delete';
import EditIcon from '@mui/icons-material/Edit';
import CodeIcon from '@mui/icons-material/Code';
import MemoryIcon from '@mui/icons-material/Memory';
import { Agent } from '../../api/AgentService';
import AgentForm from './AgentForm';
import { ToolService } from '../../api/ToolService';
import { Tool, KnowledgeSource } from '../../types/agent';
import { Theme } from '@mui/material/styles';
import { useTabDirtyState } from '../../hooks/workflow/useTabDirtyState';
import { useAgentStore } from '../../store/agent';
import { useUILayoutStore } from '../../store/uiLayout';

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
  tools?: string[];
  tool_configs?: Record<string, unknown>;  // User-specific tool configuration overrides
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
  const [tools, setTools] = useState<Tool[]>([]);

  // Local selection state
  const [isSelected, setIsSelected] = useState(false);

  // Tab dirty state management
  const { markCurrentTabDirty } = useTabDirtyState();

  // Use agent store instead of local state
  const { getAgent, updateAgent } = useAgentStore();
  const [agentData, setAgentData] = useState<Agent | null>(null);

  // Get current layout orientation
  const layoutOrientation = useUILayoutStore(state => state.layoutOrientation);

  // Load agent data using store
  useEffect(() => {
    if (data.agentId) {
      const loadAgentData = async () => {
        const agent = await getAgent(data.agentId);
        setAgentData(agent);
      };
      loadAgentData();
    }
  }, [data.agentId, getAgent]);

  useEffect(() => {
    loadTools();
  }, []);

  const loadTools = async () => {
    try {
      const toolsList = await ToolService.listEnabledTools();
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
    setNodes(nodes => nodes.filter(node => node.id !== id));
    setEdges(edges => edges.filter(edge =>
      edge.source !== id && edge.target !== id
    ));
  }, [id, setNodes, setEdges]);

  const handleEditClick = useCallback(async () => {
    try {
      // Don't manually close tooltips - let them close naturally
      document.activeElement && (document.activeElement as HTMLElement).blur();

      // Try different sources for the agent ID
      const agentIdToUse = data.agentId || data.id || data.agent_id;

      if (!agentIdToUse) {
        console.warn('Agent ID is missing in node data, using data directly:', data);
        // If there's no ID, use the data directly (might be a new unsaved agent)
        // Convert label to name for Agent type
        const agentFromData: Agent = {
          id: undefined,
          name: String(data.label || data.name || ''),
          role: String(data.role || ''),
          goal: String(data.goal || ''),
          backstory: String(data.backstory || ''),
          llm: String(data.llm || ''),
          tools: data.tools || [],
          max_iter: data.max_iter || 25,
          verbose: data.verbose || false,
          allow_delegation: data.allow_delegation || false,
          cache: data.cache || true,
          allow_code_execution: data.allow_code_execution || false,
          code_execution_mode: (data.code_execution_mode === 'unsafe' ? 'unsafe' : 'safe') as 'safe' | 'unsafe',
          memory: data.memory,
          tool_configs: data.tool_configs,
          temperature: typeof data.temperature === 'number' ? data.temperature : undefined,
          function_calling_llm: data.function_calling_llm,
          max_rpm: data.max_rpm,
          max_execution_time: data.max_execution_time,
          embedder_config: (data.embedder_config as import('../../types/agent').EmbedderConfig | undefined),
          knowledge_sources: data.knowledge_sources,
        };
        setAgentData(agentFromData);
        setIsEditing(true);
        return;
      }

      // Use store to get agent data (will fetch if not cached)
      const response = await getAgent(agentIdToUse as string);
      if (response) {
        console.log(`Got agent ${response.name} with ${response.knowledge_sources?.length || 0} knowledge sources`);
        setAgentData(response);
        setIsEditing(true);
      }
    } catch (error) {
      console.error('Failed to fetch agent data:', error);
    }
  }, [data.agentId, data.id, data.agent_id]);

  useEffect(() => {
    // Cleanup when dialog opens/closes if needed
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

      // Get current layout orientation from store
      const { layoutOrientation } = useUILayoutStore.getState();
      const sourceHandle = layoutOrientation === 'vertical' ? 'bottom' : 'right';
      const targetHandle = layoutOrientation === 'vertical' ? 'top' : 'left';

      const newEdge = {
        id: `${id}-${targetNode.id}`,
        source: id,
        target: targetNode.id,
        sourceHandle,
        targetHandle,
        type: 'default',
      };

      setEdges(edges => [...edges, newEdge]);
    }
  }, [id, getNodes, getEdges, setEdges]);

  const handleUpdateNode = useCallback(async (updatedAgent: Agent) => {
    try {
      // Update the store cache
      updateAgent(updatedAgent.id?.toString() || data.agentId, updatedAgent);

      // Update the local agentData state if it exists (for when edit dialog is open)
      setAgentData(updatedAgent);

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
              tool_configs: updatedAgent.tool_configs || {},  // Include tool_configs
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
  }, [id, setNodes, updateAgent, data.agentId]);

  // Removed problematic useEffect that was causing infinite API calls
  // Agent data is now managed by the store and fetched once on mount

  // Update agentData when node data changes (e.g., after knowledge sources are added)
  useEffect(() => {
    if (isEditing && data.knowledge_sources !== agentData?.knowledge_sources) {
      // Update agentData with new knowledge sources from node data
      setAgentData(prev => prev ? {
        ...prev,
        knowledge_sources: data.knowledge_sources || []
      } : null);
    }
  }, [data.knowledge_sources, isEditing, agentData?.knowledge_sources]);

  // Enhanced click handler: left-click opens form, right-click enables dragging
  const handleNodeClick = useCallback((event: React.MouseEvent) => {
    // Completely stop event propagation
    event.preventDefault();
    event.stopPropagation();

    // If any MUI Dialog is open, ignore canvas node clicks to prevent click-through
    const hasOpenDialog = document.querySelectorAll('.MuiDialog-root').length > 0;
    if (hasOpenDialog) {
      console.log('AgentNode click ignored because a dialog is open');
      return;
    }

    // Check if the click was on an interactive element
    const target = event.target as HTMLElement;
    const isButton = !!target.closest('button');
    const isActionButton = !!target.closest('.action-buttons');

    if (!isButton && !isActionButton) {
      if (event.button === 0) {
        // Left-click: Open agent form for editing
        console.log(`AgentNode left-click on ${id} - opening edit form`);
        handleEditClick();
      } else if (event.button === 2) {
        // Right-click: Enable dragging by selecting the node
        console.log(`AgentNode right-click on ${id} - enabling drag mode`);
        toggleSelection();
      }
    } else {
      console.log(`AgentNode click on ${id} ignored - clicked on button or action button`);
    }
  }, [id, toggleSelection, handleEditClick]);

  // Handle context menu (right-click) to prevent browser default menu
  const handleContextMenu = useCallback((event: React.MouseEvent) => {
    event.preventDefault();
    event.stopPropagation();
  }, []);

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

  // Note: Knowledge source indicators removed from AgentNode
  // Knowledge sources are now managed at task level
  // Files are stored on agents but displayed on task nodes

  return (
    <Box
      sx={{
        ...getAgentNodeStyles(),
        cursor: 'pointer'
      }}
      onClick={handleNodeClick}
      onContextMenu={handleContextMenu}
      data-agentid={data.agentId}
      data-nodeid={id}
      data-nodetype="agent"
      data-selected={isSelected ? 'true' : 'false'}
    >

      {/* Bottom handle - visible only in vertical layout */}
      <Handle
        type="source"
        position={Position.Bottom}
        id="bottom"
        style={{
          background: '#2196f3',
          width: '7px',
          height: '7px',
          opacity: layoutOrientation === 'vertical' ? 1 : 0,
          pointerEvents: layoutOrientation === 'vertical' ? 'all' : 'none'
        }}
        onDoubleClick={handleDoubleClick}
      />

      {/* Right handle - visible only in horizontal layout */}
      <Handle
        type="source"
        position={Position.Right}
        id="right"
        style={{
          background: '#2196f3',
          width: '7px',
          height: '7px',
          opacity: layoutOrientation === 'horizontal' ? 1 : 0,
          pointerEvents: layoutOrientation === 'horizontal' ? 'all' : 'none'
        }}
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


      {Boolean((data as Record<string, unknown>)?.loading) && (
        <Box
          sx={{
            position: 'absolute',
            inset: 0,
            borderRadius: '12px',
            background: (theme: { palette: { mode: string } }) => theme.palette.mode === 'light'
              ? 'linear-gradient(180deg, rgba(255,255,255,0.6), rgba(255,255,255,0.4))'
              : 'linear-gradient(180deg, rgba(0,0,0,0.3), rgba(0,0,0,0.2))',
            backdropFilter: 'blur(1px)',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            zIndex: 5,
          }}
        >
          <Box sx={{
            display: 'flex', alignItems: 'center', gap: 1,
            animation: 'bounce 1.2s ease-in-out infinite',
            '@keyframes bounce': {
              '0%': { transform: 'translateY(0)' },
              '50%': { transform: 'translateY(-3px)' },
              '100%': { transform: 'translateY(0)' },
            },
          }}>
            <CircularProgress size={16} sx={{ color: 'primary.main' }} />
            <Typography variant="caption" color="textSecondary">Creating…</Typography>
          </Box>
        </Box>
      )}

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
                  // Mark tab as dirty since agent was modified
                  markCurrentTabDirty();
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

export default React.memo(AgentNode);