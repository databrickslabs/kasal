import React, { useCallback, useRef, useState, memo, useEffect, useLayoutEffect } from 'react';
import ReactFlow, {
  Background,
  Node,
  Edge,
  NodeChange,
  EdgeChange,
  Connection,
  OnSelectionChangeParams,
  ReactFlowInstance,
  ConnectionMode,
  BackgroundVariant,
  getConnectedEdges
} from 'reactflow';
import 'reactflow/dist/style.css';
import {
  Box,
  Snackbar,
  Alert,
  Paper,
  Typography,
  List,
  ListItem,
  ListItemButton,
  ListItemText,
  IconButton,
  Tooltip,
  Divider,
  CircularProgress
} from '@mui/material';
import AddIcon from '@mui/icons-material/Add';
import RefreshIcon from '@mui/icons-material/Refresh';
import { useThemeManager } from '../../hooks/workflow/useThemeManager';
import FlowCanvasControls from './FlowCanvasControls';
import useShortcuts from '../../hooks/global/useShortcuts';
import { CrewService } from '../../api/CrewService';
import { CrewResponse } from '../../types/crews';
import EdgeConfigDialog, { EdgeConfig } from '../Flow/EdgeConfigDialog';
import { useUILayoutStore } from '../../store/uiLayout';

// Node types
import { CrewNode } from '../Flow';

// Edge types
import AnimatedEdge from '../Common/AnimatedEdge';
import CrewEdge from '../Flow/CrewEdge';

// Node and edge types configuration
const nodeTypes = {
  crewNode: CrewNode
};

const edgeTypes = {
  default: AnimatedEdge,
  crewEdge: CrewEdge
};

interface FlowCanvasProps {
  nodes: Node[];
  edges: Edge[];
  onNodesChange: (changes: NodeChange[]) => void;
  onEdgesChange: (changes: EdgeChange[]) => void;
  onConnect: (connection: Connection) => void;
  onSelectionChange?: (params: OnSelectionChangeParams) => void;
  onPaneContextMenu?: (event: React.MouseEvent) => void;
  onInit?: (instance: ReactFlowInstance) => void;
  showRunHistory?: boolean;
  executionHistoryHeight?: number;
}

// Global error handler for ResizeObserver errors
if (typeof window !== 'undefined') {
  const errorHandler = (event: ErrorEvent) => {
    if (
      event.message &&
      (event.message.includes('ResizeObserver loop') ||
       event.message.includes('ResizeObserver Loop'))
    ) {
      event.stopImmediatePropagation();
      event.preventDefault();
    }
  };

  window.addEventListener('error', errorHandler);
  window.addEventListener('unhandledrejection', (event) => {
    if (event.reason && event.reason.message &&
        (event.reason.message.includes('ResizeObserver loop') ||
         event.reason.message.includes('ResizeObserver Loop'))) {
      event.preventDefault();
    }
  });
}

const FlowCanvas: React.FC<FlowCanvasProps> = ({
  nodes,
  edges,
  onNodesChange,
  onEdgesChange,
  onConnect,
  onSelectionChange,
  onPaneContextMenu,
  onInit,
  showRunHistory = false,
  executionHistoryHeight = 0
}) => {
  const { isDarkMode } = useThemeManager();
  const [controlsVisible, _setControlsVisible] = useState(false);
  const reactFlowInstanceRef = useRef<ReactFlowInstance | null>(null);
  const containerRef = useRef<HTMLDivElement>(null);

  // Track render state to prevent unnecessary re-renders
  const [isRendering, setIsRendering] = useState(true);
  const [isStable, setIsStable] = useState(false);

  // Add error boundary for catching ReactFlow errors
  const [hasError, setHasError] = useState(false);

  // Add state for notifications
  const [showNotification, setShowNotification] = useState(false);
  const [notificationMessage, setNotificationMessage] = useState('');

  // Crew palette state
  const [crews, setCrews] = useState<CrewResponse[]>([]);
  const [loadingCrews, setLoadingCrews] = useState(false);
  const [paletteVisible, setPaletteVisible] = useState(true);

  // Edge configuration dialog state
  const [isEdgeDialogOpen, setIsEdgeDialogOpen] = useState(false);
  const [selectedEdge, setSelectedEdge] = useState<Edge | null>(null);
  const [aggregatedSourceTasks, setAggregatedSourceTasks] = useState<Array<{ crewName: string; tasks: any[] }>>([]);

  // Handle edge click for configuration
  const handleEdgeClick = useCallback((event: React.MouseEvent, edge: Edge) => {
    // Check if this edge is part of a merge group
    const mergeGroupId = edge.data?.mergeGroupId;

    // Aggregate tasks from all source nodes
    const aggregated: Array<{ crewName: string; tasks: any[] }> = [];
    const processedSources = new Set<string>(); // Track processed sources to avoid duplicates

    if (mergeGroupId) {
      // For merged edges, find ALL edges in the same merge group
      const mergeGroupEdges = edges.filter(e => e.data?.mergeGroupId === mergeGroupId);

      mergeGroupEdges.forEach(e => {
        // Each edge has a single source
        if (!processedSources.has(e.source)) {
          processedSources.add(e.source);
          const sourceNode = nodes.find(n => n.id === e.source);
          if (sourceNode && sourceNode.data?.allTasks?.length > 0) {
            aggregated.push({
              crewName: sourceNode.data?.label || sourceNode.data?.crewName || 'Unknown Crew',
              tasks: sourceNode.data.allTasks || []
            });
          }
        }
      });
    } else {
      // For non-merged edges, just use the single source
      const sourceNode = nodes.find(n => n.id === edge.source);
      if (sourceNode && sourceNode.data?.allTasks?.length > 0) {
        aggregated.push({
          crewName: sourceNode.data?.label || sourceNode.data?.crewName || 'Unknown Crew',
          tasks: sourceNode.data.allTasks || []
        });
      }
    }

    setSelectedEdge(edge);
    setAggregatedSourceTasks(aggregated);
    setIsEdgeDialogOpen(true);
  }, [edges, nodes]);

  // Load crews on mount
  useEffect(() => {
    loadCrews();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const loadCrews = async () => {
    setLoadingCrews(true);
    try {
      const crewsData = await CrewService.getCrews();
      setCrews(crewsData);
    } catch (error) {
      console.error('Error loading crews:', error);
      showTemporaryNotification('Failed to load crews');
    } finally {
      setLoadingCrews(false);
    }
  };

  // Add function to show notification
  const showTemporaryNotification = useCallback((message: string) => {
    setNotificationMessage(message);
    setShowNotification(true);

    // Auto-hide after 3 seconds
    setTimeout(() => {
      setShowNotification(false);
    }, 3000);
  }, []);

  // Handle edge configuration save
  const handleEdgeSave = useCallback((edgeId: string, config: EdgeConfig) => {
    // Find the edge to update
    const edgeIndex = edges.findIndex(e => e.id === edgeId);
    if (edgeIndex !== -1) {
      const currentEdge = edges[edgeIndex];

      // Check if this edge is part of a merge group
      const mergeGroupId = currentEdge.data?.mergeGroupId;

      if (mergeGroupId) {
        // Update ALL edges in the same merge group
        const changes: EdgeChange[] = [];

        edges.forEach((edge) => {
          if (edge.data?.mergeGroupId === mergeGroupId) {
            // Remove old edge
            changes.push({ id: edge.id, type: 'remove' });

            // Add updated edge with new config
            const updatedEdge: Edge = {
              ...edge,
              data: { ...edge.data, ...config },
              type: 'crewEdge'
            };
            changes.push({ item: updatedEdge, type: 'add' });
          }
        });

        onEdgesChange(changes);
        showTemporaryNotification(`Merged edges configured: ${config.logicType}`);
      } else {
        // Single edge - update only this one
        const updatedEdge: Edge = {
          ...currentEdge,
          data: { ...currentEdge.data, ...config },
          type: 'crewEdge' // Ensure it uses our custom edge type
        };

        // Update edge by removing old and adding updated version
        const changes: EdgeChange[] = [
          { id: edgeId, type: 'remove' },
          { item: updatedEdge, type: 'add' }
        ];

        onEdgesChange(changes);
        showTemporaryNotification(`Edge configured: ${config.logicType}`);
      }
    }
  }, [edges, onEdgesChange, showTemporaryNotification]);

  // Handle adding a crew to the canvas
  const handleAddCrewToCanvas = useCallback(async (crew: CrewResponse) => {
    try {
      // Fetch full crew details to get nodes/tasks
      const fullCrew = await CrewService.getCrew(crew.id);

      // Get current layout orientation
      const { layoutOrientation } = useUILayoutStore.getState();

      // Get existing crew nodes to calculate next position
      const existingCrewNodes = nodes.filter(n => n.type === 'crewNode');

      // Calculate order for new node
      const maxOrder = existingCrewNodes.reduce((max, node) => {
        const nodeOrder = node.data?.order ?? 0;
        return Math.max(max, nodeOrder);
      }, 0);
      const newOrder = maxOrder + 1;

      // Calculate position based on layout orientation
      let newPosition: { x: number; y: number };

      if (layoutOrientation === 'vertical') {
        // Vertical layout: add node below existing nodes
        if (existingCrewNodes.length === 0) {
          // First node
          newPosition = { x: 150, y: 100 };
        } else {
          // Find the bottommost node
          const bottomNode = existingCrewNodes.reduce((lowest, node) =>
            node.position.y > lowest.position.y ? node : lowest
          );
          // Place new node below with spacing
          newPosition = {
            x: bottomNode.position.x, // Same X as previous node
            y: bottomNode.position.y + 160 // 80 (node height) + 80 (spacing)
          };
        }
      } else {
        // Horizontal layout: add node to the right of existing nodes
        if (existingCrewNodes.length === 0) {
          // First node
          newPosition = { x: 100, y: 150 };
        } else {
          // Find the rightmost node
          const rightmostNode = existingCrewNodes.reduce((rightmost, node) =>
            node.position.x > rightmost.position.x ? node : rightmost
          );
          // Place new node to the right with spacing
          newPosition = {
            x: rightmostNode.position.x + 220, // 140 (node width) + 80 (spacing)
            y: rightmostNode.position.y // Same Y as previous node
          };
        }
      }

      // Map crew tasks to simple task objects for the dialog
      // Extract from nodes array
      const allTasks: Array<{id: string, name: string, description?: string}> = [];

      if (fullCrew.nodes && Array.isArray(fullCrew.nodes)) {
        const taskNodes = fullCrew.nodes.filter((node: any) =>
          node.type === 'taskNode' && node.data
        );

        taskNodes.forEach((taskNode: any) => {
          const taskId = taskNode.data.taskId || taskNode.id;
          const taskName = taskNode.data.label || taskNode.data.name || 'Unnamed Task';
          allTasks.push({
            id: String(taskId),
            name: String(taskName),
            description: taskNode.data.description || taskNode.data.label
          });
        });
      }

      // Also check tasks array if present
      if (fullCrew.tasks && Array.isArray(fullCrew.tasks)) {
        fullCrew.tasks.forEach((taskNode: any) => {
          const taskId = taskNode.data?.taskId || taskNode.id;
          const taskName = taskNode.data?.label || taskNode.data?.name || 'Unnamed Task';
          // Avoid duplicates
          if (!allTasks.find(t => t.id === String(taskId))) {
            allTasks.push({
              id: String(taskId),
              name: String(taskName),
              description: taskNode.data?.description || taskNode.data?.label
            });
          }
        });
      }

      // Create new crew node with proper data structure
      const newNode: Node = {
        id: `crew-${crew.id}-${Date.now()}`,
        type: 'crewNode',
        position: newPosition,
        data: {
          id: `crew-${crew.id}`,
          label: crew.name,
          crewName: crew.name,
          crewId: crew.id,
          selectedTasks: [],
          allTasks: allTasks,  // Include all tasks from the crew
          order: newOrder  // Track creation order for maintaining sequence during layout toggles
        }
      };

      // Add node to canvas
      onNodesChange([{
        type: 'add',
        item: newNode
      }]);

      showTemporaryNotification(`Added crew: ${crew.name} (${allTasks.length} tasks)`);
    } catch (error) {
      console.error('Error adding crew to canvas:', error);
      showTemporaryNotification(`Failed to add crew: ${crew.name}`);
    }
  }, [nodes, onNodesChange, showTemporaryNotification]);

  // Define handleClearCanvas before it's used
  const handleClearCanvas = useCallback(() => {
    // Get all flow node IDs
    const flowNodeIds = nodes
      .filter(node => node.type === 'crewNode')
      .map(node => node.id);

    // Create removal changes for all flow nodes
    if (flowNodeIds.length > 0) {
      const nodesToRemove = flowNodeIds.map(id => ({
        id,
        type: 'remove' as const,
      }));

      // Apply the changes to remove all flow nodes
      onNodesChange(nodesToRemove);
    }

    // Get all edges connected to flow nodes
    const edgesToRemove = edges
      .filter(edge =>
        flowNodeIds.includes(edge.source) ||
        flowNodeIds.includes(edge.target)
      )
      .map(edge => ({
        id: edge.id,
        type: 'remove' as const,
      }));

    // Apply the changes to remove all related edges
    if (edgesToRemove.length > 0) {
      onEdgesChange(edgesToRemove);
    }

    showTemporaryNotification(`Canvas cleared: removed ${flowNodeIds.length} nodes and ${edgesToRemove.length} edges`);
  }, [nodes, edges, onNodesChange, onEdgesChange, showTemporaryNotification]);

  // Handle shortcut actions
  const handleDeleteSelected = useCallback((selectedNodes: Node[], selectedEdges: Edge[]) => {
    // First, remove the selected nodes
    if (selectedNodes.length > 0) {
      const nodesToRemove = selectedNodes.map(node => ({
        id: node.id,
        type: 'remove' as const,
      }));

      onNodesChange(nodesToRemove);
    }

    // Find all edges connected to the nodes being deleted (including orphaned edges)
    const connectedEdges = selectedNodes.length > 0 ? getConnectedEdges(selectedNodes, edges) : [];

    // Combine explicitly selected edges with edges connected to deleted nodes
    const allEdgesToDelete = new Set([
      ...selectedEdges.map(edge => edge.id),
      ...connectedEdges.map(edge => edge.id)
    ]);

    // Remove all edges that need to be deleted
    if (allEdgesToDelete.size > 0) {
      const edgesToRemove = Array.from(allEdgesToDelete).map(edgeId => ({
        id: edgeId,
        type: 'remove' as const,
      }));

      onEdgesChange(edgesToRemove);
    }

    // Show success notification if something was deleted
    if (selectedNodes.length > 0 || allEdgesToDelete.size > 0) {
      showTemporaryNotification(`Deleted ${selectedNodes.length} nodes and ${allEdgesToDelete.size} edges`);
    }
  }, [onNodesChange, onEdgesChange, showTemporaryNotification, edges]);

  // Initialize shortcuts
  const { shortcuts: _shortcuts } = useShortcuts({
    flowInstance: reactFlowInstanceRef.current,
    onDeleteSelected: handleDeleteSelected,
    onClearCanvas: handleClearCanvas,
    onFitView: () => {
      if (reactFlowInstanceRef.current) {
        reactFlowInstanceRef.current.fitView({ padding: 0.2 });
        showTemporaryNotification('Fit view to all nodes');
      }
    },
    onOpenFlowDialog: () => {
      setPaletteVisible(!paletteVisible);
      showTemporaryNotification(`Crew palette ${!paletteVisible ? 'shown' : 'hidden'}`);
    },
    disabled: isRendering || hasError,
    instanceId: 'flow-canvas',
    priority: 5
  });

  // Filter to only show flow nodes
  const flowNodes = React.useMemo(() => {
    try {
      return nodes.filter(node => {
        if (!node || typeof node !== 'object') return false;

        const nodeName = node.data?.label?.toLowerCase() || '';
        const nodeType = node.type?.toLowerCase() || '';

        return (
          nodeName.includes('flow') ||
          nodeType.includes('flow') ||
          nodeType === 'crewnode' ||
          (node.data && node.data.flowConfig)
        );
      });
    } catch (error) {
      console.error('Error filtering flow nodes:', error);
      return [];
    }
  }, [nodes]);

  // Auto-fit view only on initial load or significant changes
  const prevNodeCountRef = useRef(flowNodes.length);
  const hasInitialFitRef = useRef(false);

  useEffect(() => {
    const prevCount = prevNodeCountRef.current;
    const currentCount = flowNodes.length;

    if (!isRendering && reactFlowInstanceRef.current && currentCount > 0) {
      const shouldFitView = !hasInitialFitRef.current || Math.abs(currentCount - prevCount) > 0;

      if (shouldFitView) {
        if (!hasInitialFitRef.current) {
          hasInitialFitRef.current = true;
        }

        prevNodeCountRef.current = currentCount;

        const fitViewTimer = setTimeout(() => {
          if (reactFlowInstanceRef.current) {
            window.requestAnimationFrame(() => {
              reactFlowInstanceRef.current?.fitView({
                padding: 0.2,
                includeHiddenNodes: false,
                duration: 800
              });
            });
          }
        }, 300);

        return () => clearTimeout(fitViewTimer);
      }
    }
  }, [flowNodes.length, isRendering]);

  // Ensure nodes have stable dimensions before rendering
  const nodeWithDimensions = React.useMemo(() => {
    return flowNodes.map(node => {
      if (!node.style || (!node.style.width && !node.style.height)) {
        return {
          ...node,
          style: {
            ...node.style,
            width: node.style?.width || 180,
            height: node.style?.height || 80
          }
        };
      }
      return node;
    });
  }, [flowNodes]);

  // Use layout effect to stabilize initial render with staggered approach
  useLayoutEffect(() => {
    setIsRendering(true);

    const initialTimer = setTimeout(() => {
      if (!isStable) {
        setIsStable(true);
      }
    }, 0);

    const renderTimer = setTimeout(() => {
      setIsRendering(false);
    }, 100);

    return () => {
      clearTimeout(initialTimer);
      clearTimeout(renderTimer);
    };
  }, [isStable, flowNodes.length]);

  // Reset error state when nodes or edges change
  useEffect(() => {
    if (hasError) {
      setHasError(false);
    }
  }, [nodes, edges, hasError]);

  const handleInit = useCallback((instance: ReactFlowInstance) => {
    reactFlowInstanceRef.current = instance;

    // Expose to window for debugging
    if (typeof window !== 'undefined') {
      (window as any).rfInstance = instance;
    }

    setTimeout(() => {
      if (instance && flowNodes.length > 0) {
        try {
          instance.fitView({ padding: 0.2, includeHiddenNodes: false });
        } catch (error) {
          console.warn('FlowCanvas fitView error:', error);
        }
      }
    }, 200);

    if (onInit) {
      onInit(instance);
    }
  }, [onInit, flowNodes]);

  // Filter edges that connect flow nodes
  const flowEdges = React.useMemo(() => {
    try {
      const flowNodeIds = new Set(flowNodes.map(node => node.id));

      return edges.filter(edge => {
        if (!edge || typeof edge !== 'object' || !edge.source || !edge.target) {
          return false;
        }

        // For merged edges, check if ALL sources exist in flowNodeIds
        const isMergedEdge = edge.data?.isMerged && edge.data?.sources;
        if (isMergedEdge) {
          const sources = edge.data.sources as string[];
          const allSourcesExist = sources.every(sourceId => flowNodeIds.has(sourceId));
          return allSourcesExist && flowNodeIds.has(edge.target);
        }

        // For normal edges, check source and target
        return flowNodeIds.has(edge.source) && flowNodeIds.has(edge.target);
      });
    } catch (error) {
      console.error('Error filtering flow edges:', error);
      return [];
    }
  }, [edges, flowNodes]);

  // Stable callback for node changes to prevent unnecessary renders
  const handleNodesChange = useCallback((changes: NodeChange[]) => {
    const flowNodeIds = new Set(flowNodes.map(node => node.id));

    const filteredChanges = changes.filter(change => {
      switch (change.type) {
        case 'position':
        case 'dimensions':
        case 'remove':
        case 'select':
          return flowNodeIds.has(change.id);
        case 'add':
          return change.item && flowNodeIds.has(change.item.id);
        default:
          return true;
      }
    });

    if (filteredChanges.length > 0) {
      onNodesChange(filteredChanges);
    }
  }, [flowNodes, onNodesChange]);

  // Add effect to listen for notification events
  useEffect(() => {
    const handleNotification = (event: CustomEvent<{ message: string }>) => {
      if (event.detail && event.detail.message) {
        showTemporaryNotification(event.detail.message);
      }
    };

    window.addEventListener('showNotification', handleNotification as EventListener);

    return () => {
      window.removeEventListener('showNotification', handleNotification as EventListener);
    };
  }, [showTemporaryNotification]);

  return (
    <Box
      ref={containerRef}
      sx={{
        width: '100%',
        height: '100%',
        minHeight: 0,
        display: 'flex',
        flexDirection: 'row',
        position: 'relative',
        backgroundColor: isDarkMode ? '#1a1a1a' : '#f5f5f5',
      }}
    >
      {/* Crew Palette - Left Sidebar */}
      {paletteVisible && (
        <Paper
          elevation={3}
          sx={{
            width: '250px',
            height: showRunHistory ? `calc(100% - ${executionHistoryHeight}px)` : '100%',
            borderRight: `1px solid ${isDarkMode ? 'rgba(255,255,255,0.1)' : 'rgba(0,0,0,0.1)'}`,
            backgroundColor: isDarkMode ? '#2a2a2a' : '#ffffff',
            display: 'flex',
            flexDirection: 'column',
            overflow: 'hidden'
          }}
        >
          {/* Header */}
          <Box sx={{ p: 2, borderBottom: `1px solid ${isDarkMode ? 'rgba(255,255,255,0.1)' : 'rgba(0,0,0,0.1)'}` }}>
            <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 1 }}>
              <Typography variant="h6" sx={{ fontSize: '1rem', fontWeight: 600 }}>
                Available Crews
              </Typography>
              <Tooltip title="Refresh crews">
                <IconButton size="small" onClick={loadCrews} disabled={loadingCrews}>
                  <RefreshIcon fontSize="small" />
                </IconButton>
              </Tooltip>
            </Box>
            <Typography variant="caption" color="text.secondary">
              Click to add to canvas
            </Typography>
          </Box>

          {/* Crew List */}
          <Box
            sx={{
              flex: 1,
              minHeight: 0,
              overflowY: 'auto',
              overflowX: 'hidden'
            }}
            onWheel={(e) => {
              e.stopPropagation();
            }}
            onMouseDown={(e) => {
              e.stopPropagation();
            }}
          >
            {loadingCrews ? (
              <Box sx={{ display: 'flex', justifyContent: 'center', p: 3 }}>
                <CircularProgress size={24} />
              </Box>
            ) : crews.length === 0 ? (
              <Box sx={{ p: 2 }}>
                <Typography variant="body2" color="text.secondary" align="center">
                  No crews available
                </Typography>
              </Box>
            ) : (
              <List dense sx={{ p: 0 }}>
                {crews.map((crew, index) => (
                  <React.Fragment key={crew.id}>
                    <ListItem disablePadding>
                      <ListItemButton onClick={() => handleAddCrewToCanvas(crew)}>
                        <IconButton size="small" sx={{ mr: 1, pointerEvents: 'none' }}>
                          <AddIcon fontSize="small" />
                        </IconButton>
                        <ListItemText
                          primary={crew.name}
                          primaryTypographyProps={{ fontSize: '0.875rem' }}
                        />
                      </ListItemButton>
                    </ListItem>
                    {index < crews.length - 1 && <Divider />}
                  </React.Fragment>
                ))}
              </List>
            )}
          </Box>
        </Paper>
      )}

      {/* Canvas Area */}
      <Box sx={{ flex: 1, position: 'relative' }}>
        {isRendering ? (
          <Box sx={{
            display: 'flex',
            justifyContent: 'center',
            alignItems: 'center',
            height: '100%',
            color: 'text.secondary',
            fontSize: '0.875rem'
          }}>
            {hasError ? 'Error rendering canvas' : 'Loading flow canvas...'}
          </Box>
        ) : (
          <ReactFlow
            nodes={nodeWithDimensions}
            edges={flowEdges}
            onNodesChange={handleNodesChange}
            onEdgesChange={onEdgesChange}
            onConnect={onConnect}
            onEdgeClick={handleEdgeClick}
            onInit={handleInit}
            nodeTypes={nodeTypes}
            edgeTypes={edgeTypes}
            onSelectionChange={onSelectionChange}
            onPaneContextMenu={onPaneContextMenu}
            proOptions={{ hideAttribution: true }}
            connectionMode={ConnectionMode.Loose}
            snapToGrid={true}
            minZoom={0.1}
            maxZoom={4}
            style={{ background: isDarkMode ? '#1a1a1a' : '#f5f5f5' }}
          >
            <Background
              color={isDarkMode ? '#333' : '#aaa'}
              gap={16}
              size={1}
              variant={BackgroundVariant.Dots}
            />
            {controlsVisible && <FlowCanvasControls onClearCanvas={handleClearCanvas} />}
          </ReactFlow>
        )}

        {/* Shortcuts info */}
        <Box
          sx={{
            position: 'absolute',
            bottom: 10,
            left: 10,
            fontSize: '0.75rem',
            color: 'text.secondary',
            opacity: 0.7,
            pointerEvents: 'none',
          }}
        >
          Tip: Press &quot;del&quot; to delete selected items, &quot;lf&quot; to toggle crew palette
        </Box>

        {/* Notification */}
        <Snackbar
          open={showNotification}
          autoHideDuration={3000}
          onClose={() => setShowNotification(false)}
          anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
        >
          <Alert severity="info" sx={{ width: '100%' }}>
            {notificationMessage}
          </Alert>
        </Snackbar>

        {/* Edge Configuration Dialog */}
        <EdgeConfigDialog
          open={isEdgeDialogOpen}
          onClose={() => setIsEdgeDialogOpen(false)}
          edge={selectedEdge}
          nodes={nodes}
          onSave={handleEdgeSave}
          aggregatedSourceTasks={aggregatedSourceTasks}
        />
      </Box>
    </Box>
  );
};

export default memo(FlowCanvas);
