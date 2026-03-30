import React, { useState, useEffect, useCallback } from 'react';
import ReactFlow, {
  Node,
  Edge,
  Background,
  Controls,
  MiniMap,
  useNodesState,
  useEdgesState,
  BackgroundVariant,
  NodeTypes,
  Position,
  MarkerType,
  ReactFlowProvider,
} from 'reactflow';
import 'reactflow/dist/style.css';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  IconButton,
  Box,
  Paper,
  Typography,
  Chip,
  CircularProgress,
  Alert,
  Button,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Slider,
  Stack,
} from '@mui/material';
import {
  Close as CloseIcon,
  Refresh as RefreshIcon,
  AccountTree as GraphIcon,
} from '@mui/icons-material';
import { apiClient } from '../../config/api/ApiConfig';

// Types for entity data
interface EntityData {
  label: string;
  type?: string;
  attributes?: Record<string, unknown>;
}

interface Entity {
  id: string;
  name: string;
  type: string;
  attributes?: Record<string, unknown>;
}

interface Relationship {
  source: string;
  target: string;
  type: string;
  label: string;
  strength?: number;
}

// Custom node component for entities
const EntityNode = ({ data }: { data: EntityData }) => {
  const getNodeColor = () => {
    switch (data.type) {
      case 'person':
        return '#4FC3F7'; // Light blue for people
      case 'organization':
        return '#81C784'; // Green for organizations
      case 'system':
        return '#FFB74D'; // Orange for systems
      case 'concept':
        return '#BA68C8'; // Purple for concepts
      default:
        return '#90A4AE'; // Grey for unknown
    }
  };

  return (
    <Paper
      sx={{
        padding: 2,
        borderRadius: 2,
        backgroundColor: getNodeColor(),
        color: 'white',
        minWidth: 150,
        maxWidth: 250,
        cursor: 'pointer',
        '&:hover': {
          transform: 'scale(1.05)',
          transition: 'transform 0.2s',
        },
      }}
      elevation={3}
    >
      <Typography variant="subtitle2" fontWeight="bold" gutterBottom>
        {data.label}
      </Typography>
      {data.type && (
        <Chip
          label={data.type}
          size="small"
          sx={{
            backgroundColor: 'rgba(255, 255, 255, 0.3)',
            color: 'white',
            fontSize: '0.7rem',
            height: 20,
          }}
        />
      )}
      {data.attributes && (
        <Box mt={1}>
          {Object.entries(data.attributes).slice(0, 3).map(([key, value]) => (
            <Typography key={key} variant="caption" display="block">
              {key}: {String(value)}
            </Typography>
          ))}
        </Box>
      )}
    </Paper>
  );
};

const nodeTypes: NodeTypes = {
  entity: EntityNode,
};

interface EntityVisualizationProps {
  open: boolean;
  onClose: () => void;
  indexName?: string;
  workspaceUrl?: string;
  endpointName?: string;
}

export const EntityVisualization: React.FC<EntityVisualizationProps> = ({
  open,
  onClose,
  indexName,
  workspaceUrl,
  endpointName,
}) => {
  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [layoutType, setLayoutType] = useState<'hierarchical' | 'force' | 'circular'>('force');
  const [nodeSpacing, setNodeSpacing] = useState(200);


  // Layout algorithms
  const applyLayout = useCallback((nodes: Node[], edges: Edge[], type: string, spacing: number) => {
    const layoutedNodes = [...nodes];
    
    switch (type) {
      case 'hierarchical': {
        // Simple hierarchical layout
        const levels = new Map<string, number>();
        const visited = new Set<string>();
        
        // Find root nodes (nodes with no incoming edges)
        const roots = layoutedNodes.filter(node => 
          !edges.some(edge => edge.target === node.id)
        );
        
        // BFS to assign levels
        const queue = roots.map(node => ({ node, level: 0 }));
        while (queue.length > 0) {
          const item = queue.shift();
          if (!item) continue;
          const { node, level } = item;
          if (!visited.has(node.id)) {
            visited.add(node.id);
            levels.set(node.id, level);
            
            // Find children
            const children = edges
              .filter(edge => edge.source === node.id)
              .map(edge => layoutedNodes.find(n => n.id === edge.target))
              .filter(Boolean);
            
            children.forEach(child => {
              if (child && !visited.has(child.id)) {
                queue.push({ node: child, level: level + 1 });
              }
            });
          }
        }
        
        // Position nodes based on levels
        const levelCounts = new Map<number, number>();
        layoutedNodes.forEach(node => {
          const level = levels.get(node.id) || 0;
          const count = levelCounts.get(level) || 0;
          node.position = {
            x: count * spacing,
            y: level * spacing * 1.5,
          };
          levelCounts.set(level, count + 1);
        });
        break;
      }
        
      case 'circular': {
        // Circular layout
        const radius = spacing * 2;
        const angleStep = (2 * Math.PI) / layoutedNodes.length;
        layoutedNodes.forEach((node, index) => {
          node.position = {
            x: radius + radius * Math.cos(index * angleStep),
            y: radius + radius * Math.sin(index * angleStep),
          };
        });
        break;
      }
        
      case 'force':
      default: {
        // Simple force-directed layout simulation
        const iterations = 50;
        const repulsionStrength = spacing * 10;
        const attractionStrength = 0.01;
        
        // Initialize random positions
        layoutedNodes.forEach(node => {
          node.position = {
            x: Math.random() * spacing * 4,
            y: Math.random() * spacing * 4,
          };
        });
        
        // Run simulation
        for (let i = 0; i < iterations; i++) {
          // Apply repulsion between all nodes
          for (let j = 0; j < layoutedNodes.length; j++) {
            for (let k = j + 1; k < layoutedNodes.length; k++) {
              const nodeA = layoutedNodes[j];
              const nodeB = layoutedNodes[k];
              const dx = nodeB.position.x - nodeA.position.x;
              const dy = nodeB.position.y - nodeA.position.y;
              const distance = Math.sqrt(dx * dx + dy * dy) || 1;
              const force = repulsionStrength / (distance * distance);
              
              nodeA.position.x -= (dx / distance) * force;
              nodeA.position.y -= (dy / distance) * force;
              nodeB.position.x += (dx / distance) * force;
              nodeB.position.y += (dy / distance) * force;
            }
          }
          
          // Apply attraction along edges
          edges.forEach(edge => {
            const source = layoutedNodes.find(n => n.id === edge.source);
            const target = layoutedNodes.find(n => n.id === edge.target);
            if (source && target) {
              const dx = target.position.x - source.position.x;
              const dy = target.position.y - source.position.y;
              const distance = Math.sqrt(dx * dx + dy * dy) || 1;
              const force = distance * attractionStrength;
              
              source.position.x += dx * force;
              source.position.y += dy * force;
              target.position.x -= dx * force;
              target.position.y -= dy * force;
            }
          });
        }
        break;
      }
    }
    
    return layoutedNodes;
  }, []);

  // Fetch entity data
  const fetchEntityData = useCallback(async () => {
    setLoading(true);
    setError(null);
    
    try {
      // Only fetch if we have the necessary information
      if (!indexName || !workspaceUrl || !endpointName) {
        setError('Missing configuration. Please ensure your memory backend is properly configured.');
        setLoading(false);
        return;
      }
      
      const response = await apiClient.get('/memory-backend/databricks/entity-data', {
        params: {
          index_name: indexName,
          workspace_url: workspaceUrl,
          endpoint_name: endpointName,
          limit: 100
        }
      });
      
      if (!response.data.success) {
        setError(response.data.message || 'Failed to fetch entity data from the index');
        setLoading(false);
        return;
      }
      
      const entities: Entity[] = response.data.entities || [];
      const relationships: Relationship[] = response.data.relationships || [];
      
      if (entities.length === 0) {
        setError('No entities found in the index. Entity memory will be populated as agents interact.');
        setNodes([]);
        setEdges([]);
        setLoading(false);
        return;
      }
      
      // Convert entities to nodes
      const newNodes: Node[] = entities.map((entity: Entity) => ({
        id: entity.id,
        type: 'entity',
        position: { x: 0, y: 0 }, // Will be set by layout
        data: {
          label: entity.name,
          type: entity.type,
          attributes: entity.attributes,
        },
        sourcePosition: Position.Right,
        targetPosition: Position.Left,
      }));
      
      // Convert relationships to edges
      const newEdges: Edge[] = relationships.map((rel: Relationship, index: number) => ({
        id: `edge-${index}`,
        source: rel.source,
        target: rel.target,
        label: rel.label,
        type: 'smoothstep',
        animated: true,
        markerEnd: {
          type: MarkerType.ArrowClosed,
          width: 20,
          height: 20,
        },
        style: {
          strokeWidth: 2,
          stroke: '#64B5F6',
        },
        labelStyle: {
          fontSize: 12,
          fontWeight: 500,
        },
      }));
      
      // Apply layout
      const layoutedNodes = applyLayout(newNodes, newEdges, layoutType, nodeSpacing);
      
      setNodes(layoutedNodes);
      setEdges(newEdges);
    } catch (err) {
      console.error('Error fetching entity data:', err);
      const errorMessage = err instanceof Error ? err.message : 'Failed to load entity data';
      setError(errorMessage);
      setNodes([]);
      setEdges([]);
    } finally {
      setLoading(false);
    }
  }, [indexName, workspaceUrl, endpointName, layoutType, nodeSpacing, applyLayout, setNodes, setEdges]);

  // Load data when dialog opens
  useEffect(() => {
    if (open) {
      fetchEntityData();
    }
  }, [open, fetchEntityData]);

  // Re-apply layout when settings change
  const handleLayoutChange = (newLayout: 'hierarchical' | 'force' | 'circular') => {
    setLayoutType(newLayout);
    const layoutedNodes = applyLayout(nodes, edges, newLayout, nodeSpacing);
    setNodes(layoutedNodes);
  };

  const handleSpacingChange = (_event: Event, newValue: number | number[]) => {
    const spacing = newValue as number;
    setNodeSpacing(spacing);
    const layoutedNodes = applyLayout(nodes, edges, layoutType, spacing);
    setNodes(layoutedNodes);
  };

  return (
    <Dialog
      open={open}
      onClose={onClose}
      maxWidth="xl"
      fullWidth
      PaperProps={{
        sx: {
          height: '90vh',
          maxHeight: '90vh',
        },
      }}
    >
      <DialogTitle>
        <Box display="flex" alignItems="center" justifyContent="space-between">
          <Box display="flex" alignItems="center" gap={1}>
            <GraphIcon />
            <Typography variant="h6">Entity Memory Visualization</Typography>
          </Box>
          <IconButton onClick={onClose} size="small">
            <CloseIcon />
          </IconButton>
        </Box>
      </DialogTitle>
      
      <DialogContent sx={{ p: 0, display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
        {/* Controls Bar */}
        <Paper elevation={1} sx={{ p: 2, borderRadius: 0 }}>
          <Stack direction="row" spacing={2} alignItems="center">
            <FormControl size="small" sx={{ minWidth: 150 }}>
              <InputLabel>Layout</InputLabel>
              <Select
                value={layoutType}
                label="Layout"
                onChange={(e) => handleLayoutChange(e.target.value as any)}
              >
                <MenuItem value="force">Force-Directed</MenuItem>
                <MenuItem value="hierarchical">Hierarchical</MenuItem>
                <MenuItem value="circular">Circular</MenuItem>
              </Select>
            </FormControl>
            
            <Box sx={{ width: 200 }}>
              <Typography variant="caption">Node Spacing</Typography>
              <Slider
                value={nodeSpacing}
                onChange={handleSpacingChange}
                min={100}
                max={400}
                step={50}
                marks
                size="small"
              />
            </Box>
            
            <Button
              startIcon={<RefreshIcon />}
              onClick={fetchEntityData}
              disabled={loading}
              variant="outlined"
              size="small"
            >
              Refresh
            </Button>
            
            {indexName && (
              <Chip
                label={`Index: ${indexName}`}
                size="small"
                color="primary"
                variant="outlined"
              />
            )}
          </Stack>
        </Paper>
        
        {/* Graph Container */}
        <Box sx={{ flex: 1, position: 'relative' }}>
          {loading && (
            <Box
              sx={{
                position: 'absolute',
                top: '50%',
                left: '50%',
                transform: 'translate(-50%, -50%)',
                zIndex: 10,
              }}
            >
              <CircularProgress />
            </Box>
          )}
          
          {error && (
            <Alert severity="info" sx={{ m: 2 }}>
              {error}
            </Alert>
          )}
          
          <ReactFlowProvider>
            <ReactFlow
              nodes={nodes}
              edges={edges}
              onNodesChange={onNodesChange}
              onEdgesChange={onEdgesChange}
              nodeTypes={nodeTypes}
              fitView
              attributionPosition="bottom-left"
            >
              <Background variant={BackgroundVariant.Dots} />
              <Controls />
              <MiniMap
                nodeStrokeColor={(node) => {
                  switch (node.data?.type) {
                    case 'person': return '#4FC3F7';
                    case 'organization': return '#81C784';
                    case 'system': return '#FFB74D';
                    case 'concept': return '#BA68C8';
                    default: return '#90A4AE';
                  }
                }}
                nodeColor={(node) => {
                  switch (node.data?.type) {
                    case 'person': return '#4FC3F7';
                    case 'organization': return '#81C784';
                    case 'system': return '#FFB74D';
                    case 'concept': return '#BA68C8';
                    default: return '#90A4AE';
                  }
                }}
                nodeBorderRadius={2}
              />
            </ReactFlow>
          </ReactFlowProvider>
        </Box>
      </DialogContent>
    </Dialog>
  );
};

export default EntityVisualization;