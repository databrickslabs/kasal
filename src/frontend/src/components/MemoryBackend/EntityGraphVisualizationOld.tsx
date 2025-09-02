import React, { useEffect, useState, useCallback, useRef } from 'react';
import ForceGraph2D from 'force-graph';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  IconButton,
  Box,
  Paper,
  Typography,
  Alert,
  CircularProgress,
  Button,
  Tooltip,
  Card,
  CardContent,
  Chip,
  Slider,
  Stack,
  FormControlLabel,
  Switch,
  Divider,
  TextField,
  Autocomplete,
} from '@mui/material';
import {
  Close as CloseIcon,
  Refresh as RefreshIcon,
  AccountTree as GraphIcon,
  AccountTree,
  ZoomIn as ZoomInIcon,
  ZoomOut as ZoomOutIcon,
  CenterFocusStrong as CenterIcon,
} from '@mui/icons-material';
import { apiClient } from '../../config/api/ApiConfig';

// Types for entity data
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

// Graph data structure for react-force-graph
interface GraphNode {
  id: string;
  name: string;
  type: string;
  attributes: Record<string, unknown>;
  color: string;
  size: number;
  val: number; // Node value (affects size)
  x?: number; // Node x position (set by force simulation)
  y?: number; // Node y position (set by force simulation)
}

interface GraphLink {
  source: string;
  target: string;
  type: string;
  label: string;
  color: string;
  width: number;
}

interface GraphData {
  nodes: GraphNode[];
  links: GraphLink[];
}

interface EntityGraphVisualizationProps {
  open: boolean;
  onClose: () => void;
  indexName?: string;
  workspaceUrl?: string;
  endpointName?: string;
}

const EntityGraphVisualization: React.FC<EntityGraphVisualizationProps> = ({
  open,
  onClose,
  indexName,
  workspaceUrl,
  endpointName,
}) => {
  const containerRef = useRef<HTMLDivElement>(null);
  const forceGraphRef = useRef<any>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [graphData, setGraphData] = useState<GraphData>({ nodes: [], links: [] });
  const [selectedNode, setSelectedNode] = useState<GraphNode | null>(null);
  const [forceStrength, setForceStrength] = useState(-600);
  const [linkDistance, setLinkDistance] = useState(250);
  const [showInferredNodes, setShowInferredNodes] = useState(true);
  const [deduplicateNodes, setDeduplicateNodes] = useState(false);
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  const [highlightedNodes, setHighlightedNodes] = useState<Set<string>>(new Set());
  const [focusedNodeId, setFocusedNodeId] = useState<string | null>(null);
  
  // Color scheme for different entity types (Neo4j-style colors)
  const getEntityColor = (type: string): string => {
    const colors: Record<string, string> = {
      'person': '#68CCE5',      // Neo4j light blue
      'organization': '#94D82D', // Neo4j green
      'system': '#FCC940',       // Neo4j yellow
      'concept': '#F25A29',      // Neo4j orange
      'location': '#AD4BAC',     // Neo4j purple
      'event': '#D62728',        // Neo4j red
      'conference': '#E91E63',   // Pink
      'meetup': '#FF5722',       // Deep orange
      'document': '#8FBC8F',     // Light green
      'project': '#FFB366',      // Light orange
      'technology': '#00BCD4',   // Cyan
      'tool': '#9C27B0',         // Purple
      'framework': '#3F51B5',    // Indigo
      'unknown': '#C5C5C5',      // Neo4j grey
    };
    return colors[type.toLowerCase()] || colors['unknown'];
  };

  // Color scheme for relationships
  const getRelationshipColor = (type: string): string => {
    const colors: Record<string, string> = {
      'related_to': '#999999',
      'part_of': '#2196F3',
      'uses': '#4CAF50',
      'depends_on': '#FF9800',
      'created_by': '#9C27B0',
      'managed_by': '#F44336',
      'implements': '#00BCD4',
      'extends': '#3F51B5',
      'contains': '#8BC34A',
    };
    return colors[type.toLowerCase()] || '#666666';
  };

  // Get node size based on number of connections
  const getNodeSize = (nodeId: string, relationships: Relationship[]): number => {
    const connectionCount = relationships.filter(
      rel => rel.source === nodeId || rel.target === nodeId
    ).length;
    return Math.max(5, Math.min(12, 5 + connectionCount));
  };

  // Fetch entity data from API
  const fetchEntityData = useCallback(async () => {
    console.log('[EntityGraph] fetchEntityData called with:', { indexName, workspaceUrl, endpointName });
    
    if (!indexName || !workspaceUrl || !endpointName) {
      console.error('[EntityGraph] Missing configuration:', { indexName, workspaceUrl, endpointName });
      setError('Missing configuration. Please ensure your memory backend is properly configured.');
      return;
    }

    setLoading(true);
    setError(null);
    
    console.log('[EntityGraph] Making API call to /memory-backend/databricks/entity-data');

    try {
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
        setGraphData({ nodes: [], links: [] });
        return;
      }

      const entities: Entity[] = response.data.entities || [];
      const relationships: Relationship[] = response.data.relationships || [];

      console.log('Entity data received:', {
        entities: entities.length,
        relationships: relationships.length,
        sampleEntity: entities[0],
        sampleRelationship: relationships[0],
      });

      if (entities.length === 0) {
        setError('No entities found in the index. Entity memory will be populated as agents interact.');
        setGraphData({ nodes: [], links: [] });
        return;
      }

      // Transform data for react-force-graph
      const nodes: GraphNode[] = entities.map(entity => {
        const nodeSize = getNodeSize(entity.id, relationships);
        return {
          id: entity.id,
          name: entity.name,
          type: entity.type,
          attributes: entity.attributes || {},
          color: getEntityColor(entity.type),
          size: nodeSize,
          val: nodeSize, // Used by react-force-graph for node sizing
        };
      });

      const links: GraphLink[] = relationships.map(rel => ({
        source: rel.source,
        target: rel.target,
        type: rel.type,
        label: rel.label || rel.type,
        color: getRelationshipColor(rel.type),
        width: Math.max(2, (rel.strength || 0.5) * 4),
      }));

      console.log('Graph data prepared:', {
        nodes: nodes.length,
        links: links.length,
        nodeIds: nodes.map(n => n.id),
        linkSources: links.map(l => l.source),
        linkTargets: links.map(l => l.target),
      });

      setGraphData({ nodes, links });
    } catch (err) {
      console.error('Error fetching entity data:', err);
      const errorMessage = err instanceof Error ? err.message : 'Failed to load entity data';
      setError(errorMessage);
      setGraphData({ nodes: [], links: [] });
    } finally {
      setLoading(false);
    }
  }, [indexName, workspaceUrl, endpointName]);

  // Handle node click
  const handleNodeClick = useCallback((node: GraphNode) => {
    console.log('[EntityGraph] Node clicked:', node.id, node.name);
    setSelectedNode(node);
    setFocusedNodeId(node.id);
    
    // Find all connected nodes (same logic as search select)
    const connectedNodes = new Set([node.id]);
    graphData.links.forEach(link => {
      const sourceId = typeof link.source === 'string' ? link.source : (link.source as any)?.id;
      const targetId = typeof link.target === 'string' ? link.target : (link.target as any)?.id;
      
      if (sourceId === node.id) {
        connectedNodes.add(targetId);
      }
      if (targetId === node.id) {
        connectedNodes.add(sourceId);
      }
    });
    
    console.log('[EntityGraph] Connected nodes after click:', Array.from(connectedNodes));
    setHighlightedNodes(connectedNodes);
  }, [graphData]);

  // Handle search
  const handleSearch = useCallback((query: string) => {
    if (!query) {
      setHighlightedNodes(new Set());
      return;
    }
    
    const searchLower = query.toLowerCase();
    const matchingNodes = graphData.nodes.filter(node => 
      node.name.toLowerCase().includes(searchLower) ||
      node.type.toLowerCase().includes(searchLower) ||
      node.id.toLowerCase().includes(searchLower)
    );
    
    setHighlightedNodes(new Set(matchingNodes.map(n => n.id)));
    
    // Focus on first matching node if any
    if (matchingNodes.length > 0 && forceGraphRef.current) {
      const firstMatch = matchingNodes[0];
      if (firstMatch.x !== undefined && firstMatch.y !== undefined) {
        forceGraphRef.current.centerAt(firstMatch.x, firstMatch.y, 1000);
        forceGraphRef.current.zoom(2, 1000);
      }
    }
  }, [graphData]);

  // Handle node selection from search
  const handleSearchSelect = useCallback((nodeId: string | null) => {
    if (!nodeId) {
      setFocusedNodeId(null);
      setSelectedNode(null);
      setHighlightedNodes(new Set());
      return;
    }
    
    const node = graphData.nodes.find(n => n.id === nodeId);
    if (node && forceGraphRef.current) {
      setSelectedNode(node);
      setFocusedNodeId(nodeId);
      
      // Find all connected nodes
      const connectedNodes = new Set([nodeId]);
      graphData.links.forEach(link => {
        const sourceId = typeof link.source === 'string' ? link.source : (link.source as any)?.id;
        const targetId = typeof link.target === 'string' ? link.target : (link.target as any)?.id;
        
        if (sourceId === nodeId) {
          connectedNodes.add(targetId);
        }
        if (targetId === nodeId) {
          connectedNodes.add(sourceId);
        }
      });
      
      setHighlightedNodes(connectedNodes);
      
      // Don't zoom here - let the graph update first and zoom in the useEffect
    }
  }, [graphData]);

  // Handle zoom controls
  const handleZoomIn = () => {
    if (forceGraphRef.current) {
      const currentZoom = forceGraphRef.current.zoom();
      forceGraphRef.current.zoom(currentZoom * 1.5, 400);
    }
  };

  const handleZoomOut = () => {
    if (forceGraphRef.current) {
      const currentZoom = forceGraphRef.current.zoom();
      forceGraphRef.current.zoom(currentZoom / 1.5, 400);
    }
  };

  const handleCenterGraph = () => {
    if (forceGraphRef.current) {
      forceGraphRef.current.zoomToFit(400);
    }
  };

  // Load data when dialog opens
  useEffect(() => {
    if (open) {
      fetchEntityData();
    }
  }, [open, fetchEntityData]);

  // Initialize force graph
  useEffect(() => {
    if (!open) {
      console.log('[EntityGraph] Dialog not open, skipping initialization');
      return;
    }

    // Only initialize after data is loaded and no errors
    if (loading || error) {
      console.log('[EntityGraph] Still loading or has error, skipping initialization');
      return;
    }

    // Check if we have data to display
    if (graphData.nodes.length === 0) {
      console.log('[EntityGraph] No data to display, skipping initialization');
      return;
    }

    // Small delay to ensure container is rendered and has dimensions
    const initTimeout = setTimeout(() => {
      const container = containerRef.current;
      console.log('[EntityGraph] Force graph init - container:', container, 'dimensions:', container?.offsetWidth, 'x', container?.offsetHeight);
      
      if (!container || container.offsetWidth === 0 || container.offsetHeight === 0) {
        console.log('[EntityGraph] Container not ready or has no dimensions');
        return;
      }

      // Clear any existing graph
      if (forceGraphRef.current) {
        console.log('[EntityGraph] Clearing existing force graph');
        try {
          // Properly dispose of the existing graph
          if (typeof forceGraphRef.current._destructor === 'function') {
            forceGraphRef.current._destructor();
          }
        } catch (err) {
          console.error('[EntityGraph] Error disposing graph:', err);
        }
        container.innerHTML = '';
        forceGraphRef.current = null;
      }

      console.log('[EntityGraph] Creating new force graph instance');
      
      try {
        // Create force graph instance - ForceGraph2D is a factory function
        const ForceGraphFactory = ForceGraph2D as any;
        const graph = ForceGraphFactory()(container)
      .backgroundColor('#fafafa')
      .nodeId('id')
      .nodeLabel((node: any) => `
        <div style="background: rgba(0,0,0,0.8); color: white; padding: 8px; border-radius: 4px; max-width: 200px;">
          <div style="font-weight: bold; margin-bottom: 4px;">${node.name}</div>
          <div style="font-size: 12px; color: #ccc; margin-bottom: 4px;">Type: ${node.type}</div>
          ${Object.entries(node.attributes || {}).length > 0 ? 
            '<div style="font-size: 11px;">' + 
            Object.entries(node.attributes || {}).slice(0, 3).map(([key, value]) => 
              `${key}: ${String(value)}`
            ).join('<br/>') + 
            '</div>' : ''}
        </div>
      `)
      .nodeCanvasObject((node: any, ctx: any, globalScale: number) => {
        // Draw node circle
        const nodeSize = node.size || 5;
        
        ctx.beginPath();
        ctx.arc(node.x, node.y, nodeSize, 0, 2 * Math.PI, false);
        ctx.fillStyle = node.color;
        ctx.fill();
        ctx.strokeStyle = 'rgba(0,0,0,0.3)';
        ctx.lineWidth = 2;
        ctx.stroke();
        
        // Draw node label - use actual name
        const label = node.name || node.id || 'Unknown';
        const fontSize = 14; // Fixed size for better readability
        ctx.font = `bold ${fontSize}px Arial`;
        ctx.textAlign = 'center';
        ctx.textBaseline = 'top';
        
        // Measure text for background
        const textMetrics = ctx.measureText(label);
        const textWidth = textMetrics.width;
        const textHeight = fontSize * 1.2;
        
        // Draw text background
        const padding = 4;
        const bgY = node.y + nodeSize + 3;
        ctx.fillStyle = 'rgba(255, 255, 255, 0.95)';
        ctx.fillRect(
          node.x - (textWidth + padding) / 2,
          bgY,
          textWidth + padding,
          textHeight
        );
        
        // Draw border around text background
        ctx.strokeStyle = 'rgba(0,0,0,0.1)';
        ctx.lineWidth = 1;
        ctx.strokeRect(
          node.x - (textWidth + padding) / 2,
          bgY,
          textWidth + padding,
          textHeight
        );
        
        // Draw text
        ctx.fillStyle = 'rgba(0,0,0,0.9)';
        ctx.fillText(label, node.x, bgY + padding/2);
        
        // Draw entity type below name
        if (node.type && node.type !== 'unknown') {
          const typeSize = 10;
          ctx.font = `${typeSize}px Arial`;
          ctx.fillStyle = 'rgba(100,100,100,0.8)';
          ctx.fillText(`(${node.type})`, node.x, bgY + textHeight + 2);
        }
      })
      .nodePointerAreaPaint((node: any, color: string, ctx: any) => {
        ctx.fillStyle = color;
        ctx.beginPath();
        ctx.arc(node.x, node.y, node.size || 5, 0, 2 * Math.PI, false);
        ctx.fill();
      })
      .nodeColor((node: any) => node.color)
      .nodeVal((node: any) => node.val)
      .linkLabel((link: any) => `<div style="background: rgba(0,0,0,0.8); color: white; padding: 4px; border-radius: 2px;">${link.label || link.type}</div>`)
      .linkCanvasObject((link: any, ctx: any, globalScale: number) => {
        const start = link.source;
        const end = link.target;
        
        // Skip if nodes don't have positions yet
        if (!start.x || !end.x) return;
        
        // Calculate line path
        const textPos = {
          x: (start.x + end.x) / 2,
          y: (start.y + end.y) / 2
        };
        
        // Draw the link line
        ctx.beginPath();
        ctx.moveTo(start.x, start.y);
        ctx.lineTo(end.x, end.y);
        ctx.strokeStyle = link.color || '#999999';
        ctx.lineWidth = link.width || 2;
        ctx.stroke();
        
        // Draw arrow
        const arrowLength = 10;
        const dx = end.x - start.x;
        const dy = end.y - start.y;
        const angle = Math.atan2(dy, dx);
        const endX = end.x - (end.size || 5) * Math.cos(angle);
        const endY = end.y - (end.size || 5) * Math.sin(angle);
        
        ctx.beginPath();
        ctx.moveTo(endX, endY);
        ctx.lineTo(
          endX - arrowLength * Math.cos(angle - Math.PI / 6),
          endY - arrowLength * Math.sin(angle - Math.PI / 6)
        );
        ctx.moveTo(endX, endY);
        ctx.lineTo(
          endX - arrowLength * Math.cos(angle + Math.PI / 6),
          endY - arrowLength * Math.sin(angle + Math.PI / 6)
        );
        ctx.strokeStyle = link.color || '#999999';
        ctx.lineWidth = 2;
        ctx.stroke();
        
        // Draw label if zoom is sufficient
        if (globalScale > 0.5 && link.label) {
          ctx.font = '10px Arial';
          ctx.textAlign = 'center';
          ctx.textBaseline = 'middle';
          
          // Background for label
          const labelMetrics = ctx.measureText(link.label);
          ctx.fillStyle = 'rgba(255, 255, 255, 0.9)';
          ctx.fillRect(
            textPos.x - labelMetrics.width / 2 - 2,
            textPos.y - 6,
            labelMetrics.width + 4,
            12
          );
          
          // Draw label text
          ctx.fillStyle = 'rgba(0, 0, 0, 0.8)';
          ctx.fillText(link.label, textPos.x, textPos.y);
        }
      })
      .linkDirectionalArrowLength(0) // Disable default arrows since we draw custom ones
      .onNodeClick((node: any) => {
        // We'll handle this separately to avoid closure issues
        console.log('[EntityGraph] Force graph node clicked:', node.id);
      })
      .enableNodeDrag(true)
      .enableZoomInteraction(true)
      .cooldownTicks(300)
      .d3AlphaDecay(0.005)
      .d3VelocityDecay(0.3)
      .onEngineStop(() => graph.zoomToFit(400, 100));
    
    // Configure force simulation for better spacing
    graph.d3Force('center', null); // Disable centering force
    graph.d3Force('charge')?.strength(-600).distanceMax(1000);
    graph.d3Force('link')?.distance(250).strength(0.4);
    graph.d3Force('collide')?.radius(25).strength(1.2);
    graph.d3Force('x')?.strength(0.04);
    graph.d3Force('y')?.strength(0.04);

        forceGraphRef.current = graph;
        console.log('[EntityGraph] Force graph created and stored in ref');
        
        // Set initial empty data
        graph.graphData({ nodes: [], links: [] });
        console.log('[EntityGraph] Initial empty data set');
      } catch (error) {
        console.error('[EntityGraph] Error creating force graph:', error);
        setError('Failed to initialize graph visualization');
      }
    }, 100); // 100ms delay to ensure container is ready

    return () => {
      // Cleanup
      clearTimeout(initTimeout);
      console.log('[EntityGraph] Cleaning up force graph');
      // Capture the ref value at cleanup time
      if (containerRef.current) {
        const cleanupContainer = containerRef.current;
        try {
          if (forceGraphRef.current && typeof forceGraphRef.current._destructor === 'function') {
            forceGraphRef.current._destructor();
          }
        } catch (err) {
          console.error('[EntityGraph] Error during cleanup:', err);
        }
        cleanupContainer.innerHTML = '';
        forceGraphRef.current = null;
      }
    };
  }, [open, loading, error, graphData]); // Re-initialize when dialog opens/closes, loading state, or graph data changes

  // Filter graph data based on showInferredNodes setting and focused node
  const filteredGraphData = React.useMemo(() => {
    let nodes = graphData.nodes;
    let links = graphData.links;
    
    // Apply deduplication if enabled
    if (deduplicateNodes) {
      const nodesByName = new Map<string, GraphNode>();
      const idMapping = new Map<string, string>(); // Maps old IDs to deduplicated IDs
      
      // Group nodes by name, keeping the first occurrence
      nodes.forEach(node => {
        const key = `${node.name}_${node.type}`; // Deduplicate by name and type
        if (!nodesByName.has(key)) {
          nodesByName.set(key, node);
          idMapping.set(node.id, node.id);
        } else {
          // Map this duplicate's ID to the first occurrence's ID
          const firstNode = nodesByName.get(key);
          if (firstNode) {
            idMapping.set(node.id, firstNode.id);
            
            // Merge attributes if they exist
            if (node.attributes && firstNode.attributes) {
              firstNode.attributes = { ...firstNode.attributes, ...node.attributes };
            }
          }
        }
      });
      
      nodes = Array.from(nodesByName.values());
      
      // Update links to use deduplicated node IDs and remove self-loops
      const uniqueLinks = new Map<string, GraphLink>();
      links.forEach(link => {
        const sourceId = idMapping.get(
          typeof link.source === 'string' ? link.source : (link.source as any)?.id || link.source
        ) || link.source;
        const targetId = idMapping.get(
          typeof link.target === 'string' ? link.target : (link.target as any)?.id || link.target
        ) || link.target;
        
        // Skip self-loops created by deduplication
        if (sourceId === targetId) return;
        
        // Create unique key for the link
        const linkKey = `${sourceId}_${targetId}_${link.type}`;
        if (!uniqueLinks.has(linkKey)) {
          uniqueLinks.set(linkKey, {
            ...link,
            source: sourceId as string,
            target: targetId as string
          });
        }
      });
      
      links = Array.from(uniqueLinks.values());
    }
    
    // First filter by focused node if one is selected
    if (focusedNodeId) {
      console.log('[EntityGraph] Filtering for focused node:', focusedNodeId);
      // Find all nodes connected to the focused node
      const connectedNodeIds = new Set<string>([focusedNodeId]);
      
      // Look through all links to find connected nodes
      graphData.links.forEach(link => {
        // Handle both string IDs and object references
        const sourceId = typeof link.source === 'string' ? link.source : (link.source as any)?.id || link.source;
        const targetId = typeof link.target === 'string' ? link.target : (link.target as any)?.id || link.target;
        
        if (sourceId === focusedNodeId) {
          connectedNodeIds.add(targetId);
        }
        if (targetId === focusedNodeId) {
          connectedNodeIds.add(sourceId);
        }
      });
      
      console.log('[EntityGraph] Focused node:', focusedNodeId, 'Connected nodes:', Array.from(connectedNodeIds));
      console.log('[EntityGraph] Total links:', graphData.links.length, 'First few links:', graphData.links.slice(0, 3));
      
      // Filter nodes to only show focused node and its neighbors
      nodes = nodes.filter(node => connectedNodeIds.has(node.id));
      console.log('[EntityGraph] Filtered nodes count:', nodes.length, 'from total:', graphData.nodes.length);
      
      // If we didn't find any connected nodes, include the focused node at least
      if (nodes.length === 0 && graphData.nodes.length > 0) {
        const focusedNode = graphData.nodes.find(n => n.id === focusedNodeId);
        if (focusedNode) {
          nodes = [focusedNode];
          console.log('[EntityGraph] No connections found, showing only focused node');
        }
      }
      
      // Filter links to only show connections between visible nodes
      links = links.filter(link => {
        const sourceId = typeof link.source === 'string' ? link.source : (link.source as any)?.id || link.source;
        const targetId = typeof link.target === 'string' ? link.target : (link.target as any)?.id || link.target;
        return connectedNodeIds.has(sourceId) && connectedNodeIds.has(targetId);
      });
      console.log('[EntityGraph] Filtered links count:', links.length, 'from total:', graphData.links.length);
    } else {
      console.log('[EntityGraph] No focused node, showing all nodes');
    }
    
    // Then apply the inferred nodes filter if not showing them
    if (!showInferredNodes) {
      nodes = nodes.filter(node => !node.attributes?.inferred);
      const nodeIds = new Set(nodes.map(n => n.id));
      
      links = links.filter(link => {
        const sourceId = typeof link.source === 'string' ? link.source : (link.source as any)?.id || link.source;
        const targetId = typeof link.target === 'string' ? link.target : (link.target as any)?.id || link.target;
        return nodeIds.has(sourceId) && nodeIds.has(targetId);
      });
    }
    
    return { nodes, links };
  }, [graphData, showInferredNodes, focusedNodeId, deduplicateNodes]);

  // Set up node click handler on the force graph
  useEffect(() => {
    if (forceGraphRef.current) {
      forceGraphRef.current.onNodeClick((node: any) => {
        console.log('[EntityGraph] Node clicked via force graph:', node.id);
        const clickedNode = graphData.nodes.find(n => n.id === node.id);
        if (clickedNode) {
          handleNodeClick(clickedNode);
        }
      });
    }
  }, [graphData, handleNodeClick]);

  // Update graph data
  useEffect(() => {
    if (!forceGraphRef.current) return;
    
    console.log('[EntityGraph] Updating graph with filtered data:', {
      nodes: filteredGraphData.nodes.length,
      links: filteredGraphData.links.length,
      focusedNodeId
    });
    
    // Always update the graph data
    forceGraphRef.current.graphData(filteredGraphData);
    
    // Handle zoom and positioning
    if (focusedNodeId && filteredGraphData.nodes.length > 0) {
      // Small delay to let the graph render
      setTimeout(() => {
        if (forceGraphRef.current) {
          forceGraphRef.current.zoomToFit(400, 100);
        }
      }, 300);
    } else if (filteredGraphData.nodes.length > 0) {
      // Zoom to fit on initial load
      setTimeout(() => {
        if (forceGraphRef.current) {
          forceGraphRef.current.zoomToFit(400, 50);
        }
      }, 500);
    }
  }, [filteredGraphData, focusedNodeId]);

  // Update force simulation parameters
  useEffect(() => {
    if (forceGraphRef.current) {
      forceGraphRef.current
        .d3Force('charge', forceGraphRef.current.d3Force('charge').strength(forceStrength))
        .d3Force('link', forceGraphRef.current.d3Force('link').distance(linkDistance));
    }
  }, [forceStrength, linkDistance]);

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
            <Typography variant="h6">Entity Memory Graph</Typography>
            {graphData.nodes.length > 0 && (
              <>
                {focusedNodeId && (
                  <Chip 
                    label="Focused Mode" 
                    size="small" 
                    color="secondary"
                    variant="filled"
                  />
                )}
                <Chip 
                  label={`${filteredGraphData.nodes.length} visible${deduplicateNodes ? ' (deduplicated)' : ''}`} 
                  size="small" 
                  color="primary"
                />
                <Chip 
                  label={`${filteredGraphData.links.length} relationships`} 
                  size="small" 
                  color="secondary"
                  variant="outlined"
                />
                {!focusedNodeId && (
                  <Chip 
                    label={`${graphData.nodes.filter((n: GraphNode) => !n.attributes?.inferred).length} primary`} 
                    size="small" 
                    variant="outlined"
                  />
                )}
                {!showInferredNodes && !focusedNodeId && (
                  <Chip 
                    label={`${graphData.nodes.filter((n: GraphNode) => n.attributes?.inferred).length} hidden`} 
                    size="small" 
                    variant="outlined"
                    color="default"
                  />
                )}
              </>
            )}
          </Box>
          <IconButton onClick={onClose} size="small">
            <CloseIcon />
          </IconButton>
        </Box>
      </DialogTitle>
      
      <DialogContent sx={{ p: 0, display: 'flex', overflow: 'hidden' }}>
        {/* Main visualization area */}
        <Box sx={{ flex: 1, position: 'relative', overflow: 'hidden' }}>
          {/* Controls Panel */}
          <Paper 
            elevation={3} 
            sx={{ 
              position: 'absolute', 
              top: 16, 
              left: 16, 
              zIndex: 10, 
              p: 2,
              minWidth: 300,
              maxWidth: 350
            }}
          >
            <Typography variant="subtitle2" gutterBottom>
              Graph Controls
            </Typography>
            
            <Stack spacing={2}>
              <Autocomplete
                options={graphData.nodes}
                getOptionLabel={(option) => option.name || option.id}
                groupBy={(option) => option.type}
                value={graphData.nodes.find(n => n.id === focusedNodeId) || null}
                onChange={(_, newValue) => {
                  if (newValue) {
                    handleSearchSelect(newValue.id);
                  } else {
                    handleSearchSelect(null);
                  }
                }}
                onInputChange={(_, newInputValue) => {
                  handleSearch(newInputValue);
                }}
                renderInput={(params) => (
                  <TextField
                    {...params}
                    label={focusedNodeId ? "Focused View" : "Search Entities"}
                    variant="outlined"
                    size="small"
                    placeholder="Type to search..."
                    color={focusedNodeId ? "secondary" : "primary"}
                  />
                )}
                renderOption={(props, option) => (
                  <Box component="li" {...props}>
                    <Stack direction="row" spacing={1} alignItems="center">
                      <Box
                        sx={{
                          width: 10,
                          height: 10,
                          borderRadius: '50%',
                          backgroundColor: getEntityColor(option.type),
                          border: '1px solid rgba(0,0,0,0.2)'
                        }}
                      />
                      <Typography variant="body2">{option.name}</Typography>
                      <Typography variant="caption" color="text.secondary">
                        ({option.type})
                      </Typography>
                    </Stack>
                  </Box>
                )}
                sx={{ width: '100%' }}
              />
              
              <FormControlLabel
                control={
                  <Switch
                    checked={showInferredNodes}
                    onChange={(e) => setShowInferredNodes(e.target.checked)}
                    color="primary"
                  />
                }
                label={
                  <Typography variant="body2">
                    Show Inferred Nodes ({showInferredNodes ? 'Expanded' : 'Collapsed'})
                  </Typography>
                }
              />
              
              <FormControlLabel
                control={
                  <Switch
                    checked={deduplicateNodes}
                    onChange={(e) => setDeduplicateNodes(e.target.checked)}
                    color="secondary"
                  />
                }
                label={
                  <Typography variant="body2">
                    Deduplicate by Name ({deduplicateNodes ? 'On' : 'Off'})
                  </Typography>
                }
              />
              
              <Divider />
              
              {/* Zoom Controls */}
              <Box>
                <Typography variant="body2" gutterBottom>
                  Zoom Controls
                </Typography>
                <Stack direction="row" spacing={1}>
                  <Tooltip title="Zoom In">
                    <Button
                      variant="outlined"
                      size="small"
                      onClick={handleZoomIn}
                      sx={{ minWidth: 0, flex: 1 }}
                    >
                      <ZoomInIcon fontSize="small" />
                    </Button>
                  </Tooltip>
                  <Tooltip title="Zoom Out">
                    <Button
                      variant="outlined"
                      size="small"
                      onClick={handleZoomOut}
                      sx={{ minWidth: 0, flex: 1 }}
                    >
                      <ZoomOutIcon fontSize="small" />
                    </Button>
                  </Tooltip>
                  <Tooltip title="Fit to Screen">
                    <Button
                      variant="outlined"
                      size="small"
                      onClick={handleCenterGraph}
                      sx={{ minWidth: 0, flex: 1 }}
                    >
                      <CenterIcon fontSize="small" />
                    </Button>
                  </Tooltip>
                </Stack>
              </Box>
              
              <Divider />
              
              <Box>
                <Typography variant="body2" gutterBottom>
                  Force Strength: {forceStrength}
                </Typography>
                <Slider
                  value={forceStrength}
                  onChange={(_, value) => setForceStrength(value as number)}
                  min={-2000}
                  max={-100}
                  step={50}
                  size="small"
                  valueLabelDisplay="auto"
                />
              </Box>
              
              <Box>
                <Typography variant="body2" gutterBottom>
                  Link Distance: {linkDistance}
                </Typography>
                <Slider
                  value={linkDistance}
                  onChange={(_, value) => setLinkDistance(value as number)}
                  min={100}
                  max={800}
                  step={50}
                  size="small"
                  valueLabelDisplay="auto"
                />
              </Box>

              <Button
                startIcon={<RefreshIcon />}
                onClick={fetchEntityData}
                disabled={loading}
                variant="outlined"
                size="small"
                fullWidth
              >
                Refresh Data
              </Button>
            </Stack>
          </Paper>

          {/* Legend Panel */}
          <Paper 
            elevation={3} 
            sx={{ 
              position: 'absolute', 
              bottom: 16, 
              left: 16, 
              zIndex: 10, 
              p: 2,
              maxWidth: 200
            }}
          >
            <Typography variant="subtitle2" gutterBottom>
              Entity Types
            </Typography>
            <Stack spacing={0.5}>
              {Object.entries({
                'Event': '#D62728',
                'Conference': '#E91E63',
                'Person': '#68CCE5',
                'System': '#FCC940',
                'Concept': '#F25A29',
                'Unknown': '#C5C5C5',
              }).map(([type, color]) => (
                <Box key={type} sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                  <Box 
                    sx={{ 
                      width: 12, 
                      height: 12, 
                      borderRadius: '50%', 
                      backgroundColor: color,
                      border: '1px solid rgba(0,0,0,0.2)'
                    }} 
                  />
                  <Typography variant="caption">{type}</Typography>
                </Box>
              ))}
            </Stack>
          </Paper>

          {/* Loading state */}
          {loading && (
            <Box
              sx={{
                position: 'absolute',
                top: '50%',
                left: '50%',
                transform: 'translate(-50%, -50%)',
                zIndex: 20,
                display: 'flex',
                flexDirection: 'column',
                alignItems: 'center',
                gap: 2,
                bgcolor: 'rgba(255, 255, 255, 0.9)',
                p: 3,
                borderRadius: 2,
                boxShadow: 3,
              }}
            >
              <CircularProgress />
              <Typography>Loading entity data...</Typography>
            </Box>
          )}

          {/* Error state */}
          {error && !loading && (
            <Alert severity="warning" sx={{ m: 2, position: 'absolute', top: 0, left: 0, right: 0, zIndex: 20 }}>
              {error}
            </Alert>
          )}

          {/* Force Graph Container */}
          <div 
            ref={containerRef}
            style={{
              width: '100%',
              height: '100%',
              minHeight: '600px',
              display: loading || error ? 'none' : 'block'
            }}
          />

          {/* Empty state */}
          {!loading && !error && graphData.nodes.length === 0 && (
            <Box
              sx={{
                position: 'absolute',
                top: '50%',
                left: '50%',
                transform: 'translate(-50%, -50%)',
                textAlign: 'center',
                color: 'text.secondary'
              }}
            >
              <AccountTree sx={{ fontSize: 64, mb: 2, color: 'text.disabled' }} />
              <Typography variant="h6" gutterBottom>
                No Entity Data Available
              </Typography>
              <Typography variant="body2">
                Entity relationships will appear here as agents interact and build memory.
              </Typography>
            </Box>
          )}
        </Box>

        {/* Side panel for node details */}
        {selectedNode && (
          <Paper 
            elevation={3} 
            sx={{ 
              width: 300, 
              maxWidth: 300, 
              p: 2, 
              overflow: 'auto',
              borderLeft: '1px solid #e0e0e0',
              bgcolor: '#f9f9f9'
            }}
          >
            {/* Action buttons at the top */}
            <Stack direction="row" spacing={1} sx={{ mb: 2 }}>
              <Button
                variant="outlined"
                size="small"
                fullWidth
                onClick={() => {
                  setSelectedNode(null);
                  setFocusedNodeId(null);
                  setHighlightedNodes(new Set());
                }}
              >
                Close
              </Button>
              <Button
                variant="contained"
                size="small"
                fullWidth
                onClick={() => {
                  // Focus on the selected node
                  if (forceGraphRef.current && selectedNode.x !== undefined && selectedNode.y !== undefined) {
                    forceGraphRef.current.centerAt(selectedNode.x, selectedNode.y, 1000);
                    forceGraphRef.current.zoom(2, 1000);
                  }
                }}
              >
                Focus
              </Button>
            </Stack>

            <Box sx={{ mb: 2 }}>
              <Typography variant="h6" gutterBottom sx={{ wordBreak: 'break-word' }}>
                {selectedNode.name}
              </Typography>
              <Chip 
                label={selectedNode.type} 
                size="small"
                sx={{ 
                  backgroundColor: selectedNode.color, 
                  color: 'white',
                  fontWeight: 'bold'
                }}
              />
            </Box>

            <Card variant="outlined" sx={{ mb: 2, bgcolor: 'white' }}>
              <CardContent sx={{ p: 2, '&:last-child': { pb: 2 } }}>
                <Typography variant="subtitle2" gutterBottom color="primary">
                  Entity Attributes
                </Typography>
                {Object.entries(selectedNode.attributes).length > 0 ? (
                  <Stack spacing={1}>
                    {Object.entries(selectedNode.attributes).map(([key, value]) => (
                      <Box key={key}>
                        <Typography variant="caption" color="text.secondary" sx={{ textTransform: 'capitalize' }}>
                          {key.replace(/_/g, ' ')}
                        </Typography>
                        <Typography variant="body2" sx={{ fontWeight: 'medium', wordBreak: 'break-word' }}>
                          {String(value)}
                        </Typography>
                      </Box>
                    ))}
                  </Stack>
                ) : (
                  <Typography variant="body2" color="text.secondary" sx={{ fontStyle: 'italic' }}>
                    No attributes available
                  </Typography>
                )}
              </CardContent>
            </Card>

            {/* Entity connections */}
            <Card variant="outlined" sx={{ mb: 2, bgcolor: 'white' }}>
              <CardContent sx={{ p: 2, '&:last-child': { pb: 2 } }}>
                <Typography variant="subtitle2" gutterBottom color="primary">
                  Connections
                </Typography>
                <Typography variant="body2">
                  {graphData.links.filter(link => 
                    link.source === selectedNode.id || link.target === selectedNode.id
                  ).length} relationships
                </Typography>
              </CardContent>
            </Card>
          </Paper>
        )}
      </DialogContent>
    </Dialog>
  );
};

export default EntityGraphVisualization;