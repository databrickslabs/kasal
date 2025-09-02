import React, { useEffect, useRef, useCallback } from 'react';
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
  AccountTree,
  ZoomIn as ZoomInIcon,
  ZoomOut as ZoomOutIcon,
  CenterFocusStrong as CenterIcon,
} from '@mui/icons-material';
import { apiClient } from '../../config/api/ApiConfig';
import useEntityGraphStore from '../../store/entityGraphStore';

interface Entity {
  id?: string;
  name?: string;
  type?: string;
  attributes?: Record<string, unknown>;
}

interface Relationship {
  source: string;
  target: string;
  type?: string;
  label?: string;
  direction?: 'incoming' | 'outgoing';
}

interface GraphNode {
  id: string;
  name: string;
  type: string;
  attributes: Record<string, unknown>;
  color?: string;
  size?: number;
}

interface GraphLink {
  source: string | GraphNode;
  target: string | GraphNode;
  relationship?: string;
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
  
  // Get state and actions from Zustand store
  const {
    graphData,
    filteredGraphData,
    loading,
    error,
    selectedNode,
    focusedNodeId,
    showInferredNodes,
    deduplicateNodes,
    showOrphanedNodes,
    forceStrength,
    linkDistance,
    linkCurvature,
    centerForce,
    initializeGraph,
    cleanupGraph,
    setGraphData,
    setFilteredGraphData,
    setLoading,
    setError,
    setFocusedNode,
    setSelectedNode,
    updateForceParameters,
    setLinkCurvature,
    resetFilters,
    toggleInferredNodes,
    toggleDeduplication,
    toggleOrphanedNodes,
    zoomToFit,
    zoomIn,
    zoomOut,
  } = useEntityGraphStore();

  // Color scheme for different entity types
  const getEntityColor = (type: string): string => {
    const colors: Record<string, string> = {
      'person': '#68CCE5',
      'organization': '#94D82D',
      'system': '#FCC940',
      'concept': '#F25A29',
      'location': '#AD4BAC',
      'event': '#D62728',
      'conference': '#E91E63',
      'meetup': '#FF5722',
      'document': '#8FBC8F',
      'project': '#FFB366',
      'technology': '#00BCD4',
      'tool': '#9C27B0',
      'framework': '#3F51B5',
      'unknown': '#C5C5C5',
    };
    return colors[type.toLowerCase()] || colors['unknown'];
  };

  // Fetch entity data from backend
  const fetchEntityData = useCallback(async () => {
    if (!indexName || !workspaceUrl || !endpointName) {
      console.log('[EntityGraph] Missing required props for fetching data');
      return;
    }

    console.log('[EntityGraph] Fetching entity data');
    setLoading(true);
    setError(null);

    try {
      const response = await apiClient.get('/memory-backend/databricks/entity-data', {
        params: {
          index_name: indexName,
          workspace_url: workspaceUrl,
          endpoint_name: endpointName,
        }
      });

      const { entities, relationships } = response.data;
      console.log(`[EntityGraph] Received ${entities.length} entities and ${relationships.length} relationships`);

      // Transform entities to graph nodes
      const nodeMap = new Map();
      const nodes = entities.map((entity: Entity) => {
        const node = {
          id: entity.id || entity.name,
          name: entity.name || entity.id,
          type: entity.type || 'unknown',
          attributes: entity.attributes || {},
          color: getEntityColor(entity.type || 'unknown'),
          size: 5,
        };
        nodeMap.set(node.id, node);
        return node;
      });

      // Transform relationships to graph links
      const links = relationships
        .filter((rel: Relationship) => nodeMap.has(rel.source) && nodeMap.has(rel.target))
        .map((rel: Relationship) => ({
          source: rel.source,
          target: rel.target,
          relationship: rel.type || rel.label || 'related_to',
        }));

      const graphData = { nodes, links };
      console.log('[EntityGraph] Graph data prepared:', {
        nodes: graphData.nodes.length,
        links: graphData.links.length,
      });

      setGraphData(graphData);
      setFilteredGraphData(graphData);
    } catch (err: unknown) {
      console.error('[EntityGraph] Error fetching entity data:', err);
      const errorMessage = err instanceof Error ? err.message : 
        (err as any)?.response?.data?.detail || 'Failed to fetch entity data';
      setError(errorMessage);
    } finally {
      setLoading(false);
    }
  }, [indexName, workspaceUrl, endpointName, setGraphData, setFilteredGraphData, setLoading, setError]);

  // Fetch data when dialog opens
  useEffect(() => {
    if (open) {
      fetchEntityData();
    }
  }, [open, fetchEntityData]);

  // Initialize graph when container is ready and data is loaded
  useEffect(() => {
    if (!open || loading || error || !containerRef.current) {
      return;
    }

    if (graphData.nodes.length === 0) {
      return;
    }

    // Small delay to ensure container has dimensions
    const timer = setTimeout(() => {
      if (containerRef.current) {
        console.log('[EntityGraph] Container ready, initializing graph');
        initializeGraph(containerRef.current);
      }
    }, 100);

    return () => {
      clearTimeout(timer);
    };
  }, [open, loading, error, graphData, initializeGraph]);

  // Cleanup on unmount or dialog close
  useEffect(() => {
    if (!open) {
      cleanupGraph();
      resetFilters(); // Clear all filters when dialog closes
    }
  }, [open, cleanupGraph, resetFilters]);

  // Apply filters when settings change
  useEffect(() => {
    let nodes = [...graphData.nodes];
    let links = [...graphData.links];
    
    // First, filter out orphaned nodes (nodes with no connections)
    const connectedNodeIds = new Set();
    links.forEach(link => {
      const sourceId = typeof link.source === 'object' ? (link.source as GraphNode).id : link.source;
      const targetId = typeof link.target === 'object' ? (link.target as GraphNode).id : link.target;
      connectedNodeIds.add(sourceId);
      connectedNodeIds.add(targetId);
    });
    
    // Remove orphaned nodes unless we're in focused mode or showOrphanedNodes is true
    if (!focusedNodeId && !showOrphanedNodes && connectedNodeIds.size > 0) {
      const orphanedCount = nodes.filter(node => !connectedNodeIds.has(node.id)).length;
      nodes = nodes.filter(node => connectedNodeIds.has(node.id));
      if (orphanedCount > 0) {
        console.log(`[EntityGraph] Filtered out ${orphanedCount} orphaned nodes`);
      }
    }

    // Apply focus filter if a node is focused
    if (focusedNodeId) {
      const focusedNode = nodes.find(n => n.id === focusedNodeId);
      if (focusedNode) {
        const connectedNodeIds = new Set([focusedNodeId]);
        links.forEach(link => {
          const sourceId = typeof link.source === 'object' ? (link.source as GraphNode).id : link.source;
          const targetId = typeof link.target === 'object' ? (link.target as GraphNode).id : link.target;
          
          if (sourceId === focusedNodeId) connectedNodeIds.add(targetId);
          if (targetId === focusedNodeId) connectedNodeIds.add(sourceId);
        });
        nodes = nodes.filter(n => connectedNodeIds.has(n.id));
        links = links.filter(l => {
          const sourceId = typeof l.source === 'object' ? (l.source as GraphNode).id : l.source;
          const targetId = typeof l.target === 'object' ? (l.target as GraphNode).id : l.target;
          return connectedNodeIds.has(sourceId) && connectedNodeIds.has(targetId);
        });
      }
    } else if (deduplicateNodes) {
      // Apply deduplication by merging nodes with the same name
      console.log('[EntityGraph] Starting deduplication. Original nodes:', nodes.length, 'Original links:', links.length);
      
      const idMapping = new Map(); // Maps old IDs to canonical IDs
      const uniqueNodes = new Map(); // Maps names to canonical nodes
      
      // First pass: build unique nodes and ID mapping
      nodes.forEach(node => {
        if (!uniqueNodes.has(node.name)) {
          // First occurrence of this name - use this as the canonical node
          uniqueNodes.set(node.name, node);
          idMapping.set(node.id, node.id); // Map to itself
        } else {
          // Duplicate name - map this ID to the canonical node's ID
          const canonicalNode = uniqueNodes.get(node.name);
          idMapping.set(node.id, canonicalNode.id);
          console.log(`[EntityGraph] Mapping duplicate ${node.id} (${node.name}) -> ${canonicalNode.id}`);
        }
      });
      
      console.log('[EntityGraph] ID Mapping created:', idMapping.size, 'entries');
      console.log('[EntityGraph] Unique nodes:', uniqueNodes.size);
      
      // Update nodes to unique ones
      nodes = Array.from(uniqueNodes.values());
      
      // Debug: Check if links reference valid node IDs
      const nodeIds = new Set(nodes.map(n => n.id));
      console.log('[EntityGraph] Valid node IDs after dedup:', nodeIds);
      
      // Update links to use canonical IDs
      const mappedLinks = links.map(link => {
        // Handle both string IDs and object references
        const originalSource = typeof link.source === 'object' ? (link.source as GraphNode).id : link.source;
        const originalTarget = typeof link.target === 'object' ? (link.target as GraphNode).id : link.target;
        
        const sourceId = idMapping.get(originalSource) || originalSource;
        const targetId = idMapping.get(originalTarget) || originalTarget;
        
        if (originalSource !== sourceId || originalTarget !== targetId) {
          console.log(`[EntityGraph] Remapping link: ${originalSource}->${originalTarget} to ${sourceId}->${targetId}`);
        }
        
        return {
          ...link,
          source: sourceId,
          target: targetId
        };
      });
      
      console.log('[EntityGraph] Links after mapping:', mappedLinks.length);
      
      // Filter valid links
      links = mappedLinks.filter(link => {
        const isValid = link.source !== link.target && 
                       nodeIds.has(link.source) &&
                       nodeIds.has(link.target);
        if (!isValid) {
          console.log(`[EntityGraph] Removing invalid link: ${link.source}->${link.target}`, 
                     'Self-loop:', link.source === link.target,
                     'Source exists:', nodeIds.has(link.source),
                     'Target exists:', nodeIds.has(link.target));
        }
        return isValid;
      });
      
      console.log('[EntityGraph] Links after filtering:', links.length);
      
      // Remove duplicate links (same source-target-relationship combination)
      const uniqueLinks = new Map();
      links.forEach(link => {
        const key = `${link.source}-${link.target}-${link.relationship || 'related'}`;
        if (!uniqueLinks.has(key)) {
          uniqueLinks.set(key, link);
        }
      });
      links = Array.from(uniqueLinks.values());
      
      console.log('[EntityGraph] Final deduplicated state - Nodes:', nodes.length, 'Links:', links.length);
    }

    setFilteredGraphData({ nodes, links });
  }, [graphData, focusedNodeId, deduplicateNodes, showOrphanedNodes, setFilteredGraphData]);

  // Handle search selection
  const handleSearchSelect = (value: { id: string } | null) => {
    if (value) {
      setFocusedNode(value.id);
      // Find the actual node from graphData
      const node = graphData.nodes.find(n => n.id === value.id);
      if (node) {
        setSelectedNode(node);
      }
    }
  };

  // Handle dialog close
  const handleClose = () => {
    resetFilters(); // Clear all filters
    onClose(); // Call the original onClose
  };

  return (
    <Dialog 
      open={open} 
      onClose={handleClose} 
      maxWidth="xl" 
      fullWidth
      PaperProps={{
        sx: { height: '90vh', display: 'flex', flexDirection: 'column' }
      }}
    >
      <DialogTitle sx={{ m: 0, p: 2, display: 'flex', alignItems: 'center', gap: 1 }}>
        <AccountTree />
        <Typography variant="h6">Entity Graph Visualization</Typography>
        <Box sx={{ flexGrow: 1 }} />
        
        {/* Search bar */}
        <Autocomplete
          options={graphData.nodes}
          getOptionLabel={(option) => option.name}
          groupBy={(option) => option.type}
          sx={{ width: 350 }}
          size="small"
          value={graphData.nodes.find(n => n.id === focusedNodeId) || null}
          onChange={(_, value) => handleSearchSelect(value)}
          renderInput={(params) => (
            <TextField 
              {...params} 
              label={focusedNodeId ? "Focused Entity" : "Search entities..."} 
              variant="outlined"
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
                    backgroundColor: option.color,
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
        />

        {/* Controls */}
        <Tooltip title="Zoom In">
          <IconButton onClick={zoomIn} size="small">
            <ZoomInIcon />
          </IconButton>
        </Tooltip>
        <Tooltip title="Zoom Out">
          <IconButton onClick={zoomOut} size="small">
            <ZoomOutIcon />
          </IconButton>
        </Tooltip>
        <Tooltip title="Fit to Screen">
          <IconButton onClick={zoomToFit} size="small">
            <CenterIcon />
          </IconButton>
        </Tooltip>
        <Tooltip title="Refresh">
          <IconButton onClick={fetchEntityData} size="small">
            <RefreshIcon />
          </IconButton>
        </Tooltip>
        <IconButton onClick={handleClose} sx={{ ml: 2 }}>
          <CloseIcon />
        </IconButton>
      </DialogTitle>

      <DialogContent sx={{ p: 0, position: 'relative', flex: 1, overflow: 'hidden' }}>
        <Box sx={{ display: 'flex', height: '100%' }}>
          {/* Main graph container */}
          <Box sx={{ flex: 1, position: 'relative' }}>
            {/* Left side panel container - Controls and Legend */}
            <Box sx={{ 
              position: 'absolute', 
              top: 16, 
              left: 16, 
              display: 'flex',
              flexDirection: 'column',
              gap: 2,
              zIndex: 10,
              maxHeight: 'calc(100vh - 250px)',
            }}>
              {/* Controls panel */}
              <Paper sx={{ 
                p: 2, 
                width: 320,
                boxShadow: 2
              }}>
                <Stack spacing={2}>
                  <FormControlLabel
                    control={
                      <Switch checked={showInferredNodes} onChange={toggleInferredNodes} />
                    }
                    label="Show Inferred Nodes"
                  />
                  <FormControlLabel
                    control={
                      <Switch 
                        checked={deduplicateNodes} 
                        onChange={toggleDeduplication}
                        disabled={!!focusedNodeId}
                      />
                    }
                    label={focusedNodeId ? "Deduplication disabled (focused)" : "Deduplicate Nodes"}
                  />
                  <FormControlLabel
                    control={
                      <Switch 
                        checked={showOrphanedNodes} 
                        onChange={toggleOrphanedNodes}
                        disabled={!!focusedNodeId}
                      />
                    }
                    label="Show Unconnected Nodes"
                  />
                  <Divider />
                  <Typography variant="subtitle2">Line Style</Typography>
                  <Stack direction="row" spacing={1} sx={{ mb: 1 }}>
                    <Button
                      variant={linkCurvature === 0 ? "contained" : "outlined"}
                      size="small"
                      onClick={() => setLinkCurvature(0)}
                    >
                      Straight
                    </Button>
                    <Button
                      variant={linkCurvature === 0.2 ? "contained" : "outlined"}
                      size="small"
                      onClick={() => setLinkCurvature(0.2)}
                    >
                      Curved
                    </Button>
                    <Button
                      variant={linkCurvature === 0.5 ? "contained" : "outlined"}
                      size="small"
                      onClick={() => setLinkCurvature(0.5)}
                    >
                      Arc
                    </Button>
                  </Stack>
                  <Typography variant="subtitle2" sx={{ mb: 1 }}>Cluster Spacing</Typography>
                  <Box sx={{ px: 2, mb: 2 }}>
                    <Slider
                      value={centerForce}
                      onChange={(_, value) => updateForceParameters(forceStrength, linkDistance, value as number)}
                      min={0}
                      max={1}
                      step={0.1}
                      valueLabelDisplay="auto"
                      marks={[
                        { value: 0, label: 'Spread' },
                        { value: 0.5, label: 'Balanced' },
                        { value: 1, label: 'Compact' }
                      ]}
                      sx={{
                        '& .MuiSlider-markLabel': {
                          fontSize: '0.7rem',
                        },
                        '& .MuiSlider-markLabel[data-index="2"]': {
                          transform: 'translateX(-70%)',
                        }
                      }}
                    />
                  </Box>
                  <Typography variant="subtitle2">Force Strength</Typography>
                  <Slider
                    value={forceStrength}
                    onChange={(_, value) => updateForceParameters(value as number, linkDistance, centerForce)}
                    min={-1000}
                    max={-100}
                    valueLabelDisplay="auto"
                  />
                  <Typography variant="subtitle2">Link Distance</Typography>
                  <Slider
                    value={linkDistance}
                    onChange={(_, value) => updateForceParameters(forceStrength, value as number, centerForce)}
                    min={50}
                    max={500}
                    valueLabelDisplay="auto"
                  />
                </Stack>
              </Paper>

              {/* Legend - below controls */}
              <Paper sx={{ 
                p: 2, 
                width: 320,
                boxShadow: 2
              }}>
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
                      <Box sx={{
                        width: 12,
                        height: 12,
                        borderRadius: '50%',
                        backgroundColor: color,
                        border: '1px solid rgba(0,0,0,0.2)'
                      }} />
                      <Typography variant="caption">{type}</Typography>
                    </Box>
                  ))}
                </Stack>
              </Paper>
            </Box>

            {/* Stats - Top right */}
            {!loading && !error && (
              <Paper sx={{ 
                position: 'absolute', 
                top: 16, 
                right: 16, 
                p: 2, 
                zIndex: 10,
                minWidth: 150,
                boxShadow: 2
              }}>
                <Stack spacing={1}>
                  {focusedNodeId && (
                    <Chip 
                      label="Focused View" 
                      size="small" 
                      color="secondary"
                      variant="filled"
                    />
                  )}
                  <Typography variant="caption">
                    Nodes: {filteredGraphData.nodes.length}
                    {graphData.nodes.length !== filteredGraphData.nodes.length && 
                      ` / ${graphData.nodes.length}`}
                  </Typography>
                  <Typography variant="caption">
                    Links: {filteredGraphData.links.length}
                    {graphData.links.length !== filteredGraphData.links.length && 
                      ` / ${graphData.links.length}`}
                  </Typography>
                  {deduplicateNodes && !focusedNodeId && (
                    <Chip 
                      label="Deduplicated" 
                      size="small" 
                      variant="outlined"
                    />
                  )}
                </Stack>
              </Paper>
            )}

            {/* Loading state */}
            {loading && (
              <Box sx={{
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
              }}>
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

            {/* Graph container */}
            <div
              ref={containerRef}
              style={{
                width: '100%',
                height: '100%',
                minHeight: '600px',
                display: loading || error ? 'none' : 'block',
                backgroundColor: '#fafafa',
              }}
            />
          </Box>

          {/* Right panel - Selected node details */}
          {selectedNode && (
            <Paper sx={{ width: 400, p: 2, overflow: 'auto', maxHeight: '100%' }}>
              <Card>
                <CardContent>
                  <Stack direction="row" justifyContent="space-between" alignItems="center" sx={{ mb: 2 }}>
                    <Typography variant="h6">
                      {selectedNode.name}
                    </Typography>
                    <IconButton 
                      size="small" 
                      onClick={() => {
                        setSelectedNode(null);
                        setFocusedNode(null);
                      }}
                    >
                      <CloseIcon fontSize="small" />
                    </IconButton>
                  </Stack>
                  
                  <Stack direction="row" spacing={1} sx={{ mb: 2 }}>
                    <Chip
                      label={selectedNode.type}
                      size="small"
                      sx={{ backgroundColor: selectedNode.color, color: 'white' }}
                    />
                    {focusedNodeId === selectedNode.id && (
                      <Chip
                        label="Focused"
                        size="small"
                        color="secondary"
                        variant="outlined"
                      />
                    )}
                  </Stack>

                  {/* Connection count */}
                  <Box sx={{ mb: 2, p: 1, bgcolor: 'grey.100', borderRadius: 1 }}>
                    <Typography variant="caption" color="textSecondary">
                      Connections
                    </Typography>
                    <Typography variant="body2">
                      {graphData.links.filter((l: GraphLink) => {
                        const sourceId = typeof l.source === 'object' ? (l.source as GraphNode).id : l.source;
                        const targetId = typeof l.target === 'object' ? (l.target as GraphNode).id : l.target;
                        return sourceId === selectedNode.id || targetId === selectedNode.id;
                      }).length} relationships
                    </Typography>
                  </Box>
                  
                  {/* Attributes */}
                  {Object.entries(selectedNode.attributes || {}).length > 0 && (
                    <>
                      <Typography variant="subtitle2" gutterBottom sx={{ mt: 2 }}>
                        Attributes
                      </Typography>
                      <Stack spacing={1} sx={{ mb: 2 }}>
                        {Object.entries(selectedNode.attributes).map(([key, value]) => (
                          <Box key={key}>
                            <Typography variant="caption" color="textSecondary">
                              {key}
                            </Typography>
                            <Typography variant="body2">
                              {String(value)}
                            </Typography>
                          </Box>
                        ))}
                      </Stack>
                    </>
                  )}

                  {/* Connected Entities */}
                  <Divider sx={{ my: 2 }} />
                  <Typography variant="subtitle2" gutterBottom>
                    Connected Entities
                  </Typography>
                  <Stack spacing={1} sx={{ maxHeight: 300, overflow: 'auto', mb: 2 }}>
                    {(() => {
                      const connectedNodes = new Map();
                      
                      // Find all connected nodes
                      graphData.links.forEach((link: GraphLink) => {
                        const sourceId = typeof link.source === 'object' ? (link.source as GraphNode).id : link.source;
                        const targetId = typeof link.target === 'object' ? (link.target as GraphNode).id : link.target;
                        
                        if (sourceId === selectedNode.id) {
                          const targetNode = graphData.nodes.find((n: GraphNode) => n.id === targetId);
                          if (targetNode) {
                            if (!connectedNodes.has(targetNode.id)) {
                              connectedNodes.set(targetNode.id, {
                                node: targetNode,
                                relationships: []
                              });
                            }
                            connectedNodes.get(targetNode.id).relationships.push({
                              type: link.relationship || 'related_to',
                              direction: 'outgoing'
                            });
                          }
                        }
                        if (targetId === selectedNode.id) {
                          const sourceNode = graphData.nodes.find((n: GraphNode) => n.id === sourceId);
                          if (sourceNode) {
                            if (!connectedNodes.has(sourceNode.id)) {
                              connectedNodes.set(sourceNode.id, {
                                node: sourceNode,
                                relationships: []
                              });
                            }
                            connectedNodes.get(sourceNode.id).relationships.push({
                              type: link.relationship || 'related_to',
                              direction: 'incoming'
                            });
                          }
                        }
                      });

                      if (connectedNodes.size === 0) {
                        return (
                          <Typography variant="body2" color="textSecondary">
                            No connections found
                          </Typography>
                        );
                      }

                      return Array.from(connectedNodes.values()).map(({ node, relationships }) => (
                        <Card 
                          key={node.id} 
                          variant="outlined" 
                          sx={{ 
                            p: 1, 
                            cursor: 'pointer',
                            '&:hover': { bgcolor: 'action.hover' }
                          }}
                          onClick={() => {
                            setSelectedNode(node);
                            setFocusedNode(node.id);
                          }}
                        >
                          <Stack direction="row" spacing={1} alignItems="center">
                            <Box
                              sx={{
                                width: 12,
                                height: 12,
                                borderRadius: '50%',
                                backgroundColor: node.color || getEntityColor(node.type),
                                border: '1px solid rgba(0,0,0,0.2)',
                                flexShrink: 0
                              }}
                            />
                            <Box sx={{ flex: 1, minWidth: 0 }}>
                              <Typography variant="body2" noWrap>
                                {node.name}
                              </Typography>
                              <Stack direction="row" spacing={0.5} flexWrap="wrap">
                                <Typography variant="caption" color="textSecondary">
                                  {node.type}
                                </Typography>
                                {relationships.map((rel: Relationship, idx: number) => (
                                  <Chip
                                    key={idx}
                                    label={`${rel.direction === 'incoming' ? '←' : '→'} ${rel.type}`}
                                    size="small"
                                    variant="outlined"
                                    sx={{ height: 18, fontSize: '0.7rem' }}
                                  />
                                ))}
                              </Stack>
                            </Box>
                          </Stack>
                        </Card>
                      ));
                    })()}
                  </Stack>

                  <Stack direction="row" spacing={1} sx={{ mt: 2 }}>
                    {focusedNodeId !== selectedNode.id && (
                      <Button
                        variant="contained"
                        size="small"
                        onClick={() => setFocusedNode(selectedNode.id)}
                      >
                        Focus on Node
                      </Button>
                    )}
                    {focusedNodeId === selectedNode.id && (
                      <Button
                        variant="outlined"
                        size="small"
                        onClick={() => {
                          setFocusedNode(null);
                        }}
                      >
                        Show All
                      </Button>
                    )}
                  </Stack>
                </CardContent>
              </Card>
            </Paper>
          )}
        </Box>
      </DialogContent>
    </Dialog>
  );
};

export default EntityGraphVisualization;