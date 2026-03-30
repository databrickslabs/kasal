import React, { useEffect, useRef, useCallback } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  IconButton,
  Box,
  Typography,
  Alert,
  CircularProgress,
  Tooltip,
  Stack,
  TextField,
  Autocomplete,
  Chip,
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
import useEntityGraphStore, { EntityNode } from '../../store/entityGraphStore';
import { useThemeStore } from '../../store/theme';
import { useEntityGraphFilters } from '../../hooks/global/useEntityGraphFilters';
import { getEntityColor } from '../../utils/entityColors';
import Logger from '../../utils/logger';
import GraphControlsPanel from './GraphControlsPanel';
import GraphLegend from './GraphLegend';
import GraphStatsPanel from './GraphStatsPanel';
import NodeDetailsPanel from './NodeDetailsPanel';

const logger = new Logger({ prefix: 'EntityGraph' });

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
}

interface EntityGraphVisualizationProps {
  open: boolean;
  onClose: () => void;
  indexName?: string;
  workspaceUrl?: string;
  endpointName?: string;
  /** When set to 'lakebase', fetches entity data from the Lakebase endpoint instead of Databricks */
  dataSource?: 'databricks' | 'lakebase';
  /** Lakebase instance name (used when dataSource is 'lakebase') */
  lakebaseInstanceName?: string;
}

const EntityGraphVisualization: React.FC<EntityGraphVisualizationProps> = ({
  open,
  onClose,
  indexName,
  workspaceUrl,
  endpointName,
  dataSource = 'databricks',
  lakebaseInstanceName,
}) => {
  const containerRef = useRef<HTMLDivElement>(null);
  const { isDarkMode } = useThemeStore();

  // Use individual selectors to avoid re-renders from hover/highlight state changes
  const graphData = useEntityGraphStore((s) => s.graphData);
  const loading = useEntityGraphStore((s) => s.loading);
  const error = useEntityGraphStore((s) => s.error);
  const focusedNodeId = useEntityGraphStore((s) => s.focusedNodeId);
  const selectedNode = useEntityGraphStore((s) => s.selectedNode);
  const hiddenEntityTypes = useEntityGraphStore((s) => s.hiddenEntityTypes);
  const storeDarkMode = useEntityGraphStore((s) => s.isDarkMode);
  const initializeGraph = useEntityGraphStore((s) => s.initializeGraph);
  const cleanupGraph = useEntityGraphStore((s) => s.cleanupGraph);
  const setGraphData = useEntityGraphStore((s) => s.setGraphData);
  const setFilteredGraphData = useEntityGraphStore((s) => s.setFilteredGraphData);
  const setLoading = useEntityGraphStore((s) => s.setLoading);
  const setError = useEntityGraphStore((s) => s.setError);
  const setFocusedNode = useEntityGraphStore((s) => s.setFocusedNode);
  const setSelectedNode = useEntityGraphStore((s) => s.setSelectedNode);
  const resetFilters = useEntityGraphStore((s) => s.resetFilters);
  const toggleEntityTypeVisibility = useEntityGraphStore((s) => s.toggleEntityTypeVisibility);
  const zoomToFit = useEntityGraphStore((s) => s.zoomToFit);
  const zoomIn = useEntityGraphStore((s) => s.zoomIn);
  const zoomOut = useEntityGraphStore((s) => s.zoomOut);
  const setIsDarkMode = useEntityGraphStore((s) => s.setIsDarkMode);
  const centerOnNode = useEntityGraphStore((s) => s.centerOnNode);

  const { filteredNodes, filteredLinks, availableEntityTypes } = useEntityGraphFilters();

  // Sync theme dark mode into the graph store
  useEffect(() => {
    if (isDarkMode !== storeDarkMode) {
      setIsDarkMode(isDarkMode);
    }
  }, [isDarkMode, storeDarkMode, setIsDarkMode]);

  // Sync filtered data into store when filters change
  useEffect(() => {
    setFilteredGraphData({ nodes: filteredNodes, links: filteredLinks });
  }, [filteredNodes, filteredLinks, setFilteredGraphData]);

  // Fetch entity data from backend
  const fetchEntityData = useCallback(async () => {
    if (dataSource === 'lakebase') {
      // Lakebase mode — no index/workspace/endpoint needed
      logger.debug('Fetching entity data from Lakebase');
    } else {
      if (!indexName || !workspaceUrl || !endpointName) {
        logger.debug('Missing required props for fetching data');
        return;
      }
    }

    logger.debug('Fetching entity data');
    setLoading(true);
    setError(null);

    try {
      let response;
      if (dataSource === 'lakebase') {
        const params: Record<string, string | number> = { entity_table: 'crew_entity_memory', limit: 200 };
        if (lakebaseInstanceName) params.instance_name = lakebaseInstanceName;
        response = await apiClient.get('/memory-backend/lakebase/entity-data', { params });
      } else {
        response = await apiClient.get('/memory-backend/databricks/entity-data', {
          params: {
            index_name: indexName,
            workspace_url: workspaceUrl,
            endpoint_name: endpointName,
          },
        });
      }

      const { entities, relationships } = response.data;
      logger.debug(`Received ${entities.length} entities and ${relationships.length} relationships`);

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

      const links = relationships
        .filter((rel: Relationship) => nodeMap.has(rel.source) && nodeMap.has(rel.target))
        .map((rel: Relationship) => ({
          source: rel.source,
          target: rel.target,
          relationship: rel.type || rel.label || 'related_to',
        }));

      const data = { nodes, links };
      logger.debug('Graph data prepared:', { nodes: data.nodes.length, links: data.links.length });
      setGraphData(data);
    } catch (err: unknown) {
      logger.error('Error fetching entity data:', err);
      const errorMessage =
        err instanceof Error
          ? err.message
          : (err as { response?: { data?: { detail?: string } } })?.response?.data?.detail ||
            'Failed to fetch entity data';
      setError(errorMessage);
    } finally {
      setLoading(false);
    }
  }, [dataSource, lakebaseInstanceName, indexName, workspaceUrl, endpointName, setGraphData, setLoading, setError]);

  // Fetch data when dialog opens
  useEffect(() => {
    if (open) {
      fetchEntityData();
    }
  }, [open, fetchEntityData]);

  // Initialize graph when container is ready and data is loaded
  useEffect(() => {
    if (!open || loading || error || !containerRef.current) return;
    if (graphData.nodes.length === 0) return;

    const timer = setTimeout(() => {
      if (containerRef.current) {
        logger.debug('Container ready, initializing graph');
        initializeGraph(containerRef.current);
      }
    }, 100);

    return () => clearTimeout(timer);
  }, [open, loading, error, graphData, initializeGraph]);

  // Cleanup on dialog close
  useEffect(() => {
    if (!open) {
      cleanupGraph();
      resetFilters();
    }
  }, [open, cleanupGraph, resetFilters]);

  // Handle search selection with camera animation
  const handleSearchSelect = (value: EntityNode | null) => {
    if (value) {
      setFocusedNode(value.id);
      setSelectedNode(value);
      centerOnNode(value.id);
    }
  };

  const handleClose = () => {
    resetFilters();
    onClose();
  };

  return (
    <Dialog
      open={open}
      onClose={handleClose}
      maxWidth="xl"
      fullWidth
      PaperProps={{ sx: { height: '90vh', display: 'flex', flexDirection: 'column' } }}
    >
      <DialogTitle sx={{ m: 0, p: 2, display: 'flex', alignItems: 'center', gap: 1 }}>
        <AccountTree />
        <Typography variant="h6">Entity Graph Visualization</Typography>
        <Box sx={{ flexGrow: 1 }} />

        {/* Entity type filter chips */}
        <Stack direction="row" spacing={0.5} sx={{ mr: 1, flexWrap: 'wrap', maxWidth: 300 }}>
          {availableEntityTypes.slice(0, 6).map(({ type, color }) => {
            const isHidden = hiddenEntityTypes.has(type);
            return (
              <Chip
                key={type}
                label={type}
                size="small"
                onClick={() => toggleEntityTypeVisibility(type)}
                sx={{
                  backgroundColor: isHidden ? 'transparent' : color + '30',
                  borderColor: color,
                  color: isHidden ? 'text.disabled' : 'text.primary',
                  textTransform: 'capitalize',
                  fontSize: '0.7rem',
                  height: 24,
                  textDecoration: isHidden ? 'line-through' : 'none',
                }}
                variant="outlined"
              />
            );
          })}
        </Stack>

        {/* Search bar */}
        <Autocomplete
          options={graphData.nodes}
          getOptionLabel={(option) => option.name}
          groupBy={(option) => option.type}
          sx={{ width: 300 }}
          size="small"
          value={graphData.nodes.find((n) => n.id === focusedNodeId) || null}
          onChange={(_, value) => handleSearchSelect(value)}
          renderInput={(params) => (
            <TextField
              {...params}
              label={focusedNodeId ? 'Focused Entity' : 'Search entities...'}
              variant="outlined"
              color={focusedNodeId ? 'secondary' : 'primary'}
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
                    border: '1px solid rgba(0,0,0,0.2)',
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

        {/* Zoom controls */}
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
          {/* Main graph area */}
          <Box sx={{ flex: 1, position: 'relative' }}>
            {/* Left panels: Controls + Legend */}
            <Box
              sx={{
                position: 'absolute',
                top: 16,
                left: 16,
                display: 'flex',
                flexDirection: 'column',
                gap: 2,
                zIndex: 10,
                maxHeight: 'calc(100vh - 250px)',
                overflowY: 'auto',
                overflowX: 'hidden',
              }}
            >
              <GraphControlsPanel />
              <GraphLegend availableEntityTypes={availableEntityTypes} />
            </Box>

            {/* Stats panel (top right) */}
            <GraphStatsPanel />

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
                  bgcolor: isDarkMode ? 'rgba(26, 26, 46, 0.9)' : 'rgba(255, 255, 255, 0.9)',
                  color: isDarkMode ? '#e0e0e0' : undefined,
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
              <Alert
                severity="warning"
                sx={{ m: 2, position: 'absolute', top: 0, left: 0, right: 0, zIndex: 20 }}
              >
                {error}
              </Alert>
            )}

            {/* Graph container - always mounted to preserve ref */}
            <div
              ref={containerRef}
              style={{
                width: '100%',
                height: '100%',
                minHeight: '600px',
                visibility: loading || error ? 'hidden' : 'visible',
                backgroundColor: isDarkMode ? '#1a1a2e' : '#fafafa',
              }}
            />
          </Box>

          {/* Right panel: Node details */}
          {selectedNode && <NodeDetailsPanel />}
        </Box>
      </DialogContent>
    </Dialog>
  );
};

export default EntityGraphVisualization;
