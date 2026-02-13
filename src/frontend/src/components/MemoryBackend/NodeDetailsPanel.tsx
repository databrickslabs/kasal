import React, { useMemo } from 'react';
import {
  Box,
  Paper,
  Stack,
  Typography,
  IconButton,
  Card,
  CardContent,
  Chip,
  Button,
  Divider,
} from '@mui/material';
import { Close as CloseIcon } from '@mui/icons-material';
import useEntityGraphStore, { EntityNode, EntityLink } from '../../store/entityGraphStore';
import { getEntityColor } from '../../utils/entityColors';

interface ConnectedNodeInfo {
  node: EntityNode;
  relationships: Array<{ type: string; direction: 'incoming' | 'outgoing' }>;
}

function getNodeId(nodeOrId: string | EntityNode): string {
  return typeof nodeOrId === 'object' ? nodeOrId.id : nodeOrId;
}

const NodeDetailsPanel: React.FC = () => {
  // Use individual selectors to prevent re-renders from hover/highlight changes
  const selectedNode = useEntityGraphStore((s) => s.selectedNode);
  const focusedNodeId = useEntityGraphStore((s) => s.focusedNodeId);
  const graphData = useEntityGraphStore((s) => s.graphData);
  const setSelectedNode = useEntityGraphStore((s) => s.setSelectedNode);
  const setFocusedNode = useEntityGraphStore((s) => s.setFocusedNode);

  const connectionCount = useMemo(() => {
    if (!selectedNode) return 0;
    return graphData.links.filter((l: EntityLink) => {
      return getNodeId(l.source) === selectedNode.id || getNodeId(l.target) === selectedNode.id;
    }).length;
  }, [selectedNode, graphData.links]);

  const connectedNodes = useMemo<ConnectedNodeInfo[]>(() => {
    if (!selectedNode) return [];

    const map = new Map<string, ConnectedNodeInfo>();

    graphData.links.forEach((link: EntityLink) => {
      const sourceId = getNodeId(link.source);
      const targetId = getNodeId(link.target);

      if (sourceId === selectedNode.id) {
        const targetNode = graphData.nodes.find((n) => n.id === targetId);
        if (targetNode) {
          if (!map.has(targetNode.id)) {
            map.set(targetNode.id, { node: targetNode, relationships: [] });
          }
          map.get(targetNode.id)!.relationships.push({
            type: link.relationship || 'related_to',
            direction: 'outgoing',
          });
        }
      }
      if (targetId === selectedNode.id) {
        const sourceNode = graphData.nodes.find((n) => n.id === sourceId);
        if (sourceNode) {
          if (!map.has(sourceNode.id)) {
            map.set(sourceNode.id, { node: sourceNode, relationships: [] });
          }
          map.get(sourceNode.id)!.relationships.push({
            type: link.relationship || 'related_to',
            direction: 'incoming',
          });
        }
      }
    });

    return Array.from(map.values());
  }, [selectedNode, graphData]);

  if (!selectedNode) return null;

  return (
    <Paper sx={{ width: 400, p: 2, overflow: 'auto', maxHeight: '100%' }}>
      <Card>
        <CardContent>
          <Stack direction="row" justifyContent="space-between" alignItems="center" sx={{ mb: 2 }}>
            <Typography variant="h6">{selectedNode.name}</Typography>
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
              <Chip label="Focused" size="small" color="secondary" variant="outlined" />
            )}
          </Stack>

          {/* Connection count */}
          <Box sx={{ mb: 2, p: 1, bgcolor: 'grey.100', borderRadius: 1 }}>
            <Typography variant="caption" color="textSecondary">
              Connections
            </Typography>
            <Typography variant="body2">{connectionCount} relationships</Typography>
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
                    <Typography variant="body2">{String(value)}</Typography>
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
            {connectedNodes.length === 0 ? (
              <Typography variant="body2" color="textSecondary">
                No connections found
              </Typography>
            ) : (
              connectedNodes.map(({ node, relationships }) => (
                <Card
                  key={node.id}
                  variant="outlined"
                  sx={{
                    p: 1,
                    cursor: 'pointer',
                    '&:hover': { bgcolor: 'action.hover' },
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
                        flexShrink: 0,
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
                        {relationships.map((rel, idx) => (
                          <Chip
                            key={idx}
                            label={`${rel.direction === 'incoming' ? '\u2190' : '\u2192'} ${rel.type}`}
                            size="small"
                            variant="outlined"
                            sx={{ height: 18, fontSize: '0.7rem' }}
                          />
                        ))}
                      </Stack>
                    </Box>
                  </Stack>
                </Card>
              ))
            )}
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
                onClick={() => setFocusedNode(null)}
              >
                Show All
              </Button>
            )}
          </Stack>
        </CardContent>
      </Card>
    </Paper>
  );
};

export default NodeDetailsPanel;
