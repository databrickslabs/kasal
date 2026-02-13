import React from 'react';
import { Paper, Stack, Typography, Chip } from '@mui/material';
import useEntityGraphStore from '../../store/entityGraphStore';

const GraphStatsPanel: React.FC = () => {
  const graphData = useEntityGraphStore((s) => s.graphData);
  const filteredGraphData = useEntityGraphStore((s) => s.filteredGraphData);
  const focusedNodeId = useEntityGraphStore((s) => s.focusedNodeId);
  const deduplicateNodes = useEntityGraphStore((s) => s.deduplicateNodes);
  const loading = useEntityGraphStore((s) => s.loading);
  const error = useEntityGraphStore((s) => s.error);

  if (loading || error) return null;

  return (
    <Paper
      sx={{
        position: 'absolute',
        top: 16,
        right: 16,
        p: 2,
        zIndex: 10,
        minWidth: 150,
        boxShadow: 2,
      }}
    >
      <Stack spacing={1}>
        {focusedNodeId && (
          <Chip label="Focused View" size="small" color="secondary" variant="filled" />
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
          <Chip label="Deduplicated" size="small" variant="outlined" />
        )}
      </Stack>
    </Paper>
  );
};

export default GraphStatsPanel;
