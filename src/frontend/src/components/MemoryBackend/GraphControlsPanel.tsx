import React from 'react';
import {
  Box,
  Paper,
  Stack,
  Typography,
  FormControlLabel,
  Switch,
  Divider,
  Button,
  Slider,
  IconButton,
  Collapse,
  Tooltip,
} from '@mui/material';
import {
  Settings as SettingsIcon,
  ExpandLess,
  ExpandMore,
} from '@mui/icons-material';
import useEntityGraphStore from '../../store/entityGraphStore';

const GraphControlsPanel: React.FC = () => {
  // Use individual selectors to prevent re-renders from unrelated state changes
  const showInferredNodes = useEntityGraphStore((s) => s.showInferredNodes);
  const deduplicateNodes = useEntityGraphStore((s) => s.deduplicateNodes);
  const showOrphanedNodes = useEntityGraphStore((s) => s.showOrphanedNodes);
  const focusedNodeId = useEntityGraphStore((s) => s.focusedNodeId);
  const linkCurvature = useEntityGraphStore((s) => s.linkCurvature);
  const forceStrength = useEntityGraphStore((s) => s.forceStrength);
  const linkDistance = useEntityGraphStore((s) => s.linkDistance);
  const centerForce = useEntityGraphStore((s) => s.centerForce);
  const collapsed = useEntityGraphStore((s) => s.controlsPanelCollapsed);
  const toggleInferredNodes = useEntityGraphStore((s) => s.toggleInferredNodes);
  const toggleDeduplication = useEntityGraphStore((s) => s.toggleDeduplication);
  const toggleOrphanedNodes = useEntityGraphStore((s) => s.toggleOrphanedNodes);
  const setLinkCurvature = useEntityGraphStore((s) => s.setLinkCurvature);
  const updateForceParameters = useEntityGraphStore((s) => s.updateForceParameters);
  const setControlsPanelCollapsed = useEntityGraphStore((s) => s.setControlsPanelCollapsed);

  return (
    <Paper sx={{ boxShadow: 2, width: 320 }}>
      <Box
        sx={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          px: 2,
          py: 1,
          cursor: 'pointer',
        }}
        onClick={() => setControlsPanelCollapsed(!collapsed)}
      >
        <Stack direction="row" spacing={1} alignItems="center">
          <SettingsIcon fontSize="small" />
          <Typography variant="subtitle2">Controls</Typography>
        </Stack>
        <Tooltip title={collapsed ? 'Expand' : 'Collapse'}>
          <IconButton size="small">
            {collapsed ? <ExpandMore /> : <ExpandLess />}
          </IconButton>
        </Tooltip>
      </Box>
      <Collapse in={!collapsed} timeout={200} unmountOnExit={false}>
        <Divider />
        <Box sx={{ p: 2 }}>
          <Stack spacing={2}>
            <FormControlLabel
              control={<Switch checked={showInferredNodes} onChange={toggleInferredNodes} />}
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
              label={focusedNodeId ? 'Deduplication disabled (focused)' : 'Deduplicate Nodes'}
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
            <Stack direction="row" spacing={1}>
              <Button
                variant={linkCurvature === 0 ? 'contained' : 'outlined'}
                size="small"
                onClick={() => setLinkCurvature(0)}
              >
                Straight
              </Button>
              <Button
                variant={linkCurvature === 0.2 ? 'contained' : 'outlined'}
                size="small"
                onClick={() => setLinkCurvature(0.2)}
              >
                Curved
              </Button>
              <Button
                variant={linkCurvature === 0.5 ? 'contained' : 'outlined'}
                size="small"
                onClick={() => setLinkCurvature(0.5)}
              >
                Arc
              </Button>
            </Stack>
            <Typography variant="subtitle2">Cluster Spacing</Typography>
            <Box sx={{ px: 2 }}>
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
                  { value: 1, label: 'Compact' },
                ]}
                sx={{
                  '& .MuiSlider-markLabel': { fontSize: '0.7rem' },
                  '& .MuiSlider-markLabel[data-index="2"]': { transform: 'translateX(-70%)' },
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
        </Box>
      </Collapse>
    </Paper>
  );
};

export default GraphControlsPanel;
