import React from 'react';
import {
  Box,
  Paper,
  Stack,
  Typography,
  Chip,
  IconButton,
  Collapse,
  Divider,
  Tooltip,
} from '@mui/material';
import {
  Palette as PaletteIcon,
  ExpandLess,
  ExpandMore,
} from '@mui/icons-material';
import useEntityGraphStore from '../../store/entityGraphStore';
import { AvailableEntityType } from '../../hooks/global/useEntityGraphFilters';

interface GraphLegendProps {
  availableEntityTypes: AvailableEntityType[];
}

const GraphLegend: React.FC<GraphLegendProps> = ({ availableEntityTypes }) => {
  // Use individual selectors to prevent re-renders from unrelated state changes
  const collapsed = useEntityGraphStore((s) => s.legendPanelCollapsed);
  const hiddenEntityTypes = useEntityGraphStore((s) => s.hiddenEntityTypes);
  const setLegendPanelCollapsed = useEntityGraphStore((s) => s.setLegendPanelCollapsed);
  const toggleEntityTypeVisibility = useEntityGraphStore((s) => s.toggleEntityTypeVisibility);

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
        onClick={() => setLegendPanelCollapsed(!collapsed)}
      >
        <Stack direction="row" spacing={1} alignItems="center">
          <PaletteIcon fontSize="small" />
          <Typography variant="subtitle2">Entity Types</Typography>
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
          <Stack spacing={0.5}>
            {availableEntityTypes.map(({ type, color, count }) => {
              const isHidden = hiddenEntityTypes.has(type);
              return (
                <Box
                  key={type}
                  sx={{
                    display: 'flex',
                    alignItems: 'center',
                    gap: 1,
                    cursor: 'pointer',
                    opacity: isHidden ? 0.4 : 1,
                    '&:hover': { opacity: isHidden ? 0.6 : 0.8 },
                    transition: 'opacity 0.2s',
                  }}
                  onClick={() => toggleEntityTypeVisibility(type)}
                >
                  <Box
                    sx={{
                      width: 12,
                      height: 12,
                      borderRadius: '50%',
                      backgroundColor: color,
                      border: '1px solid rgba(0,0,0,0.2)',
                      flexShrink: 0,
                    }}
                  />
                  <Typography
                    variant="caption"
                    sx={{
                      flex: 1,
                      textTransform: 'capitalize',
                      textDecoration: isHidden ? 'line-through' : 'none',
                    }}
                  >
                    {type}
                  </Typography>
                  <Chip
                    label={count}
                    size="small"
                    variant="outlined"
                    sx={{ height: 18, fontSize: '0.65rem' }}
                  />
                </Box>
              );
            })}
            {availableEntityTypes.length === 0 && (
              <Typography variant="caption" color="text.secondary">
                No entities loaded
              </Typography>
            )}
          </Stack>
        </Box>
      </Collapse>
    </Paper>
  );
};

export default GraphLegend;
