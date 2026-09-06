import React from 'react';
import { Box, Skeleton, Typography } from '@mui/material';

export const ExecutionHistorySkeleton: React.FC = () => (
  <Box role="status" aria-label="Loading job history" sx={{ height: '100%', bgcolor: 'background.paper', p: 2, boxSizing: 'border-box', overflow: 'hidden' }}>
    <Typography sx={{ fontSize: 14, fontWeight: 600, mb: 2 }}>Job history</Typography>
    {[0, 1, 2].map(row => <Box key={row} sx={{ display: 'flex', alignItems: 'center', gap: 3, height: 52 }}>
      <Box sx={{ flex: 1 }}><Skeleton width="65%" height={18} /><Skeleton width="35%" height={14} /></Box>
      <Skeleton variant="rounded" width={84} height={24} sx={{ borderRadius: 3 }} /><Skeleton width={64} height={20} />
    </Box>)}
  </Box>
);
export default ExecutionHistorySkeleton;
