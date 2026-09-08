import { Box, Stack, Typography } from '@mui/material';
import type { DatabricksConfig } from '../../../api/databricks/DatabricksService';

export default function InstalledDatabricksStorage({ config }: { config: DatabricksConfig }) {
  const rows = [
    ['Default model', config.default_model || 'Not assigned'],
    ['Generated files', config.volume_path || 'No output volume assigned'],
    ['Memory and knowledge', config.lakebase_managed ? 'Lakebase · configured during installation' : 'No Lakebase resource assigned'],
  ];
  return (
    <Stack spacing={2} sx={{ my: 3 }}>
      <Typography variant="subtitle1" fontWeight={600}>Installation resources</Typography>
      {rows.map(([label, value]) => (
        <Box key={label}>
          <Typography variant="body2" fontWeight={500}>{label}</Typography>
          <Typography variant="body2" color="text.secondary" sx={{ overflowWrap: 'anywhere' }}>{value}</Typography>
        </Box>
      ))}
      <Typography variant="caption" color="text.secondary">
        Manage these resources in Databricks Apps. Knowledge uploads are embedded in Lakebase; raw uploads are not retained.
      </Typography>
    </Stack>
  );
}
