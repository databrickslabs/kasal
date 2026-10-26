import { Accordion, AccordionDetails, AccordionSummary, Alert, Box, Chip, Typography } from '@mui/material';
import { ExpandMore, Storage } from '@mui/icons-material';

export interface DatabaseInfo {
  success: boolean;
  database_path?: string;
  database_type?: string;
  size_mb?: number;
  created_at?: string;
  modified_at?: string;
  tables?: Record<string, number>;
  total_tables?: number;
  error?: string;
  lakebase_enabled?: boolean;
  lakebase_instance?: string;
  lakebase_endpoint?: string;
  connection_error?: string;
}

interface Props {
  info: DatabaseInfo;
  formatSize: (size: number) => string;
  formatDate: (date: string) => string;
}

/** Keep health visible; technical metadata and row counts are available on demand. */
export default function DatabaseOverview({ info, formatSize, formatDate }: Props) {
  const backend = info.database_type?.toUpperCase() || 'Unknown';
  const records = Object.values(info.tables || {}).reduce((sum, count) => sum + count, 0);
  return <Box component="section" aria-label="Database overview" sx={{ mb: 2 }}>
    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5, flexWrap: 'wrap', py: 1 }}>
      <Box sx={{ p: 1, borderRadius: 2, bgcolor: 'action.hover', display: 'flex' }}><Storage color="action" fontSize="small" /></Box>
      <Box sx={{ flex: 1, minWidth: 180 }}>
        <Typography variant="subtitle2">Current Database Backend: {info.connection_error ? 'FALLBACK' : backend}</Typography>
        <Typography variant="caption" color="text.secondary">
          {info.connection_error ? `${backend} configured but unreachable` : 'Storage used by this installation'}
        </Typography>
      </Box>
      <Box sx={{ display: 'flex', gap: 2, flexWrap: 'wrap' }}>
        {info.total_tables != null && <Typography variant="body2">{info.total_tables} tables</Typography>}
        {info.tables && <Typography variant="body2" color="text.secondary">{records.toLocaleString()} rows</Typography>}
        {info.database_type !== 'lakebase' && <Typography variant="body2" color="text.secondary">{formatSize(info.size_mb || 0)}</Typography>}
      </Box>
    </Box>
    {info.database_type === 'lakebase' && info.lakebase_instance && <Typography variant="body2" sx={{ my: 1, overflowWrap: 'anywhere' }}>
      {info.connection_error ? `Configured Lakebase instance (NOT in use): ${info.lakebase_instance}` : `Connected to Lakebase instance: ${info.lakebase_instance}`}
    </Typography>}
    {info.connection_error && <Alert severity="error" sx={{ my: 1 }}>{info.connection_error}</Alert>}
    {info.error && !info.connection_error && <Alert severity="error" sx={{ my: 1 }}>{info.error}</Alert>}
    <Accordion disableGutters elevation={0}>
      <AccordionSummary expandIcon={<ExpandMore />} sx={{ '&&': { minHeight: 36, px: 0 }, '& .MuiAccordionSummary-content': { my: 0.5 } }}>
        <Typography variant="caption" color="text.secondary">Database details</Typography>
      </AccordionSummary>
      <AccordionDetails sx={{ '&&': { px: 0, py: 1 } }}>
        <Box component="dl" sx={{ m: 0, display: 'grid', gridTemplateColumns: { xs: '1fr', sm: '100px minmax(0, 1fr)' }, gap: 1, fontSize: 13,
          '& dt': { color: 'text.secondary' }, '& dd': { m: 0, overflowWrap: 'anywhere' } }}>
          {info.lakebase_endpoint && <><Box component="dt">Endpoint</Box><Box component="dd">{info.lakebase_endpoint}</Box></>}
          {info.database_path && <><Box component="dt">Path</Box><Box component="dd">{info.database_path}</Box></>}
          {info.created_at && <><Box component="dt">Created</Box><Box component="dd">{formatDate(info.created_at)}</Box></>}
          {info.modified_at && <><Box component="dt">Modified</Box><Box component="dd">{formatDate(info.modified_at)}</Box></>}
        </Box>
        {info.tables && <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.75, mt: 1.5, maxHeight: 200, overflowY: 'auto' }}>
          {Object.entries(info.tables).map(([table, count]) => <Chip key={table} label={`${table} (${count} rows)`} size="small" variant="outlined" />)}
        </Box>}
      </AccordionDetails>
    </Accordion>
  </Box>;
}
