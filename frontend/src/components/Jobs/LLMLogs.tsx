import React, { useState, useEffect, useCallback } from 'react';
import {
  Card,
  CardContent,
  Typography,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  Box,
  Chip,
  IconButton,
  Collapse,
  TablePagination,
  CircularProgress,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  SelectChangeEvent,
} from '@mui/material';
import KeyboardArrowDown from '@mui/icons-material/KeyboardArrowDown';
import KeyboardArrowUp from '@mui/icons-material/KeyboardArrowUp';
import { LLMLog, LogRowProps } from '../../types/common';
import logService from '../../api/LogService';
import { useTranslation } from 'react-i18next';

// Row component for expandable details
const LogRow: React.FC<LogRowProps> = ({ log }) => {
  const [open, setOpen] = useState<boolean>(false);
  const { t } = useTranslation();

  const formatTimestamp = (timestamp: string): string => {
    return new Date(timestamp).toLocaleString();
  };

  const formatDuration = (ms: number): string => {
    return `${(ms / 1000).toFixed(2)}s`;
  };

  const formatTokens = (tokens: number | undefined): string => {
    return tokens ? tokens.toLocaleString() : 'N/A';
  };

  return (
    <>
      <TableRow sx={{ '& > *': { borderBottom: 'unset' } }}>
        <TableCell padding="checkbox">
          <IconButton
            aria-label="expand row"
            size="small"
            onClick={() => setOpen(!open)}
          >
            {open ? <KeyboardArrowUp /> : <KeyboardArrowDown />}
          </IconButton>
        </TableCell>
        <TableCell>{formatTimestamp(log.created_at)}</TableCell>
        <TableCell>{log.endpoint}</TableCell>
        <TableCell>{log.model}</TableCell>
        <TableCell align="right">{formatTokens(log.tokens_used)}</TableCell>
        <TableCell align="right">{formatDuration(log.duration_ms)}</TableCell>
        <TableCell>
          <Chip
            label={log.status}
            color={log.status === 'success' ? 'success' : 'error'}
            size="small"
            variant="outlined"
          />
        </TableCell>
      </TableRow>
      <TableRow>
        <TableCell style={{ paddingBottom: 0, paddingTop: 0 }} colSpan={7}>
          <Collapse in={open} timeout="auto" unmountOnExit>
            <Box sx={{ margin: 2 }}>
              <Typography variant="h6" gutterBottom component="div" color="primary">
                {t('logs.details.title')}
              </Typography>
              
              <Typography variant="subtitle2" gutterBottom>
                {t('logs.details.prompt')}
              </Typography>
              <Paper sx={{ p: 2, mb: 2, bgcolor: '#f5f5f5', maxHeight: '200px', overflow: 'auto' }}>
                <pre style={{ margin: 0, whiteSpace: 'pre-wrap', fontSize: '0.875rem' }}>
                  {log.prompt}
                </pre>
              </Paper>

              <Typography variant="subtitle2" gutterBottom>
                {t('logs.details.response')}
              </Typography>
              <Paper sx={{ p: 2, mb: 2, bgcolor: '#f5f5f5', maxHeight: '400px', overflow: 'auto' }}>
                <pre style={{ margin: 0, whiteSpace: 'pre-wrap', fontSize: '0.875rem' }}>
                  {log.response}
                </pre>
              </Paper>

              {log.extra_data && (
                <>
                  <Typography variant="subtitle2" gutterBottom>
                    {t('logs.details.additionalData')}
                  </Typography>
                  <Paper sx={{ p: 2, bgcolor: '#f5f5f5', maxHeight: '200px', overflow: 'auto' }}>
                    <pre style={{ margin: 0, whiteSpace: 'pre-wrap', fontSize: '0.875rem' }}>
                      {JSON.stringify(log.extra_data, null, 2)}
                    </pre>
                  </Paper>
                </>
              )}

              {log.error_message && (
                <>
                  <Typography variant="subtitle2" gutterBottom color="error">
                    {t('logs.details.error')}
                  </Typography>
                  <Paper sx={{ p: 2, bgcolor: '#fff3f3', maxHeight: '200px', overflow: 'auto' }}>
                    <pre style={{ margin: 0, whiteSpace: 'pre-wrap', color: '#d32f2f', fontSize: '0.875rem' }}>
                      {log.error_message}
                    </pre>
                  </Paper>
                </>
              )}
            </Box>
          </Collapse>
        </TableCell>
      </TableRow>
    </>
  );
};

const Logs: React.FC = () => {
  const [logs, setLogs] = useState<LLMLog[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [page, setPage] = useState<number>(0);
  const [rowsPerPage, setRowsPerPage] = useState<number>(10);
  const [endpoint, setEndpoint] = useState<string>('all');
  const { t } = useTranslation();

  const fetchLogs = useCallback(async () => {
    try {
      setLoading(true);
      const data = await logService.getLLMLogs({
        page,
        per_page: rowsPerPage,
        endpoint: endpoint !== 'all' ? endpoint : undefined
      });
      setLogs(data);
    } catch (error) {
      console.error('Error fetching logs:', error);
    } finally {
      setLoading(false);
    }
  }, [page, rowsPerPage, endpoint]);

  useEffect(() => {
    fetchLogs();
    // Refresh logs every 30 seconds
    const interval = setInterval(fetchLogs, 30000);
    return () => clearInterval(interval);
  }, [fetchLogs]);

  const handleChangePage = (event: unknown, newPage: number) => {
    setPage(newPage);
  };

  const handleChangeRowsPerPage = (event: React.ChangeEvent<HTMLInputElement>) => {
    setRowsPerPage(parseInt(event.target.value, 10));
    setPage(0);
  };

  const handleEndpointChange = (event: SelectChangeEvent) => {
    setEndpoint(event.target.value);
    setPage(0);
  };

  if (loading) {
    return (
      <Box sx={{ display: 'flex', justifyContent: 'center', p: 3 }}>
        <CircularProgress />
      </Box>
    );
  }

  return (
    <Card sx={{ mt: 8 }}>
      <CardContent>
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 3 }}>
          <Typography variant="h5">{t('logs.title')}</Typography>
          <FormControl sx={{ minWidth: 200 }}>
            <InputLabel>{t('logs.filterByEndpoint')}</InputLabel>
            <Select
              value={endpoint}
              onChange={handleEndpointChange}
              label={t('logs.filterByEndpoint')}
            >
              <MenuItem value="all">{t('logs.allEndpoints')}</MenuItem>
              <MenuItem value="generate-crew">Generate Crew</MenuItem>
              <MenuItem value="generate-task">Generate Task</MenuItem>
              <MenuItem value="generate-agent">Generate Agent</MenuItem>
            </Select>
          </FormControl>
        </Box>
        
        <TableContainer component={Paper} sx={{ maxHeight: 'calc(100vh - 250px)', overflow: 'auto' }}>
          <Table stickyHeader>
            <TableHead>
              <TableRow>
                <TableCell padding="checkbox" />
                <TableCell>{t('logs.columns.timestamp')}</TableCell>
                <TableCell>{t('logs.columns.endpoint')}</TableCell>
                <TableCell>{t('logs.columns.model')}</TableCell>
                <TableCell align="right">{t('logs.columns.tokens')}</TableCell>
                <TableCell align="right">{t('logs.columns.duration')}</TableCell>
                <TableCell>{t('logs.columns.status')}</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {logs.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={7} align="center">
                    {t('logs.noLogs')}
                  </TableCell>
                </TableRow>
              ) : (
                logs.map((log) => (
                  <LogRow key={log.id} log={log} />
                ))
              )}
            </TableBody>
          </Table>
        </TableContainer>
        <TablePagination
          component="div"
          count={-1}
          rowsPerPage={rowsPerPage}
          page={page}
          onPageChange={handleChangePage}
          onRowsPerPageChange={handleChangeRowsPerPage}
          rowsPerPageOptions={[10, 25, 50, 100]}
          labelDisplayedRows={({ from, to }) => `${from}-${to}`}
        />
      </CardContent>
    </Card>
  );
};

export default Logs; 