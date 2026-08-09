import { useState, useEffect, useCallback } from 'react';
import {
  Card,
  CardContent,
  Typography,
  Box,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  IconButton,
  Button,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Snackbar,
  Alert,
  AlertColor,
  CircularProgress,
  Tooltip,
  Chip,
} from '@mui/material';
import AddIcon from '@mui/icons-material/Add';
import DeleteIcon from '@mui/icons-material/Delete';
import CodeIcon from '@mui/icons-material/Code';
import InfoIcon from '@mui/icons-material/Info';
import EditIcon from '@mui/icons-material/Edit';
import { SchemaService } from '../../../api/workflow/SchemaService';
import { Schema } from '../../../types/workflow/schema';
import SchemaDialog from '../../Common/SchemaDialog';
import { schemaToFields } from '../../../utils/schemaModel';

interface NotificationState {
  open: boolean;
  message: string;
  severity: AlertColor;
}


function ObjectManagement(): JSX.Element {
  const [schemas, setSchemas] = useState<Schema[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);
  const [createDialog, setCreateDialog] = useState<boolean>(false);
  const [viewDialog, setViewDialog] = useState<boolean>(false);
  const [editDialog, setEditDialog] = useState<boolean>(false);
  const [currentSchema, setCurrentSchema] = useState<Schema | null>(null);
  const [notification, setNotification] = useState<NotificationState>({
    open: false,
    message: '',
    severity: 'success',
  });

  const showNotification = useCallback((message: string, severity: AlertColor = 'success') => {
    setNotification({ open: true, message, severity });
  }, []);

  const fetchSchemas = useCallback(async () => {
    setLoading(true);
    try {
      const schemaService = SchemaService.getInstance();
      const schemasData = await schemaService.getSchemas();
      setSchemas(schemasData);
      setError(null);
    } catch (err) {
      console.error('Error fetching schemas:', err);
      setError(err instanceof Error ? err.message : 'Error fetching schemas');
      showNotification('Failed to load schemas', 'error');
    } finally {
      setLoading(false);
    }
  }, [showNotification]);

  useEffect(() => {
    fetchSchemas();
  }, [fetchSchemas]);

  // Format JSON for display
  const formatJSON = (obj: unknown): string => {
    try {
      if (typeof obj === 'string') return JSON.stringify(JSON.parse(obj), null, 2);
      return JSON.stringify(obj || {}, null, 2);
    } catch {
      return typeof obj === 'string' ? obj : '{}';
    }
  };

  // Get field count from schema
  // Counted through the same parser the editor uses, so the number in the list
  // and the rows in the dialog can never disagree — a hand-rolled
  // `Object.keys(properties)` counted nodes the editor does not show as fields.
  const getFieldCount = (schema: Schema): number =>
    schemaToFields(schema.schema_definition).length;


  const resetForm = (): void => setCurrentSchema(null);


  // Edit — the dialog parses the definition itself, so this just points it at
  // the schema.
  const handleEdit = (schema: Schema) => {
    setCurrentSchema(schema);
    setEditDialog(true);
  };


  // Delete
  const handleDelete = async (name: string) => {
    if (!window.confirm(`Delete "${name}"?`)) return;
    try {
      const schemaService = SchemaService.getInstance();
      if (await schemaService.deleteSchema(name)) {
        await fetchSchemas();
        showNotification(`Deleted "${name}"`);
      }
    } catch (err) {
      showNotification(err instanceof Error ? err.message : 'Error', 'error');
    }
  };

  if (loading && schemas.length === 0) {
    return (
      <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', minHeight: 200 }}>
        <CircularProgress />
        <Typography variant="body2" sx={{ ml: 2 }}>
          Loading schemas...
        </Typography>
      </Box>
    );
  }

  return (
    <Card sx={{ mt: 2 }}>
      <CardContent>
        {/* Header */}
        <Box sx={{ display: 'flex', alignItems: 'center', mb: 2, justifyContent: 'space-between' }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <CodeIcon />
            <Typography variant="h6">Schemas</Typography>
            <Tooltip title="Define data structures for task outputs" arrow>
              <InfoIcon fontSize="small" color="action" />
            </Tooltip>
          </Box>
          <Button variant="contained" size="small" startIcon={<AddIcon />} onClick={() => setCreateDialog(true)}>
            New
          </Button>
        </Box>

        {error && <Alert severity="error" sx={{ mb: 2 }}>{error}</Alert>}

        {/* Table */}
        <TableContainer component={Paper} variant="outlined">
          <Table size="small">
            <TableHead>
              <TableRow>
                <TableCell>Name</TableCell>
                <TableCell>Description</TableCell>
                <TableCell align="center">Fields</TableCell>
                <TableCell align="right">Actions</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {schemas.length === 0 ? (
                <TableRow><TableCell colSpan={4} align="center">No schemas</TableCell></TableRow>
              ) : (
                schemas.map((schema) => (
                  <TableRow key={schema.id} hover>
                    <TableCell>
                      <Typography variant="body2" fontWeight={500}>{schema.name}</Typography>
                    </TableCell>
                    <TableCell>
                      <Typography variant="body2" color="text.secondary" noWrap sx={{ maxWidth: 250 }}>
                        {schema.description}
                      </Typography>
                    </TableCell>
                    <TableCell align="center">
                      <Chip label={getFieldCount(schema)} size="small" variant="outlined" />
                    </TableCell>
                    <TableCell align="right">
                      <Tooltip title="View"><IconButton size="small" onClick={() => { setCurrentSchema(schema); setViewDialog(true); }}><CodeIcon fontSize="small" /></IconButton></Tooltip>
                      <Tooltip title="Edit"><IconButton size="small" color="primary" onClick={() => handleEdit(schema)}><EditIcon fontSize="small" /></IconButton></Tooltip>
                      <Tooltip title="Delete"><IconButton size="small" color="error" onClick={() => handleDelete(schema.name)}><DeleteIcon fontSize="small" /></IconButton></Tooltip>
                    </TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </TableContainer>
      </CardContent>

      {/* One dialog for create and edit, shared with the flow router config.
          There used to be three (create here, edit here, and a separate
          create-only one in the flow builder); they had already drifted apart,
          so the same schema looked different depending on which door you
          opened it through. */}
      <SchemaDialog
        open={createDialog || editDialog}
        schema={editDialog ? currentSchema : null}
        schemaType="schema"
        onClose={() => { setCreateDialog(false); setEditDialog(false); resetForm(); }}
        onSaved={async (saved) => {
          const wasEdit = editDialog;
          setCreateDialog(false);
          setEditDialog(false);
          resetForm();
          await fetchSchemas();
          showNotification(`${wasEdit ? 'Updated' : 'Created'} "${saved.name}"`);
        }}
      />

      {/* View Dialog */}
      <Dialog open={viewDialog} onClose={() => setViewDialog(false)} maxWidth="xs" fullWidth>
        <DialogTitle sx={{ pb: 1 }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <CodeIcon fontSize="small" />
            <Typography variant="subtitle1">{currentSchema?.name}</Typography>
          </Box>
        </DialogTitle>
        <DialogContent>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 1 }}>{currentSchema?.description}</Typography>
          <Paper variant="outlined" sx={{ p: 1.5, maxHeight: 250, overflow: 'auto', fontFamily: 'monospace', fontSize: 11, whiteSpace: 'pre-wrap', bgcolor: 'grey.50' }}>
            {currentSchema && formatJSON(currentSchema.schema_definition)}
          </Paper>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setViewDialog(false)} size="small">Close</Button>
          <Button onClick={() => { setViewDialog(false); if (currentSchema) handleEdit(currentSchema); }} variant="contained" size="small" startIcon={<EditIcon />}>Edit</Button>
        </DialogActions>
      </Dialog>


      {/* Notification */}
      <Snackbar open={notification.open} autoHideDuration={3000} onClose={() => setNotification(p => ({ ...p, open: false }))}>
        <Alert onClose={() => setNotification(p => ({ ...p, open: false }))} severity={notification.severity}>{notification.message}</Alert>
      </Snackbar>
    </Card>
  );
}

export default ObjectManagement;
