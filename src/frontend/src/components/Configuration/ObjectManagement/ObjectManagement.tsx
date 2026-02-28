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
  TextField,
  Snackbar,
  Alert,
  AlertColor,
  CircularProgress,
  Tooltip,
  Chip,
  Select,
  MenuItem,
  FormControl,
  Checkbox,
  FormControlLabel,
  Accordion,
  AccordionSummary,
  AccordionDetails,
} from '@mui/material';
import AddIcon from '@mui/icons-material/Add';
import DeleteIcon from '@mui/icons-material/Delete';
import CodeIcon from '@mui/icons-material/Code';
import InfoIcon from '@mui/icons-material/Info';
import EditIcon from '@mui/icons-material/Edit';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import CheckIcon from '@mui/icons-material/Check';
import CloseIcon from '@mui/icons-material/Close';
import { SchemaService } from '../../../api/SchemaService';
import { Schema, SchemaCreate } from '../../../types/schema';

interface NotificationState {
  open: boolean;
  message: string;
  severity: AlertColor;
}

interface SchemaField {
  name: string;
  type: 'string' | 'integer' | 'number' | 'boolean' | 'array' | 'object';
  required: boolean;
}

const FIELD_TYPES = ['string', 'integer', 'number', 'boolean', 'array', 'object'] as const;

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

  // Create form state
  const [newSchemaName, setNewSchemaName] = useState('');
  const [newSchemaDescription, setNewSchemaDescription] = useState('');
  const [schemaFields, setSchemaFields] = useState<SchemaField[]>([]);

  // Inline field add state
  const [isAddingField, setIsAddingField] = useState(false);
  const [newFieldName, setNewFieldName] = useState('');
  const [newFieldType, setNewFieldType] = useState<SchemaField['type']>('string');
  const [newFieldRequired, setNewFieldRequired] = useState(false);

  // JSON import state
  const [jsonImportValue, setJsonImportValue] = useState('');
  const [jsonImportError, setJsonImportError] = useState('');

  // Edit form state
  const [editFields, setEditFields] = useState<SchemaField[]>([]);

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

  // Build JSON schema from fields
  const buildSchemaDefinition = (fields: SchemaField[]): Record<string, unknown> => ({
    type: 'object',
    properties: fields.reduce((acc, field) => ({
      ...acc,
      [field.name]: { type: field.type }
    }), {}),
    required: fields.filter(f => f.required).map(f => f.name)
  });

  // Parse JSON schema to fields
  const parseSchemaToFields = (schema: Record<string, unknown>): SchemaField[] => {
    const properties = (schema.properties || {}) as Record<string, { type?: string }>;
    const required = (schema.required || []) as string[];
    return Object.entries(properties).map(([name, prop]) => ({
      name,
      type: (prop.type as SchemaField['type']) || 'string',
      required: required.includes(name),
    }));
  };

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
  const getFieldCount = (schema: Schema): number => {
    try {
      const def = typeof schema.schema_definition === 'string'
        ? JSON.parse(schema.schema_definition)
        : schema.schema_definition;
      return Object.keys(def?.properties || {}).length;
    } catch {
      return 0;
    }
  };

  // Add field
  const handleAddField = () => {
    if (!newFieldName.trim()) {
      showNotification('Field name required', 'error');
      return;
    }
    if (schemaFields.some(f => f.name === newFieldName.trim())) {
      showNotification('Field already exists', 'error');
      return;
    }
    setSchemaFields([...schemaFields, {
      name: newFieldName.trim(),
      type: newFieldType,
      required: newFieldRequired
    }]);
    setNewFieldName('');
    setNewFieldType('string');
    setNewFieldRequired(false);
    setIsAddingField(false);
  };

  // JSON import
  const handleJsonImport = () => {
    try {
      const parsed = JSON.parse(jsonImportValue);
      setSchemaFields(parseSchemaToFields(parsed));
      setJsonImportError('');
      showNotification('Imported', 'success');
    } catch {
      setJsonImportError('Invalid JSON');
    }
  };

  // Reset form
  const resetForm = () => {
    setNewSchemaName('');
    setNewSchemaDescription('');
    setSchemaFields([]);
    setJsonImportValue('');
    setJsonImportError('');
    setIsAddingField(false);
  };

  // Create
  const handleCreate = async () => {
    if (!newSchemaName || !newSchemaDescription) {
      showNotification('Name and description required', 'error');
      return;
    }
    if (schemaFields.length === 0) {
      showNotification('Add at least one field', 'error');
      return;
    }

    try {
      const data: SchemaCreate = {
        name: newSchemaName,
        description: newSchemaDescription,
        schema_type: 'schema', // Default type
        schema_definition: buildSchemaDefinition(schemaFields),
      };

      const schemaService = SchemaService.getInstance();
      const result = await schemaService.createSchema(data);
      if (result) {
        setCreateDialog(false);
        resetForm();
        await fetchSchemas();
        showNotification(`Created "${result.name}"`);
      }
    } catch (err) {
      showNotification(err instanceof Error ? err.message : 'Error', 'error');
    }
  };

  // Edit
  const handleEdit = (schema: Schema) => {
    setCurrentSchema(schema);
    const def = typeof schema.schema_definition === 'string'
      ? JSON.parse(schema.schema_definition)
      : schema.schema_definition;
    setEditFields(parseSchemaToFields(def));
    setEditDialog(true);
  };

  const handleEditFieldChange = (index: number, field: Partial<SchemaField>) => {
    setEditFields(editFields.map((f, i) => i === index ? { ...f, ...field } : f));
  };

  const handleSaveEdit = async () => {
    if (!currentSchema) return;
    const validFields = editFields.filter(f => f.name.trim());
    if (validFields.length === 0) {
      showNotification('Add at least one field', 'error');
      return;
    }

    try {
      const data = {
        name: currentSchema.name,
        description: currentSchema.description,
        schema_type: currentSchema.schema_type,
        schema_definition: buildSchemaDefinition(validFields),
      };

      const schemaService = SchemaService.getInstance();
      const result = await schemaService.updateSchema(currentSchema.name, data);
      if (result) {
        setEditDialog(false);
        await fetchSchemas();
        showNotification(`Updated "${currentSchema.name}"`);
      }
    } catch (err) {
      showNotification(err instanceof Error ? err.message : 'Error', 'error');
    }
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

      {/* Create Dialog */}
      <Dialog open={createDialog} onClose={() => { setCreateDialog(false); resetForm(); }} maxWidth="xs" fullWidth>
        <DialogTitle>New Schema</DialogTitle>
        <DialogContent>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1.5, mt: 1 }}>
            <TextField
              label="Name"
              value={newSchemaName}
              onChange={(e) => setNewSchemaName(e.target.value)}
              size="small"
              placeholder="e.g., UserProfile"
            />
            <TextField
              label="Description"
              value={newSchemaDescription}
              onChange={(e) => setNewSchemaDescription(e.target.value)}
              size="small"
              placeholder="What this schema represents"
            />

            {/* Fields */}
            <Typography variant="caption" color="text.secondary" sx={{ mt: 1 }}>Fields</Typography>

            {schemaFields.length > 0 && (
              <Paper variant="outlined" sx={{ p: 1 }}>
                {schemaFields.map((f, i) => (
                  <Box key={i} sx={{ display: 'flex', alignItems: 'center', gap: 0.5, py: 0.5, borderBottom: i < schemaFields.length - 1 ? '1px solid' : 'none', borderColor: 'divider' }}>
                    <Typography variant="body2" sx={{ flex: 1, fontFamily: 'monospace', fontSize: 12 }}>{f.name}</Typography>
                    <Chip label={f.type} size="small" sx={{ fontSize: 10 }} />
                    {f.required && <Chip label="req" size="small" color="primary" sx={{ fontSize: 10 }} />}
                    <IconButton size="small" onClick={() => setSchemaFields(schemaFields.filter((_, idx) => idx !== i))}><DeleteIcon sx={{ fontSize: 14 }} /></IconButton>
                  </Box>
                ))}
              </Paper>
            )}

            {isAddingField ? (
              <Paper variant="outlined" sx={{ p: 1 }}>
                <Box sx={{ display: 'flex', gap: 0.5, alignItems: 'center', flexWrap: 'wrap' }}>
                  <TextField placeholder="name" value={newFieldName} onChange={(e) => setNewFieldName(e.target.value)} size="small" sx={{ width: 100 }} autoFocus InputProps={{ style: { fontSize: 12 } }} />
                  <FormControl size="small" sx={{ minWidth: 80 }}>
                    <Select value={newFieldType} onChange={(e) => setNewFieldType(e.target.value as SchemaField['type'])} sx={{ fontSize: 12 }}>
                      {FIELD_TYPES.map(t => <MenuItem key={t} value={t} sx={{ fontSize: 12 }}>{t}</MenuItem>)}
                    </Select>
                  </FormControl>
                  <FormControlLabel control={<Checkbox checked={newFieldRequired} onChange={(e) => setNewFieldRequired(e.target.checked)} size="small" />} label={<Typography variant="caption">Req</Typography>} />
                  <IconButton size="small" color="primary" onClick={handleAddField}><CheckIcon sx={{ fontSize: 16 }} /></IconButton>
                  <IconButton size="small" onClick={() => setIsAddingField(false)}><CloseIcon sx={{ fontSize: 16 }} /></IconButton>
                </Box>
              </Paper>
            ) : (
              <Button size="small" startIcon={<AddIcon />} onClick={() => setIsAddingField(true)}>Add Field</Button>
            )}

            {/* JSON Import */}
            <Accordion disableGutters sx={{ boxShadow: 'none', '&:before': { display: 'none' }, border: '1px solid', borderColor: 'divider', borderRadius: 1 }}>
              <AccordionSummary expandIcon={<ExpandMoreIcon />} sx={{ minHeight: 36, '& .MuiAccordionSummary-content': { my: 0 } }}>
                <Typography variant="caption" color="text.secondary">Paste JSON</Typography>
              </AccordionSummary>
              <AccordionDetails sx={{ pt: 0 }}>
                <TextField
                  value={jsonImportValue}
                  onChange={(e) => setJsonImportValue(e.target.value)}
                  size="small"
                  multiline
                  rows={3}
                  fullWidth
                  error={!!jsonImportError}
                  helperText={jsonImportError}
                  placeholder='{"type":"object","properties":{...}}'
                  InputProps={{ style: { fontFamily: 'monospace', fontSize: 11 } }}
                />
                <Button size="small" onClick={handleJsonImport} disabled={!jsonImportValue} sx={{ mt: 0.5 }}>Import</Button>
              </AccordionDetails>
            </Accordion>
          </Box>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => { setCreateDialog(false); resetForm(); }} size="small">Cancel</Button>
          <Button onClick={handleCreate} variant="contained" size="small">Create</Button>
        </DialogActions>
      </Dialog>

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

      {/* Edit Dialog */}
      <Dialog open={editDialog} onClose={() => setEditDialog(false)} maxWidth="xs" fullWidth>
        <DialogTitle sx={{ pb: 1 }}>Edit: {currentSchema?.name}</DialogTitle>
        <DialogContent>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1.5, mt: 1 }}>
            <TextField
              label="Description"
              value={currentSchema?.description || ''}
              onChange={(e) => setCurrentSchema(prev => prev ? { ...prev, description: e.target.value } : null)}
              size="small"
            />

            <Typography variant="caption" color="text.secondary">Fields</Typography>
            <Paper variant="outlined" sx={{ p: 1 }}>
              {editFields.map((f, i) => (
                <Box key={i} sx={{ display: 'flex', alignItems: 'center', gap: 0.5, py: 0.5, borderBottom: i < editFields.length - 1 ? '1px solid' : 'none', borderColor: 'divider' }}>
                  <TextField value={f.name} onChange={(e) => handleEditFieldChange(i, { name: e.target.value })} size="small" sx={{ width: 90 }} InputProps={{ style: { fontFamily: 'monospace', fontSize: 12 } }} />
                  <FormControl size="small" sx={{ minWidth: 70 }}>
                    <Select value={f.type} onChange={(e) => handleEditFieldChange(i, { type: e.target.value as SchemaField['type'] })} sx={{ fontSize: 11 }}>
                      {FIELD_TYPES.map(t => <MenuItem key={t} value={t} sx={{ fontSize: 11 }}>{t}</MenuItem>)}
                    </Select>
                  </FormControl>
                  <Checkbox checked={f.required} onChange={(e) => handleEditFieldChange(i, { required: e.target.checked })} size="small" />
                  <IconButton size="small" onClick={() => setEditFields(editFields.filter((_, idx) => idx !== i))}><DeleteIcon sx={{ fontSize: 14 }} /></IconButton>
                </Box>
              ))}
              <Button size="small" startIcon={<AddIcon />} onClick={() => setEditFields([...editFields, { name: '', type: 'string', required: false }])} sx={{ mt: 0.5 }}>Add</Button>
            </Paper>
          </Box>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setEditDialog(false)} size="small">Cancel</Button>
          <Button onClick={handleSaveEdit} variant="contained" size="small">Save</Button>
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
