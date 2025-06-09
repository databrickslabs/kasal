import React, { useState, useEffect, useCallback } from 'react';
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
  Tabs,
  Tab,
  Tooltip,
  Chip,
} from '@mui/material';
import AddIcon from '@mui/icons-material/Add';
import DeleteIcon from '@mui/icons-material/Delete';
import CodeIcon from '@mui/icons-material/Code';
import InfoIcon from '@mui/icons-material/Info';
import EditIcon from '@mui/icons-material/Edit';
import { SchemaService } from '../../../api/SchemaService';
import { Schema, SchemaCreate } from '../../../types/schema';

interface NotificationState {
  open: boolean;
  message: string;
  severity: AlertColor;
}

function ObjectManagement(): JSX.Element {
  const [schemas, setSchemas] = useState<Schema[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);
  const [editDialog, setEditDialog] = useState<boolean>(false);
  const [createDialog, setCreateDialog] = useState<boolean>(false);
  const [viewSchemaDialog, setViewSchemaDialog] = useState<boolean>(false);
  const [currentSchema, setCurrentSchema] = useState<Schema | null>(null);
  const [notification, setNotification] = useState<NotificationState>({
    open: false,
    message: '',
    severity: 'success',
  });
  const [activeTab, setActiveTab] = useState<number>(0);
  const [schemaTypes, setSchemaTypes] = useState<string[]>([]);
  const [newSchema, setNewSchema] = useState<SchemaCreate>({
    name: '',
    description: '',
    schema_type: '',
    schema_definition: {},
  });
  const [jsonErrors, setJsonErrors] = useState<Record<string, string>>({});

  // Tabs change handler
  const handleTabChange = (_event: React.SyntheticEvent, newValue: number) => {
    setActiveTab(newValue);
  };

  // Show notification helper
  const showNotification = useCallback((message: string, severity: AlertColor = 'success') => {
    setNotification({
      open: true,
      message,
      severity,
    });
  }, []);

  // Fetch schemas
  const fetchSchemas = useCallback(async () => {
    setLoading(true);
    try {
      const schemaService = SchemaService.getInstance();
      const schemasData = await schemaService.getSchemas();
      
      setSchemas(schemasData);
      
      // Extract unique schema types
      const types = Array.from(new Set(schemasData.map(schema => schema.schema_type)));
      setSchemaTypes(types);
      
      setError(null);
    } catch (error) {
      console.error('Error fetching schemas:', error);
      setError(error instanceof Error ? error.message : 'Error fetching schemas');
      showNotification('Failed to load schemas', 'error');
    } finally {
      setLoading(false);
    }
  }, [showNotification]);

  // Load schemas on component mount
  useEffect(() => {
    fetchSchemas();
  }, [fetchSchemas]);

  const handleView = (schema: Schema) => {
    console.log('View schema data:', schema);
    console.log('Schema definition type:', typeof schema.schema_definition);
    console.log('Schema definition value:', schema.schema_definition);
    setCurrentSchema(schema);
    setViewSchemaDialog(true);
  };

  const handleCreate = () => {
    setCreateDialog(true);
  };

  const handleDelete = async (schemaName: string) => {
    if (window.confirm(`Are you sure you want to delete the schema "${schemaName}"?`)) {
      try {
        const schemaService = SchemaService.getInstance();
        const success = await schemaService.deleteSchema(schemaName);
        
        if (success) {
          await fetchSchemas();
          showNotification(`Schema "${schemaName}" deleted successfully`);
        } else {
          showNotification(`Failed to delete schema "${schemaName}"`, 'error');
        }
      } catch (error) {
        showNotification(error instanceof Error ? error.message : 'Error deleting schema', 'error');
      }
    }
  };

  const handleSaveEdit = async () => {
    if (!currentSchema) return;
    
    // Reset JSON errors
    setJsonErrors({});
    
    // Validate schema_definition JSON
    let schemaDefString = '';
    if (typeof currentSchema.schema_definition === 'string') {
      schemaDefString = currentSchema.schema_definition;
    } else {
      schemaDefString = formatJSON(currentSchema.schema_definition);
    }
    
    const schemaDefinition = parseJSON(schemaDefString);
    if (!schemaDefinition) {
      setJsonErrors(prev => ({ ...prev, schema_definition: "Invalid JSON format" }));
      return;
    }
    
    // Validate other JSON fields if they exist
    let fieldDescriptions: Record<string, unknown> | undefined;
    if (currentSchema.field_descriptions) {
      let fieldDescString = '';
      if (typeof currentSchema.field_descriptions === 'string') {
        fieldDescString = currentSchema.field_descriptions;
      } else {
        fieldDescString = formatJSON(currentSchema.field_descriptions);
      }
      
      fieldDescriptions = parseJSON(fieldDescString);
      if (!fieldDescriptions) {
        setJsonErrors(prev => ({ ...prev, field_descriptions: "Invalid JSON format" }));
        return;
      }
    }
    
    let exampleData: Record<string, unknown> | undefined;
    if (currentSchema.example_data) {
      let exampleDataString = '';
      if (typeof currentSchema.example_data === 'string') {
        exampleDataString = currentSchema.example_data;
      } else {
        exampleDataString = formatJSON(currentSchema.example_data);
      }
      
      exampleData = parseJSON(exampleDataString);
      if (!exampleData) {
        setJsonErrors(prev => ({ ...prev, example_data: "Invalid JSON format" }));
        return;
      }
    }
    
    // Proceed with save if all validations pass
    try {
      // Prepare update data
      const updateData = {
        name: currentSchema.name,
        description: currentSchema.description,
        schema_type: currentSchema.schema_type,
        schema_definition: schemaDefinition,
        field_descriptions: fieldDescriptions,
        example_data: exampleData,
        keywords: currentSchema.keywords || [],
        tools: currentSchema.tools || [],
      };
      
      const schemaService = SchemaService.getInstance();
      const result = await schemaService.updateSchema(currentSchema.name, updateData);
      
      if (result) {
        setEditDialog(false);
        await fetchSchemas();
        showNotification(`Schema "${currentSchema.name}" updated successfully`);
      } else {
        showNotification(`Failed to update schema "${currentSchema.name}"`, 'error');
      }
    } catch (error) {
      showNotification(error instanceof Error ? error.message : 'An unknown error occurred', 'error');
    }
  };

  const handleCreateSubmit = async () => {
    // Reset JSON errors
    setJsonErrors({});
    
    // Validate required fields
    if (!newSchema.name || !newSchema.schema_type || !newSchema.description) {
      showNotification('Name, description, and schema type are required', 'error');
      return;
    }
    
    // Validate schema_definition format
    let schemaDefinition: Record<string, unknown>;
    if (typeof newSchema.schema_definition === 'string') {
      const parsedSchema = parseJSON(newSchema.schema_definition);
      if (!parsedSchema) {
        setJsonErrors(prev => ({ ...prev, schema_definition: "Invalid JSON format" }));
        return;
      }
      schemaDefinition = parsedSchema;
    } else {
      schemaDefinition = newSchema.schema_definition || {};
    }
    
    if (Object.keys(schemaDefinition).length === 0) {
      setJsonErrors(prev => ({ ...prev, schema_definition: "Schema cannot be empty" }));
      return;
    }
    
    // Validate other JSON fields if they exist
    let fieldDescriptions: Record<string, unknown> | undefined;
    if (newSchema.field_descriptions) {
      if (typeof newSchema.field_descriptions === 'string') {
        const parsedDesc = parseJSON(newSchema.field_descriptions);
        if (!parsedDesc) {
          setJsonErrors(prev => ({ ...prev, field_descriptions: "Invalid JSON format" }));
          return;
        }
        fieldDescriptions = parsedDesc;
      } else {
        fieldDescriptions = newSchema.field_descriptions;
      }
    }
    
    let exampleData: Record<string, unknown> | undefined;
    if (newSchema.example_data) {
      if (typeof newSchema.example_data === 'string') {
        const parsedExample = parseJSON(newSchema.example_data);
        if (!parsedExample) {
          setJsonErrors(prev => ({ ...prev, example_data: "Invalid JSON format" }));
          return;
        }
        exampleData = parsedExample;
      } else {
        exampleData = newSchema.example_data;
      }
    }
    
    try {
      // Prepare create data with validated fields
      const createData: SchemaCreate = {
        name: newSchema.name,
        description: newSchema.description,
        schema_type: newSchema.schema_type,
        schema_definition: schemaDefinition || {},
        field_descriptions: fieldDescriptions || {},
        example_data: exampleData || undefined,
        keywords: Array.isArray(newSchema.keywords) ? newSchema.keywords : [],
        tools: Array.isArray(newSchema.tools) ? newSchema.tools : [],
      };
      
      // Format check - ensure schema_definition is never an empty string or undefined
      if (!createData.schema_definition || 
          typeof createData.schema_definition === 'string' || 
          Object.keys(createData.schema_definition).length === 0) {
        // Default minimal schema
        createData.schema_definition = {
          "type": "object",
          "properties": {},
          "required": []
        };
      }
      
      console.log("Submitting schema with data:", JSON.stringify(createData, null, 2));
      
      const schemaService = SchemaService.getInstance();
      const result = await schemaService.createSchema(createData);
      
      if (result) {
        setCreateDialog(false);
        setNewSchema({
          name: '',
          description: '',
          schema_type: '',
          schema_definition: {},
        });
        await fetchSchemas();
        showNotification(`Schema "${result.name}" created successfully`);
      } else {
        showNotification('Failed to create schema', 'error');
      }
    } catch (error) {
      // Improved error handling to show detailed validation errors
      console.error(`Error creating schema "${newSchema.name}":`, error);
      
      let errorMessage = 'Error creating schema';
      
      if (error instanceof Error) {
        // Extract validation error details
        if (error.message.includes('Validation error:')) {
          try {
            // Extract the JSON part of the error message
            const jsonStart = error.message.indexOf('{');
            if (jsonStart >= 0) {
              const jsonPart = error.message.substring(jsonStart);
              const validationDetails = JSON.parse(jsonPart);
              
              // Handle different validation error formats
              if (Array.isArray(validationDetails)) {
                // Handle array of validation errors
                errorMessage = validationDetails.map(detail => 
                  detail.loc && detail.msg ? 
                  `Field '${detail.loc.join('.')}': ${detail.msg}` : 
                  detail.msg || String(detail)
                ).join(', ');
              } else if (typeof validationDetails === 'object') {
                // Handle object with validation errors
                errorMessage = Object.entries(validationDetails)
                  .map(([key, value]) => `${key}: ${value}`)
                  .join(', ');
              } else {
                // Fallback to stringify
                errorMessage = JSON.stringify(validationDetails);
              }
            } else {
              // Fallback to the error message
              errorMessage = error.message;
            }
          } catch (parseError) {
            // Fallback to the original error message if JSON parsing fails
            errorMessage = error.message;
          }
        } else {
          // Use the error message directly
          errorMessage = error.message;
        }
      } else if (typeof error === 'string') {
        errorMessage = error;
      }
      
      showNotification(`Failed to create schema: ${errorMessage}`, 'error');
    }
  };

  const handleCloseNotification = () => {
    setNotification({ ...notification, open: false });
  };

  // Helper function to safely get object keys
  const safeObjectKeys = (obj: Record<string, unknown> | undefined | null): string[] => {
    return obj ? Object.keys(obj) : [];
  };

  // Format JSON for display
  const formatJSON = (json: Record<string, unknown> | string | undefined | null): string => {
    try {
      // Handle undefined or null
      if (json === undefined || json === null) {
        return '{}';
      }
      
      // Handle the case where json is already a string (potentially with escaped quotes)
      if (typeof json === 'string') {
        // Try parsing the string to an object first
        try {
          // Check if it might be a double-stringified JSON
          if (json.startsWith('"') && json.endsWith('"')) {
            try {
              // Try to parse as double-stringified JSON
              const unescaped = JSON.parse(json);
              if (typeof unescaped === 'string') {
                const doubleUnescaped = JSON.parse(unescaped);
                return JSON.stringify(doubleUnescaped, null, 2);
              }
            } catch (e) {
              // Ignore error, continue with normal parsing
            }
          }
          
          const parsed = JSON.parse(json);
          return JSON.stringify(parsed, null, 2);
        } catch (e) {
          // If parsing fails, return as is
          return json;
        }
      }
      // Handle normal object case
      return JSON.stringify(json, null, 2);
    } catch (error) {
      console.error('Error formatting JSON:', error);
      // Return original if parsing fails
      return typeof json === 'string' ? json : JSON.stringify(json || {}, null, 2);
    }
  };

  // Parse JSON string to object, returning undefined instead of null for better type compatibility
  const parseJSON = (jsonString: string): Record<string, unknown> | undefined => {
    try {
      // Trim whitespace first to avoid common parsing issues
      const trimmed = jsonString.trim();
      
      // Check if the input is completely empty
      if (!trimmed) {
        return {};
      }
      
      // Try to fix common JSON errors before parsing
      let fixedJson = trimmed;
      
      // 1. Add missing closing brackets/braces
      const openBraces = (fixedJson.match(/{/g) || []).length;
      const closedBraces = (fixedJson.match(/}/g) || []).length;
      if (openBraces > closedBraces) {
        fixedJson += '}'.repeat(openBraces - closedBraces);
      }
      
      // 2. Add missing quotes around property names
      fixedJson = fixedJson.replace(/([{,]\s*)([a-zA-Z0-9_]+)(\s*:)/g, '$1"$2"$3');
      
      // 3. Remove trailing commas
      fixedJson = fixedJson.replace(/,\s*([}\]])/g, '$1');
      
      return JSON.parse(fixedJson);
    } catch (error) {
      console.error('Error parsing JSON:', error);
      return undefined;
    }
  };

  // Get filtered schemas by type based on active tab
  const getFilteredSchemas = (): Schema[] => {
    if (activeTab === 0) return schemas;
    const selectedType = schemaTypes[activeTab - 1];
    return schemas.filter(schema => schema.schema_type === selectedType);
  };

  // Add a helper method to safely access schema JSON
  const getSchemaData = (schema: Schema | null, field: keyof Schema) => {
    if (!schema) return null;
    
    // Special handling for schema_definition field
    if (field === 'schema_definition') {
      console.log(`Getting schema_definition for ${schema.name}`);
      const schemaData = schema.schema_definition;
      console.log('Raw schema data:', schemaData);
      
      if (!schemaData) {
        console.log('Schema data is empty');
        return {};
      }
      
      // Handle case where data might still be a string in the database
      if (typeof schemaData === 'string') {
        console.log('Schema data is a string, attempting to parse');
        try {
          return JSON.parse(schemaData);
        } catch (error) {
          console.error('Error parsing schema string:', error);
          return schemaData; // Return the string if it can't be parsed as JSON
        }
      }
      
      return schemaData;
    }
    
    const data = schema[field];
    if (!data) return null;
    
    // Handle case where data might still be a string in the database
    if (typeof data === 'string' && field !== 'name' && field !== 'description' && field !== 'schema_type') {
      try {
        return JSON.parse(data);
      } catch {
        return data;
      }
    }
    
    return data;
  };

  // Add a helper function to update JSON fields with error handling
  const updateJsonField = (
    fieldName: 'schema_definition' | 'field_descriptions' | 'example_data', 
    value: string
  ) => {
    try {
      // Try to fix and parse the JSON
      const parsedJson = parseJSON(value);
      
      // If we have a valid JSON object
      if (parsedJson !== undefined) {
        // Clear any previous error for this field
        setJsonErrors(prev => ({ ...prev, [fieldName]: '' }));
        
        // Update the schema with the parsed JSON
        setCurrentSchema(prev => {
          if (!prev) return null;
          return { ...prev, [fieldName]: parsedJson };
        });
      } 
      // Even if validation fails, still update the raw text to allow editing
      else {
        setJsonErrors(prev => ({ ...prev, [fieldName]: 'Invalid JSON format' }));
        
        // Store as string temporarily during editing
        setCurrentSchema(prev => {
          if (!prev) return null;
          return { ...prev, [fieldName]: value };
        });
      }
    } catch (error) {
      console.error(`Error updating ${fieldName}:`, error);
      setJsonErrors(prev => ({ ...prev, [fieldName]: error instanceof Error ? error.message : 'Invalid JSON' }));
    }
  };

  // Add a function to update JSON fields in the new schema form
  const updateNewSchemaJsonField = (
    fieldName: 'schema_definition' | 'field_descriptions' | 'example_data', 
    value: string
  ) => {
    try {
      // Store the original value to preserve user input during editing
      const tempField = { [fieldName]: value };
      setNewSchema(prev => ({ ...prev, ...tempField }));
      
      // Try to fix and parse the JSON in a non-blocking way
      const parsedJson = parseJSON(value);
      
      // If parsing was successful, update the field with the parsed object
      if (parsedJson !== undefined) {
        // Clear any previous error
        setJsonErrors(prev => ({ ...prev, [fieldName]: '' }));
        
        // Actually update with the parsed JSON after validation
        setNewSchema(prev => ({ ...prev, [fieldName]: parsedJson }));
      } else {
        // If parsing failed but we want to let the user continue typing
        setJsonErrors(prev => ({ ...prev, [fieldName]: 'Invalid JSON format' }));
      }
    } catch (error) {
      console.error(`Error updating ${fieldName}:`, error);
      setJsonErrors(prev => ({ ...prev, [fieldName]: error instanceof Error ? error.message : 'Invalid JSON' }));
    }
  };

  if (loading && schemas.length === 0) {
    return (
      <Box sx={{ display: 'flex', justifyContent: 'center', p: 3 }}>
        <CircularProgress />
      </Box>
    );
  }

  return (
    <Card sx={{ mt: 2 }}>
      <CardContent>
        <Box sx={{ display: 'flex', alignItems: 'center', mb: 3, justifyContent: 'space-between' }}>
          <Box sx={{ display: 'flex', alignItems: 'center' }}>
            <CodeIcon sx={{ mr: 1 }} />
            <Typography variant="h5">Object Management</Typography>
            <Tooltip title={
              <Box sx={{ p: 1 }}>
                <Typography variant="body2">
                  Object Management allows you to create, edit, and manage Pydantic schemas used throughout the system.
                </Typography>
                <Typography variant="body2" sx={{ mt: 1 }}>
                  Pydantic schemas define the structure and validation rules for data objects, ensuring 
                  consistent data formats for inputs and outputs across the application.
                </Typography>
                <Typography variant="body2" sx={{ mt: 1 }}>
                  These schemas are used for standardizing outputs from AI operations, validating API requests/responses, 
                  and enforcing type safety in your data workflows.
                </Typography>
              </Box>
            } arrow>
              <IconButton size="small" color="primary" sx={{ ml: 1 }}>
                <InfoIcon fontSize="small" />
              </IconButton>
            </Tooltip>
          </Box>
          <Button
            variant="contained"
            color="primary"
            startIcon={<AddIcon />}
            onClick={handleCreate}
          >
            Add Schema
          </Button>
        </Box>

        {error && (
          <Alert severity="error" sx={{ mb: 2 }}>
            {error}
          </Alert>
        )}

        <Box sx={{ borderBottom: 1, borderColor: 'divider', mb: 2 }}>
          <Tabs 
            value={activeTab} 
            onChange={handleTabChange}
            variant="scrollable"
            scrollButtons="auto"
          >
            <Tab label="All Schemas" />
            {schemaTypes.map((type, index) => (
              <Tab key={type} label={type} />
            ))}
          </Tabs>
        </Box>

        <TableContainer component={Paper}>
          <Table>
            <TableHead>
              <TableRow>
                <TableCell>Name</TableCell>
                <TableCell>Type</TableCell>
                <TableCell>Description</TableCell>
                <TableCell>Keywords</TableCell>
                <TableCell>Last Updated</TableCell>
                <TableCell>Actions</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {getFilteredSchemas().length === 0 ? (
                <TableRow>
                  <TableCell colSpan={6} align="center">
                    No schemas found
                  </TableCell>
                </TableRow>
              ) : (
                getFilteredSchemas().map((schema) => (
                  <TableRow key={schema.id}>
                    <TableCell>{schema.name}</TableCell>
                    <TableCell>{schema.schema_type}</TableCell>
                    <TableCell>{schema.description}</TableCell>
                    <TableCell>
                      <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
                        {schema.keywords?.map((keyword, index) => (
                          <Chip key={index} label={keyword} size="small" />
                        ))}
                      </Box>
                    </TableCell>
                    <TableCell>
                      {new Date(schema.updated_at).toLocaleString()}
                    </TableCell>
                    <TableCell>
                      <Box sx={{ display: 'flex' }}>
                        <Tooltip title="View & Edit Schema">
                          <IconButton 
                            size="small" 
                            onClick={() => handleView(schema)}
                            color="primary"
                          >
                            <CodeIcon fontSize="small" />
                          </IconButton>
                        </Tooltip>
                        <Tooltip title="Delete">
                          <IconButton 
                            size="small" 
                            onClick={() => handleDelete(schema.name)}
                            color="error"
                          >
                            <DeleteIcon fontSize="small" />
                          </IconButton>
                        </Tooltip>
                      </Box>
                    </TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </TableContainer>
      </CardContent>

      {/* View Schema Dialog */}
      <Dialog 
        open={viewSchemaDialog} 
        onClose={() => setViewSchemaDialog(false)}
        maxWidth="md"
        fullWidth
      >
        <DialogTitle>Schema: {currentSchema?.name}</DialogTitle>
        <DialogContent>
          <Box sx={{ mt: 2 }}>
            <Typography variant="subtitle1">Schema JSON:</Typography>
            {currentSchema && (
              <Paper 
                variant="outlined" 
                sx={{ 
                  p: 2, 
                  maxHeight: '400px', 
                  overflow: 'auto',
                  fontFamily: 'monospace',
                  whiteSpace: 'pre-wrap',
                  backgroundColor: '#f5f5f5'
                }}
              >
                {currentSchema.name === "CustomToolOutput" && currentSchema.schema_definition && 
                  typeof currentSchema.schema_definition === 'string' ?
                  currentSchema.schema_definition :
                  formatJSON(getSchemaData(currentSchema, 'schema_definition'))}
              </Paper>
            )}

            {getSchemaData(currentSchema, 'field_descriptions') && safeObjectKeys(getSchemaData(currentSchema, 'field_descriptions')).length > 0 && (
              <Box sx={{ mt: 2 }}>
                <Typography variant="subtitle1">Field Descriptions:</Typography>
                <Paper 
                  variant="outlined" 
                  sx={{ 
                    p: 2, 
                    maxHeight: '200px', 
                    overflow: 'auto',
                    fontFamily: 'monospace',
                    whiteSpace: 'pre-wrap',
                    backgroundColor: '#f5f5f5'
                  }}
                >
                  {formatJSON(getSchemaData(currentSchema, 'field_descriptions') || {})}
                </Paper>
              </Box>
            )}
            
            {getSchemaData(currentSchema, 'example_data') && safeObjectKeys(getSchemaData(currentSchema, 'example_data')).length > 0 && (
              <Box sx={{ mt: 2 }}>
                <Typography variant="subtitle1">Example Data:</Typography>
                <Paper 
                  variant="outlined" 
                  sx={{ 
                    p: 2, 
                    maxHeight: '200px', 
                    overflow: 'auto',
                    fontFamily: 'monospace',
                    whiteSpace: 'pre-wrap',
                    backgroundColor: '#f5f5f5'
                  }}
                >
                  {formatJSON(getSchemaData(currentSchema, 'example_data') || {})}
                </Paper>
              </Box>
            )}
          </Box>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setViewSchemaDialog(false)}>Close</Button>
          <Button 
            onClick={() => {
              setViewSchemaDialog(false);
              setCurrentSchema(currentSchema);
              setEditDialog(true);
            }} 
            variant="contained" 
            color="primary"
            startIcon={<EditIcon />}
          >
            Edit
          </Button>
        </DialogActions>
      </Dialog>

      {/* Edit Schema Dialog */}
      <Dialog 
        open={editDialog} 
        onClose={() => setEditDialog(false)}
        maxWidth="md"
        fullWidth
      >
        <DialogTitle>Edit Schema</DialogTitle>
        <DialogContent>
          <TextField
            label="Name"
            value={currentSchema?.name || ''}
            onChange={(e) => setCurrentSchema(prev => prev ? { ...prev, name: e.target.value } : null)}
            fullWidth
            margin="normal"
            disabled
          />
          <TextField
            label="Description"
            value={currentSchema?.description || ''}
            onChange={(e) => setCurrentSchema(prev => prev ? { ...prev, description: e.target.value } : null)}
            fullWidth
            margin="normal"
          />
          <TextField
            label="Schema Type"
            value={currentSchema?.schema_type || ''}
            onChange={(e) => setCurrentSchema(prev => prev ? { ...prev, schema_type: e.target.value } : null)}
            fullWidth
            margin="normal"
          />
          
          {/* Enhanced JSON editor */}
          <Box sx={{ mt: 2, mb: 2 }}>
            <Typography variant="subtitle1" gutterBottom>Schema JSON:</Typography>
            <TextField
              value={typeof currentSchema?.schema_definition === 'string' 
                ? currentSchema.schema_definition 
                : formatJSON(currentSchema?.schema_definition)}
              onChange={(e) => updateJsonField('schema_definition', e.target.value)}
              fullWidth
              multiline
              rows={10}
              variant="outlined"
              error={!!jsonErrors.schema_definition}
              helperText={jsonErrors.schema_definition || ""}
              InputProps={{
                style: { fontFamily: 'monospace' }
              }}
            />
          </Box>

          {/* Field descriptions editor */}
          <Box sx={{ mt: 2, mb: 2 }}>
            <Typography variant="subtitle1" gutterBottom>Field Descriptions:</Typography>
            <TextField
              value={typeof currentSchema?.field_descriptions === 'string' 
                ? currentSchema.field_descriptions 
                : formatJSON(getSchemaData(currentSchema, 'field_descriptions') || {})}
              onChange={(e) => updateJsonField('field_descriptions', e.target.value)}
              fullWidth
              multiline
              rows={6}
              variant="outlined"
              error={!!jsonErrors.field_descriptions}
              helperText={jsonErrors.field_descriptions || ""}
              InputProps={{
                style: { fontFamily: 'monospace' }
              }}
            />
          </Box>

          {/* Example data editor */}
          <Box sx={{ mt: 2, mb: 2 }}>
            <Typography variant="subtitle1" gutterBottom>Example Data:</Typography>
            <TextField
              value={typeof currentSchema?.example_data === 'string' 
                ? currentSchema.example_data 
                : formatJSON(getSchemaData(currentSchema, 'example_data') || {})}
              onChange={(e) => updateJsonField('example_data', e.target.value)}
              fullWidth
              multiline
              rows={10}
              variant="outlined"
              error={!!jsonErrors.example_data}
              helperText={jsonErrors.example_data || ""}
              InputProps={{
                style: { fontFamily: 'monospace' }
              }}
            />
          </Box>

          <TextField
            label="Keywords (comma separated)"
            value={currentSchema?.keywords?.join(', ') || ''}
            onChange={(e) => {
              const keywords = e.target.value.split(',').map(k => k.trim()).filter(k => k);
              setCurrentSchema(prev => prev ? { ...prev, keywords } : null);
            }}
            fullWidth
            margin="normal"
          />

          <TextField
            label="Tools (comma separated)"
            value={currentSchema?.tools?.join(', ') || ''}
            onChange={(e) => {
              const tools = e.target.value.split(',').map(t => t.trim()).filter(t => t);
              setCurrentSchema(prev => prev ? { ...prev, tools } : null);
            }}
            fullWidth
            margin="normal"
          />
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setEditDialog(false)}>Cancel</Button>
          <Button onClick={handleSaveEdit} variant="contained" color="primary">Save</Button>
        </DialogActions>
      </Dialog>

      {/* Create Schema Dialog */}
      <Dialog 
        open={createDialog} 
        onClose={() => setCreateDialog(false)}
        maxWidth="md"
        fullWidth
      >
        <DialogTitle>Create New Schema</DialogTitle>
        <DialogContent>
          <TextField
            label="Name"
            value={newSchema.name}
            onChange={(e) => setNewSchema(prev => ({ ...prev, name: e.target.value }))}
            fullWidth
            margin="normal"
            required
          />
          <TextField
            label="Description"
            value={newSchema.description}
            onChange={(e) => setNewSchema(prev => ({ ...prev, description: e.target.value }))}
            fullWidth
            margin="normal"
          />
          <TextField
            label="Schema Type"
            value={newSchema.schema_type}
            onChange={(e) => setNewSchema(prev => ({ ...prev, schema_type: e.target.value }))}
            fullWidth
            margin="normal"
            required
          />
          
          {/* Enhanced JSON editor for new schema */}
          <Box sx={{ mt: 2, mb: 2 }}>
            <Typography variant="subtitle1" gutterBottom>Schema JSON:</Typography>
            <TextField
              value={typeof newSchema.schema_definition === 'string' 
                ? newSchema.schema_definition 
                : formatJSON(newSchema.schema_definition || {})}
              onChange={(e) => updateNewSchemaJsonField('schema_definition', e.target.value)}
              fullWidth
              multiline
              rows={12}
              variant="outlined"
              required
              error={!!jsonErrors.schema_definition}
              helperText={jsonErrors.schema_definition || ""}
              InputProps={{
                style: { fontFamily: 'monospace' }
              }}
            />
          </Box>

          {/* Field descriptions editor for new schema */}
          <Box sx={{ mt: 2, mb: 2 }}>
            <Typography variant="subtitle1" gutterBottom>Field Descriptions (Optional):</Typography>
            <TextField
              value={typeof newSchema.field_descriptions === 'string'
                ? newSchema.field_descriptions
                : formatJSON(newSchema.field_descriptions || {})}
              onChange={(e) => updateNewSchemaJsonField('field_descriptions', e.target.value)}
              fullWidth
              multiline
              rows={6}
              variant="outlined"
              error={!!jsonErrors.field_descriptions}
              helperText={jsonErrors.field_descriptions || ""}
              InputProps={{
                style: { fontFamily: 'monospace' }
              }}
            />
          </Box>

          {/* Example data editor for new schema */}
          <Box sx={{ mt: 2, mb: 2 }}>
            <Typography variant="subtitle1" gutterBottom>Example Data (Optional):</Typography>
            <TextField
              value={typeof newSchema.example_data === 'string'
                ? newSchema.example_data
                : formatJSON(newSchema.example_data || {})}
              onChange={(e) => updateNewSchemaJsonField('example_data', e.target.value)}
              fullWidth
              multiline
              rows={10}
              variant="outlined"
              error={!!jsonErrors.example_data}
              helperText={jsonErrors.example_data || ""}
              InputProps={{
                style: { fontFamily: 'monospace' }
              }}
            />
          </Box>

          <TextField
            label="Keywords (comma separated)"
            value={newSchema.keywords?.join(', ') || ''}
            onChange={(e) => {
              const keywords = e.target.value.split(',').map(k => k.trim()).filter(k => k);
              setNewSchema(prev => ({ ...prev, keywords }));
            }}
            fullWidth
            margin="normal"
          />

          <TextField
            label="Tools (comma separated)"
            value={newSchema.tools?.join(', ') || ''}
            onChange={(e) => {
              const tools = e.target.value.split(',').map(t => t.trim()).filter(t => t);
              setNewSchema(prev => ({ ...prev, tools }));
            }}
            fullWidth
            margin="normal"
          />
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setCreateDialog(false)}>Cancel</Button>
          <Button onClick={handleCreateSubmit} variant="contained" color="primary">Create</Button>
        </DialogActions>
      </Dialog>

      {/* Notifications */}
      <Snackbar
        open={notification.open}
        autoHideDuration={6000}
        onClose={handleCloseNotification}
      >
        <Alert onClose={handleCloseNotification} severity={notification.severity}>
          {notification.message}
        </Alert>
      </Snackbar>
    </Card>
  );
}

export default ObjectManagement; 