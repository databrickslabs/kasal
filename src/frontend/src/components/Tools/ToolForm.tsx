import React, { useState, useEffect } from 'react';
import {
  Box,
  Button,
  Card,
  CardContent,
  TextField,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Stack,
  Typography,
  Alert,
  Snackbar,
  SelectChangeEvent,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  IconButton,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Tabs,
  Tab,
  Switch
} from '@mui/material';
import DeleteIcon from '@mui/icons-material/Delete';
import EditIcon from '@mui/icons-material/Edit';
import AddIcon from '@mui/icons-material/Add';
import { Tool, ToolIcon } from '../../types/tool';
import { Tool as ServiceTool, ToolService } from '../../api/ToolService';
import { useTranslation } from 'react-i18next';
import SecurityDisclaimer from './SecurityDisclaimer';

const toolIcons: ToolIcon[] = [
  { value: 'screwdriver-wrench', label: 'Screwdriver Wrench' },
  { value: 'search', label: 'Search' },
  { value: 'code', label: 'Code' },
  { value: 'database', label: 'Database' },
  { value: 'file', label: 'File' },
  { value: 'globe', label: 'Web' },
  { value: 'robot', label: 'Robot' },
  { value: 'cogs', label: 'Settings' },
  { value: 'development', label: 'Development' }
];

// Custom tools from tool_factory.py
const customTools = [
  'GenieTool', 
  'PerplexityTool',
  'DatabricksCustomTool',
  'DatabricksJobsTool',
  'PythonPPTXTool'
];

const convertServiceToolToTool = (serviceTool: ServiceTool): Tool => {
  // Determine the category based on the tool title
  let category: 'PreBuilt' | 'Custom' = 'PreBuilt';
  
  if (customTools.includes(serviceTool.title)) {
    category = 'Custom';
  }
  
  return {
    id: String(serviceTool.id),
    title: serviceTool.title,
    description: serviceTool.description,
    icon: serviceTool.icon,
    config: serviceTool.config,
    category,
    enabled: serviceTool.enabled !== undefined ? serviceTool.enabled : true
  };
};

const ToolForm: React.FC = () => {
  const { t } = useTranslation();
  const [formData, setFormData] = useState<Tool>({
    id: '',
    title: '',
    description: '',
    icon: '',
    config: {},
    category: 'PreBuilt'
  });
  const [tools, setTools] = useState<Tool[]>([]);
  const [filteredTools, setFilteredTools] = useState<Tool[]>([]);
  const [searchQuery, setSearchQuery] = useState('');
  const [isFormOpen, setIsFormOpen] = useState(false);
  const [isEditing, setIsEditing] = useState(false);
  const [activeTab, setActiveTab] = useState<number>(0);
  const [securityDisclaimerOpen, setSecurityDisclaimerOpen] = useState(false);
  const [pendingToggleTool, setPendingToggleTool] = useState<Tool | null>(null);
  const [notification, setNotification] = useState<{
    open: boolean;
    message: string;
    severity: 'success' | 'error';
  }>({
    open: false,
    message: '',
    severity: 'success',
  });


  useEffect(() => {
    loadTools();
  }, []);


  useEffect(() => {
    const filtered = tools.filter(tool => 
      tool.title.toLowerCase().includes(searchQuery.toLowerCase()) ||
      tool.description.toLowerCase().includes(searchQuery.toLowerCase())
    );
    setFilteredTools(filtered);
  }, [tools, searchQuery]);

  const loadTools = async () => {
    try {
      const toolsList = await ToolService.listTools();
      setTools(toolsList.map(convertServiceToolToTool));
    } catch (error) {
      console.error('Error loading tools:', error);
      setNotification({
        open: true,
        message: 'Error loading tools',
        severity: 'error',
      });
    }
  };


  const handleChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement> | SelectChangeEvent<string>
  ) => {
    const { name, value } = e.target;
    setFormData(prev => ({
      ...prev,
      [name as string]: value,
    }));
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    try {
      const { id, category, ...formDataWithoutIdAndCategory } = formData;
      const cleanFormData = {
        ...formDataWithoutIdAndCategory,
        config: formData.config ? JSON.parse(JSON.stringify(formData.config)) : {}
      };

      if (isEditing && formData.id) {
        const updatedTool = await ToolService.updateTool(Number(formData.id), cleanFormData);
        
        // Update the tools list with the new data
        setTools(prevTools => 
          prevTools.map(tool => 
            tool.id === formData.id ? convertServiceToolToTool(updatedTool) : tool
          )
        );

        setNotification({
          open: true,
          message: t('tools.regular.messages.updateSuccess'),
          severity: 'success',
        });

        // Close the form immediately after successful update
        handleCloseForm();
      } else {
        const newTool = await ToolService.createTool(cleanFormData);
        setNotification({
          open: true,
          message: t('tools.regular.messages.createSuccess'),
          severity: 'success',
        });
        setTools(prevTools => [...prevTools, convertServiceToolToTool(newTool)]);
        handleCloseForm();
      }
    } catch (error) {
      console.error('Error in handleSubmit:', error);
      setNotification({
        open: true,
        message: error instanceof Error ? error.message : t('tools.regular.messages.errorSaving'),
        severity: 'error',
      });
    }
  };

  const handleCloseForm = () => {
    setIsFormOpen(false);
    setIsEditing(false);
    setFormData({
      id: '',
      title: '',
      description: '',
      icon: '',
      config: {},
      category: 'PreBuilt'
    });
  };

  const handleOpenEditForm = async (tool: Tool) => {
    setFormData(tool);
    setIsEditing(true);
    setIsFormOpen(true);

    try {
      const latestTool = await ToolService.getTool(Number(tool.id));
      if (latestTool) {
        setFormData(convertServiceToolToTool(latestTool));
      } else {
        setNotification({
          open: true,
          message: 'Could not fetch latest tool data',
          severity: 'error',
        });
      }
    } catch (error) {
      setNotification({
        open: true,
        message: error instanceof Error ? error.message : 'Error fetching tool data',
        severity: 'error',
      });
    }
  };

  const handleDelete = async (id: string) => {
    if (window.confirm(t('tools.regular.confirmations.delete'))) {
      try {
        await ToolService.deleteTool(Number(id));
        
        // Update the tools list by removing the deleted tool
        setTools(prevTools => prevTools.filter(tool => tool.id !== id));
        setNotification({
          open: true,
          message: t('tools.regular.messages.deleteSuccess'),
          severity: 'success',
        });
      } catch (error) {
        console.error('Error deleting tool:', error);
        setNotification({
          open: true,
          message: error instanceof Error ? error.message : t('tools.regular.messages.errorDeleting'),
          severity: 'error',
        });
      }
    }
  };

  const handleToggleEnabled = async (id: string) => {
    const tool = tools.find(t => t.id === id);
    if (!tool) return;

    // If tool is currently disabled and user wants to enable it, show security disclaimer
    if (!tool.enabled) {
      setPendingToggleTool(tool);
      setSecurityDisclaimerOpen(true);
      return;
    }

    // If tool is currently enabled and user wants to disable it, proceed directly
    await performToggleEnabled(id);
  };

  const performToggleEnabled = async (id: string) => {
    try {
      const { enabled } = await ToolService.toggleToolEnabled(Number(id));
      
      // Update the tools list with the new enabled state
      const updatedTools = (prevTools: Tool[]) => 
        prevTools.map(tool => 
          tool.id === id ? { ...tool, enabled } : tool
        );
      
      setTools(updatedTools);
      
      const status = enabled ? 'enabled' : 'disabled';
      setNotification({
        open: true,
        message: `Tool ${status} successfully`,
        severity: 'success',
      });

      // Get all tools again to ensure we have the latest state
      const refreshedTools = await ToolService.listTools();
      const formattedTools = refreshedTools.map(tool => convertServiceToolToTool(tool));

      console.log("Dispatching tool state change event with tools:", formattedTools);

      // Dispatch custom event with the fresh tool data
      const toolUpdateEvent = new CustomEvent<{toolId: string; enabled: boolean; tools: Tool[]}>('toolStateChanged', { 
        detail: { 
          toolId: id, 
          enabled,
          tools: formattedTools
        } 
      });
      window.dispatchEvent(toolUpdateEvent);
    } catch (error) {
      console.error('Error toggling tool state:', error);
      setNotification({
        open: true,
        message: error instanceof Error ? error.message : 'Error toggling tool state',
        severity: 'error',
      });
    }
  };

  const handleSecurityDisclaimerConfirm = async () => {
    if (pendingToggleTool?.id) {
      await performToggleEnabled(pendingToggleTool.id);
    }
    setSecurityDisclaimerOpen(false);
    setPendingToggleTool(null);
  };

  const handleSecurityDisclaimerClose = () => {
    setSecurityDisclaimerOpen(false);
    setPendingToggleTool(null);
  };

  const handleCloseNotification = () => {
    setNotification(prev => ({ ...prev, open: false }));
  };

  const handleTabChange = (_event: React.SyntheticEvent, newValue: number) => {
    setActiveTab(newValue);
  };

  const handleSearchChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setSearchQuery(e.target.value);
  };


  // Filter tools by the current category
  const prebuiltTools = filteredTools.filter(tool => tool.category === 'PreBuilt' || !tool.category);
  const customToolsList = filteredTools.filter(tool => tool.category === 'Custom');

  return (
    <Box sx={{ p: 3 }}>
      <Stack spacing={3}>
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Typography variant="h4">{t('tools.regular.title')}</Typography>
          <Button
            variant="contained"
            startIcon={<AddIcon />}
            onClick={() => setIsFormOpen(true)}
          >
            {t('tools.regular.addTool')}
          </Button>
        </Box>

        <TextField
          fullWidth
          variant="outlined"
          placeholder={t('tools.regular.searchPlaceholder')}
          value={searchQuery}
          onChange={handleSearchChange}
          sx={{ mb: 2 }}
        />

        <Card>
          <CardContent>
            <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
              <Typography variant="h6">
                {t('tools.regular.existingTools')}
              </Typography>
              <Box>
                <IconButton
                  color="primary"
                  onClick={() => {
                    setIsEditing(false);
                    setFormData(prev => ({ ...prev, category: activeTab === 0 ? 'PreBuilt' : 'Custom' }));
                    setIsFormOpen(true);
                  }}
                  size="small"
                >
                  <AddIcon />
                </IconButton>
              </Box>
            </Box>
            
            <Tabs 
              value={activeTab} 
              onChange={handleTabChange} 
              sx={{ mb: 2 }}
            >
              <Tab label={t('tools.regular.tabs.prebuilt')} />
              <Tab label={t('tools.regular.tabs.custom')} />
            </Tabs>

            {activeTab === 0 && (
              <TableContainer component={Paper} sx={{ mt: 0 }}>
                <Table>
                  <TableHead>
                    <TableRow>
                      <TableCell>{t('tools.regular.fields.name')}</TableCell>
                      <TableCell>{t('tools.regular.fields.description')}</TableCell>
                      <TableCell align="center">{t('common.status')}</TableCell>
                      <TableCell align="center">{t('common.actions')}</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {prebuiltTools.length === 0 ? (
                      <TableRow>
                        <TableCell colSpan={4} align="center">
                          No PreBuilt tools found
                        </TableCell>
                      </TableRow>
                    ) : (
                      prebuiltTools.map((tool) => (
                        <TableRow key={tool.id}>
                          <TableCell>{tool.title}</TableCell>
                          <TableCell>
                            {tool.description.length > 100
                              ? `${tool.description.substring(0, 100)}...`
                              : tool.description}
                          </TableCell>
                          <TableCell align="center">
                            <Switch
                              checked={tool.enabled !== false}
                              onChange={() => tool.id && handleToggleEnabled(tool.id)}
                              color="primary"
                              inputProps={{ 'aria-label': 'toggle tool enabled state' }}
                            />
                          </TableCell>
                          <TableCell align="center">
                            <IconButton
                              size="small"
                              onClick={() => handleOpenEditForm(tool)}
                              aria-label={t('common.edit')}
                            >
                              <EditIcon fontSize="small" />
                            </IconButton>
                            <IconButton
                              size="small"
                              onClick={() => tool.id && handleDelete(tool.id)}
                              aria-label={t('common.delete')}
                            >
                              <DeleteIcon fontSize="small" />
                            </IconButton>
                          </TableCell>
                        </TableRow>
                      ))
                    )}
                  </TableBody>
                </Table>
              </TableContainer>
            )}

            {activeTab === 1 && (
              <TableContainer component={Paper} sx={{ mt: 0 }}>
                <Table>
                  <TableHead>
                    <TableRow>
                      <TableCell>{t('tools.regular.fields.name')}</TableCell>
                      <TableCell>{t('tools.regular.fields.description')}</TableCell>
                      <TableCell align="center">{t('common.status')}</TableCell>
                      <TableCell align="center">{t('common.actions')}</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {customToolsList.length === 0 ? (
                      <TableRow>
                        <TableCell colSpan={4} align="center">
                          No Custom tools found
                        </TableCell>
                      </TableRow>
                    ) : (
                      customToolsList.map((tool) => (
                        <TableRow key={tool.id}>
                          <TableCell>{tool.title}</TableCell>
                          <TableCell>
                            {tool.description.length > 100
                              ? `${tool.description.substring(0, 100)}...`
                              : tool.description}
                          </TableCell>
                          <TableCell align="center">
                            <Switch
                              checked={tool.enabled !== false}
                              onChange={() => tool.id && handleToggleEnabled(tool.id)}
                              color="primary"
                              inputProps={{ 'aria-label': 'toggle tool enabled state' }}
                            />
                          </TableCell>
                          <TableCell align="center">
                            <IconButton
                              size="small"
                              onClick={() => handleOpenEditForm(tool)}
                              aria-label={t('common.edit')}
                            >
                              <EditIcon fontSize="small" />
                            </IconButton>
                            <IconButton
                              size="small"
                              onClick={() => tool.id && handleDelete(tool.id)}
                              aria-label={t('common.delete')}
                            >
                              <DeleteIcon fontSize="small" />
                            </IconButton>
                          </TableCell>
                        </TableRow>
                      ))
                    )}
                  </TableBody>
                </Table>
              </TableContainer>
            )}

          </CardContent>
        </Card>

        <Dialog 
          open={isFormOpen} 
          onClose={handleCloseForm}
          maxWidth="sm"
          fullWidth
        >
          <DialogTitle>
            {isEditing ? t('tools.regular.editTool') : t('tools.regular.addTool')}
          </DialogTitle>
          <form onSubmit={handleSubmit}>
            <DialogContent>
              <Stack spacing={2}>
                <TextField
                  fullWidth
                  label={t('tools.regular.form.title')}
                  name="title"
                  value={formData.title}
                  onChange={handleChange}
                  required
                />
                <TextField
                  fullWidth
                  label={t('tools.regular.form.description')}
                  name="description"
                  value={formData.description}
                  onChange={handleChange}
                  multiline
                  rows={4}
                  required
                />
                <FormControl fullWidth>
                  <InputLabel>{t('tools.regular.form.icon')}</InputLabel>
                  <Select
                    name="icon"
                    value={formData.icon}
                    onChange={handleChange}
                    required
                  >
                    {toolIcons.map((icon) => (
                      <MenuItem key={icon.value} value={icon.value}>
                        {icon.label}
                      </MenuItem>
                    ))}
                  </Select>
                </FormControl>
                
                <FormControl fullWidth>
                  <InputLabel>{t('tools.regular.form.category')}</InputLabel>
                  <Select
                    name="category"
                    value={formData.category || 'PreBuilt'}
                    onChange={handleChange}
                    required
                  >
                    <MenuItem value="PreBuilt">{t('tools.regular.categories.prebuilt')}</MenuItem>
                    <MenuItem value="Custom">{t('tools.regular.categories.custom')}</MenuItem>
                  </Select>
                </FormControl>
                
                {/* Dynamic Config Key-Value Form */}
                <Box>
                  <Typography variant="subtitle1" gutterBottom sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    {t('tools.regular.form.configuration')}
                    <Button
                      size="small"
                      onClick={() => {
                        setFormData(prev => ({
                          ...prev,
                          config: {
                            ...prev.config,
                            '': ''  // Add empty key-value pair
                          }
                        }));
                      }}
                    >
                      {t('tools.regular.form.addParameter')}
                    </Button>
                  </Typography>
                  <Stack spacing={2}>
                    {Object.entries(formData.config || {}).map(([key, value], index) => (
                      <Stack key={index} direction="row" spacing={2} alignItems="center">
                        <TextField
                          label={t('tools.regular.form.parameterName')}
                          value={key}
                          onChange={(e) => {
                            const newKey = e.target.value;
                            setFormData(prev => {
                              const newConfig = { ...prev.config };
                              delete newConfig[key];
                              newConfig[newKey] = value;
                              return {
                                ...prev,
                                config: newConfig
                              };
                            });
                          }}
                          fullWidth
                        />
                        <TextField
                          label={t('tools.regular.form.value')}
                          value={value}
                          onChange={(e) => {
                            const newValue = e.target.value;
                            setFormData(prev => ({
                              ...prev,
                              config: {
                                ...prev.config,
                                [key]: newValue
                              }
                            }));
                          }}
                          fullWidth
                        />
                        <IconButton
                          onClick={() => {
                            setFormData(prev => {
                              const newConfig = { ...prev.config };
                              delete newConfig[key];
                              return {
                                ...prev,
                                config: newConfig
                              };
                            });
                          }}
                          color="error"
                          size="small"
                        >
                          <DeleteIcon />
                        </IconButton>
                      </Stack>
                    ))}
                  </Stack>
                </Box>
              </Stack>
            </DialogContent>
            <DialogActions>
              <Button onClick={handleCloseForm}>{t('common.cancel')}</Button>
              <Button type="submit" variant="contained" color="primary">
                {isEditing ? t('common.save') : t('common.save')}
              </Button>
            </DialogActions>
          </form>
        </Dialog>

        <Snackbar
          open={notification.open}
          autoHideDuration={6000}
          onClose={handleCloseNotification}
          anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
        >
          <Alert
            onClose={handleCloseNotification}
            severity={notification.severity}
            sx={{ width: '100%' }}
          >
            {notification.message}
          </Alert>
        </Snackbar>

        <SecurityDisclaimer
          open={securityDisclaimerOpen}
          onClose={handleSecurityDisclaimerClose}
          onConfirm={handleSecurityDisclaimerConfirm}
          tool={pendingToggleTool}
        />
      </Stack>
    </Box>
  );
};

export default ToolForm; 