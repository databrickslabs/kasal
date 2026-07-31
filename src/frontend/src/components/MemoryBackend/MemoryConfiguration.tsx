/**
 * Databricks One-Click Setup Component
 * 
 * Simplified setup for Databricks Vector Search memory backend.
 * Just enter workspace URL and click setup - everything else is automatic.
 */

import React, { useState, useEffect, useRef, useCallback } from 'react';
import {
  Autocomplete,
  Box,
  Paper,
  Typography,
  Alert,
  CircularProgress,
  RadioGroup,
  FormControlLabel,
  Radio,
  Collapse,
  IconButton,
  Divider,
  Button,
  Chip,
  TextField,
} from '@mui/material';
import {
  Memory as MemoryIcon,
  Storage as StorageIcon,
  CheckCircle as CheckCircleIcon,
  Error as ErrorIcon,
  Refresh as RefreshIcon,
  Save as SaveIcon,
} from '@mui/icons-material';
import { AxiosError } from 'axios';
import { apiClient } from '../../config/api/ApiConfig';
import { useMemoryBackendStore } from '../../store/memoryBackend';
import { MemoryTuningPanel } from './MemoryTuningPanel';
import {
  MemoryBackendType,
  EndpointInfo,
  IndexInfo,
  SavedConfigInfo,
  IndexInfoState,
  DatabricksMemoryConfig as DatabricksConfig,
  LakebaseMemoryConfig,
  MemoryTuningConfig,
  DEFAULT_LAKEBASE_CONFIG,
} from '../../types/config/memoryBackend';
import { MemoryBackendService } from '../../api/memory/MemoryBackendService';
import { MemoryRecordsBrowser } from './MemoryRecordsBrowser';

export const MemoryConfiguration: React.FC = () => {
  const [mode, setMode] = useState<'disabled' | 'lakebase'>('disabled');
  const [, setDetectedWorkspaceUrl] = useState<string | null>(null);
  const [error, setError] = useState('');
  const [savedConfig, setSavedConfig] = useState<SavedConfigInfo | null>(null);
  const [, setEndpointStatuses] = useState<Record<string, EndpointInfo>>({});
  const [verifiedResources, setVerifiedResources] = useState<{ endpoints: Record<string, EndpointInfo>, indexes: Record<string, IndexInfo> } | null>(null);
  const [, setIndexInfoMap] = useState<Record<string, IndexInfoState>>({});
  const [hasCheckedInitialConfig, setHasCheckedInitialConfig] = useState(false);

  // Lakebase state
  const [lakebaseConfig, setLakebaseConfig] = useState<LakebaseMemoryConfig>(DEFAULT_LAKEBASE_CONFIG);
  const [lakebaseStatus, setLakebaseStatus] = useState<{ success: boolean; message: string } | null>(null);
  const [lakebaseLoading, setLakebaseLoading] = useState(false);
  const [lakebaseInstances, setLakebaseInstances] = useState<Array<{ name: string; state: string; capacity?: string; read_write_dns?: string; type?: 'provisioned' | 'autoscaling' }>>([]);
  const [lakebaseInstancesLoading, setLakebaseInstancesLoading] = useState(false);
  const [instanceSearch, setInstanceSearch] = useState('');
  const [instancePage, setInstancePage] = useState(1);
  const [instanceHasMore, setInstanceHasMore] = useState(false);
  const instanceSearchTimeout = useRef<ReturnType<typeof setTimeout> | null>(null);
  const [lakebaseTableStats, setLakebaseTableStats] = useState<Record<string, { table_name: string; exists: boolean; row_count: number }> | null>(null);
  const [memoryBrowserOpen, setMemoryBrowserOpen] = useState(false);
  
  const { updateConfig, config } = useMemoryBackendStore();
  
  
  // Load existing configuration and detect workspace URL on mount
  useEffect(() => {
    loadExistingConfig();
    detectWorkspaceUrl();
  }, []); // eslint-disable-line react-hooks/exhaustive-deps
  
  // Verify resources after config is loaded
  useEffect(() => {
    if (savedConfig?.workspace_url && savedConfig.backend_id) {
      // Small delay to ensure config is fully loaded
      const timer = setTimeout(() => {
        verifyActualResources();
      }, 500);
      return () => clearTimeout(timer);
    }
  }, [savedConfig?.backend_id]); // eslint-disable-line react-hooks/exhaustive-deps
  
  // Debug savedConfig changes
  useEffect(() => {
    console.log('savedConfig changed:', savedConfig);
  }, [savedConfig]);

  // Check endpoint statuses and verify resources when config changes
  useEffect(() => {
    const checkResources = async () => {
      if (savedConfig?.workspace_url) {
        await verifyActualResources();
        // Skip individual endpoint status checks since verify resources already has the data
        // if (savedConfig?.endpoints) {
        //   await checkEndpointStatuses();
        // }
      }
    };
    checkResources();
  }, [savedConfig]); // eslint-disable-line react-hooks/exhaustive-deps
  
  // Update endpoint statuses from verified resources
  useEffect(() => {
    if (verifiedResources && savedConfig?.endpoints) {
      const statuses: Record<string, EndpointInfo> = {};
      
      // Check memory endpoint
      if (savedConfig.endpoints.memory?.name) {
        if (verifiedResources.endpoints[savedConfig.endpoints.memory.name]) {
          const endpointInfo = verifiedResources.endpoints[savedConfig.endpoints.memory.name];
          statuses.memory = {
            name: endpointInfo.name,
            state: endpointInfo.state || 'UNKNOWN',
            ready: endpointInfo.ready || false,
            can_delete_indexes: endpointInfo.state === 'ONLINE'
          };
        } else {
          // Endpoint doesn't exist in Databricks
          statuses.memory = {
            name: savedConfig.endpoints.memory.name,
            state: 'NOT_FOUND',
            ready: false,
            can_delete_indexes: false
          };
        }
      }
      
      // Check document endpoint
      if (savedConfig.endpoints.document?.name) {
        if (verifiedResources.endpoints[savedConfig.endpoints.document.name]) {
          const endpointInfo = verifiedResources.endpoints[savedConfig.endpoints.document.name];
          statuses.document = {
            name: endpointInfo.name,
            state: endpointInfo.state || 'UNKNOWN',
            ready: endpointInfo.ready || false,
            can_delete_indexes: endpointInfo.state === 'ONLINE'
          };
        } else {
          // Endpoint doesn't exist in Databricks
          statuses.document = {
            name: savedConfig.endpoints.document.name,
            state: 'NOT_FOUND',
            ready: false,
            can_delete_indexes: false
          };
        }
      }
      
      setEndpointStatuses(statuses);
    }
  }, [verifiedResources, savedConfig]);
  
  const detectWorkspaceUrl = async () => {
    try {
      // Use the same approach as DatabricksConfiguration to get workspace URL from environment
      const response = await apiClient.get('/databricks/environment');
      if (response.data?.databricks_host) {
        setDetectedWorkspaceUrl(response.data.databricks_host);
        console.log(`Detected workspace URL from environment: ${response.data.databricks_host}`);
      }
    } catch (error) {
      console.log('Could not detect workspace URL from environment:', error);
    }
  };
  
  const loadExistingConfig = async () => {
    try {
      // First try to get the default memory backend configuration
      const response = await apiClient.get('/memory-backend/configs/default');
      console.log('Default config response:', response.data);
      console.log('Response data type:', typeof response.data);
      console.log('Response data keys:', response.data ? Object.keys(response.data) : 'null');
      console.log('Response databricks_config:', response.data?.databricks_config);
      
      // Check if response is empty (no default config)
      if (!response.data || Object.keys(response.data).length === 0) {
        // Try to get all configs and use the first one
        try {
          const allConfigsResponse = await apiClient.get('/memory-backend/configs');
          console.log('All configs response:', allConfigsResponse.data);
          
          if (allConfigsResponse.data && allConfigsResponse.data.length > 0) {
            // Use the first configuration
            const firstConfig = allConfigsResponse.data[0];
            processConfigResponse(firstConfig);
            setHasCheckedInitialConfig(true);
            return;
          }
        } catch {
          console.log('No memory backend configurations found');
        }
        
        console.log('No memory backend configuration found - this is normal for new users');
        setMode('disabled');  // Default to disabled when no config exists
        setHasCheckedInitialConfig(true);
        return;
      }
      
      processConfigResponse(response.data);
      setHasCheckedInitialConfig(true);
    } catch (error) {
      // Check if it's a 404 error (no default config)
      if (error instanceof AxiosError && error.response?.status === 404) {
        // Try to get all configs as fallback
        try {
          const allConfigsResponse = await apiClient.get('/memory-backend/configs');
          if (allConfigsResponse.data && allConfigsResponse.data.length > 0) {
            const firstConfig = allConfigsResponse.data[0];
            processConfigResponse(firstConfig);
            setHasCheckedInitialConfig(true);
            return;
          }
        } catch {
          console.log('No memory backend configurations found');
          setMode('disabled');  // Default to disabled when no config exists
        }
      }
      // Only log actual errors
      console.error('Failed to load existing configuration:', error);
      setMode('disabled');  // Default to disabled on error
      setHasCheckedInitialConfig(true);
    }
  };
  
  const processConfigResponse = (configData: { backend_type?: string; databricks_config?: DatabricksConfig; lakebase_config?: LakebaseMemoryConfig; cognitive_config?: MemoryTuningConfig; id?: string }) => {
    console.log('processConfigResponse - Full configData:', configData);
    console.log('processConfigResponse - backend_type:', configData?.backend_type);
    console.log('processConfigResponse - databricks_config:', configData?.databricks_config);

    // Hydrate the tuning values into the store so the panel reflects the
    // persisted values (recall speed knobs, exploration budget, memory LLM).
    if (configData?.cognitive_config) {
      updateConfig({ cognitive_config: configData.cognitive_config });
    }

    if (configData && configData.backend_type === MemoryBackendType.DATABRICKS && configData.databricks_config) {
      const config = configData.databricks_config;
      console.log('Databricks Config:', config);
      console.log('Config endpoint_name:', config.endpoint_name);
      console.log('Config document_endpoint_name:', config.document_endpoint_name);
      console.log('Config memory_index:', config.memory_index);

      const savedInfo: SavedConfigInfo = {
        backend_id: configData.id,
        workspace_url: config.workspace_url,
        catalog: config.catalog || 'ml',
        schema: config.schema || 'agents',
        endpoints: {
          memory: config.endpoint_name ? { name: config.endpoint_name } : undefined,
          document: config.document_endpoint_name ? { name: config.document_endpoint_name } : undefined,
        },
        indexes: {
          unified: config.memory_index ? { name: config.memory_index } : undefined,
          document: config.document_index ? { name: config.document_index } : undefined,
        },
      };
      console.log('SavedInfo:', savedInfo);
      setSavedConfig(savedInfo);
      setMode('lakebase');
    } else if (configData && configData.backend_type === MemoryBackendType.LAKEBASE) {
      // Lakebase backend
      setSavedConfig({
        backend_id: configData.id
      });
      if (configData.lakebase_config) {
        setLakebaseConfig(configData.lakebase_config);
        // Always refresh table stats from the live instance so tables_initialized
        // reflects reality (the persisted flag can lag behind if the user saved
        // before initializing tables).
        if (configData.lakebase_config.instance_name) {
          loadLakebaseTableStats(configData.lakebase_config.instance_name);
        }
      }
      setMode('lakebase');
      // Seed the instance search with the saved name so the Autocomplete
      // displays it (and so loadLakebaseInstances pulls the matching record
      // back into the options list).
      const savedInstance = configData.lakebase_config?.instance_name || '';
      setInstanceSearch(savedInstance);
      loadLakebaseInstances(savedInstance);
    } else if (configData && configData.backend_type === MemoryBackendType.DEFAULT) {
      // When in disabled mode, clear the saved databricks config but keep the backend_id
      setSavedConfig({
        backend_id: configData.id
      });
      setMode('disabled');
    }
  };

  const handleModeChange = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const newMode = event.target.value as 'disabled' | 'lakebase';
    setMode(newMode);

    if (newMode === 'disabled') {
      // Update store to use DEFAULT backend with all memory types disabled
      updateConfig({
        backend_type: MemoryBackendType.DEFAULT,
      });

      // Delete all configurations from the memory backend table and create disabled config
      try {
        // Use the new endpoint that deletes all configs and creates a disabled one
        const result = await MemoryBackendService.switchToDisabledMode();
        console.log('Switched to disabled mode:', result);

        // Keep the new disabled (local) config's id so memory tuning can be
        // persisted against it via PUT /configs/{id}.
        setSavedConfig(result?.id ? { backend_id: result.id } : null);

        // Show success message
        setError(''); // Clear any previous errors
      } catch (error) {
        console.error('Failed to save disabled mode:', error);
        setError('Failed to save disabled mode. Please try again.');
      }
    } else if (newMode === 'lakebase') {
      // Reset lakebase status when switching to lakebase and load instances
      setLakebaseStatus(null);
      setLakebaseConfig(DEFAULT_LAKEBASE_CONFIG);
      loadLakebaseInstances();
    }
  };

  const loadLakebaseInstances = useCallback(async (search?: string, page?: number, append?: boolean) => {
    setLakebaseInstancesLoading(true);
    try {
      const requestPage = page || 1;
      const params: Record<string, string | number> = {
        page: requestPage,
        page_size: 30,
      };
      const searchQuery = search !== undefined ? search : instanceSearch;
      if (searchQuery) {
        params.search = searchQuery;
      }
      const response = await apiClient.get<{
        items: Array<{ name: string; state: string; capacity?: string; read_write_dns?: string; type?: 'provisioned' | 'autoscaling' }>;
        total: number;
        page: number;
        total_pages: number;
      }>('/database-management/lakebase/instances', { params });
      const newItems = response.data.items || [];
      if (append) {
        setLakebaseInstances(prev => [...prev, ...newItems]);
      } else {
        setLakebaseInstances(newItems);
      }
      setInstancePage(response.data.page || 1);
      setInstanceHasMore((response.data.page || 1) < (response.data.total_pages || 1));
    } catch (err) {
      console.error('Failed to load Lakebase instances:', err);
      if (!append) setLakebaseInstances([]);
    } finally {
      setLakebaseInstancesLoading(false);
    }
  }, [instanceSearch]);

  const loadLakebaseTableStats = async (instanceName: string) => {
    try {
      const stats = await MemoryBackendService.getLakebaseTableStats(instanceName);
      setLakebaseTableStats(stats);
      const allExist = Object.values(stats).every((s: { exists: boolean }) => s.exists);
      setLakebaseConfig(prev => ({ ...prev, tables_initialized: allExist }));
    } catch {
      setLakebaseTableStats(null);
    }
  };

  const handleSaveLakebaseConfig = async () => {
    try {
      const saveResult = await apiClient.post('/memory-backend/lakebase/save-config', {
        lakebase_config: lakebaseConfig,
        cognitive_config: config.cognitive_config,
      });
      setSavedConfig({ backend_id: saveResult.data.backend_id });
      updateConfig({
        backend_type: MemoryBackendType.LAKEBASE,
        lakebase_config: lakebaseConfig,
      });
      setLakebaseStatus({ success: true, message: 'Configuration saved successfully' });
    } catch (error) {
      setLakebaseStatus({
        success: false,
        message: `Failed to save configuration: ${error instanceof Error ? error.message : 'Unknown error'}`,
      });
    }
  };

  // Persist the LOCAL (DEFAULT / SQLite) memory backend as an ACTIVE config so
  // crew execution loads its tuning (memory LLM, recall thresholds)
  // via get_active_config. Local has no connection step, so this creates the
  // active config AND saves the tuning in one call — there's no pre-existing
  // backend_id to PUT against (the chicken-and-egg that left the old button
  // permanently disabled). Lakebase saves the same cognitive_config through
  // handleSaveLakebaseConfig / Initialize Tables.
  const handleSaveLocalMemoryConfig = async () => {
    try {
      const result = await MemoryBackendService.saveDefaultConfig(config);
      if (!result.success) {
        setError(result.message || 'Failed to save local memory configuration');
        return;
      }
      if (result.backend_id) {
        setSavedConfig({ backend_id: result.backend_id });
      }
      window.dispatchEvent(
        new CustomEvent('show-notification', {
          detail: { message: 'Local memory settings saved', severity: 'success' },
        }),
      );
    } catch (saveError) {
      setError(
        `Failed to save local memory configuration: ${saveError instanceof Error ? saveError.message : 'Unknown error'}`,
      );
    }
  };



  const updateBackendConfiguration = async (updatedConfig: SavedConfigInfo) => {
    if (!updatedConfig.backend_id) {
      console.error('No backend ID found, cannot update configuration');
      return;
    }

    try {
      // Build the update payload based on the current state
      const updatePayload = {
        databricks_config: {
          workspace_url: updatedConfig.workspace_url,
          catalog: updatedConfig.catalog,
          schema: updatedConfig.schema,
          endpoint_name: updatedConfig.endpoints?.memory?.name || null,
          document_endpoint_name: updatedConfig.endpoints?.document?.name || null,
          memory_index: updatedConfig.indexes?.unified?.name || null,
          document_index: updatedConfig.indexes?.document?.name || null,
          auth_type: 'default',
          embedding_dimension: 1024
        }
      };

      const response = await apiClient.put(
        `/memory-backend/configs/${updatedConfig.backend_id}`,
        updatePayload
      );

      // Dispatch event to notify other components about memory backend configuration change
      window.dispatchEvent(new CustomEvent('memory-backend-updated', {
        detail: { config: response.data }
      }));

      if (response.data) {
        console.log('Backend configuration updated successfully');
      }
    } catch (error) {
      console.error('Failed to update backend configuration:', error);
      // Don't show error to user as the deletion was successful
    }
  };

  const verifyActualResources = async () => {
    if (!savedConfig?.workspace_url) return;
    
    try {
      const response = await apiClient.get('/memory-backend/databricks/verify-resources', {
        params: {
          workspace_url: savedConfig.workspace_url,
          backend_id: savedConfig.backend_id
        }
      });
      
      if (response.data.success) {
        console.log('Databricks resources verification:', response.data.resources);
        setVerifiedResources(response.data.resources);
        
        // Update saved config to reflect actual state
        const updatedConfig = { ...savedConfig };
        let hasChanges = false;
        
        // Check endpoints
        console.log('Checking endpoints against verified resources:');
        console.log('Verified endpoints:', response.data.resources.endpoints);
        console.log('Saved endpoints:', savedConfig.endpoints);
        
        if (savedConfig.endpoints?.memory) {
          console.log(`Checking memory endpoint: ${savedConfig.endpoints.memory.name}`);
          if (!response.data.resources.endpoints[savedConfig.endpoints.memory.name]) {
            console.log('Memory endpoint not found in Databricks, removing from config');
            updatedConfig.endpoints = { ...updatedConfig.endpoints, memory: undefined };
            hasChanges = true;
          }
        }
        if (savedConfig.endpoints?.document) {
          console.log(`Checking document endpoint: ${savedConfig.endpoints.document.name}`);
          if (!response.data.resources.endpoints[savedConfig.endpoints.document.name]) {
            console.log('Document endpoint not found in Databricks, removing from config');
            updatedConfig.endpoints = { ...updatedConfig.endpoints, document: undefined };
            hasChanges = true;
          }
        }
        
        // Check indexes
        console.log('Checking indexes against verified resources:');
        console.log('Verified indexes:', response.data.resources.indexes);
        console.log('Saved indexes:', savedConfig.indexes);
        
        if (savedConfig.indexes?.unified) {
          console.log(`Checking unified memory index: ${savedConfig.indexes.unified.name}`);
          if (!response.data.resources.indexes[savedConfig.indexes.unified.name]) {
            console.log('Unified memory index not found in Databricks, removing from config');
            updatedConfig.indexes = { ...updatedConfig.indexes, unified: undefined };
            hasChanges = true;
          }
        }
        if (savedConfig.indexes?.document) {
          console.log(`Checking document index: ${savedConfig.indexes.document.name}`);
          if (!response.data.resources.indexes[savedConfig.indexes.document.name]) {
            console.log('Document index not found in Databricks, removing from config');
            updatedConfig.indexes = { ...updatedConfig.indexes, document: undefined };
            hasChanges = true;
          }
        }
        
        if (hasChanges) {
          setSavedConfig(updatedConfig);
          // Update backend configuration to reflect actual state
          await updateBackendConfiguration(updatedConfig);
        }
      }
    } catch (error) {
      console.error('Failed to verify Databricks resources:', error);
    }
  };


  // Map UI index-type (``memory`` | ``document``) to the shape of







  const fetchIndexInfo = async (indexName: string, endpointName: string) => {
    if (!savedConfig?.workspace_url) return;
    
    // Set loading state
    setIndexInfoMap(prev => ({
      ...prev,
      [indexName]: { doc_count: 0, loading: true }
    }));
    
    try {
      const response = await apiClient.get('/memory-backend/databricks/index-info', {
        params: {
          workspace_url: savedConfig.workspace_url,
          index_name: indexName,
          endpoint_name: endpointName
        }
      });
      
      if (response.data.success) {
        setIndexInfoMap(prev => ({
          ...prev,
          [indexName]: { 
            doc_count: response.data.doc_count || 0, 
            loading: false,
            status: response.data.status || 'UNKNOWN',
            ready: response.data.ready || false,
            index_type: response.data.index_type || 'UNKNOWN'
          }
        }));
      } else {
        // Check if it's a "not found" error
        const isNotFound = response.data.message?.toLowerCase().includes('not found') || 
                          response.data.message?.toLowerCase().includes('does not exist');
        
        setIndexInfoMap(prev => ({
          ...prev,
          [indexName]: { 
            doc_count: 0, 
            loading: false, 
            error: response.data.message,
            status: isNotFound ? 'NOT_FOUND' : 'ERROR',
            ready: false,
            index_type: 'DELETED'
          }
        }));
      }
    } catch (error) {
      console.error(`Failed to fetch info for index ${indexName}:`, error);
      
      // Check if it's a 404 error
      const is404 = error instanceof AxiosError && error.response?.status === 404;
      const errorMessage = error instanceof AxiosError 
        ? (error.response?.data?.detail || error.message) 
        : 'Failed to fetch index info';
      
      setIndexInfoMap(prev => ({
        ...prev,
        [indexName]: { 
          doc_count: 0, 
          loading: false, 
          error: errorMessage,
          status: is404 ? 'NOT_FOUND' : 'ERROR',
          ready: false,
          index_type: is404 ? 'DELETED' : 'UNKNOWN'
        }
      }));
    }
  };

  // Fetch index info when savedConfig changes
  useEffect(() => {
    if (savedConfig?.indexes && savedConfig?.workspace_url) {
      const indexes = savedConfig.indexes;
      
      // Fetch info for each configured index
      if (indexes.unified?.name && savedConfig.endpoints?.memory?.name) {
        fetchIndexInfo(indexes.unified.name, savedConfig.endpoints.memory.name);
      }
      if (indexes.document?.name && savedConfig.endpoints?.document?.name) {
        fetchIndexInfo(indexes.document.name, savedConfig.endpoints.document.name);
      }
    }
  }, [savedConfig?.indexes, savedConfig?.workspace_url]); // eslint-disable-line react-hooks/exhaustive-deps



  // NOTE: the "re-seed documentation" action is gone — it drove the
  // crewai-docs scraper (/documentation-embeddings/seed-all), removed with
  // the crewai->kasal engine migration.



  // Show loading state while checking for existing configuration
  if (!hasCheckedInitialConfig) {
    return (
      <Box>
        <Box sx={{ 
          display: 'flex', 
          alignItems: 'center', 
          justifyContent: 'space-between', 
          mb: 3 
        }}>
          <Box sx={{ display: 'flex', alignItems: 'center' }}>
            <MemoryIcon sx={{ mr: 1.5, color: 'primary.main', fontSize: '1.4rem' }} />
            <Typography variant="h6">
              Memory Configuration
            </Typography>
          </Box>
        </Box>
        
        <Paper 
          variant="outlined" 
          sx={{ 
            p: 3,
            borderRadius: 2,
            bgcolor: 'background.paper',
            display: 'flex',
            justifyContent: 'center',
            alignItems: 'center',
            minHeight: 200
          }}
        >
          <Box sx={{ textAlign: 'center' }}>
            <CircularProgress size={40} sx={{ mb: 2 }} />
            <Typography variant="body2" color="text.secondary">
              Loading memory configuration...
            </Typography>
          </Box>
        </Paper>
      </Box>
    );
  }

  return (
    <Box>
      <Box sx={{ 
        display: 'flex', 
        alignItems: 'center', 
        justifyContent: 'space-between', 
        mb: 3 
      }}>
        <Box sx={{ display: 'flex', alignItems: 'center' }}>
          <MemoryIcon sx={{ mr: 1.5, color: 'primary.main', fontSize: '1.4rem' }} />
          <Typography variant="h6">
            Memory Configuration
          </Typography>
        </Box>
      </Box>
      
      <Paper 
        variant="outlined" 
        sx={{ 
          p: 3,
          borderRadius: 2,
          bgcolor: 'background.paper'
        }}
      >

      {error && (
        <Alert severity="error" sx={{ mb: 2 }} onClose={() => setError('')}>
          {error}
        </Alert>
      )}

      <RadioGroup value={mode} onChange={handleModeChange} row sx={{ mb: 2 }}>
        <FormControlLabel
          value="lakebase"
          control={<Radio size="small" />}
          label={
            <Box sx={{ display: 'flex', alignItems: 'center' }}>
              <StorageIcon sx={{ fontSize: 18, mr: 0.5 }} />
              Lakebase (pgvector)
            </Box>
          }
          sx={{ mr: 3 }}
        />
        <FormControlLabel
          value="disabled"
          control={<Radio size="small" />}
          label={
            <Box sx={{ display: 'flex', alignItems: 'center' }}>
              <MemoryIcon sx={{ fontSize: 18, mr: 0.5 }} />
              Local
            </Box>
          }
        />
      </RadioGroup>


      <Collapse in={mode === 'lakebase'}>
        <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2.5 }}>

          {/* Status alert or info */}
          {lakebaseStatus ? (
            <Alert
              severity={lakebaseStatus.success ? 'success' : 'error'}
              onClose={() => setLakebaseStatus(null)}
              sx={{ '& .MuiAlert-message': { whiteSpace: 'pre-line' } }}
            >
              {lakebaseStatus.message}
            </Alert>
          ) : (
            <Alert severity="info">
              Uses your existing Lakebase PostgreSQL instance with pgvector for vector similarity search.
              Memory tables are created alongside your application tables — no additional infrastructure required.
            </Alert>
          )}

          {/* Databricks App Setup instructions */}
          <Alert severity="warning" sx={{ '& .MuiAlert-message': { width: '100%' } }}>
            <Typography variant="subtitle2" sx={{ mb: 0.5 }}>Databricks App Setup</Typography>
            <Typography variant="body2" sx={{ mb: 0.5 }}>
              Before connecting, complete these steps so your App&apos;s service principal can
              authenticate to and use Lakebase:
            </Typography>
            <Box component="ol" sx={{ m: 0, pl: 2.5, '& li': { mb: 0.5 } }}>
              <Typography component="li" variant="body2">
                <strong>Add the Lakebase instance as a Database resource to your App</strong> in the
                Databricks UI (App → <em>Edit</em> → <em>Resources</em> → <em>Add resource</em> →{' '}
                <em>Database</em>) with permission <em>&quot;Can connect and create&quot;</em>.
                This is <strong>required</strong>: it automatically creates a linked PostgreSQL role
                for the service principal. Without it you will get{' '}
                <em>&quot;password authentication failed&quot;</em>, because a plain Postgres role is
                not bound to the App&apos;s Databricks identity.
              </Typography>
              <Typography component="li" variant="body2">
                Enable the <strong>pgvector</strong> extension <strong>once</strong> as the instance owner
                (the App&apos;s service principal cannot create it — that requires superuser):
                <Box
                  component="code"
                  sx={{
                    display: 'block',
                    mt: 0.5,
                    p: 1,
                    bgcolor: 'action.hover',
                    borderRadius: 1,
                    fontFamily: 'monospace',
                    fontSize: '0.8rem',
                  }}
                >
                  CREATE EXTENSION IF NOT EXISTS vector;
                </Box>
              </Typography>
            </Box>
            <Typography variant="caption" color="text.secondary" sx={{ mt: 0.5, display: 'block' }}>
              See{' '}
              <a
                href="https://docs.databricks.com/en/dev-tools/databricks-apps/lakebase.html"
                target="_blank"
                rel="noopener noreferrer"
                style={{ color: 'inherit' }}
              >
                Databricks Lakebase App docs
              </a>
              {' '}for details.
            </Typography>
          </Alert>

          {/* Connection section */}
          <Box>
            <Typography variant="subtitle2" color="text.secondary" sx={{ mb: 1.5, textTransform: 'uppercase', fontSize: '0.7rem', letterSpacing: '0.08em' }}>
              Connection
            </Typography>
            <Box sx={{ display: 'flex', gap: 1, alignItems: 'flex-start' }}>
              <Autocomplete
                fullWidth
                freeSolo
                options={lakebaseInstances}
                getOptionLabel={(option) => typeof option === 'string' ? option : option.name}
                value={
                  lakebaseInstances.find(i => i.name === lakebaseConfig.instance_name)
                  || (lakebaseConfig.instance_name || null)
                }
                inputValue={instanceSearch}
                onInputChange={(_e, value, reason) => {
                  setInstanceSearch(value);
                  if (reason === 'input') {
                    if (instanceSearchTimeout.current) clearTimeout(instanceSearchTimeout.current);
                    instanceSearchTimeout.current = setTimeout(() => {
                      setInstancePage(1);
                      loadLakebaseInstances(value, 1);
                    }, 300);
                  }
                }}
                onChange={(_e, value) => {
                  if (value && typeof value !== 'string') {
                    setLakebaseConfig(prev => ({ ...prev, instance_name: value.name, tables_initialized: false }));
                    setLakebaseStatus(null);
                    setLakebaseTableStats(null);
                  }
                }}
                onOpen={() => {
                  if (lakebaseInstances.length === 0 && !lakebaseInstancesLoading) {
                    loadLakebaseInstances('', 1);
                  }
                }}
                loading={lakebaseInstancesLoading}
                filterOptions={(x) => x}
                isOptionEqualToValue={(option, value) => option.name === value.name}
                ListboxProps={{
                  onScroll: (event) => {
                    const listbox = event.currentTarget;
                    if (
                      listbox.scrollTop + listbox.clientHeight >= listbox.scrollHeight - 20 &&
                      instanceHasMore &&
                      !lakebaseInstancesLoading
                    ) {
                      const nextPage = instancePage + 1;
                      setInstancePage(nextPage);
                      loadLakebaseInstances(instanceSearch, nextPage, true);
                    }
                  },
                  style: { maxHeight: 300 },
                }}
                renderOption={(props, option) => (
                  <li {...props} key={option.name}>
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, width: '100%', py: 0.5 }}>
                      <Box sx={{ flex: 1, minWidth: 0 }}>
                        <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
                          <Typography variant="body2" noWrap>{option.name}</Typography>
                          {option.type && (
                            <Chip
                              label={option.type === 'autoscaling' ? 'Auto' : 'Prov'}
                              size="small"
                              variant="outlined"
                              color={option.type === 'autoscaling' ? 'primary' : 'default'}
                              sx={{ height: 18, fontSize: '0.65rem', '& .MuiChip-label': { px: 0.5 } }}
                            />
                          )}
                        </Box>
                      </Box>
                      <Chip
                        label={option.state || 'UNKNOWN'}
                        size="small"
                        color={
                          option.state === 'ACTIVE' || option.state === 'AVAILABLE' || option.state === 'RUNNING'
                            ? 'success'
                            : option.state === 'STOPPED' || option.state === 'STOPPING'
                            ? 'warning'
                            : option.state === 'ERROR' || option.state === 'FAILED'
                            ? 'error'
                            : 'default'
                        }
                        sx={{ height: 20, fontSize: '0.7rem' }}
                      />
                    </Box>
                  </li>
                )}
                renderInput={(params) => (
                  <TextField
                    {...params}
                    size="small"
                    label="Lakebase Instance"
                    placeholder="Search instances..."
                    required
                    InputProps={{
                      ...params.InputProps,
                      endAdornment: (
                        <>
                          {lakebaseInstancesLoading ? <CircularProgress size={18} /> : null}
                          {params.InputProps.endAdornment}
                        </>
                      ),
                    }}
                    helperText={
                      lakebaseInstances.length === 0 && !lakebaseInstancesLoading
                        ? 'No instances found. Create one in Database Management first.'
                        : 'Search and select a Lakebase instance for memory storage'
                    }
                  />
                )}
              />
              <IconButton
                size="small"
                onClick={() => loadLakebaseInstances('', 1)}
                disabled={lakebaseInstancesLoading}
                sx={{ mt: '8px' }}
              >
                <RefreshIcon fontSize="small" />
              </IconButton>
              <Button
                variant="outlined"
                size="small"
                startIcon={lakebaseLoading ? <CircularProgress size={14} /> : undefined}
                disabled={lakebaseLoading || !lakebaseConfig.instance_name}
                onClick={async () => {
                  setLakebaseLoading(true);
                  try {
                    const result = await MemoryBackendService.testLakebaseConnection(lakebaseConfig.instance_name);
                    setLakebaseStatus(result);
                  } catch {
                    setLakebaseStatus({ success: false, message: 'Connection test failed' });
                  } finally {
                    setLakebaseLoading(false);
                  }
                }}
                sx={{ mt: '4px', minWidth: 'auto', px: 1.5, whiteSpace: 'nowrap' }}
              >
                Test Connection
              </Button>
            </Box>
          </Box>

          {/* Configuration section */}
          <Box>
            <Typography variant="subtitle2" color="text.secondary" sx={{ mb: 1.5, textTransform: 'uppercase', fontSize: '0.7rem', letterSpacing: '0.08em' }}>
              Configuration
            </Typography>
            <TextField
              fullWidth
              label="Embedding Dimension"
              type="number"
              size="small"
              value={lakebaseConfig.embedding_dimension || 1024}
              onChange={(e) => {
                setLakebaseConfig(prev => ({
                  ...prev,
                  embedding_dimension: parseInt(e.target.value) || 1024,
                }));
              }}
              helperText="Must match your embedding model (1024 for databricks-gte-large-en)"
            />
          </Box>

          <Divider />

          {/* Setup section */}
          <Box>
            <Typography variant="subtitle2" color="text.secondary" sx={{ mb: 1.5, textTransform: 'uppercase', fontSize: '0.7rem', letterSpacing: '0.08em' }}>
              Setup
            </Typography>
            <Box sx={{ display: 'flex', gap: 1.5, alignItems: 'center' }}>
              <Button
                variant="contained"
                startIcon={lakebaseLoading ? <CircularProgress size={18} /> : <StorageIcon />}
                disabled={lakebaseLoading || !lakebaseConfig.instance_name}
                onClick={async () => {
                  setLakebaseLoading(true);
                  try {
                    const result = await MemoryBackendService.initializeLakebaseTables({
                      instance_name: lakebaseConfig.instance_name,
                      embedding_dimension: lakebaseConfig.embedding_dimension,
                      memory_table: lakebaseConfig.memory_table,
                    });
                    if (!result.success) {
                      setLakebaseStatus({ success: false, message: result.message });
                      return;
                    }

                    setLakebaseConfig(prev => ({ ...prev, tables_initialized: true }));

                    let saveErrorMessage: string | null = null;
                    try {
                      const saveResult = await apiClient.post('/memory-backend/lakebase/save-config', {
                        lakebase_config: {
                          ...lakebaseConfig,
                          tables_initialized: true,
                        },
                        cognitive_config: config.cognitive_config,
                      });
                      setSavedConfig({ backend_id: saveResult.data.backend_id });
                      updateConfig({
                        backend_type: MemoryBackendType.LAKEBASE,
                        lakebase_config: { ...lakebaseConfig, tables_initialized: true },
                      });
                    } catch (saveError) {
                      console.error('Failed to save Lakebase config:', saveError);
                      saveErrorMessage = saveError instanceof Error ? saveError.message : 'Unknown error';
                    }

                    if (lakebaseConfig.instance_name) {
                      await loadLakebaseTableStats(lakebaseConfig.instance_name);
                    }

                    if (saveErrorMessage) {
                      setLakebaseStatus({
                        success: false,
                        message: `Tables initialized, but saving the configuration failed: ${saveErrorMessage}. The config will not persist on refresh.`,
                      });
                    } else {
                      setLakebaseStatus({ success: true, message: result.message });
                    }
                  } catch {
                    setLakebaseStatus({ success: false, message: 'Failed to initialize tables' });
                  } finally {
                    setLakebaseLoading(false);
                  }
                }}
                sx={{ py: 1 }}
              >
                {lakebaseConfig.tables_initialized ? 'Re-initialize Tables' : 'Initialize Tables'}
              </Button>
              <Button
                variant="outlined"
                startIcon={<SaveIcon />}
                disabled={!lakebaseConfig.instance_name}
                onClick={handleSaveLakebaseConfig}
                sx={{ py: 1 }}
              >
                Save Configuration
              </Button>
              <Button
                variant="text"
                size="small"
                startIcon={<RefreshIcon />}
                disabled={lakebaseLoading || !lakebaseConfig.instance_name}
                onClick={async () => {
                  setLakebaseLoading(true);
                  try {
                    await loadLakebaseTableStats(lakebaseConfig.instance_name!);
                  } finally {
                    setLakebaseLoading(false);
                  }
                }}
              >
                Refresh Status
              </Button>
            </Box>
            <Typography variant="caption" color="text.secondary" sx={{ mt: 0.5, display: 'block' }}>
              Enables pgvector extension and creates memory tables with HNSW indexes
            </Typography>
          </Box>

          {/* Table Status Display */}
          {(lakebaseTableStats || lakebaseConfig.tables_initialized) && (
            <>
              <Divider />
              <Box>
                <Box sx={{ display: 'flex', alignItems: 'center', mb: 1.5 }}>
                  <Typography variant="subtitle2" color="text.secondary" sx={{ textTransform: 'uppercase', fontSize: '0.7rem', letterSpacing: '0.08em' }}>
                    Memory Tables
                  </Typography>
                  {lakebaseConfig.tables_initialized && (
                    <Chip
                      icon={<CheckCircleIcon />}
                      label="Active"
                      color="success"
                      size="small"
                      sx={{ ml: 1.5, height: 22 }}
                    />
                  )}
                  <Box sx={{ flexGrow: 1 }} />
                  <Button
                    variant="outlined"
                    size="small"
                    startIcon={<StorageIcon />}
                    onClick={() => setMemoryBrowserOpen(true)}
                    disabled={!lakebaseConfig.tables_initialized}
                  >
                    Browse Memory
                  </Button>
                </Box>
                {lakebaseTableStats ? (
                  <Paper variant="outlined" sx={{ overflow: 'hidden' }}>
                    <Box component="table" sx={{ width: '100%', borderCollapse: 'collapse' }}>
                      <Box component="thead">
                        <Box component="tr" sx={{ bgcolor: 'action.hover' }}>
                          <Box component="th" sx={{ px: 2, py: 1, textAlign: 'left' }}>
                            <Typography variant="caption" fontWeight={600} color="text.secondary">Type</Typography>
                          </Box>
                          <Box component="th" sx={{ px: 2, py: 1, textAlign: 'left' }}>
                            <Typography variant="caption" fontWeight={600} color="text.secondary">Table Name</Typography>
                          </Box>
                          <Box component="th" sx={{ px: 2, py: 1, textAlign: 'center' }}>
                            <Typography variant="caption" fontWeight={600} color="text.secondary">Status</Typography>
                          </Box>
                          <Box component="th" sx={{ px: 2, py: 1, textAlign: 'right' }}>
                            <Typography variant="caption" fontWeight={600} color="text.secondary">Rows</Typography>
                          </Box>
                        </Box>
                      </Box>
                      <Box component="tbody">
                        {Object.entries(lakebaseTableStats).map(([type, stats]) => (
                          <Box component="tr" key={type} sx={{ '&:not(:last-child)': { borderBottom: '1px solid', borderColor: 'divider' } }}>
                            <Box component="td" sx={{ px: 2, py: 1.5 }}>
                              <Typography variant="body2" fontWeight={500} sx={{ textTransform: 'capitalize' }}>
                                {type.replace(/_/g, ' ')}
                              </Typography>
                            </Box>
                            <Box component="td" sx={{ px: 2, py: 1.5 }}>
                              <Typography variant="body2" color="text.secondary" sx={{ fontFamily: 'monospace', fontSize: '0.8rem' }}>
                                {stats.table_name}
                              </Typography>
                            </Box>
                            <Box component="td" sx={{ px: 2, py: 1.5, textAlign: 'center' }}>
                              {stats.exists ? (
                                <Chip icon={<CheckCircleIcon />} label="Ready" color="success" size="small" variant="outlined" sx={{ height: 22, fontSize: '0.75rem' }} />
                              ) : (
                                <Chip icon={<ErrorIcon />} label="Missing" color="error" size="small" variant="outlined" sx={{ height: 22, fontSize: '0.75rem' }} />
                              )}
                            </Box>
                            <Box component="td" sx={{ px: 2, py: 1.5, textAlign: 'right' }}>
                              <Typography variant="body2" color="text.secondary">
                                {stats.exists ? stats.row_count.toLocaleString() : '—'}
                              </Typography>
                            </Box>
                          </Box>
                        ))}
                      </Box>
                    </Box>
                  </Paper>
                ) : (
                  <Typography variant="body2" color="text.secondary">
                    Tables initialized. Click &quot;Refresh Status&quot; to see details.
                  </Typography>
                )}
              </Box>
            </>
          )}

          {/* Recall-speed & memory-LLM tuning. Persisted with the Lakebase
              config via the Save Configuration / Initialize Tables buttons. */}
          <MemoryTuningPanel />
        </Box>
      </Collapse>

      <Collapse in={mode === 'disabled'}>
        <Alert
          severity="info"
          sx={{ mt: 2 }}
          action={
            <Button
              color="inherit"
              size="small"
              startIcon={<StorageIcon />}
              onClick={() => setMemoryBrowserOpen(true)}
            >
              Browse Memory
            </Button>
          }
        >
          Kasal memory is stored locally in SQLite under
          <code style={{ margin: '0 4px' }}>kasal_default_&lt;group&gt;/memory/</code>
          relative to the backend working directory — one store per teamspace, no
          external infrastructure required. Click &ldquo;Browse Memory&rdquo; to
          inspect the records your crews have persisted.
        </Alert>

        {/* Memory tuning (recall weights, memory LLM) applies to local
            memory too — but only once saved as an ACTIVE config, which is what the
            Save button below does (it creates/updates the local backend config). */}
        <MemoryTuningPanel />

        <Box
          sx={{
            mt: 2,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            gap: 2,
            flexWrap: 'wrap',
          }}
        >
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            {savedConfig?.backend_id && (
              <Chip
                icon={<CheckCircleIcon />}
                label="Saved"
                color="success"
                size="small"
                variant="outlined"
              />
            )}
            <Typography variant="caption" color="text.secondary">
              {savedConfig?.backend_id
                ? 'Saved — this tuning applies to every local crew run in the teamspace.'
                : 'Not saved yet. Save to apply this tuning (memory LLM, recall speed) to local crew runs.'}
            </Typography>
          </Box>
          <Button
            variant="contained"
            startIcon={<SaveIcon />}
            onClick={handleSaveLocalMemoryConfig}
          >
            Save Local Memory Settings
          </Button>
        </Box>
      </Collapse>

      <MemoryRecordsBrowser
        open={memoryBrowserOpen}
        onClose={() => setMemoryBrowserOpen(false)}
      />

      {/* Result Dialog */}
            
      
      </Paper>
    </Box>
  );
};