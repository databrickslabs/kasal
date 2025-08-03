/**
 * Databricks One-Click Setup Component
 * 
 * Simplified setup for Databricks Vector Search memory backend.
 * Just enter workspace URL and click setup - everything else is automatic.
 */

import React, { useState, useEffect } from 'react';
import {
  Box,
  Paper,
  Typography,
  Alert,
  CircularProgress,
  RadioGroup,
  FormControlLabel,
  Radio,
  Collapse,
} from '@mui/material';
import {
  CloudSync as CloudSyncIcon,
  Memory as MemoryIcon,
  PowerSettingsNew as PowerOffIcon,
} from '@mui/icons-material';
import { AxiosError } from 'axios';
import { apiClient } from '../../config/api/ApiConfig';
import { useMemoryBackendStore } from '../../store/memoryBackend';
import DatabricksVectorSearchService from '../../api/DatabricksVectorSearchService';
import { 
  MemoryBackendType, 
  EndpointInfo, 
  IndexInfo, 
  SavedConfigInfo, 
  SetupResult,
  IndexInfoState,
  ManualConfig,
  DatabricksMemoryConfig as DatabricksConfig
} from '../../types/memoryBackend';
import { EMBEDDING_MODELS } from './constants';
import { 
  validateVectorSearchIndexName
} from './databricksVectorSearchUtils';
import { SetupResultDialog } from './SetupResultDialog';
import { IndexManagementTable } from './IndexManagementTable';
import { ConfigurationDisplay } from './ConfigurationDisplay';
import { ManualConfigurationForm } from './ManualConfigurationForm';
import { AutomaticSetupForm } from './AutomaticSetupForm';
import { EditConfigurationForm } from './EditConfigurationForm';
import { EndpointsDisplay } from './EndpointsDisplay';

export const DatabricksOneClickSetup: React.FC = () => {
  const [mode, setMode] = useState<'disabled' | 'databricks'>('databricks');
  const [setupMode, setSetupMode] = useState<'auto' | 'manual'>('auto');
  const [workspaceUrl, setWorkspaceUrl] = useState('');
  const [catalog, setCatalog] = useState('ml');
  const [schema, setSchema] = useState('agents');
  const [embeddingModel, setEmbeddingModel] = useState('databricks-bge-large-en');
  const [isSettingUp, setIsSettingUp] = useState(false);
  const [setupResult, setSetupResult] = useState<SetupResult | null>(null);
  const [showResultDialog, setShowResultDialog] = useState(false);
  const [error, setError] = useState('');
  const [savedConfig, setSavedConfig] = useState<SavedConfigInfo | null>(null);
  const [endpointStatuses, setEndpointStatuses] = useState<Record<string, EndpointInfo>>({});
  const [verifiedResources, setVerifiedResources] = useState<{ endpoints: Record<string, EndpointInfo>, indexes: Record<string, IndexInfo> } | null>(null);
  const [isEditingConfig, setIsEditingConfig] = useState(false);
  const [editedConfig, setEditedConfig] = useState<SavedConfigInfo | null>(null);
  const [indexInfoMap, setIndexInfoMap] = useState<Record<string, IndexInfoState>>({});
  const [hasCheckedInitialConfig, setHasCheckedInitialConfig] = useState(false);
  const [manualConfig, setManualConfig] = useState<ManualConfig>({
    workspace_url: '',
    endpoint_name: '',
    document_endpoint_name: '',
    short_term_index: '',
    long_term_index: '',
    entity_index: '',
    document_index: '',
    embedding_model: 'databricks-bge-large-en', // Default to BGE Large
  });
  
  const { updateConfig } = useMemoryBackendStore();
  
  
  // Load existing configuration on mount
  useEffect(() => {
    loadExistingConfig();
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
        } catch (allConfigsError) {
          console.log('No memory backend configurations found');
        }
        
        console.log('No memory backend configuration found - this is normal for new users');
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
        } catch (allConfigsError) {
          console.log('No memory backend configurations found');
        }
      }
      // Only log actual errors
      console.error('Failed to load existing configuration:', error);
      setHasCheckedInitialConfig(true);
    }
  };
  
  const processConfigResponse = (configData: { backend_type?: string; databricks_config?: DatabricksConfig; id?: string }) => {
    console.log('processConfigResponse - Full configData:', configData);
    console.log('processConfigResponse - backend_type:', configData?.backend_type);
    console.log('processConfigResponse - databricks_config:', configData?.databricks_config);
    
    if (configData && configData.backend_type === MemoryBackendType.DATABRICKS && configData.databricks_config) {
      const config = configData.databricks_config;
      console.log('Databricks Config:', config);
      console.log('Config endpoint_name:', config.endpoint_name);
      console.log('Config document_endpoint_name:', config.document_endpoint_name);
      console.log('Config short_term_index:', config.short_term_index);
      console.log('Config long_term_index:', config.long_term_index);
      
      const savedInfo: SavedConfigInfo = {
        backend_id: configData.id,
        workspace_url: config.workspace_url,
        catalog: config.catalog || 'ml',
        schema: config.schema || 'agents',
        endpoints: {
          memory: config.endpoint_name ? { name: config.endpoint_name } : undefined,
          document: config.document_endpoint_name ? { name: config.document_endpoint_name } : undefined
        },
        indexes: {
          short_term: config.short_term_index ? { name: config.short_term_index } : undefined,
          long_term: config.long_term_index ? { name: config.long_term_index } : undefined,
          entity: config.entity_index ? { name: config.entity_index } : undefined,
          document: config.document_index ? { name: config.document_index } : undefined
        }
      };
      console.log('SavedInfo:', savedInfo);
      console.log('Has endpoints:', !!savedInfo.endpoints);
      console.log('Endpoints memory:', savedInfo.endpoints?.memory);
      console.log('Endpoints document:', savedInfo.endpoints?.document);
      setSavedConfig(savedInfo);
      setMode('databricks');
    } else if (configData && configData.backend_type === MemoryBackendType.DEFAULT) {
      // When in disabled mode, clear the saved databricks config but keep the backend_id
      setSavedConfig({
        backend_id: configData.id
      });
      setMode('disabled');
    }
  };

  const handleModeChange = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const newMode = event.target.value as 'disabled' | 'databricks';
    setMode(newMode);
    
    if (newMode === 'disabled') {
      // Update store to use DEFAULT backend with all memory types disabled
      updateConfig({
        backend_type: MemoryBackendType.DEFAULT,
        enable_short_term: false,
        enable_long_term: false,
        enable_entity: false,
      });
      
      // Delete all configurations from the memory backend table and create disabled config
      try {
        // Use the new endpoint that deletes all configs and creates a disabled one
        const result = await DatabricksVectorSearchService.switchToDisabledMode();
        console.log('Switched to disabled mode:', result);
        
        // Clear saved config
        setSavedConfig(null);
        
        // Show success message
        setError(''); // Clear any previous errors
      } catch (error) {
        console.error('Failed to save disabled mode:', error);
        setError('Failed to save disabled mode. Please try again.');
      }
    }
  };

  const handleSetup = async () => {
    if (!workspaceUrl) {
      setError('Please enter your Databricks workspace URL');
      return;
    }

    setIsSettingUp(true);
    setError('');
    setSetupResult(null);

    try {
      // Delete all existing configurations
      try {
        await DatabricksVectorSearchService.deleteAllConfigurations();
        console.log('Deleted all existing configurations');
      } catch (error) {
        console.error('Failed to get/delete existing configurations:', error);
        // Continue anyway - the one-click setup might still work
      }
      
      // Clean up disabled configurations
      await DatabricksVectorSearchService.cleanupDisabledConfigurations();
      
      // Perform one-click setup
      const result = await DatabricksVectorSearchService.performOneClickSetup({
        workspace_url: workspaceUrl,
        catalog: catalog,
        schema: schema,
        embedding_dimension: EMBEDDING_MODELS.find(m => m.value === embeddingModel)?.dimension || 768
      });

      setSetupResult(result);
      setShowResultDialog(true);

      if (result.success) {
        // Save the configuration details
        setSavedConfig({
          workspace_url: workspaceUrl,
          catalog: result.catalog || catalog,
          schema: result.schema || schema,
          endpoints: result.endpoints,
          indexes: result.indexes
        });
        
        // Clear workspace URL on success
        setWorkspaceUrl('');
        
        // Update store with Databricks configuration
        updateConfig({
          backend_type: MemoryBackendType.DATABRICKS,
          enable_short_term: true,
          enable_long_term: true,
          enable_entity: true,
        });
        
        // Reload the configuration from backend after successful setup
        // This ensures the saved configuration is loaded properly with the new backend_id
        setTimeout(() => {
          loadExistingConfig();
        }, 1500); // Increased delay to ensure backend has fully processed the new configuration
      }
    } catch (error) {
      console.error('Setup failed:', error);
      const errorMessage = error instanceof AxiosError
        ? error.response?.data?.detail
        : error instanceof Error ? error.message : 'Failed to complete setup';
      setError(errorMessage || 'Failed to complete setup. Please check your configuration and try again.');
    } finally {
      setIsSettingUp(false);
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
          short_term_index: updatedConfig.indexes?.short_term?.name || null,
          long_term_index: updatedConfig.indexes?.long_term?.name || null,
          entity_index: updatedConfig.indexes?.entity?.name || null,
          document_index: updatedConfig.indexes?.document?.name || null,
          auth_type: 'default',
          embedding_dimension: 768
        }
      };

      const response = await apiClient.put(
        `/memory-backend/configs/${updatedConfig.backend_id}`,
        updatePayload
      );

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
        
        if (savedConfig.indexes?.short_term) {
          console.log(`Checking short_term index: ${savedConfig.indexes.short_term.name}`);
          if (!response.data.resources.indexes[savedConfig.indexes.short_term.name]) {
            console.log('Short-term index not found in Databricks, removing from config');
            updatedConfig.indexes = { ...updatedConfig.indexes, short_term: undefined };
            hasChanges = true;
          }
        }
        if (savedConfig.indexes?.long_term) {
          console.log(`Checking long_term index: ${savedConfig.indexes.long_term.name}`);
          if (!response.data.resources.indexes[savedConfig.indexes.long_term.name]) {
            console.log('Long-term index not found in Databricks, removing from config');
            updatedConfig.indexes = { ...updatedConfig.indexes, long_term: undefined };
            hasChanges = true;
          }
        }
        if (savedConfig.indexes?.entity) {
          console.log(`Checking entity index: ${savedConfig.indexes.entity.name}`);
          if (!response.data.resources.indexes[savedConfig.indexes.entity.name]) {
            console.log('Entity index not found in Databricks, removing from config');
            updatedConfig.indexes = { ...updatedConfig.indexes, entity: undefined };
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


  const handleDeleteIndex = async (indexType: 'short_term' | 'long_term' | 'entity' | 'document') => {
    if (!savedConfig?.indexes?.[indexType]) return;
    
    const indexName = savedConfig.indexes[indexType]?.name;
    if (!indexName) return;
    
    // Check if index is already deleted
    const indexInfo = indexInfoMap[indexName];
    if (indexInfo?.status === 'NOT_FOUND' || indexInfo?.index_type === 'DELETED') {
      setError(`Cannot delete index: ${indexType.replace('_', ' ')} index has already been deleted from Databricks.`);
      return;
    }
    
    // Check which endpoint this index belongs to
    const endpointType = indexType === 'document' ? 'document' : 'memory';
    const endpointStatus = endpointStatuses[endpointType];
    
    // Check if endpoint is ready for index deletion
    if (endpointStatus && !endpointStatus.can_delete_indexes) {
      setError(`Cannot delete index: Endpoint is ${endpointStatus.state}. Indexes can only be deleted when the endpoint is ONLINE.`);
      return;
    }
    
    const confirmDelete = window.confirm(`Are you sure you want to delete the ${indexType.replace('_', ' ')} index?`);
    if (!confirmDelete) return;
    
    setIsSettingUp(true);
    setError('');
    
    try {
      const response = await apiClient.delete('/memory-backend/databricks/index', {
        data: {
          workspace_url: savedConfig.workspace_url,
          index_name: savedConfig.indexes[indexType]?.name,
          endpoint_name: indexType === 'document' ? savedConfig.endpoints?.document?.name : savedConfig.endpoints?.memory?.name
        }
      });
      
      if (response.data.success) {
        // Update saved config to remove the deleted index
        const updatedConfig = {
          ...savedConfig,
          indexes: {
            ...savedConfig.indexes,
            [indexType]: undefined
          }
        };
        setSavedConfig(updatedConfig);
        
        // Update the backend configuration
        await updateBackendConfiguration(updatedConfig);
      } else {
        setError(response.data.message || 'Failed to delete index');
      }
    } catch (error) {
      console.error('Failed to delete index:', error);
      setError('Failed to delete index. Please try again.');
    } finally {
      setIsSettingUp(false);
    }
  };

  const handleStartEdit = () => {
    setEditedConfig(JSON.parse(JSON.stringify(savedConfig))); // Deep copy
    setIsEditingConfig(true);
  };

  const handleCancelEdit = () => {
    setEditedConfig(null);
    setIsEditingConfig(false);
  };

  const handleSaveEdit = async () => {
    if (!editedConfig || !editedConfig.backend_id) return;
    
    setIsSettingUp(true);
    setError('');
    
    try {
      // Update the backend configuration
      await updateBackendConfiguration(editedConfig);
      
      // Update local state
      setSavedConfig(editedConfig);
      setIsEditingConfig(false);
      setEditedConfig(null);
      
      // Verify the new resources
      setTimeout(() => {
        verifyActualResources();
      }, 500);
    } catch (error) {
      console.error('Failed to save configuration:', error);
      setError('Failed to save configuration. Please try again.');
    } finally {
      setIsSettingUp(false);
    }
  };

  const handleEditChange = (field: string, value: string | undefined) => {
    if (!editedConfig) return;
    
    const parts = field.split('.');
    const newConfig = { ...editedConfig };
    
    if (parts[0] === 'endpoints' && parts[1] && parts[2] === 'name') {
      if (!newConfig.endpoints) newConfig.endpoints = {};
      if (value) {
        newConfig.endpoints[parts[1] as 'memory' | 'document'] = { name: value };
      } else {
        newConfig.endpoints[parts[1] as 'memory' | 'document'] = undefined;
      }
    } else if (parts[0] === 'indexes' && parts[1] && parts[2] === 'name') {
      if (!newConfig.indexes) newConfig.indexes = {};
      if (value) {
        newConfig.indexes[parts[1] as 'short_term' | 'long_term' | 'entity' | 'document'] = { name: value };
      } else {
        newConfig.indexes[parts[1] as 'short_term' | 'long_term' | 'entity' | 'document'] = undefined;
      }
    }
    
    setEditedConfig(newConfig);
  };


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
      
      // Fetch info for each index
      if (indexes.short_term?.name && savedConfig.endpoints?.memory?.name) {
        fetchIndexInfo(indexes.short_term.name, savedConfig.endpoints.memory.name);
      }
      if (indexes.long_term?.name && savedConfig.endpoints?.memory?.name) {
        fetchIndexInfo(indexes.long_term.name, savedConfig.endpoints.memory.name);
      }
      if (indexes.entity?.name && savedConfig.endpoints?.memory?.name) {
        fetchIndexInfo(indexes.entity.name, savedConfig.endpoints.memory.name);
      }
      if (indexes.document?.name && savedConfig.endpoints?.document?.name) {
        fetchIndexInfo(indexes.document.name, savedConfig.endpoints.document.name);
      }
    }
  }, [savedConfig?.indexes, savedConfig?.workspace_url]); // eslint-disable-line react-hooks/exhaustive-deps

  const handleManualSave = async () => {
    // Check all required fields
    if (!manualConfig.workspace_url || 
        !manualConfig.endpoint_name || 
        !manualConfig.document_endpoint_name ||
        !manualConfig.short_term_index ||
        !manualConfig.long_term_index ||
        !manualConfig.entity_index ||
        !manualConfig.document_index) {
      setError('All fields are required. Please provide workspace URL, both endpoints, and all four indexes.');
      return;
    }
    
    // Validate index name formats
    const indexesToValidate = [
      { name: 'Short-term index', value: manualConfig.short_term_index },
      { name: 'Long-term index', value: manualConfig.long_term_index },
      { name: 'Entity index', value: manualConfig.entity_index },
      { name: 'Document index', value: manualConfig.document_index }
    ];
    
    for (const index of indexesToValidate) {
      if (!validateVectorSearchIndexName(index.value)) {
        setError(`${index.name} must be in the format: catalog.schema.indexname (e.g., ml.agents.short_term_memory)`);
        return;
      }
    }
    
    setIsSettingUp(true);
    setError('');
    
    try {
      // First, clean up any disabled configurations
      try {
        const cleanupResult = await apiClient.delete('/memory-backend/configs/disabled/cleanup');
        console.log('Cleaned up disabled configurations:', cleanupResult.data);
      } catch (cleanupError) {
        // It's okay if cleanup fails, continue with setup
        console.log('No disabled configurations to clean up');
      }
      
      // Extract catalog and schema from the index name if possible
      let catalogName = catalog;
      let schemaName = schema;
      
      // Try to extract from short_term_index (format: catalog.schema.table)
      const indexParts = manualConfig.short_term_index.split('.');
      if (indexParts.length >= 3) {
        catalogName = indexParts[0];
        schemaName = indexParts[1];
      }
      
      // Always check if we have an existing config first
      let existingConfigId = savedConfig?.backend_id;
      
      // If we don't have a saved config, check if there's a default one
      if (!existingConfigId) {
        try {
          const defaultResponse = await apiClient.get('/memory-backend/configs/default');
          if (defaultResponse.data && defaultResponse.data.id) {
            existingConfigId = defaultResponse.data.id;
          }
        } catch (error) {
          // No default config exists, that's fine
        }
      }
      
      // Create or update configuration data
      const configData = {
        name: 'Databricks Vector Search Configuration', // Always update the name
        description: 'Manually configured Databricks Vector Search',
        backend_type: MemoryBackendType.DATABRICKS,
        enable_short_term: true,
        enable_long_term: true,
        enable_entity: true,
        databricks_config: {
          workspace_url: manualConfig.workspace_url,
          endpoint_name: manualConfig.endpoint_name,
          document_endpoint_name: manualConfig.document_endpoint_name,
          short_term_index: manualConfig.short_term_index,
          long_term_index: manualConfig.long_term_index,
          entity_index: manualConfig.entity_index,
          document_index: manualConfig.document_index,
          auth_type: 'default',
          embedding_dimension: EMBEDDING_MODELS.find(m => m.value === manualConfig.embedding_model)?.dimension || 768,
          catalog: catalogName,
          schema: schemaName,
        }
      };
      
      let response;
      if (existingConfigId) {
        // Update existing configuration
        response = await apiClient.put(`/memory-backend/configs/${existingConfigId}`, configData);
      } else {
        // Create new configuration
        response = await apiClient.post('/memory-backend/configs', configData);
        if (response.data) {
          // Set as default
          await apiClient.post(`/memory-backend/configs/${response.data.id}/set-default`);
        }
      }
      
      if (response.data) {
        // Update store
        updateConfig({
          backend_type: MemoryBackendType.DATABRICKS,
          enable_short_term: true,
          enable_long_term: true,
          enable_entity: true,
        });
        
        // Reload configuration
        await loadExistingConfig();
        
        // Clear manual config form
        setManualConfig({
          workspace_url: '',
          endpoint_name: '',
          document_endpoint_name: '',
          short_term_index: '',
          long_term_index: '',
          entity_index: '',
          document_index: '',
          embedding_model: 'databricks-bge-large-en', // Reset to default
        });
      }
    } catch (error) {
      console.error('Failed to save manual configuration:', error);
      setError('Failed to save configuration. Please try again.');
    } finally {
      setIsSettingUp(false);
    }
  };

  const handleEmptyIndex = async (indexType: 'short_term' | 'long_term' | 'entity' | 'document') => {
    if (!savedConfig?.indexes?.[indexType] || !savedConfig?.workspace_url || !savedConfig?.backend_id) return;
    
    const indexName = savedConfig.indexes[indexType]?.name;
    if (!indexName) return;
    
    const confirmEmpty = window.confirm(`Are you sure you want to empty the ${indexType.replace('_', ' ')} index? This will delete all documents but keep the index structure.`);
    if (!confirmEmpty) return;
    
    setIsSettingUp(true);
    setError('');
    
    try {
      // Get the backend configuration to find embedding dimension
      const configResponse = await apiClient.get(`/memory-backend/configs/${savedConfig.backend_id}`);
      const embeddingDimension = configResponse.data?.databricks_config?.embedding_dimension || 768;
      
      const endpointName = indexType === 'document' 
        ? savedConfig.endpoints?.document?.name 
        : savedConfig.endpoints?.memory?.name;
      
      if (!endpointName) {
        setError('Could not determine endpoint for this index');
        return;
      }
      
      const response = await apiClient.post('/memory-backend/databricks/empty-index', {
        workspace_url: savedConfig.workspace_url,
        index_name: indexName,
        endpoint_name: endpointName,
        index_type: indexType,
        embedding_dimension: embeddingDimension
      });
      
      if (response.data.success) {
        // Refresh index info
        fetchIndexInfo(indexName, endpointName);
        
        // Show success message
        setError(''); // Clear any errors
      } else {
        setError(response.data.message || 'Failed to empty index');
      }
    } catch (error) {
      console.error('Failed to empty index:', error);
      setError('Failed to empty index. Please try again.');
    } finally {
      setIsSettingUp(false);
    }
  };

  const handleReseedDocumentation = async () => {
    if (!savedConfig?.indexes?.document || !savedConfig?.workspace_url || !savedConfig?.backend_id) return;
    
    const confirmReseed = window.confirm('Are you sure you want to re-seed the documentation? This will empty the index and reload all documentation.');
    if (!confirmReseed) return;
    
    setIsSettingUp(true);
    setError('');
    
    try {
      // First empty the index
      const indexName = savedConfig.indexes.document.name;
      const endpointName = savedConfig.endpoints?.document?.name;
      
      if (!endpointName) {
        setError('Could not determine document endpoint');
        return;
      }
      
      // Get the backend configuration to find embedding dimension
      const configResponse = await apiClient.get(`/memory-backend/configs/${savedConfig.backend_id}`);
      const embeddingDimension = configResponse.data?.databricks_config?.embedding_dimension || 768;
      
      // Empty the index
      const emptyResponse = await apiClient.post('/memory-backend/databricks/empty-index', {
        workspace_url: savedConfig.workspace_url,
        index_name: indexName,
        endpoint_name: endpointName,
        index_type: 'document',
        embedding_dimension: embeddingDimension
      });
      
      if (!emptyResponse.data.success) {
        throw new Error(emptyResponse.data.message || 'Failed to empty document index');
      }
      
      // Trigger documentation re-seeding
      const seedResponse = await apiClient.post('/documentation-embeddings/seed-all');
      
      if (seedResponse.data.success) {
        // Show success in result dialog
        setSetupResult({
          success: true,
          message: 'Documentation re-seeding initiated successfully. This may take a few minutes to complete.',
          info: 'The document index has been emptied and re-seeding has started. Documents will be processed in the background.'
        });
        setShowResultDialog(true);
        
        // Refresh index info after a delay to show progress
        setTimeout(() => {
          fetchIndexInfo(indexName, endpointName);
        }, 5000);
      } else {
        setError(seedResponse.data.message || 'Failed to initiate documentation re-seeding');
      }
    } catch (error) {
      console.error('Error re-seeding documentation:', error);
      const errorMessage = error instanceof Error ? error.message : 'Failed to re-seed documentation';
      if (error && typeof error === 'object' && 'response' in error) {
        const responseError = error as { response?: { data?: { detail?: string } } };
        setError(responseError.response?.data?.detail || errorMessage);
      } else {
        setError(errorMessage);
      }
    } finally {
      setIsSettingUp(false);
    }
  };

  const handleDeleteEndpoint = async (endpointType: 'memory' | 'document') => {
    if (!savedConfig?.endpoints?.[endpointType]) return;
    
    const endpointStatus = endpointStatuses[endpointType];
    const isNotFound = endpointStatus?.state === 'NOT_FOUND';
    
    // Check if this is the last endpoint
    const remainingEndpoints = Object.entries(savedConfig.endpoints || {})
      .filter(([key, value]) => key !== endpointType && value !== undefined)
      .length;
    
    const isLastEndpoint = remainingEndpoints === 0;
    
    const confirmMessage = isNotFound 
      ? `The ${endpointType} endpoint no longer exists in Databricks. Remove it from configuration?`
      : isLastEndpoint 
        ? `This is the last endpoint. Deleting it will disable memory backend entirely. Continue?`
        : `Are you sure you want to delete the ${endpointType} endpoint?`;
    
    const confirmDelete = window.confirm(confirmMessage);
    if (!confirmDelete) return;
    
    setIsSettingUp(true);
    setError('');
    
    try {
      // If it's the last endpoint, delete all configs and switch to disabled mode
      if (isLastEndpoint) {
        // Use the new endpoint that deletes all configs and creates a disabled one
        const result = await DatabricksVectorSearchService.switchToDisabledMode();
        console.log('Switched to disabled mode:', result);
        
        // Update local state
        setMode('disabled');
        setSavedConfig(null);
        
        // Update store
        updateConfig({
          backend_type: MemoryBackendType.DEFAULT,
          enable_short_term: false,
          enable_long_term: false,
          enable_entity: false,
        });
        
        return;
      }
      
      // Otherwise, proceed with normal endpoint deletion
      if (isNotFound) {
        // Endpoint doesn't exist in Databricks, just remove from config
        const updatedConfig = {
          ...savedConfig,
          endpoints: {
            ...savedConfig.endpoints,
            [endpointType]: undefined
          }
        };
        setSavedConfig(updatedConfig);
        
        // Update backend configuration
        await updateBackendConfiguration(updatedConfig);
      } else {
        // Endpoint exists, delete it from Databricks
        const response = await apiClient.delete('/memory-backend/databricks/endpoint', {
          data: {
            workspace_url: savedConfig.workspace_url,
            endpoint_name: savedConfig.endpoints[endpointType]?.name
          }
        });
        
        if (response.data.success) {
          // Update saved config to remove the deleted endpoint
          const updatedConfig = {
            ...savedConfig,
            endpoints: {
              ...savedConfig.endpoints,
              [endpointType]: undefined
            }
          };
          setSavedConfig(updatedConfig);
          
          // Update the backend configuration
          await updateBackendConfiguration(updatedConfig);
        } else {
          setError(response.data.message || 'Failed to delete endpoint');
        }
      }
    } catch (error) {
      console.error('Failed to delete endpoint:', error);
      setError('Failed to delete endpoint. Please try again.');
    } finally {
      setIsSettingUp(false);
    }
  };

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

      <RadioGroup value={mode} onChange={handleModeChange} row sx={{ mb: 2 }}>
        <FormControlLabel
          value="databricks"
          control={<Radio size="small" />}
          label={
            <Box sx={{ display: 'flex', alignItems: 'center' }}>
              <CloudSyncIcon sx={{ fontSize: 18, mr: 0.5 }} />
              Databricks Vector Search
            </Box>
          }
          sx={{ mr: 3 }}
        />
        <FormControlLabel
          value="disabled"
          control={<Radio size="small" />}
          label={
            <Box sx={{ display: 'flex', alignItems: 'center' }}>
              <PowerOffIcon sx={{ fontSize: 18, mr: 0.5 }} />
              Disabled
            </Box>
          }
        />
      </RadioGroup>

      <Collapse in={mode === 'databricks'}>
        <Box>
          {(!savedConfig || !savedConfig.workspace_url) && (
            <>
              <Alert severity="info" sx={{ mb: 2 }}>
                Choose how to configure Databricks Vector Search
              </Alert>
              
              <RadioGroup value={setupMode} onChange={(e) => setSetupMode(e.target.value as 'auto' | 'manual')} row sx={{ mb: 2 }}>
                <FormControlLabel
                  value="auto"
                  control={<Radio size="small" />}
                  label={
                    <Box>
                      <Typography variant="body2" fontWeight="medium">Auto-create</Typography>
                      <Typography variant="caption" color="text.secondary">
                        Automatically creates endpoints and indexes
                      </Typography>
                    </Box>
                  }
                  sx={{ mr: 4 }}
                />
                <FormControlLabel
                  value="manual"
                  control={<Radio size="small" />}
                  label={
                    <Box>
                      <Typography variant="body2" fontWeight="medium">Manual setup</Typography>
                      <Typography variant="caption" color="text.secondary">
                        Use existing endpoints and indexes
                      </Typography>
                    </Box>
                  }
                />
              </RadioGroup>
            </>
          )}

          {error && (
            <Alert severity="error" sx={{ mb: 2 }} onClose={() => setError('')}>
              {error}
            </Alert>
          )}

          {/* Auto-create setup */}
          {setupMode === 'auto' && (!savedConfig || !savedConfig.workspace_url) && (
            <AutomaticSetupForm
              workspaceUrl={workspaceUrl}
              catalog={catalog}
              schema={schema}
              embeddingModel={embeddingModel}
              isSettingUp={isSettingUp}
              error={error}
              onWorkspaceUrlChange={setWorkspaceUrl}
              onCatalogChange={setCatalog}
              onSchemaChange={setSchema}
              onEmbeddingModelChange={setEmbeddingModel}
              onSetup={handleSetup}
            />
          )}

          {/* Manual setup */}
          {setupMode === 'manual' && (!savedConfig || !savedConfig.workspace_url) && (
            <ManualConfigurationForm
              manualConfig={manualConfig}
              isSettingUp={isSettingUp}
              error={error}
              onConfigChange={setManualConfig}
              onSave={handleManualSave}
            />
          )}
          
          {/* Display saved configuration */}
          {(() => {
            console.log('SavedConfig state:', savedConfig);
            console.log('Has workspace_url:', !!savedConfig?.workspace_url);
            console.log('Has endpoints:', !!savedConfig?.endpoints);
            console.log('Has indexes:', !!savedConfig?.indexes);
            console.log('Endpoints detail:', savedConfig?.endpoints);
            console.log('Indexes detail:', savedConfig?.indexes);
            console.log('Memory endpoint:', savedConfig?.endpoints?.memory);
            console.log('Document endpoint:', savedConfig?.endpoints?.document);
            console.log('Condition check:', !!(savedConfig?.endpoints && (savedConfig.endpoints.memory || savedConfig.endpoints.document) || 
              savedConfig?.indexes && (savedConfig.indexes.short_term || savedConfig.indexes.long_term || savedConfig.indexes.entity || savedConfig.indexes.document)));
            return null;
          })()}
          {savedConfig && savedConfig.workspace_url && (
            <ConfigurationDisplay
              savedConfig={savedConfig}
              isEditingConfig={isEditingConfig}
              isSettingUp={isSettingUp}
              onStartEdit={handleStartEdit}
              onSaveEdit={handleSaveEdit}
              onCancelEdit={handleCancelEdit}
              onRefresh={verifyActualResources}
            >

              {true && (
                <Box sx={{ mt: 1.5 }}>
                  {isEditingConfig ? (
                    <EditConfigurationForm
                      editedConfig={editedConfig}
                      onEditChange={handleEditChange}
                    />
                  ) : (
                    <>
                      <EndpointsDisplay
                        savedConfig={savedConfig}
                        endpointStatuses={endpointStatuses}
                        onDeleteEndpoint={handleDeleteEndpoint}
                      />
                      
                      {/* Indexes Table */}
                      <Box>
                        <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mb: 0.5 }}>
                          Indexes:
                        </Typography>
                        <Box>
                        {/* Knowledge Base Table */}
                        {savedConfig.indexes?.document && (
                          <IndexManagementTable
                            title="Knowledge Base"
                            subtitle="The knowledge base contains documentation embeddings used to provide context when creating crews, agents, and tasks. This helps the AI understand available tools, patterns, and best practices."
                            savedConfig={savedConfig}
                            endpointName={savedConfig.endpoints?.document?.name}
                            endpointType="document"
                            indexes={[
                              { type: 'document', name: savedConfig.indexes.document?.name }
                            ]}
                            indexInfoMap={indexInfoMap}
                            endpointStatuses={endpointStatuses}
                            isSettingUp={isSettingUp}
                            onRefresh={handleReseedDocumentation}
                            onEmpty={(indexType) => handleEmptyIndex(indexType as 'short_term' | 'long_term' | 'entity' | 'document')}
                            onDelete={(indexType) => handleDeleteIndex(indexType as 'short_term' | 'long_term' | 'entity' | 'document')}
                          />
                        )}

                        {/* Memory Indexes Table */}
                        {(savedConfig.indexes?.short_term || savedConfig.indexes?.long_term || savedConfig.indexes?.entity) && (
                          <IndexManagementTable
                            title="Memory Indexes"
                            subtitle="Memory indexes store agent conversations and interactions during runtime. Short-term memory holds recent context, long-term memory persists important information across sessions, and entity memory tracks relationships and facts about people, organizations, and concepts."
                            savedConfig={savedConfig}
                            endpointName={savedConfig.endpoints?.memory?.name}
                            endpointType="memory"
                            indexes={[
                              savedConfig.indexes.short_term && { type: 'short_term' as const, name: savedConfig.indexes.short_term.name },
                              savedConfig.indexes.long_term && { type: 'long_term' as const, name: savedConfig.indexes.long_term.name },
                              savedConfig.indexes.entity && { type: 'entity' as const, name: savedConfig.indexes.entity.name }
                            ].filter((index): index is { type: 'short_term' | 'long_term' | 'entity', name: string } => Boolean(index))}
                            indexInfoMap={indexInfoMap}
                            endpointStatuses={endpointStatuses}
                            isSettingUp={isSettingUp}
                            onEmpty={(indexType) => handleEmptyIndex(indexType as 'short_term' | 'long_term' | 'entity' | 'document')}
                            onDelete={(indexType) => handleDeleteIndex(indexType as 'short_term' | 'long_term' | 'entity' | 'document')}
                          />
                        )}
                        </Box>
                      </Box>
                    </>
                  )}
                </Box>
              )}
            </ConfigurationDisplay>
          )}
        </Box>
      </Collapse>

      <Collapse in={mode === 'disabled'}>
        <Alert severity="info" sx={{ mt: 2 }}>
          Memory storage is disabled. Agents will not persist information between conversations.
        </Alert>
      </Collapse>

      {/* Result Dialog */}
      <SetupResultDialog
        open={showResultDialog}
        onClose={() => setShowResultDialog(false)}
        setupResult={setupResult}
        workspaceUrl={workspaceUrl}
        savedConfigWorkspaceUrl={savedConfig?.workspace_url}
      />
      </Paper>
    </Box>
  );
};