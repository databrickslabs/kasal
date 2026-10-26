import { useState, useEffect, useCallback } from 'react';
import { readSettingsNavigation } from '../../features/configuration/lib/settingsNavigation';
import { useAPIKeysStore } from '../../store/apiKeys';

export interface DialogManagerResult {
  isScheduleDialogOpen: boolean;
  setScheduleDialogOpen: React.Dispatch<React.SetStateAction<boolean>>;
  isAPIKeysDialogOpen: boolean;
  setIsAPIKeysDialogOpen: React.Dispatch<React.SetStateAction<boolean>>;
  isConfigurationDialogOpen: boolean;
  setIsConfigurationDialogOpen: React.Dispatch<React.SetStateAction<boolean>>;
  isTutorialOpen: boolean;
  setIsTutorialOpen: React.Dispatch<React.SetStateAction<boolean>>;
  handleCloseTutorial: () => void;
}

export const useDialogManager = (
  setHasSeenTutorial: (value: boolean) => void
): DialogManagerResult => {
  // Dialog states
  const [isScheduleDialogOpen, setScheduleDialogOpen] = useState(false);
  const [isAPIKeysDialogOpen, setIsAPIKeysDialogOpen] = useState(false);
  const [isConfigurationDialogOpen, setIsConfigurationDialogOpen] = useState(() => readSettingsNavigation() !== null);
  const [isTutorialOpen, setIsTutorialOpen] = useState(false);

  // Check URL for configuration parameters on component mount
  useEffect(() => {
    const urlParams = new URLSearchParams(window.location.search);
    const configParam = urlParams.get('config');
    const providerParam = urlParams.get('provider');
    
    if (configParam === 'apikeys') {
      // Open the API Keys dialog
      setIsAPIKeysDialogOpen(true);
      
      // If provider is specified, open the editor for that provider via Zustand
      if (providerParam) {
        // Use the Zustand store to trigger editing the API key
        useAPIKeysStore.getState().openApiKeyEditor(providerParam);
      }
      
      // Remove query parameters from URL to prevent reopening on refresh
      window.history.replaceState({}, document.title, window.location.pathname);
    }
  }, []);

  // Handle closing tutorial
  const handleCloseTutorial = useCallback(() => {
    setIsTutorialOpen(false);
    setHasSeenTutorial(true);
  }, [setHasSeenTutorial]);

  // Listen for the openConfigAPIKeysInternal event
  useEffect(() => {
    const handleOpenAPIKeys = () => {
      setIsAPIKeysDialogOpen(true);
    };
    
    window.addEventListener('openConfigAPIKeysInternal', handleOpenAPIKeys);
    
    return () => {
      window.removeEventListener('openConfigAPIKeysInternal', handleOpenAPIKeys);
    };
  }, []);

  return {
    isScheduleDialogOpen,
    setScheduleDialogOpen,
    isAPIKeysDialogOpen,
    setIsAPIKeysDialogOpen,
    isConfigurationDialogOpen,
    setIsConfigurationDialogOpen,
    isTutorialOpen,
    setIsTutorialOpen,
    handleCloseTutorial
  };
}; 