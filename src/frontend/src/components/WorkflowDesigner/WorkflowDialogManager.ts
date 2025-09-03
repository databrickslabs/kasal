import { useState, useEffect, useCallback } from 'react';
import { useAPIKeysStore } from '../../store/apiKeys';

export interface DialogManagerResult {
  isAgentDialogOpen: boolean;
  setIsAgentDialogOpen: React.Dispatch<React.SetStateAction<boolean>>;
  isTaskDialogOpen: boolean;
  setIsTaskDialogOpen: React.Dispatch<React.SetStateAction<boolean>>;
  isCrewPlanningOpen: boolean;
  setCrewPlanningOpen: React.Dispatch<React.SetStateAction<boolean>>;
  isScheduleDialogOpen: boolean;
  setScheduleDialogOpen: React.Dispatch<React.SetStateAction<boolean>>;
  isAPIKeysDialogOpen: boolean;
  setIsAPIKeysDialogOpen: React.Dispatch<React.SetStateAction<boolean>>;
  isToolsDialogOpen: boolean;
  setIsToolsDialogOpen: React.Dispatch<React.SetStateAction<boolean>>;
  isLogsDialogOpen: boolean;
  setIsLogsDialogOpen: React.Dispatch<React.SetStateAction<boolean>>;
  isConfigurationDialogOpen: boolean;
  setIsConfigurationDialogOpen: React.Dispatch<React.SetStateAction<boolean>>;
  isFlowDialogOpen: boolean;
  setIsFlowDialogOpen: React.Dispatch<React.SetStateAction<boolean>>;
  isTutorialOpen: boolean;
  setIsTutorialOpen: React.Dispatch<React.SetStateAction<boolean>>;
  handleCloseTutorial: () => void;
}

export const useDialogManager = (
  hasSeenTutorial: boolean,
  setHasSeenTutorial: (value: boolean) => void
): DialogManagerResult => {
  // Dialog states
  const [isAgentDialogOpen, setIsAgentDialogOpen] = useState(false);
  const [isTaskDialogOpen, setIsTaskDialogOpen] = useState(false);
  const [isCrewPlanningOpen, setCrewPlanningOpen] = useState(false);
  const [isScheduleDialogOpen, setScheduleDialogOpen] = useState(false);
  const [isAPIKeysDialogOpen, setIsAPIKeysDialogOpen] = useState(false);
  const [isToolsDialogOpen, setIsToolsDialogOpen] = useState(false);
  const [isLogsDialogOpen, setIsLogsDialogOpen] = useState(false);
  const [isConfigurationDialogOpen, setIsConfigurationDialogOpen] = useState(false);
  const [isFlowDialogOpen, setIsFlowDialogOpen] = useState(false);
  const [isTutorialOpen, setIsTutorialOpen] = useState(!hasSeenTutorial);
  
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

  // Handler for opening tools dialog
  const _handleOpenToolsDialog = useCallback(() => {
    setIsToolsDialogOpen(true);
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
    isAgentDialogOpen,
    setIsAgentDialogOpen,
    isTaskDialogOpen,
    setIsTaskDialogOpen,
    isCrewPlanningOpen,
    setCrewPlanningOpen,
    isScheduleDialogOpen,
    setScheduleDialogOpen,
    isAPIKeysDialogOpen,
    setIsAPIKeysDialogOpen,
    isToolsDialogOpen,
    setIsToolsDialogOpen,
    isLogsDialogOpen,
    setIsLogsDialogOpen,
    isConfigurationDialogOpen,
    setIsConfigurationDialogOpen,
    isFlowDialogOpen,
    setIsFlowDialogOpen,
    isTutorialOpen,
    setIsTutorialOpen,
    handleCloseTutorial
  };
}; 