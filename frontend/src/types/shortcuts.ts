export type ShortcutAction = 
  | 'deleteSelected' 
  | 'clearCanvas' 
  | 'undo' 
  | 'redo'
  | 'selectAll'
  | 'copy'
  | 'paste'
  | 'zoomIn'
  | 'zoomOut'
  | 'fitView'
  | 'toggleFullscreen'
  | 'openAgentDialog'
  | 'openTaskDialog'
  | 'openCrewPlanningDialog'
  | 'generateConnections'
  | 'openSaveCrew'
  | 'executeCrew'
  | 'executeFlow'
  | 'showRunResult'
  | 'openCrewFlowDialog'
  | 'openFlowDialog'
  | 'changeLLMForAllAgents'
  | 'changeMaxRPMForAllAgents'
  | 'changeToolsForAllAgents'
  | 'openLLMDialog'
  | 'openToolDialog'
  | 'openMCPConfigDialog'
  | 'openMaxRPMDialog';

export type KeySequence = string[];

export interface ShortcutConfig {
  action: ShortcutAction;
  keys: KeySequence;
  description: string;
}

export interface ShortcutsContextType {
  shortcuts: ShortcutConfig[];
  showShortcuts: boolean;
  toggleShortcuts: () => void;
  setShortcutsVisible: (visible: boolean) => void;
  setShortcuts: (shortcuts: ShortcutConfig[]) => void;
} 