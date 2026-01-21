import { create } from 'zustand';
import { UILayoutState } from '../utils/CanvasLayoutManager';

interface UILayoutStore extends UILayoutState {
  // Actions to update the UI state
  updateScreenDimensions: (width: number, height: number) => void;
  setChatPanelWidth: (width: number) => void;
  setChatPanelCollapsed: (collapsed: boolean) => void;
  setChatPanelVisible: (visible: boolean) => void;
  setExecutionHistoryHeight: (height: number) => void;
  setExecutionHistoryVisible: (visible: boolean) => void;
  setLeftSidebarExpanded: (expanded: boolean) => void;
  setLeftSidebarVisible: (visible: boolean) => void;
  setPanelPosition: (position: number) => void;
  setAreFlowsVisible: (visible: boolean) => void;
  setChatPanelSide: (side: 'left' | 'right') => void;
  setLayoutOrientation: (orientation: 'vertical' | 'horizontal') => void;

  // Computed getters
  getUILayoutState: () => UILayoutState;
}

// Helper function to load persisted state from localStorage
const loadPersistedState = () => {
  try {
    const stored = localStorage.getItem('ui-layout-storage');
    if (stored) {
      return JSON.parse(stored);
    }
  } catch (error) {
    console.error('Failed to load persisted UI state:', error);
  }
  return {};
};

// Helper function to save state to localStorage
const saveToLocalStorage = (state: Partial<UILayoutState>) => {
  try {
    const stored = localStorage.getItem('ui-layout-storage') || '{}';
    const current = JSON.parse(stored);
    const updated = { ...current, ...state };
    localStorage.setItem('ui-layout-storage', JSON.stringify(updated));
  } catch (error) {
    console.error('Failed to save UI state:', error);
  }
};

// Load initial persisted state
const persistedState = loadPersistedState();

export const useUILayoutStore = create<UILayoutStore>((set, get) => ({
  // Default UI state with persisted values
  screenWidth: typeof window !== 'undefined' ? window.innerWidth : 1200,
  screenHeight: typeof window !== 'undefined' ? window.innerHeight : 800,
  tabBarHeight: 48,
  leftSidebarVisible: true,
  leftSidebarExpanded: false,
  leftSidebarBaseWidth: 48,
  leftSidebarExpandedWidth: 280,
  rightSidebarVisible: true,
  rightSidebarWidth: 48,
  chatPanelVisible: persistedState.chatPanelVisible !== undefined ? persistedState.chatPanelVisible : true,
  chatPanelCollapsed: persistedState.chatPanelCollapsed !== undefined ? persistedState.chatPanelCollapsed : false,
  chatPanelWidth: persistedState.chatPanelWidth || 450,
  chatPanelCollapsedWidth: 60,
  chatPanelSide: persistedState.chatPanelSide || 'right',
  executionHistoryVisible: persistedState.executionHistoryVisible !== undefined ? persistedState.executionHistoryVisible : false,
  executionHistoryHeight: persistedState.executionHistoryHeight || 60,
  panelPosition: persistedState.panelPosition || 50,
  areFlowsVisible: persistedState.areFlowsVisible !== undefined ? persistedState.areFlowsVisible : false,
  layoutOrientation: persistedState.layoutOrientation || 'horizontal',

  // Actions
  updateScreenDimensions: (width: number, height: number) =>
    set({ screenWidth: width, screenHeight: height }),

  setChatPanelWidth: (width: number) => {
    set({ chatPanelWidth: width });
    saveToLocalStorage({ chatPanelWidth: width });
  },

  setChatPanelCollapsed: (collapsed: boolean) => {
    set({ chatPanelCollapsed: collapsed });
    saveToLocalStorage({ chatPanelCollapsed: collapsed });
  },

  setChatPanelVisible: (visible: boolean) => {
    set({ chatPanelVisible: visible });
    saveToLocalStorage({ chatPanelVisible: visible });
  },

  setExecutionHistoryHeight: (height: number) => {
    set({ executionHistoryHeight: height });
    saveToLocalStorage({ executionHistoryHeight: height });
  },

  setExecutionHistoryVisible: (visible: boolean) => {
    set({ executionHistoryVisible: visible });
    saveToLocalStorage({ executionHistoryVisible: visible });
  },

  setLeftSidebarExpanded: (expanded: boolean) =>
    set({ leftSidebarExpanded: expanded }),

  setLeftSidebarVisible: (visible: boolean) =>
    set({ leftSidebarVisible: visible }),

  setPanelPosition: (position: number) => {
    set({ panelPosition: position });
    saveToLocalStorage({ panelPosition: position });
  },

  setAreFlowsVisible: (visible: boolean) => {
    set({ areFlowsVisible: visible });
    saveToLocalStorage({ areFlowsVisible: visible });
  },

  setChatPanelSide: (side: 'left' | 'right') => {
    set({ chatPanelSide: side });
    saveToLocalStorage({ chatPanelSide: side });
  },
  setLayoutOrientation: (orientation: 'vertical' | 'horizontal') => {
    set({ layoutOrientation: orientation });
    saveToLocalStorage({ layoutOrientation: orientation });
  },


  // Computed getter that returns the current UI layout state
  getUILayoutState: (): UILayoutState => {
    const state = get();
    return {
      screenWidth: state.screenWidth,
      screenHeight: state.screenHeight,
      tabBarHeight: state.tabBarHeight,
      leftSidebarVisible: state.leftSidebarVisible,
      leftSidebarExpanded: state.leftSidebarExpanded,
      leftSidebarBaseWidth: state.leftSidebarBaseWidth,
      leftSidebarExpandedWidth: state.leftSidebarExpandedWidth,
      rightSidebarVisible: state.rightSidebarVisible,
      rightSidebarWidth: state.rightSidebarWidth,
      chatPanelVisible: state.chatPanelVisible,
      chatPanelCollapsed: state.chatPanelCollapsed,
      chatPanelWidth: state.chatPanelWidth,
      chatPanelCollapsedWidth: state.chatPanelCollapsedWidth,
      chatPanelSide: state.chatPanelSide,
      executionHistoryVisible: state.executionHistoryVisible,
      executionHistoryHeight: state.executionHistoryHeight,
      panelPosition: state.panelPosition,
      areFlowsVisible: state.areFlowsVisible,
      layoutOrientation: state.layoutOrientation,
    };
  },
}));

// Helper hook to get just the UI layout state for the canvas layout manager
export const useUILayoutState = (): UILayoutState => {
  return useUILayoutStore(state => state.getUILayoutState());
};

// Expose store on window for debugging
if (typeof window !== 'undefined') {
  (window as any).useUILayoutStore = useUILayoutStore;
}